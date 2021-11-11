package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
)

func main() {
	loopPtr := flag.Bool("loop", false, "continuously loop ingest")
	logLevelPtr := flag.Int("log", 1, "logging level, 1=min(Default), 3=max")
	testingPtr := flag.Bool("testing", false, "only do first 10 pages of leaderboard")
	noSavePtr := flag.Bool("nosave", false, "do not save any discovered changes to DB")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	// Connect and keep our connection up as long as our app is running
	client, err := dbConnect("AOE4DB_ConnectionString")
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database("quickmatch-stats").Collection("current-1v1")

	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// We build a map as a local cache to prevent ourselves from having to query the database for
	//	every single record. This saves a LOT of money by preventing constant database calls in exchange
	// 	for a little extra sync app RAM (we only need to cache the current dataset, not any of the history)
	// Ex: Runtime without cache ~15-20 mins, runtime with cache ~15-20 seconds
	fmt.Printf("Building cache...")
	leaderboardCache, err := buildCache(collection)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Completed. Cache length: %v records\n", len(leaderboardCache))

	fmt.Printf("Starting aoe4db sync. Opts- Loop: %v, Log: %v, Testing: %v\n", *loopPtr, *logLevelPtr, *testingPtr)
	for {
		start := time.Now()
		fmt.Printf("Starting run. Time: %v\n", start)
		maxPages := 1
		totalUsersChanged := 0
		totalInserts := 0
		totalModified := 0
		totalUpserts := 0

		// Batch by page of source dataset
		for page := 1; page <= maxPages; page++ {
			pageStart := time.Now()
			if *logLevelPtr >= 2 {
				fmt.Printf("Fetching page %v...\n", page)
			}
			responseBody, err := getSourceData(page)
			if err != nil {
				log.Fatal(err)
			}
			if len(responseBody.Items) == 0 {
				break
			}

			var users []User
			leaderboardCache, users, err = cacheCompare(leaderboardCache, responseBody.Items)
			if err != nil {
				log.Fatal(err)
			}
			totalUsersChanged += len(users)

			if *logLevelPtr >= 2 {
				fmt.Printf("Users Changed: %v\n", len(users))
			}
			var result *mongo.BulkWriteResult
			if len(users) > 0 && !*noSavePtr {
				if *logLevelPtr >= 2 {
					fmt.Printf("Saving...\n")
				}
				result, err = saveData(collection, users, pageStart)
				if err != nil {
					log.Fatal(err)
				}
				totalInserts += int(result.InsertedCount)
				totalModified += int(result.ModifiedCount)
				totalUpserts += int(result.UpsertedCount)
				if *logLevelPtr >= 3 {
					fmt.Printf("Insert count: %v\nModified count: %v\nUpsert count: %v\n", result.InsertedCount, result.ModifiedCount, result.UpsertedCount)
				}
			}
			if page == 1 {
				maxPages = int(responseBody.Count/100) + 1
			}
			pageDuration := time.Since(pageStart)
			if *logLevelPtr >= 2 {
				fmt.Printf("Completed in %v\n", pageDuration)
			}
			if *testingPtr && page >= 10 {
				break
			}
		}
		duration := time.Since(start)
		fmt.Printf("All pages finished. Total Runtime: %v, Total Users Changed: %v, Total (Inserts,Modified,Upserts): (%v,%v,%v)\n", duration, totalUsersChanged, totalInserts, totalModified, totalUpserts)

		if !*loopPtr {
			break
		}
	}
}
