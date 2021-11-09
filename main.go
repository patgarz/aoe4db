package main

import (
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
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	leaderboardCache := make(map[int]User)

	fmt.Printf("Starting aoe4db sync. Opts- Loop: %v, Log: %v, Testing: %v\n", *loopPtr, *logLevelPtr, *testingPtr)
	for {
		start := time.Now()
		fmt.Printf("Starting run. Time: %v\n", start)
		maxPages := 1
		totalUsersChanged := 0
		totalInserts := 0
		totalModified := 0
		totalUpserts := 0

		// Batch by page: More memory efficient + MongoDB has max 1000 operations per batch
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
			if len(users) > 0 {
				if *logLevelPtr >= 2 {
					fmt.Printf("Saving...\n")
				}
				result, err = saveData(users)
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
