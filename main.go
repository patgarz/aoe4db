package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	loopPtr := flag.Bool("loop", false, "continuously loop ingest")
	logLevelPtr := flag.Int("log", 1, "logging level, 1=min(Default), 3=max")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	fmt.Printf("Starting aoe4db sync. Opts- Loop: %v, Log: %v\n", *loopPtr, *logLevelPtr)
	for {
		start := time.Now()
		fmt.Printf("Starting run. Time: %v\n", start)
		maxPages := 1
		totalInserts := 0
		totalModified := 0
		totalUpserts := 0

		// Batch by page: More memory efficient + MongoDB has max 1000 operations per batch
		for page := 1; page <= maxPages; page++ {
			pageStart := time.Now()
			fmt.Printf("Fetching page %v...", page)
			responseBody, err := getSourceData(page)
			if err != nil {
				log.Fatal(err)
			}
			if len(responseBody.Items) == 0 {
				break
			}

			result, err := saveData(responseBody.Items)
			if err != nil {
				log.Fatal(err)
			}
			totalInserts += int(result.InsertedCount)
			totalModified += int(result.ModifiedCount)
			totalUpserts += int(result.UpsertedCount)
			if *logLevelPtr >= 3 {
				fmt.Printf("\nInsert count: %v\nModified count: %v\nUpsert count: %v\n", result.InsertedCount, result.ModifiedCount, result.UpsertedCount)
			}
			if *logLevelPtr >= 2 {
				fmt.Printf("Saving...")
			}
			if page == 1 {
				maxPages = int(responseBody.Count/100) + 1
			}
			pageDuration := time.Since(pageStart)
			fmt.Printf("Completed in %v\n", pageDuration)
		}
		duration := time.Since(start)
		fmt.Printf("All pages finished. Total Runtime: %v, Total (Inserts,Modified,Upserts): (%v,%v,%v)\n", duration, totalInserts, totalModified, totalUpserts)

		if !*loopPtr {
			break
		}
	}
}
