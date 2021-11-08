package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	start := time.Now()
	fmt.Printf("Starting run. Time: %v\n", start)
	maxPages := 1

	// Batch by page: More memory efficient + MongoDB has max 1000 operations per batch
	for page := 1; page <= maxPages; page++ {
		pageStart := time.Now()
		fmt.Printf("Fetching page %v...", page)
		responseBody, err := getSourceData(page)
		if err != nil {
			log.Fatal(err)
		}

		err = saveData(responseBody.Items)
		if err != nil {
			log.Fatal(err)
		}
		if len(responseBody.Items) == 0 {
			break
		}
		fmt.Printf("Saving...")
		if page == 1 {
			maxPages = int(responseBody.Count/100) + 1
		}
		pageDuration := time.Since(pageStart)
		fmt.Printf("Completed in %v\n", pageDuration)
	}
	duration := time.Since(start)
	fmt.Printf("All pages finished. Total Runtime: %v\n", duration)
}
