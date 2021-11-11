package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func buildCache(collection *mongo.Collection) (map[int]User, error) {
	leaderboardCache := make(map[int]User)

	findOpts := options.Find()
	findOpts.SetProjection(bson.M{
		"_id":      0,
		"rlUserId": 1,
		"wins":     1,
		"losses":   1,
		"elo":      1,
		"rank":     1,
	})

	response, err := collection.Find(context.TODO(), bson.M{}, findOpts)
	if err != nil {
		log.Fatal(err)
	}

	// response is read in as a "cursor" that iterates over each individual document
	for response.Next(context.TODO()) {

		// create a value into which the single document can be decoded
		var user User
		err := response.Decode(&user)
		if err != nil {
			log.Fatal(err)
		}
		leaderboardCache[user.RlUserId] = User{
			RlUserId: user.RlUserId,
			Wins:     user.Wins,
			Losses:   user.Losses,
			Elo:      user.Elo,
			Rank:     user.Rank,
		}
	}

	if err := response.Err(); err != nil {
		log.Fatal(err)
	}

	// Close the cursor once finished
	response.Close(context.TODO())

	return leaderboardCache, nil
}

func cacheCompare(leaderboardCache map[int]User, users []User) (map[int]User, []User, error) {

	var changedUsers []User
	for _, user := range users {
		if leaderboardCache[user.RlUserId] != user {
			if leaderboardCache[user.RlUserId].Wins+leaderboardCache[user.RlUserId].Losses <
				user.Wins+user.Losses {
				changedUsers = append(changedUsers, user)
				leaderboardCache[user.RlUserId] = user
			}
		}
	}

	return leaderboardCache, changedUsers, nil
}
