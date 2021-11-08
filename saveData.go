package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func saveData(users []User) (*mongo.BulkWriteResult, error) {

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}
	connectionString := os.Getenv("AOE4DB_ConnectionString")
	if connectionString == "" {
		log.Fatal("You must set your 'AOE4DB_ConnectionString' environmental variable. See\n\t https://docs.mongodb.com/drivers/go/current/usage-examples/#environment-variable")
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(connectionString))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	collection := client.Database("quickmatch-stats").Collection("current-1v1")
	now := time.Now()

	var operations []mongo.WriteModel
	for _, v := range users {

		platform := "Unknown"
		if strings.Contains(v.AvatarUrl, "steamcdn") {
			platform = "Steam"
		} else if strings.Contains(v.UserId, "xboxlive") {
			platform = "Xbox"
		}

		totalGames := v.Wins + v.Losses
		// Split into two operations:
		// 1) Update user's rank, maxrank, minrank on rank change - rank can drop without playing
		// 2) Update user's elo, games, etc. on totalGames change (these only change after new games)
		operationRank := mongo.NewUpdateOneModel()
		operationRank.SetFilter(bson.M{
			"rlUserId": v.RlUserId,
			"rank": bson.M{
				"$ne": v.Rank,
			},
		})
		operationRank.SetUpdate(bson.M{
			"$set": bson.M{
				"rank": v.Rank,
			},
			"$max": bson.M{
				"bestRank": v.Rank,
			},
			"$min": bson.M{
				"worstRank": v.Rank,
			},
			"$push": bson.M{
				"rankHistory": bson.M{
					"rank":      v.Rank,
					"timestamp": now,
				},
			},
		})

		operationGames := mongo.NewUpdateOneModel()
		operationGames.SetFilter(bson.M{
			"rlUserId": v.RlUserId,
			"totalGames": bson.M{
				"$ne": totalGames,
			},
		})
		operationGames.SetUpdate(bson.M{
			"$set": bson.M{
				"gameId":       v.GameId,
				"userId":       v.UserId,
				"userName":     v.UserName,
				"avatarUrl":    v.AvatarUrl,
				"playerNumber": v.PlayerNumber,
				"elo":          v.Elo,
				"eloRating":    v.EloRating,
				"region":       v.Region,
				"wins":         v.Wins,
				"winPercent":   v.WinPercent,
				"losses":       v.Losses,
				"winStreak":    v.WinStreak,
				"platform":     platform,
				"totalGames":   totalGames,
			},
			"$currentDate": bson.M{
				"lastGame": true,
			},
			"$max": bson.M{
				"bestStreak":     v.WinStreak,
				"bestElo":        v.Elo,
				"bestWinPercent": v.WinPercent,
			},
			"$min": bson.M{
				"worstStreak":     v.WinStreak,
				"worstElo":        v.Elo,
				"worstWinPercent": v.WinPercent,
			},
			"$addToSet": bson.M{
				"alternateNames": v.UserName,
			},
			"$push": bson.M{
				"eloHistory": bson.M{
					"elo":       v.Elo,
					"timestamp": now,
				},
			},
		})

		operationGames.SetUpsert(true)
		operationRank.SetUpsert(true)
		operations = append(operations, operationGames, operationRank)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)

	result, err := collection.BulkWrite(context.TODO(), operations, &bulkOption)
	if err != nil && !strings.Contains(err.Error(), "duplicate key error") {
		return nil, err
	}
	return result, nil
}
