package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func dbConnect(ConnectionStringName string) (*mongo.Client, error) {
	connectionString := os.Getenv("AOE4DB_ConnectionString")
	if connectionString == "" {
		log.Fatal("You must set your 'AOE4DB_ConnectionString' environmental variable. See\n\t https://docs.mongodb.com/drivers/go/current/usage-examples/#environment-variable")
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(connectionString))
	if err != nil {
		panic(err)
	}

	return client, nil
}

func saveData(collection *mongo.Collection, users []User, now time.Time) (*mongo.BulkWriteResult, error) {

	var operations []mongo.WriteModel
	for _, v := range users {

		platform := "Unknown"
		if strings.Contains(v.AvatarUrl, "steamcdn") {
			platform = "Steam"
		} else if strings.Contains(v.AvatarUrl, "xboxlive") {
			platform = "Xbox"
		}

		totalGames := v.Wins + v.Losses

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
				"lastGame":     now,
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
				"history": bson.M{
					"elo":       v.Elo,
					"rank":      v.Rank,
					"timestamp": now,
				},
			},
		})

		operationGames.SetUpsert(true)
		operations = append(operations, operationGames)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)

	result, err := collection.BulkWrite(context.TODO(), operations, &bulkOption)
	if err != nil && !strings.Contains(err.Error(), "duplicate key error") {
		return nil, err
	}
	return result, nil
}
