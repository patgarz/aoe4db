package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type User struct {
	GameId       int     `json:"gameId"`
	UserId       string  `json:"userId"`
	RlUserId     int     `json:"rlUserId"`
	UserName     string  `json:"userName"`
	AvatarUrl    string  `json:"avatarUrl"`
	PlayerNumber int     `json:"playerNumber"`
	Elo          int     `json:"elo"`
	EloRating    int     `json:"eloRating"`
	Rank         int     `json:"rank"`
	Region       int     `json:"region"`
	Wins         int     `json:"wins"`
	WinPercent   float32 `json:"winPercent"`
	Losses       int     `json:"losses"`
	WinStreak    int     `json:"winStreak"`
}

type leaderboardRequest struct {
	Region       int    `json:"region"`
	Versus       string `json:"versus"`
	MatchType    string `json:"matchType"`
	TeamSize     string `json:"teamSize"`
	SearchPlayer string `json:"searchPlayer"`
	Page         int    `json:"page"`
	Count        int    `json:"count"`
}

type apiResult struct {
	Count int    `json:"count"`
	Items []User `json:"items"`
}

func getSourceData(page int) (apiResult, error) {

	var apiResult apiResult

	leaderboardEndpoint := "https://api.ageofempires.com/api/ageiv/Leaderboard"

	requestBody := leaderboardRequest{
		Region:       7,
		Versus:       "players",
		MatchType:    "unranked",
		TeamSize:     "1v1",
		SearchPlayer: "",
		Page:         page,
		Count:        100,
	}

	jsonRequestBody, err := json.Marshal(requestBody)
	if err != nil {
		return apiResult, err
	}

	response, err := http.Post(leaderboardEndpoint, "application/json", bytes.NewBuffer(jsonRequestBody))
	if err != nil {
		return apiResult, err
	}
	defer response.Body.Close()

	responseBodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return apiResult, err
	}

	json.Unmarshal(responseBodyBytes, &apiResult)

	return apiResult, nil
}
