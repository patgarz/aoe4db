package main

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
