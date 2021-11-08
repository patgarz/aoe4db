# AOE4DB

Pulls information from https://api.ageofempires.com/ageiv/stats and delivers it to a MongoDB Atlas instance. Detects changes and records history accordingly, which is missing from official API. Official API is also heavily rate limited, which suppresses its usefulness in any application that may use it in realtime.
