#!/usr/bin/env bash
APP_ID=$(curl -s http://localhost:4041/api/v1/applications | python3 -c 'import json,sys; apps=json.load(sys.stdin); print(apps[0]["id"])' 2>/dev/null)
echo "APP=$APP_ID"
curl -s "http://localhost:4041/api/v1/applications/$APP_ID/jobs" | python3 -m json.tool | grep -E '"jobId"|"status"|"numTasks"|"numActiveTasks"|"numCompletedTasks"|"numFailedTasks"'
