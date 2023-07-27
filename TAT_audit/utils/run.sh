#!/bin/bash

# define date range for running audit (intended to run on a Monday and audit 3 weeks from previous Friday)
start=$(date -d "24 days ago" +%Y-%m-%d)
end=$(date -d "3 days ago" +%Y-%m-%d)

python3 TAT_queries.py -s $start -e $end

# upload HTML report to Slack, and post associated .csv in thread with the file
response=$(curl -F file=@$(ls *.html) \
    -F channels=$SLACK_CHANNEL \
    -F "initial_comment=Latest bioinformatics turn around times for between $start and $end" \
    -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/files.upload)

thread=$(jq -r '.file.shares.private[][].ts' <<< $response)

curl -F file=@$(ls *.csv) \
    -F channels=$SLACK_CHANNEL \
    -F thread_ts=$thread \
    -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/files.upload
