#!/bin/bash
# Script for wrapping running of TAT audit and pushing results to Slack
#
# Inputs:
#   $1 -> days ago to start auditing from
#   $2 -> days ago to end auditing to


# define date range for running audit
start=$(date -d "$1 days ago" +%Y-%m-%d)
end=$(date -d "$2 days ago" +%Y-%m-%d)

python3 TAT_queries.py -s $start -e $end

# upload HTML report to Slack, and post associated .csv in thread with the file
response=$(curl -F file=@$(ls *.html) \
    -F channels=$SLACK_CHANNEL \
    -F "initial_comment=:bar_chart: Latest bioinformatics turn around times for between \`$start\` and \`$end\`" \
    -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/files.upload)

thread=$(jq -r '.file.shares.private[][].ts' <<< $response)

curl -F file=@$(ls *.csv) \
    -F channels=$SLACK_CHANNEL \
    -F thread_ts=$thread \
    -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/files.upload
