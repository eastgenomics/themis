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
if [[ -s $(find . -maxdepth 1 -name "*.html") ]]; then
    response=$(curl -F file=@$(find . -maxdepth 1 -name "*.html") \
        -F channels=$SLACK_CHANNEL \
        -F "initial_comment=:bar_chart: Latest bioinformatics turn around times for between \`$start\` and \`$end\`" \
        -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/files.upload)

    thread=$(jq -r '.file.shares.private[][].ts' <<< $response)

    curl -F file=@$(find . -name "*.csv") \
         -F channels=$SLACK_CHANNEL \
         -F thread_ts=$thread \
         -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/files.upload
else
    # failed to generate report, send error message with log if present
    echo "Failed generating report"
    tick='```'
    if [[ -s TAT_queries_debug.log ]]; then
        curl -F channel=$SLACK_CHANNEL \
             -F text=":rotating_light: Error generating turn around times report
                ${tick}$(cat TAT_queries_debug.log)${tick}" \
             -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/chat.postMessage
    else
    # no log file, just send error message
        curl -F channel=$SLACK_CHANNEL \
             -F text=":rotating_light: Error generating turn around times report" \
             -H "Authorization: Bearer $SLACK_TOKEN" https://slack.com/api/chat.postMessage
    fi
fi