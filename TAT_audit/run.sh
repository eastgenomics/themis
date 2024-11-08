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

    html_file=$(find . -maxdepth 1 -name "*.html")
    html_file_size=$(stat -c %s "$html_file")

    # Get an upload URL and file ID for the HTML file
    html_upload_response=$(curl \
        https://slack.com/api/files.getUploadURLExternal \
        -H "Authorization: Bearer $SLACK_TOKEN" \
        -F filename=$html_file \
        -F length=$html_file_size)

    html_upload_url=$(echo $html_upload_response | jq -r .upload_url)
    html_file_id=$(echo $html_upload_response | jq -r .file_id)

    # Upload HTML file by making POST request to upload URL
    curl -X POST $html_upload_url \
        -F filename="@$html_file"

    sleep 5

    # Call files.completeUploadExternal to finalise the upload
    html_post_response=$(curl -X POST \
        https://slack.com/api/files.completeUploadExternal \
        -H "Authorization: Bearer $SLACK_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"files\": [{ \"id\": \"$html_file_id\" }], \"channel_id\": \"$SLACK_CHANNEL\", \"initial_comment\": \":bar_chart: Latest bioinformatics turn around times for between \`$start\` and \`$end\`\"}")

    # Get thread ts
    thread=$(echo "$html_post_response" | jq -r ".files[].shares.private | to_entries | .[].value[0].ts")

    # Do the same for the CSV file
    csv_file=$(find . -name "*.csv")
    csv_file_size=$(stat -c %s "$csv_file")

    # Get an upload URL and file ID for the CSV file
    csv_upload_response=$(curl -s \
        https://slack.com/api/files.getUploadURLExternal \
        -H "Authorization: Bearer $SLACK_TOKEN" \
        -F filename=$csv_file \
        -F length=$csv_file_size)

    csv_upload_url=$(echo $csv_upload_response | jq -r .upload_url)
    csv_file_id=$(echo $csv_upload_response | jq -r .file_id)

    # Upload CSV file by making POST request to upload URL
    curl -X POST $csv_upload_url \
        -F filename="@$csv_file"

    # Call files.completeUploadExternal to finalise the CSV upload to the
    # thread made earlier
    curl -X POST \
        https://slack.com/api/files.completeUploadExternal \
        -H "Authorization: Bearer $SLACK_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"files\": [{ \"id\": \"$csv_file_id\" }], \"channel_id\": \"$SLACK_CHANNEL\", \"thread_ts\": \"$thread\"}"

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