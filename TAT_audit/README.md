# Turnaround times
This repo contains code to generate an audit summary report for bioinformatics processing turnaround times (TATs) for our assays.

## Installation
The required Python package dependencies to query the APIs and create the final HTML file can be installed with:

```
pip install -r requirements.txt
```

Please note Python 3.9+ is required.

Variables should be set to the environment; this can be done by creating an .env file and exporting the variables to the environment.

```
DX_TOKEN=<redacted, str>
JIRA_EMAIL=<redacted, str>
JIRA_TOKEN=<redacted, str>
STAGING_AREA_PROJ_ID=<redacted, str>
DEFAULT_MONTHS=3
ASSAYS='["CEN", "MYE", "TSO500", "TWE", "PCAN"]'
CANCELLED_STATUSES='["Data cannot be processed", "Data cannot be released", "Data not received"]'
OPEN_STATUSES='["New", "Data Received", "Data processed", "On hold", "Urgent samples released"]'
LAST_JOBS='{"TWE": "eggd_generate_variant_workbook", "CEN": "eggd_artemis", "MYE": "eggd_MultiQC", "TSO500": "eggd_MultiQC", "PCAN": "eggd_starfusion"}'
```
If no start and end dates are supplied as command line arguments, the DEFAULT_MONTHS variable will be used to determine the previous number of months to audit from the date the script is run.

## Description
The script works by:
- Querying DNAnexus to find all of the 002 projects for each assay, either between the dates specified (+- 5 days either side) or, if no dates are specified, within the last X number of months (`DEFAULT_MONTHS`) from the date the script was run. For each run:
    - Checking the run date within the 002 project name is within the audit period
- Finding the time the files were uploaded to the Staging Area
    - This is the time the `*.lane.all.log` file was uploaded for that run
- Finding the time the first job started running
    - For all assays this is via the eggd_conductor job
- Finding the relevant Jira ticket for the run (in both the Jira open and closed sequencing run ticket queues) to obtain the current status and/or resolution time
- Finding the time the last job was completed
    - The job to search for is included in the config file and we take the time the final job completed before the Jira ticket was last resolved to prevent obtaining reanalysis jobs

- Timing calculations:
    - **Upload to processing time** is calculated from upload to first job
    - **Pipeline running** is calculated from the first job launch to the completion of the final job in the pipeline
    - **Processing end to release** is the time from the final job completing to the Jira ticket changing to 'All samples released'
        - If a ticket is at status 'All samples released' it queries the specific ticket to find when the ticket was resolved
        - If at 'Urgent samples released' the time is calculated from the last job completing to the time the script is run and plotted instead of this step
        - If at 'On hold' it calculates this time from the last processing step (upload time, first job launch time or final job completing) to the time the script is run and plotted instead of this step
- The **overall turnaround time** is calculated as the time the Jira ticket changed from 'All samples released' minus the upload time

- A HTML file is then created, including plots of the run TATs for each assay, a stats table to show the average time taken for each step and the overall TAT for that assay and a plot to show upload day of the week vs overall TAT
- A CSV is created to display the underlying data, showing the timestamps for each step and the days between them

The script also:
- Shows any Jira tickets in the time period for these assays at 'All samples released' but with no 002 project found
- Shows any run tickets where the run was not completed (the ticket is resolved but the data was not released)
- Shows runs where manual review is required (e.g. one of the timings was not found - these runs are not included in compliance calculations)
- Shows where there are mismatches (within 2 differences) between:
    - The Jira ticket name and the 002 project name
    - The 002 project name and the Staging Area run folder name

## Usage
Run the script to query the last X (int supplied in credentials.json) number of months from today via:

```
python TAT_queries.py
```

If you would like to specify the start and end dates, you can enter these as arguments like so:

```
python TAT_queries.py -s 2022-05-01 -e 2022-11-01
```
You can also enter a font size for the title of the TAT subplots as this might depend on the length of the period being audited, e.g.:
```
python TAT_queries.py -s 2022-05-01 -e 2022-11-01 -f 14
```

If any arguments are entered, both start and end dates must be supplied. The script will create a HTML file in the directory you're currently in, named to include the start and end dates of the audit period. If the script is run twice for the same period, if a summary report has been previously generated for these dates this will be replaced.


## Docker

A Dockerfile has been written to wrap the script, this is available to run after building as such.

Example command:
```
docker run --env-file tat_credentials.env python3 utils/TAT_queries.py -s 2022-05-01 -e 2022-11-01
```

An additional script has been written to wrap the above and push the report to Slack. To use this a Slack bot token and Slack channel must also be provided in the environment variables:
```
SLACK_TOKEN=<redacted>
SLACK_CHANNEL=<redacted>
TAT_STANDARD_DAYS=3
```

This can be run with `docker run --env-file tat_credentials.env /bin/bash run.sh 21 1`, which would audit the previous 3 weeks and push both the HTML and CSV output to the specified Slack channel. The only inputs are the number of days ago to start auditing from, and number of days ago to audit until.
