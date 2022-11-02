# Turnaround times
This repo contains the script to generate an audit summary report for bioinformatics processing turnaround times (TAT) for our CEN, MYE, TWE, TSO500 and SNP services.

## Installation
The required Python package dependencies to query the APIs and create the final HTML file can be installed with:

```
pip install -r requirements.txt
```

Config variables should be passed in a `credentials.json` file. This should be placed within TAT_audit, the same directory level as `requirements.txt`. The default months will be used as the number of months to audit from the current date if start and end dates are not supplied as command line arguments.

```json
{
    "DX_TOKEN": "XXX",
    "JIRA_EMAIL": "XXX",
    "JIRA_TOKEN": "XXX",
    "STAGING_AREA_PROJ_ID": "XXX",
    "DEFAULT_MONTHS" : "6"
}
```
## Description
The script works by:
- Querying DNAnexus to find all of the 002 projects for each assay, either between the dates specified or within the last X number of months from the date the script was run
- Finding the time the log file was uploaded in Staging Area for that run (or the first file uploaded in the Staging Area folder if a SNP run)
    - This is because for SNP runs files from the MiSeq are manually uploaded rather than with dx-streaming-upload
- Finding the time the first job was started in the 002 project and the time the first MultiQC job was completed
- Finding the relevant Jira ticket for the run (in both the Jira open and closed sequencing run ticket queues)
- If a ticket is at status 'All samples released' it queries the specific ticket to find when the ticket was resolved.
    - If at 'Urgent samples released' it calculates the time at this status from the last MultiQC job to the time the script is run.
    - If at 'On hold' it calculates this time from the last processing step to the time the script is run
- Timing calculations:
    - Upload to processing time is calculated from upload to first 002 job
    - Pipeline running time is from the first 002 job to the end of the last MultiQC job
    - Processing end to release is the time from the last MultiQC job to the Jira ticket changing to 'All samples released'
- The overall turnaround time is calculated as the time the Jira ticket changed from 'All samples released' minus the upload time
- Creating a HTML including plots the run TATs for each assay and generates and displays a small stats table to show averages for each step and overall TAT for that assay
- Creating a CSV with the underlying data showing the timestamps for each step and the days between them

The script also:
- Shows any Jira tickets in the time period for these assays at 'All samples released' but with no 002 project found (within the audit period)
- Shows any run tickets where the run was not completed (the ticket is resolved but the data was not released)
- Shows runs where manual review is required (e.g. one of the timings was not found - these runs are not included in compliance calculations)
- Shows where there were mismatches (within 2 differences) between:
    - The Jira ticket name and the 002 project name
    - The 002 project name and the Staging Area run folder name

## Usage
Run the script to query the last X (number supplied in credentials.json) number of months from today via:

```
python TAT_queries.py
```

If you would like to specify the start and end dates, you can enter these as arguments like so:

```
python TAT_queries.py -s 2022-05-01 -e 2022-11-01
```

If being entered, both start and end dates must be supplied. This will create a HTML file in the directory you're currently in, named to include the number of weeks queried and the date the script was run.
