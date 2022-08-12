import datetime as dt
import dxpy as dx
import json
import logging
import numpy as np
import os
import pandas as pd
import plotly.graph_objects as go
import requests
import sys
import time

from collections import defaultdict
from requests.auth import HTTPBasicAuth

pd.options.mode.chained_assignment = None

# Get tokens etc from credentials file
with open("credentials.json", "r") as json_file:
    CREDENTIALS = json.load(json_file)

DX_TOKEN = CREDENTIALS.get('DX_TOKEN')
JIRA_EMAIL = CREDENTIALS.get('JIRA_EMAIL')
JIRA_TOKEN = CREDENTIALS.get('JIRA_TOKEN')
STAGING_AREA_PROJ_ID = CREDENTIALS.get('STAGING_AREA_PROJ_ID')
JIRA_NAME = CREDENTIALS.get('JIRA_NAME')

# Get path the script is run in
CURRENT_DIR = os.path.abspath(os.getcwd())

# Create and configure logger
LOG_FORMAT = "%(asctime)s — %(name)s — %(levelname)s — %(lineno)d — %(message)s"

# Set level to debug, format with date and time and re-write file each time
logging.basicConfig(
    filename=f'{CURRENT_DIR}/TAT_queries_debug.log',
    level=logging.DEBUG,
    format=LOG_FORMAT,
    filemode='w')

# Set up logger
logger = logging.getLogger("main log")

# Jira API things
auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)

headers = {
   "Accept": "application/json"
}

# Audit settings and relevant dates (used in plot titles)
NO_OF_AUDIT_WEEKS = 12

today_date = dt.date.today()
x_weeks = dt.timedelta(weeks = NO_OF_AUDIT_WEEKS)
begin_date_of_audit = today_date - x_weeks


def login() -> None:
    """
        Logs into DNAnexus
        Parameters
        ----------
        token : str
            authorisation token for DNAnexus, from settings.py
        Raises
        ------
        Error
            Raised when DNAnexus user authentification check fails
    """

    DX_SECURITY_CONTEXT = {
        "auth_token_type": "Bearer",
        "auth_token": DX_TOKEN
    }

    dx.set_security_context(DX_SECURITY_CONTEXT)

    try:
        dx.api.system_whoami()
        logger.info("DNAnexus login successful")
    except Exception as err:
        logger.error("Error logging in to DNAnexus")
        sys.exit(1)


def get_002_projects_in_period(assay_type):
    """
    Gets all the 002 projects ending with the relevant assay type
    That have been created in the last X weeks
    Parameters
    ----------
    assay_type : str
        e.g. 'CEN'
    Returns
    -------
    assay_response : list
        list of dicts
    """
    # Search projs in last X weeks starting with 002, ending with assay type
    # Return only relevant describe fields
    assay_response = list(dx.find_projects(
        level='VIEW',
        created_after=f'-{NO_OF_AUDIT_WEEKS}w',
        name=f"002*{assay_type}",
        name_mode="glob",
        describe={
            'fields': {
                'id': True, 'name': True, 'created': True
            }
        }
    ))

    return assay_response


def add_in_002_created_time_and_assay(assay_type, assay_response):
    """
    Adds the time the 002 project was created for that run to a dict
    Parameters
    ----------
    assay_type : str
        e.g. 'CEN'
    assay_response : list
        list of dicts from get_002_projects_in_period() function
    Returns
    -------
    run_dict : collects.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run
    """
    run_dict = defaultdict(dict)

    # For each proj name, get run name by removing _002 and _assay name
    # For key with relevant run name
    # Add key project ID with project dx_id as value
    # Add key 002_project_created with the time the 002 project was created
    # Add key assay_type with the str name of the assay type
    for project in assay_response:
        run_name = project['describe']['name'].removeprefix(
            '002_'
        ).removesuffix(f'_{assay_type}')
        run_dict[run_name]['project_id'] = project['id']
        run_dict[run_name]['002_project_created'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(
                project['describe']['created'] / 1000
            )
        )
        run_dict[run_name]['assay_type'] = assay_type

    return run_dict


def add_log_file_time(run_dict):
    """
    Adds the time the log file was created for that run to a dict
    Parameters
    ----------
    run_dict :  collects.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run
    Returns
    -------
    run_dict : collects.defaultdict(dict)
        dictionary where key is run name with dict inside and log_file_created
        added
    """
    # For each run, get the time in epoch the .lane.all.log file was created
    for run_name in run_dict:
        log_file_info = list(
            dx.find_data_objects(
                project=STAGING_AREA_PROJ_ID,
                folder=f'/{run_name}/runs',
                classname='file',
                name="*.lane.all.log",
                name_mode="glob",
                describe={
                    'fields': {
                        'name': True,
                        'created': True
                    }
                }
            )
        )

        # For key with relevant run name
        # Add log_file_created key with time logfile created in datetime format
        log_time = log_file_info[0]['describe']['created'] / 1000
        run_dict[run_name]['log_file_created'] = (
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(log_time))
        )

    return run_dict


def find_earliest_002_job(run_dict):
    """
    Adds the time the earliest job was run in the relevant 002 project for
    that run
    Parameters
    ----------
    run_dict :  collects.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run
    Returns
    -------
    run_dict : collects.defaultdict(dict)
        dictionary where key is run name with dict inside and earliest_002 job
        added
    """
    # For run, use proj ID for 002 project and get all the jobs in the proj
    for run in run_dict:
        jobs = list(dx.search.find_jobs(
            project=run_dict[run]['project_id'],
            describe={
            'fields': {
                'id': True, 'name': True, 'created': True
            }
        }
        ))

        # Get the earliest created time of the jobs
        first_job = (
            min(data['describe']['created'] for data in jobs) / 1000
        )

        # For key with relevant run name
        # Add earliest_002_job key with time of earliest job in datetime format
        run_dict[run]['earliest_002_job'] = (
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(first_job))
        )

    return run_dict


def add_successful_multiQC_time(run_dict):
    """
    Adds the time the multiQC job finished in the relevant 002 proj for that
    run
    Parameters
    ----------
    run_dict :  collects.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run
    Returns
    -------
    run_dict : collects.defaultdict(dict)
        dictionary where key is run name with dict inside and multiQC_finished
        key and val added
    """
    # For each run from the dict
    # Find executions that include the name *MultiQC*
    # As both eggd_MultiQC and MultiQC_v1.1.2 used
    for run in run_dict:
        jobs = list(dx.search.find_executions(
            project= run_dict[run]['project_id'],
            state='done',
            name='*MultiQC*',
            name_mode='glob',
            describe={
            'fields': {
                'id': True, 'project': True, 'name': True,
                'executableName': True, 'stoppedRunning': True
            }
        }
        ))

        # If more than 1 job, take the finished time as the latest one
        # If only 1 job just get the finished time
        if jobs:
            if len(jobs) >1:
                multiqc_fin = (
                    max(
                        data['describe']['stoppedRunning'] for data in jobs
                    ) / 1000
                )
            else:
                multiqc_fin = jobs[0]['describe']['stoppedRunning'] / 1000

            multi_qc_completed = (
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(multiqc_fin))
            )

        else:
            multi_qc_completed = None

        # If no jobs, set finished time to None
        # For key with relevant run name, add multiQC_finished value to dict
        run_dict[run]['multiQC_finished'] = multi_qc_completed

    return run_dict


def create_info_dict(assay_type):
    """
    Creates a nested dict with all relevant info for an assay type
    using the other functions
    run
    Parameters
    ----------
    assay_type : str
        e.g. 'CEN'
    Returns
    -------
    assay_run_dict : collects.defaultdict(dict)
        dictionary where key is run name with dict inside and all relevant
        audit info inside
    """
    # Do all the steps for the assay type
    assay_response = get_002_projects_in_period(assay_type)
    assay_run_dict = add_in_002_created_time_and_assay(
        assay_type, assay_response
    )
    assay_run_dict = add_successful_multiQC_time(assay_run_dict)
    find_earliest_002_job(assay_run_dict)

    # SNP runs are uploaded manually so don't have lane.all.log file
    if assay_type == 'SNP':
        pass
    else:
        add_log_file_time(assay_run_dict)

    return assay_run_dict


def add_001_demultiplex_job(all_assays_dict):
    """
    Adds the time the demultiplexing started for each run in the merged dict
    Parameters
    ----------
    all_assays_dict :  collects.defaultdict(dict)
        dictionary where all the assay types are merged. Each key is run
        with all the relevant audit info
    Returns
    -------
    all_assays_dict : collects.defaultdict(dict)
        merged dict with added 'demultiplex_started' key
    """
    # Find the jobs in staging area in last X weeks that are done
    find_jobs = list(dx.search.find_jobs(
        project=STAGING_AREA_PROJ_ID,
        created_after=f'-{NO_OF_AUDIT_WEEKS}w',
        state='done',
        describe={
            'fields': {
                'id': True, 'name': True, 'executableName': True,
                'created': True, 'folder': True
            }
        }
    ))

    # Get the relevant run name from the folder
    for job in find_jobs:
        first, run_name, *rest = job['describe']['folder'].split("/")
        # If the run name from the job is in our dict
        # Add the time demultiplex started to the dict
        demulti_time = job['describe']['created'] / 1000
        if run_name in all_assays_dict:
            all_assays_dict[run_name]['demultiplex_started'] = (
                time.strftime('%Y-%m-%d %H:%M:%S',
                time.localtime(demulti_time))
            )

    return all_assays_dict


def get_jira_info(queue_id, jira_name):
    url = (
        f"https://{jira_name}.atlassian.net/rest/servicedeskapi/servicedesk/"
        f"4/queue/{queue_id}/issue"
    )

    response = requests.request(
       "GET",
       url,
       headers=headers,
       auth=auth
    )

    queue_response = json.loads(response.text)

    return queue_response


def add_jira_info(all_assays_dict, jira_response):
    for issue in jira_response['values']:
        run_name = issue['fields']['summary']

        if run_name in all_assays_dict:
            final_status = issue['fields']['status']['name']
            status_time_misec = (
                issue['fields']['customfield_10032'][
                    'completedCycles'
                ][0]['stopTime']['epochMillis'] / 1000
            )
            res_time = (
                dt.datetime.fromtimestamp(
                    status_time_misec
                ).replace(microsecond=0)
            )
            res_time_str = res_time.strftime('%Y-%m-%d %H:%M:%S')
            all_assays_dict[run_name]['final_jira_status'] = final_status
            all_assays_dict[run_name]['jira_resolved'] = res_time_str

    return all_assays_dict


def create_all_assays_df(all_assays_dict):
    """
    Creates a df with all the assay types, run names and relevant audit info
    Parameters
    ----------
    all_assays_dict :  collects.defaultdict(dict)
        dictionary where all the assay types are merged. Each key is run
        with all the relevant audit info
    Returns
    -------
    all_assays_df : pd.DataFrame()
        dataframe with a row for each run
    """
    # Convert dict to df with run names as column
    all_assays_df = pd.DataFrame(
        all_assays_dict.values()).assign(run_name = all_assays_dict.keys()
    )

    # Reorder columns
    all_assays_df = all_assays_df[[
        'assay_type', 'run_name', 'log_file_created', 'demultiplex_started',
        '002_project_created', 'earliest_002_job', 'multiQC_finished',
        'final_jira_status', 'jira_resolved'
    ]]

    cols_to_convert = [
        'log_file_created', 'demultiplex_started', '002_project_created',
        'earliest_002_job', 'multiQC_finished', 'jira_resolved'
    ]

    # Convert cols to pandas datetime type
    all_assays_df[cols_to_convert] = all_assays_df[cols_to_convert].apply(
        pd.to_datetime, format='%Y-%m-%d %H:%M:%S'
    )

    return all_assays_df


def add_calculation_columns(all_assays_df):
    """
    Adds columns to the df for log file to earliest 002 job
    And bioinfo processing time
    Parameters
    ----------
    all_assays_df :  pd.DataFrame()
        dataframe with a row for each run
    Returns
    -------
    all_assays_df : pd.DataFrame()
        dataframe with a row for each run and extra columns
    """
    # Add new column for type between log file and earliest 002 job
    all_assays_df['log_file_to_first_002_job'] = (
        (all_assays_df['earliest_002_job'] - all_assays_df['log_file_created'])
        / np.timedelta64(1, 'D')
    )

    # Add new column for bioinfx processing time from earliest 002 job
    # To the time that the last successful MultiQC job finished
    all_assays_df['processing_time'] = (
        (all_assays_df['multiQC_finished'] - all_assays_df['earliest_002_job'])
        / np.timedelta64(1, 'D')
    )

    # Add new column for time from MultiQC end to Jira resolution
    all_assays_df['processing_end_to_release'] = (
        (all_assays_df['jira_resolved'] - all_assays_df['multiQC_finished'])
        / np.timedelta64(1, 'D')
    )

    # Add new column for time from log file creation to Jira resolution
    all_assays_df['upload_to_release'] = (
        (all_assays_df['jira_resolved'] - all_assays_df['log_file_created'])
        / np.timedelta64(1, 'D')
    )

    return all_assays_df


def extract_assay_df(all_assays_df, assay_type):
    """
    Gets relevant rows from the total df
    Parameters
    ----------
    all_assays_df :  pd.DataFrame()
        dataframe with a row for each run
    assay_type : str
        e.g. 'CEN'
    Returns
    -------
    assay_df : pd.DataFrame()
        dataframe with only rows from that assay type
    """
    # Get df with the rows of the relevant assay type
    assay_df = all_assays_df.loc[all_assays_df['assay_type'] == assay_type]

    return assay_df

def create_TAT_fig(assay_df, assay_type):
    """
    Creates stacked bar for each run of that assay type
    With relevant time periods on bar
    Parameters
    ----------
    assay_df :  pd.DataFrame()
        dataframe with only rows from that assay type
    assay_type : str
        e.g. 'CEN'
    """
    # Add trace for Log file to first 002 job
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["log_file_to_first_002_job"],
            name="Upload to processing start",
            legendrank=3
        )
    )

    # Add trace for bioinformatics run time
    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["processing_time"],
            name="Pipeline running",
            legendrank=2
        )
    )

    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["processing_end_to_release"],
            name="End of processing to release",
            legendrank=1,
            text=round(assay_df['upload_to_release'])
        )
    )

    fig.add_hline(y=3, line_dash="dash")

    fig.update_traces(
        hovertemplate=(
            '<br><b>Run</b>: %{x}<br>'
            '<b>Stage</b>: %{data.name}<br>'
            '<b>Days</b>: %{y:.2f}<br>'
            '<extra></extra>'
        ),
        textposition='outside'
    )

    # Update relevant aspects of chart
    fig.update_layout(
        barmode='stack',
        title={
            'text': f"{assay_type} Turnaround Times {begin_date_of_audit} -"
                    f" {today_date}",
            'xanchor': 'center',
            'x': 0.5,
            'font_size': 20
        },
        xaxis_title="Run name",
        yaxis_title="Number of days",
        width=1000,
        height=600,
        font_family='Helvetica'
    )

    return fig


def main():
    """Main function to create html report"""
    login()

    logger.info("Creating dicts for each assay")
    all_assays_dict = {}
    assay_types = ['TWE', 'CEN', 'MYE', 'TSO500', 'SNP']
    for assay_type in assay_types:
        all_assays_dict.update(create_info_dict(assay_type))

    logger.info("Adding demultiplex job")
    add_001_demultiplex_job(all_assays_dict)

    logger.info("Getting JIRA ticket info for closed seq runs")
    closed_runs_response = get_jira_info(35, JIRA_NAME)

    logger.info("Adding closed tickets info to dict")
    add_jira_info(all_assays_dict, closed_runs_response)

    logger.info("Merging to one df")
    all_assays_df = create_all_assays_df(all_assays_dict)
    logger.info("Adding TAT steps info")
    add_calculation_columns(all_assays_df)

    logger.info("Extracting each df")
    CEN_df = extract_assay_df(all_assays_df, 'CEN')
    MYE_df = extract_assay_df(all_assays_df, 'MYE')
    TSO500_df = extract_assay_df(all_assays_df, 'TSO500')
    TWE_df = extract_assay_df(all_assays_df, 'TWE')
    SNP_df = extract_assay_df(all_assays_df, 'SNP')
    logger.info("Creating figs")
    CEN_fig = create_TAT_fig(CEN_df, 'CEN')
    MYE_fig = create_TAT_fig(MYE_df, 'MYE')
    TSO500_fig = create_TAT_fig(TSO500_df, 'TSO500')
    TWE_fig = create_TAT_fig(TWE_df, 'TWE')
    SNP_fig = create_TAT_fig(SNP_df, 'SNP')

    with open('turnaround_graphs.html', 'a') as f:
        f.write(CEN_fig.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write(MYE_fig.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write(TSO500_fig.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write(TWE_fig.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write(SNP_fig.to_html(full_html=False, include_plotlyjs='cdn'))


if __name__ == "__main__":
    main()
