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
from jinja2 import Environment, FileSystemLoader
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
NO_OF_AUDIT_WEEKS = 20

ASSAY_TYPES = ['TWE', 'CEN', 'MYE', 'TSO500', 'SNP']

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
    Gets all the 002 projects ending with the relevant assay type from DNAnexus
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
        run_name = (
            project['describe']['name']
        ).removeprefix('002_').removesuffix(f'_{assay_type}')
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
        if log_file_info:
            log_time = log_file_info[0]['describe']['created'] / 1000
            log_time_str = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(log_time)
            )


            if log_time_str < run_dict[run_name]['earliest_002_job']:
                run_dict[run_name]['log_file_created'] = log_time_str

            # If log file is upload is after earliest 002 job
            # Go into the processed folder and get the log file time instead
            else:
                actual_log_file_info = list(
                    dx.find_data_objects(
                        project=STAGING_AREA_PROJ_ID,
                        folder=f'/processed/{run_name}/runs',
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

                actual_log_time = actual_log_file_info[0]['describe']['created'] / 1000
                run_dict[run_name]['log_file_created'] = (
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(actual_log_time))
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

        if jobs:
        # Get the earliest created time of the jobs
            min_job = (
                min(data['describe']['created'] for data in jobs) / 1000
            )
            first_job = (
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(min_job))
            )

        else:
            first_job = None

        # For key with relevant run name
        # Add earliest_002_job key with time of earliest job in datetime format
        run_dict[run]['earliest_002_job'] = first_job

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
    """
    Get the info from Jira API. As can't change size of response and seems to
    Be limited to 50, loop over each page of response to get all tickets
    Until response is empty
    Parameters
    ----------
    queue_id :  int
        int of the ID for the relevant servicedesk queue
    jira_name : str
        the org's name in Jira
    Returns
    -------
    queue_response :  list
        list of dicts with response from Jira API request
    """
    url = (
        f"https://{jira_name}.atlassian.net/rest/servicedeskapi/servicedesk/"
        f"4/queue/{queue_id}/issue"
    )

    data = []
    new_data = True
    start = 0
    page_size = 50

    while new_data:
        queue_response = requests.request(
            "GET",
            url=f"{url}?start={start}",
            headers=headers,
            auth=auth
        )

        new_data = json.loads(queue_response.text)['values']
        data += new_data
        start += page_size

    return data


def add_jira_info_closed_issues(all_assays_dict, jira_response):
    """
    Adds the Jira ticket resolution time and final status of runs in the Closed 
    Sequencing runs queue
    Parameters
    ----------
    all_assays_dict :  collects.defaultdict(dict)
        dictionary where all the assay types are merged. Each key is run name
        with all the relevant audit info
    jira_response : dict
        API response from Jira
    Returns
    -------
    all_assays_dict :  collects.defaultdict(dict)
        dict with the final Jira status and the time of resolution added
    """
    # Run name is normally the summary of the ticket
    for issue in jira_response:
        run_name = issue['fields']['summary']

        # If this matches run in our dict
        # Add in final Jira status and the time of resolution in datetime
        if run_name in all_assays_dict:
            final_jira_status = issue['fields']['status']['name']
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
            all_assays_dict[run_name]['final_jira_status'] = final_jira_status
            all_assays_dict[run_name]['jira_resolved'] = res_time_str

    return all_assays_dict


def add_jira_info_open_issues(all_assays_dict, open_jira_response):
    """
    Adds the Jira ticket current status for those open and the current time
    To the all_assays_dict
    Parameters
    ----------
    all_assays_dict :  collects.defaultdict(dict)
        dictionary where all the assay types are merged. Each key is run name
        with all the relevant audit info
    jira_response : dict
        API response from Jira
    Returns
    -------
    all_assays_dict :  collects.defaultdict(dict)
        dict with the current Jira status and current time added
    """
    for issue in open_jira_response:
        run_name = issue['fields']['summary']

        if run_name in all_assays_dict:
            current_jira_status = issue['fields']['status']['name']
            current_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            all_assays_dict[run_name]['current_jira_status'] = current_jira_status
            all_assays_dict[run_name]['current_time'] = current_time

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
        'assay_type','run_name', 'log_file_created', 'demultiplex_started',
        '002_project_created', 'earliest_002_job', 'multiQC_finished',
        'final_jira_status', 'current_jira_status', 'jira_resolved',
        'current_time'
    ]]

    cols_to_convert = [
        'log_file_created','demultiplex_started','002_project_created',
        'earliest_002_job', 'multiQC_finished', 'jira_resolved',
        'current_time'
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
        (all_assays_df['jira_resolved'] - all_assays_df['multiQC_finished']).where(
        all_assays_df['final_jira_status'] == "All samples released")
        / np.timedelta64(1, 'D')
    )

    # Add new column for time from log file creation to Jira resolution
    all_assays_df['upload_to_release'] = (
        (all_assays_df['jira_resolved'] - all_assays_df['log_file_created']).where(
        all_assays_df['final_jira_status'] == "All samples released"
        ) / np.timedelta64(1, 'D')
    )

    # Add the time since MultiQC to now for open tickets with urgents released
    all_assays_df['urgents_time'] = ((
        all_assays_df['current_time'] - all_assays_df['multiQC_finished']
    ).where(all_assays_df['current_jira_status'] == 'Urgent samples released')
    / np.timedelta64(1, 'D'))

    # Add the time since MultiQC to now for open tickets on hold
    all_assays_df['on_hold_time'] = ((
        all_assays_df['current_time'] - all_assays_df['multiQC_finished']
    ).where(all_assays_df['current_jira_status'] == 'On hold')
    / np.timedelta64(1, 'D'))

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
    assay_df = all_assays_df.loc[
        all_assays_df['assay_type'] == assay_type
    ].reset_index()

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
    Returns
    -------
    html_fig : str
        Plotly figure as HTML string
    """
    # Add trace for Log file to first 002 job
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["log_file_to_first_002_job"],
            name="Upload to processing start",
            legendrank=4
        )
    )

    # Add trace for bioinformatics run time
    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["processing_time"],
            name="Pipeline running",
            legendrank=3
        )
    )

    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["processing_end_to_release"],
            name="End of processing to release",
            legendrank=2,
            text=round(assay_df['upload_to_release'])
        )
    )

    if assay_df['current_jira_status'].isnull().values.all():
        pass
    else:
        if "Urgent samples released" in assay_df.current_jira_status.values:
            fig.add_trace(
                go.Bar(
                    x=assay_df["run_name"],
                    y=assay_df["urgents_time"],
                    name="Only urgents released",
                )
            )

        if "On hold" in assay_df.current_jira_status.values:
            fig.add_trace(
                go.Bar(
                    x=assay_df["run_name"],
                    y=assay_df["on_hold_time"],
                    name="On hold",
                )
            )

    fig.add_hline(y=3, line_dash="dash")

    fig.update_xaxes(tickangle=45)

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
        width=1100,
        height=700,
        font_family='Helvetica'
    )

    html_fig = fig.to_html(full_html=False, include_plotlyjs='cdn')

    return html_fig


def make_stats_table(assay_df):
    """
    Creates a table of relevant TAT stats to be shown under chart
    Parameters
    ----------
    assay_df :  pd.DataFrame()
        dataframe with all rows for a specific assay type
    Returns
    -------
    stats_table : str
        dataframe as a HTML string to pass to DataTables
    """
    # Count runs to include as compliant that are less than 3
    # Count runs to include overall
    compliant_runs = assay_df.loc[
        assay_df['upload_to_release'] <=3, 'upload_to_release'].count() + (
            assay_df.loc[assay_df['urgents_time'] <=3, 'urgents_time'].count()
        )
    relevant_run_count = assay_df[
        assay_df.upload_to_release.notna() | assay_df.urgents_time.notna()
    ].shape[0]

    compliance_percentage = (compliant_runs / relevant_run_count) * 100

    stats_df = pd.DataFrame(
        {
            'Mean turnaround time': assay_df['upload_to_release'].mean(),
            'Median turnaround time': assay_df['upload_to_release'].median(),
            'Mean upload to first 002 job time': (
                assay_df['log_file_to_first_002_job'].mean()
            ),
            'Mean pipeline running time': assay_df['processing_time'].mean(),
            'Mean processing end to release time': (
                assay_df['processing_end_to_release'].mean()
            ),
            'Compliance with audit standards': (
                f"({compliant_runs}/{relevant_run_count}) "
                f"{round(compliance_percentage, 2)}%"
            )
        }, index=[assay_df.index.values[-1]]
    ).T.reset_index()

    stats_df.rename(columns={
        "index": "Metric", stats_df.columns[1]: ""
    }, inplace=True)

    stats_table = stats_df.to_html(
        index=False,
        float_format='{:.2f}'.format,
        classes='table table-striped"',
        justify='left'
    )

    return stats_table


def find_runs_for_manual_review(assay_df):
    """
    Finds any runs that should be manually checked
    Parameters
    ----------
    assay_df :  pd.DataFrame()
        dataframe with all rows for a specific assay type
    Returns
    -------
    manual_review_dict : dict
        dict with issue as key and any runs with that issue as value
        if dict values empty passes as empty dict to be checked in HTML to
        Decide whether to show 'Runs to be manually reviewed' text
    """
    manual_review_dict = defaultdict(dict)

    # If no Jira status and no current Jira status found flag
    manual_review_dict['no_jira_tix'] = list(
        assay_df.loc[(assay_df['final_jira_status'].isna())
        & (assay_df['current_jira_status'].isna())]['run_name']
    )

    # If days between log file and 002 job is negative flag
    manual_review_dict['job_002_before_log'] = list(
        assay_df.loc[(assay_df['log_file_to_first_002_job'] < 0)]['run_name']
    )

    # If days between processing end + release is negative flag
    manual_review_dict['reports_before_multiqc'] = list(
        assay_df.loc[(assay_df['processing_end_to_release'] < 0)]['run_name']
    )

    # If related log file was never found flag
    manual_review_dict['no_log_file'] = list(
        assay_df.loc[(assay_df['log_file_created'].isna())]['run_name']
    )

    # If no 002 job was found flag
    manual_review_dict['no_002_found'] = list(
        assay_df.loc[(assay_df['earliest_002_job'].isna())]['run_name']
    )

    # If no MultiQC job was found flag
    manual_review_dict['no_multiqc_found'] = list(
        assay_df.loc[(assay_df['multiQC_finished'].isna())]['run_name']
    )

    # If there are runs to be flagged in dict pass
    # If all vals empty pass empty dict so can check in Jinja2 if defined
    if any(manual_review_dict.values()):
        pass
    else:
        manual_review_dict = {}

    return manual_review_dict


def create_assay_objects(all_assays_df, assay_type):
    """
    Create the stats table, find issues and make fig for each assay
    Parameters
    ----------
    all_assays_df :  pd.DataFrame()
        dataframe with a row for each run with all audit metrics
    assay_type : str
        service e.g. 'CEN'
    Returns
    -------
    assay_stats : str
        dataframe of TAT stats as HTML string
    assay_issues : dict
        dict with issue as key and any runs with that issue as value
        if dict values empty passes as empty dict to be checked in HTML to
        Decide whether to show 'Runs to be manually reviewed' text
    assay_fig : str
        Plotly fig as HTML string
    """
    assay_df = extract_assay_df(all_assays_df, assay_type)
    assay_stats = make_stats_table(assay_df)
    assay_issues = find_runs_for_manual_review(assay_df)
    assay_fig = create_TAT_fig(assay_df, assay_type)

    return assay_stats, assay_issues, assay_fig


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

    logger.info("Getting + adding JIRA ticket info for closed seq runs")
    closed_runs_response = get_jira_info(35, JIRA_NAME)
    add_jira_info_closed_issues(all_assays_dict, closed_runs_response)

    logger.info("Getting + adding JIRA ticket info for open seq runs")
    open_jira_response = get_jira_info(34, JIRA_NAME)
    add_jira_info_open_issues(all_assays_dict, open_jira_response)

    logger.info("Creating df for all assays")
    all_assays_df = create_all_assays_df(all_assays_dict)
    logger.info("Adding calculation columns")
    add_calculation_columns(all_assays_df)

    logger.info("Generating objects for each assay")
    CEN_stats, CEN_issues, CEN_fig = create_assay_objects(
        all_assays_df, 'CEN'
    )
    MYE_stats, MYE_issues, MYE_fig = create_assay_objects(
        all_assays_df, 'MYE'
    )
    TSO500_stats, TSO500_issues, TSO500_fig = create_assay_objects(
        all_assays_df, 'TSO500'
    )
    TWE_stats, TWE_issues, TWE_fig = create_assay_objects(
        all_assays_df, 'TWE'
    )
    SNP_stats, SNP_issues, SNP_fig = create_assay_objects(
        all_assays_df, 'SNP'
    )

    # Add the charts, tables and issues into the Jinja2 template
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("audit_template.html")

    logger.info("Adding all the info to HTML template")
    filename = f"turnaround_times_previous_{NO_OF_AUDIT_WEEKS}_weeks.html"
    content = template.render(
        chart_1=CEN_fig,
        averages_1=CEN_stats,
        runs_to_review_1=CEN_issues,
        chart_2=MYE_fig,
        averages_2=MYE_stats,
        runs_to_review_2=MYE_issues,
        chart_3=TSO500_fig,
        averages_3=TSO500_stats,
        runs_to_review_3=TSO500_issues,
        chart_4=TWE_fig,
        averages_4=TWE_stats,
        runs_to_review_4=TWE_issues,
        chart_5=SNP_fig,
        averages_5=SNP_stats,
        runs_to_review_5=SNP_issues
    )

    logger.info("Writing final report file")
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)


if __name__ == "__main__":
    main()
