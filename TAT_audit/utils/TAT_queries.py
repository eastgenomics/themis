import argparse
import datetime as dt
import dxpy as dx
import json
import Levenshtein
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

# Add in option to change no of weeks to audit as first command line arg
# Defaults to 26 (6 months)
parser = argparse.ArgumentParser(description='Audit settings')
parser.add_argument(
    '--no_of_audit_weeks',
    type=int,
    default=26,
    help='Integer number of weeks to audit before today'
)
args = parser.parse_args()

# Set audit number of weeks (used to get relevant dates + in plot titles)
NO_OF_AUDIT_WEEKS = args.no_of_audit_weeks

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
LOG_FORMAT = (
    "%(asctime)s — %(name)s — %(levelname)s"
    " — %(lineno)d — %(message)s"
)

# Set level to debug, format with date and time and re-write file each time
logging.basicConfig(
    filename=f'{CURRENT_DIR}/TAT_queries_debug.log',
    level=logging.INFO,
    format=LOG_FORMAT,
    filemode='w'
)

# Set up logger
logger = logging.getLogger("main log")

# Jira API things
auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)
headers = {
   "Accept": "application/json"
}

# Assay types we want to audit
ASSAY_TYPES = ['TWE', 'CEN', 'MYE', 'TSO500', 'SNP']

# Working out relevant dates for audit
today_date = dt.date.today()
formatted_today_date = today_date.strftime('%y%m%d')
x_weeks = dt.timedelta(weeks=NO_OF_AUDIT_WEEKS)
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
    that have been created in the last X number of weeks
    Parameters
    ----------
    assay_type : str
        e.g. 'CEN'
    Returns
    -------
    assay_response : list
        list of dicts, each with info for a project
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


def create_run_dict_add_assay(assay_type, assay_response):
    """
    Adds the run name, DX project ID and assay type for each run to dict
    Parameters
    ----------
    assay_type : str
        e.g. 'CEN'
    assay_response : list
        list of dicts from get_002_projects_in_period() function
    Returns
    -------
    run_dict : collections.defaultdict(dict)
        dictionary where key is run name and dict inside has relevant info
        for that run
    """
    run_dict = defaultdict(dict)

    # For each proj name, get run name by removing _002 and _assay name
    # For key with relevant run name
    # Add key project ID with project dx_id as value
    # Add key assay_type with the str name of the assay type
    for project in assay_response:
        run_name = (
            project['describe']['name'].removeprefix('002_').removesuffix(
                f'_{assay_type}'
            )
        )
        # Check if the date of the run is after the begin date of the audit
        # Because 002 project may have been made after actual run date
        if run_name.split('_')[0] >= begin_date_of_audit.strftime('%y%m%d'):
            run_dict[run_name]['project_id'] = project['id']
            run_dict[run_name]['assay_type'] = assay_type

    return run_dict


def get_staging_folders():
    """
    Gets the names of all of the folders in Staging Area
    to be used for matching later

    Returns
    -------
    staging_folders : list
        list of folder names with '/' removed from beginning
    """

    staging_folder_names = list(
        dx.dxfile_functions.list_subfolders(
            project=STAGING_AREA_PROJ_ID,
            path='/',
            recurse=False
        )
    )

    # Remove the '/' from the run folder name for later matching
    staging_folders = [name.removeprefix('/') for name in staging_folder_names]

    return staging_folders


def find_files_in_folder(folder_name, assay_type, log_file_bug):
    """
    Find the files in the relevant Staging_Area52 folder

    Parameters
    ----------
    folder_name : str
        the run name to look for as a folder
    assay_type : str
        the assay service e.g. 'CEN'
    log_file_bug : boolean True or False
        whether the log file upload is affected by bug

    Returns
    -------
    log_file_info : list of dicts
        list response from dxpy of files from that folder
    """
    if assay_type == 'SNP':
        # Files on MiSeq are manually uploaded (not with dx-streaming-upload)
        # So the files are within the named folder
        folder_to_search = f'/{folder_name}/'
        file_name = "*"
    else:
        # Files are within named folder but within sub-folder runs
        folder_to_search = f'/{folder_name}/runs'
        file_name = "*.lane.all.log"

    # If issues uploading to StagingArea
    # The real log file is in processed/ folder
    if log_file_bug:
        folder_to_search = f'/processed/{folder_name}/runs'

    log_file_info = list(
        dx.find_data_objects(
            project=STAGING_AREA_PROJ_ID,
            folder=folder_to_search,
            name=file_name,
            name_mode='glob',
            classname='file',
            created_after=f'-{NO_OF_AUDIT_WEEKS}w',
            describe={
                'fields': {
                    'name': True,
                    'created': True
                }
            }
        )
    )

    return log_file_info


def add_log_file_time(run_dict, assay_type):
    """
    Adds the time the log file was created for that run to a dict and
    returns any mismatched names between run folders + 002 proj names
    Parameters
    ----------
    run_dict : collections.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run
    assay_type : str
        e.g. 'CEN'
    Returns
    -------
    run_dict : collections.defaultdict(dict)
        dictionary where key is run name with dict inside and upload_time
        added
    typo_run_folders : defaultdict(list)
        dict where key is assay type and value is list of dicts
        containing each mismatched run for that assay type
    """
    staging_folders = get_staging_folders()
    typo_run_folders = defaultdict(list)
    typo_folder_info = None

    for folder_name in staging_folders:
        for run_name in run_dict.keys():
            distance = get_distance(folder_name, run_name)
            if distance <= 2:
                log_file_info = find_files_in_folder(
                    folder_name, assay_type, False
                )
                if distance > 0:
                    # If mismatches between names, create dict with info
                    typo_folder_info = {
                        'folder_name': folder_name,
                        'project_name_002': run_name,
                        'assay_type': run_dict[run_name]['assay_type']
                    }

                # If files are found
                if log_file_info:

                    # If SNP run, files uploaded manually to staging area
                    # So get time first file was uploaded (sometimes
                    # people add random files later so can't use last file)
                    if assay_type == 'SNP':
                        min_file_upload = min(
                            data['describe']['created']
                            for data in log_file_info
                        ) / 1000
                        upload_time = time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(min_file_upload)
                        )

                        run_dict[run_name]['upload_time'] = upload_time

                    else:
                        # For non-SNP run, get the time in epoch
                        # that the .lane.all.log file was created
                        log_time = (
                            log_file_info[0]['describe']['created'] / 1000
                        )
                        upload_time = time.strftime(
                            '%Y-%m-%d %H:%M:%S', time.localtime(log_time)
                        )

                        # If log file was uploaded before the first 002 job
                        # Add in log file upload time
                        if upload_time < run_dict[run_name]['earliest_002_job']:
                            run_dict[run_name]['upload_time'] = upload_time

                        # Else, if log file upload is after earliest 002 job
                        # Go into the processed folder + get log time instead
                        else:
                            log_file_info = find_files_in_folder(
                                run_name, assay_type, True
                            )

                            actual_log_time = (
                                log_file_info[0]['describe']['created']
                                / 1000
                            )
                            upload_time = (time.strftime(
                                '%Y-%m-%d %H:%M:%S', time.localtime(
                                    actual_log_time
                                )
                            ))
                            run_dict[run_name]['upload_time'] = upload_time

    # If there are typos add them to defaultdict
    if typo_folder_info:
        typo_run_folders[assay_type].append(typo_folder_info)

    return run_dict, typo_run_folders


def find_jobs_in_project(project_id):
    """
    Finds all the jobs in a project

    Parameters
    ----------
    project_id : str
        dx ID of the project

    Returns
    -------
    jobs : list
        list of dicts containing jobs for that project
    """
    jobs = list(dx.search.find_jobs(
        project=project_id,
        describe={
            'fields': {
                'id': True, 'name': True, 'created': True
            }
        }
    ))

    return jobs


def find_earliest_002_job(run_dict):
    """
    Adds the time the earliest job was run in the relevant 002 project for
    that run
    Parameters
    ----------
    run_dict :  collections.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run
    Returns
    -------
    run_dict : collections.defaultdict(dict)
        dictionary where key is run name with dict inside and earliest_002 job
        added
    """
    # For run, use proj ID for 002 project and get all the jobs in the proj
    for run in run_dict:
        project_id = run_dict[run]['project_id']
        jobs = find_jobs_in_project(project_id)

        # Get the earliest created time of the jobs
        if jobs:
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


def find_multiqc_job(project_id):
    """
    Find MultiQC jobs in the 002 project

    Parameters
    ----------
    project_id : str
        dx ID for the project

    Returns
    -------
    multi_qc_jobs : list
        list of dicts of MultiQC executions in that project
    """
    multi_qc_jobs = list(dx.search.find_executions(
        project=project_id,
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

    return multi_qc_jobs


def add_successful_multiqc_time(run_dict):
    """
    Adds the time the multiQC job finished in the relevant 002 proj for run

    Parameters
    ----------
    run_dict : collections.defaultdict(dict)
        dictionary where key is run name and dict inside with relevant info
        for that run

    Returns
    -------
    run_dict : collections.defaultdict(dict)
        dictionary where key is run name with dict inside and multiQC_finished
        key and val added
    """
    # For each run from the dict
    # Find executions that include the name *MultiQC*
    # As both eggd_MultiQC and MultiQC_v1.1.2 used
    for run in run_dict:
        project_id = run_dict[run]['project_id']
        multi_qc_jobs = find_multiqc_job(project_id)

        # If more than 1 job, take the finished time as the latest one
        # If only 1 job just get the finished time
        if multi_qc_jobs:
            if len(multi_qc_jobs) > 1:
                multiqc_fin = (
                    max(
                        data['describe']['stoppedRunning']
                        for data in multi_qc_jobs
                    ) / 1000
                )
            else:
                multiqc_fin = (
                    multi_qc_jobs[0]['describe']['stoppedRunning'] / 1000
                )

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
    Parameters
    ----------
    assay_type : str
        e.g. 'CEN'
    Returns
    -------
    assay_run_dict : collections.defaultdict(dict)
        dictionary where key is run name with dict inside and all relevant
        audit info inside
    typo_run_folders : collections.defaultdict(list)
        dict where key is assay type and value is list of dicts
        containing each mismatched run for that assay type
    """
    # Do all the steps for the assay type
    assay_response = get_002_projects_in_period(assay_type)
    assay_run_dict = create_run_dict_add_assay(
        assay_type, assay_response
    )
    assay_run_dict = add_successful_multiqc_time(assay_run_dict)
    find_earliest_002_job(assay_run_dict)

    assay_run_dict, typo_run_folders = add_log_file_time(
        assay_run_dict, assay_type
    )

    return assay_run_dict, typo_run_folders


def get_jira_info(queue_id):
    """
    Get the info from Jira API. As can't change size of response and seems to
    be limited to 50, loop over each page of response to get all tickets
    until response is empty
    Parameters
    ----------
    queue_id :  int
        int of the ID for the relevant servicedesk queue
    Returns
    -------
    response_data :  list
        list of dicts with response from Jira API request
    """
    base_url = (
        f"https://{JIRA_NAME}.atlassian.net/rest/servicedeskapi/servicedesk/"
        f"4/queue/{queue_id}/issue"
    )

    response_data = []
    new_data = True
    start = 0
    page_size = 50

    while new_data:
        queue_response = requests.request(
            "GET",
            url=f"{base_url}?start={start}",
            headers=headers,
            auth=auth
        )

        new_data = json.loads(queue_response.text)['values']
        response_data += new_data
        start += page_size

    return response_data


def get_distance(string1, string2):
    """
    Get the distance as integer between two strings
    (do not have to be equal length)

    Parameters
    ----------
    string1 : str
        the first string
    string2 : str
         the second string
    Returns
    -------
    distance : int
        the number of differences between the strings
    """
    distance = Levenshtein.distance(string1, string2)

    return distance


def get_closest_match_in_dict(ticket_name, my_dict):
    """
    Checks for run names in the dict that are only off by 2 characters
    in the Jira ticket name

    Parameters
    ----------
    ticket_name :  str
        the summary name of the Jira ticket (should be run name)
    my_dict : collections.defaultdict
        dict that contains run as key and all audit info as key val pairs

    Returns
    -------
    closest_key_in_dict : str or None
        the key in the dict that either matches the ticket name completely
        or is off by 2. If no relevant key found returns none
    typo_ticket_info : dict or None
        info of the tickets where they mismatch by 2
    """
    typo_ticket_info = None
    closest_key_in_dict = None
    for key in my_dict.keys():
        # Get the distance between the names
        # If 1 or 0 get the closest key in the dict
        distance = get_distance(ticket_name, key)
        if distance <= 2:
            closest_key_in_dict = key
            if distance > 0:
                typo_ticket_info = {
                    'jira_ticket_name': ticket_name,
                    'project_name_002': closest_key_in_dict,
                    'assay_type': my_dict[closest_key_in_dict]['assay_type']
                }

    return closest_key_in_dict, typo_ticket_info


def query_specific_ticket(ticket_id):
    """
    Query a specific Jira ticket to get accurate status change to
    'All samples released' time

    Parameters
    ----------
    ticket_id : int
        ID of the Jira ticket

    Returns
    -------
    ticket_data : dict
        dict of info about that ticket
    """
    url = (
        f"https://{JIRA_NAME}.atlassian.net/rest/servicedeskapi/request/"
        f"{ticket_id}?expand=changelog"
    )

    ticket_info = requests.request(
        "GET",
        url=url,
        headers=headers,
        auth=auth
    )

    ticket_data = json.loads(ticket_info.text)

    return ticket_data


def get_status_change_time(ticket_data):
    """
    Queries the individual Jira ticket to get the correct resolution time
    since when querying all tickets in a queue it is not correct
    Parameters
    ----------
    ticket_data :  dict
        dict of info about the ticket
    Returns
    -------
    res_time_str :  str
        the time of Jira resolution as e.g. "2022-08-10 12:54"
    """
    status = ticket_data['currentStatus']['status']
    if status == 'All samples released':
        time_of_release = (
            ticket_data['currentStatus']['statusDate']['epochMillis'] / 1000
        )
        res_time = (
            dt.datetime.fromtimestamp(
                time_of_release
            ).replace(microsecond=0)
        )
        res_time_str = res_time.strftime('%Y-%m-%d %H:%M:%S')

    return res_time_str


def add_jira_info_closed_issues(all_assays_dict, closed_response):
    """
    Adds the Jira ticket resolution time and final status of runs in the
    closed sequencing runs queue
    Parameters
    ----------
    all_assays_dict :  collections.defaultdict(dict)
        dictionary containing runs for all assay types. Each key is run name
        with all the relevant audit info
    closed_response : list
        API response as list of dicts from Jira for closed sequencing runs
    Returns
    -------
    all_assays_dict :  collections.defaultdict(dict)
        dict with the final Jira status and the time of resolution added
    """
    typo_tickets = []
    runs_no_002_proj = []
    cancelled_list = []
    # Summary of the ticket should be the run name
    for issue in closed_response:
        ticket_name = issue['fields']['summary']
        ticket_id = issue['id']
        start_date, start_time = issue['fields']['created'].split("T")
        start_time = start_time.split(".")[0]
        date_time_created = dt.datetime.strptime(
            f"{start_date} {start_time}", '%Y-%m-%d %H:%M:%S'
        )
        jira_status = issue['fields']['status']['name']
        # If this matches run name in our dict (or is off by 2 chars)
        # Get relevant run name key in dict
        # Add in final Jira status and the time of resolution in datetime
        closest_dict_key, typo_ticket_info = (
            get_closest_match_in_dict(ticket_name, all_assays_dict)
        )

        if typo_ticket_info:
            typo_tickets.append(typo_ticket_info)

        if closest_dict_key:
            # Jira resolution time is incorrect so query the ticket
            # For accurate info
            ticket_data = query_specific_ticket(ticket_id)
            res_time_str = get_status_change_time(ticket_data)
            all_assays_dict[closest_dict_key]['jira_status'] = (
                jira_status
            )
            all_assays_dict[closest_dict_key]['jira_resolved'] = (
                res_time_str
            )
        else:
            # No key in our dict found (no 002 project exists for it)
            # Get relevant info
            # Try and get the key which stores the assay type
            # If 'SNP Genotyping' change to 'SNP'
            # Otherwise if key does not exist, set to Unknown
            assay_type_field = issue.get('fields').get('customfield_10070')
            if assay_type_field:
                assay_type_val = assay_type_field[0].get('value')
                assay_type = assay_type_val.replace(' Genotyping', '')
            else:
                assay_type = 'Unknown'

            # If ticket not in dict (has no 002 project)
            # Check it's within audit time + assays we are interested in
            if (
                assay_type in ASSAY_TYPES
                and start_date >= begin_date_of_audit.strftime('%Y-%m-%d')
            ):
                # If reports have been released for the run
                # Get the time ticket changed to 'All samples released'
                if jira_status == 'All samples released':
                    ticket_data = query_specific_ticket(ticket_id)
                    res_time_str = get_status_change_time(ticket_data)
                    res_time = dt.datetime.strptime(
                        res_time_str, '%Y-%m-%d %H:%M:%S'
                    )

                    # Get TAT in days as float
                    turnaround_time_days = (
                        res_time - date_time_created
                    ).days
                    remainder = round(
                        ((res_time - date_time_created).seconds)
                        / 86400, 1
                    )
                    turnaround_time = turnaround_time_days + remainder

                    runs_no_002_proj.append({
                        'run_name': ticket_name,
                        'assay_type': assay_type,
                        'jira_ticket_created': date_time_created,
                        'jira_ticket_resolved': res_time_str,
                        'estimated_TAT': turnaround_time
                    })
                else:
                    # Data not released, add to cancelled list
                    not_released_statuses = [
                        "Data cannot be processed",
                        "Data cannot be released",
                        "Data not received"
                    ]
                    if jira_status in not_released_statuses:
                        cancelled_list.append({
                            'run_name': ticket_name,
                            'assay_type': assay_type,
                            'date_jira_ticket_created': date_time_created,
                            'reason_not_released': jira_status
                        })

    return all_assays_dict, typo_tickets, runs_no_002_proj, cancelled_list


def add_jira_info_open_issues(all_assays_dict, open_jira_response):
    """
    Adds the Jira ticket current status for those open and the current time
    to the all_assays_dict
    Parameters
    ----------
    all_assays_dict :  collections.defaultdict(dict)
        dictionary where all the assay types are merged. Each key is run name
        with all the relevant audit info
    jira_response : dict
        API response from Jira
    Returns
    -------
    all_assays_dict :  collections.defaultdict(dict)
        dict with the current Jira status and current time added
    new_runs_list : list
        list of dicts for open runs that don't have a 002 project yet
    typo_tickets : list
        list of dicts with info on proj names which differ to tickets
    """
    new_runs_list = []
    typo_tickets = []
    # Summary of the ticket should be the run name
    for issue in open_jira_response:
        ticket_name = issue['fields']['summary']
        jira_status = issue['fields']['status']['name']

        # If this matches run name in our dict (or is off by 1 char)
        # Get relevant run name key in dict
        closest_dict_key, typo_ticket_info = (
            get_closest_match_in_dict(ticket_name, all_assays_dict)
        )
        if closest_dict_key:
            all_assays_dict[closest_dict_key]['jira_status'] = (
                jira_status
            )

        # If not in our dict (i.e. it's a new run with no 002 proj yet)
        else:
            start_time = issue['fields']['created'].split("T")[0]
            assay_type = issue['fields']['customfield_10070'][0]['value']
            if assay_type == 'SNP Genotyping':
                run_type = 'SNP'
            else:
                run_type = assay_type

            if start_time >= begin_date_of_audit.strftime('%Y-%m-%d'):
                new_runs_list.append({
                    'run_name': ticket_name,
                    'assay_type': run_type,
                    'date_jira_ticket_created': start_time,
                    'current_status': jira_status
                })

        if typo_ticket_info:
            typo_tickets.append(typo_ticket_info)

    return all_assays_dict, new_runs_list, typo_tickets


def create_all_assays_df(all_assays_dict):
    """
    Creates a df with all the assay types, run names and relevant audit info
    Parameters
    ----------
    all_assays_dict :  collections.defaultdict(dict)
        dictionary where all the assay types are merged. Each key is run
        with all the relevant audit info
    Returns
    -------
    all_assays_df : pd.DataFrame()
        dataframe with a row for each run
    """
    # Convert dict to df with run names as column
    all_assays_df = pd.DataFrame(
        all_assays_dict.values()
    ).assign(run_name=all_assays_dict.keys())

    # Reorder columns
    all_assays_df = all_assays_df[[
        'assay_type', 'run_name', 'upload_time', 'earliest_002_job',
        'multiQC_finished', 'jira_status', 'jira_resolved'
    ]]

    cols_to_convert = [
        'upload_time', 'earliest_002_job', 'multiQC_finished', 'jira_resolved'
    ]

    # Convert cols to pandas datetime type
    all_assays_df[cols_to_convert] = all_assays_df[cols_to_convert].apply(
        pd.to_datetime, format='%Y-%m-%d %H:%M:%S'
    )

    return all_assays_df


def add_calculation_columns(all_assays_df):
    """
    Adds columns to the df for log file to earliest 002 job
    and bioinfo processing time
    Parameters
    ----------
    all_assays_df :  pd.DataFrame()
        dataframe with a row for each run
    Returns
    -------
    all_assays_df : pd.DataFrame()
        dataframe with a row for each run and extra calculation columns
    """
    current_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Add new column for time between log file and earliest 002 job
    all_assays_df['upload_to_first_002_job'] = (
        (all_assays_df['earliest_002_job'] - all_assays_df['upload_time'])
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
        (
            all_assays_df['jira_resolved'] - all_assays_df['multiQC_finished']
        ).where(
            all_assays_df['jira_status'] == "All samples released"
        ) / np.timedelta64(1, 'D')
    )

    # Add new column for time from log file creation to Jira resolution
    all_assays_df['upload_to_release'] = (
        (all_assays_df['jira_resolved'] - all_assays_df['upload_time']).where(
            all_assays_df['jira_status'] == "All samples released"
        ) / np.timedelta64(1, 'D')
    )

    # Add the time since MultiQC to now for open tickets with urgents released
    all_assays_df['urgents_time'] = (
        (
            pd.to_datetime(current_time, format='%Y-%m-%d %H:%M:%S')
            - all_assays_df['multiQC_finished']
        ).where(all_assays_df['jira_status'] == 'Urgent samples released')
        / np.timedelta64(1, 'D')
    )

    # Add the time since the last processing step which exists to now
    # For open tickets that are on hold
    all_assays_df['on_hold_time'] = (
        (
            pd.to_datetime(current_time, format='%Y-%m-%d %H:%M:%S')
            - all_assays_df.ffill(axis=1).iloc[:, 4]
        ).where(all_assays_df['jira_status'] == 'On hold')
        / np.timedelta64(1, 'D')
    )

    return all_assays_df


def extract_assay_df(all_assays_df, assay_type):
    """
    Gets relevant rows for an assay type from the total df
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
    with relevant time periods on bar
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
            y=assay_df["upload_to_first_002_job"],
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

    # Add trace for release, only add full TAT above bar if we have
    # Upload to release time
    fig.add_trace(
        go.Bar(
            x=assay_df["run_name"],
            y=assay_df["processing_end_to_release"],
            name="End of processing to release",
            legendrank=2,
            text=round(assay_df['upload_to_release'])
        )
    )

    if assay_df['jira_status'].isnull().values.all():
        pass
    else:
        if "Urgent samples released" in assay_df.jira_status.values:
            fig.add_trace(
                go.Bar(
                    x=assay_df["run_name"],
                    y=assay_df["urgents_time"],
                    name="Urgent samples released",
                    marker_color='#FFA15A'
                )
            )

        if "On hold" in assay_df.jira_status.values:
            fig.add_trace(
                go.Bar(
                    x=assay_df["run_name"],
                    y=assay_df["on_hold_time"],
                    name="On hold",
                    marker_color='#FECB52'
                )
            )

    fig.add_hline(y=3, line_dash="dash")

    fig.update_xaxes(tickangle=45, categoryorder='category ascending')

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
        font_family='Helvetica',
    )

    html_fig = fig.to_html(full_html=False, include_plotlyjs=False)

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
    # Count runs to include as compliant that are less than 3 days TAT
    # Count runs to include overall
    # Add current turnaround for urgent samples released runs
    # To be included in compliance
    compliant_runs = (
        assay_df.loc[assay_df['upload_to_release'] <= 3, 'upload_to_release']
    ).count() + (
        assay_df.loc[assay_df['urgents_time'] <= 3, 'urgents_time'].count()
    )
    relevant_run_count = assay_df[
        assay_df.upload_to_release.notna() | assay_df.urgents_time.notna()
    ].shape[0]

    compliance_percentage = (compliant_runs / relevant_run_count) * 100

    stats_df = pd.DataFrame({
        'Mean turnaround time': assay_df['upload_to_release'].mean(),
        'Median turnaround time': assay_df['upload_to_release'].median(),
        'Mean upload to first 002 job time': (
            assay_df['upload_to_first_002_job'].mean()
        ),
        'Mean pipeline running time': assay_df['processing_time'].mean(),
        'Mean processing end to release time': (
            assay_df['processing_end_to_release'].mean()
        ),
        'Compliance with audit standards': (
            f"({compliant_runs}/{relevant_run_count}) "
            f"{round(compliance_percentage, 2)}%"
        )
    }, index=[assay_df.index.values[-1]]).T.reset_index()

    stats_df.rename(
        columns={
            "index": "Metric", stats_df.columns[1]: "Days"
        }, inplace=True
    )

    stats_table = stats_df.to_html(
        index=False,
        float_format='{:.2f}'.format,
        classes='table table-striped"',
        justify='left'
    )

    return stats_table


def find_runs_for_manual_review(assay_df):
    """
    Finds any runs that should be manually checked because certain metrics
    could not be extracted by the script
    Parameters
    ----------
    assay_df :  pd.DataFrame()
        dataframe with all rows for a specific assay type
    Returns
    -------
    manual_review_dict : dict
        dict with issue as key and any runs with that issue as value
    """
    manual_review_dict = defaultdict(dict)

    # If no Jira status and no current Jira status found flag
    manual_review_dict['no_jira_tix'] = list(
        assay_df.loc[
            (assay_df['jira_status'].isna())
            & (assay_df['jira_status'].isna())
        ]['run_name']
    )

    # If days between log file and 002 job is negative flag
    manual_review_dict['job_002_before_log'] = list(
        assay_df.loc[(assay_df['upload_to_first_002_job'] < 0)]['run_name']
    )

    # If days between processing end + release is negative flag
    manual_review_dict['reports_before_multiqc'] = list(
        assay_df.loc[(assay_df['processing_end_to_release'] < 0)]['run_name']
    )

    # If related log file was never found flag
    manual_review_dict['no_log_file'] = list(
        assay_df.loc[(assay_df['upload_time'].isna())]['run_name']
    )

    # If no 002 job was found flag
    manual_review_dict['no_002_found'] = list(
        assay_df.loc[(assay_df['earliest_002_job'].isna())]['run_name']
    )

    # If no MultiQC job was found flag
    manual_review_dict['no_multiqc_found'] = list(
        assay_df.loc[(assay_df['multiQC_finished'].isna())]['run_name']
    )

    # If there are runs to be flagged in dict, pass
    # Or if all vals empty pass empty dict so can check in Jinja2 if defined
    # To decide whether to show 'Runs to be manually reviewed' text
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
    typo_folders_list = []
    for assay_type in ASSAY_TYPES:
        assay_run_dict, typo_run_folders = create_info_dict(assay_type)
        all_assays_dict.update(assay_run_dict)
        if typo_run_folders:
            typo_folders_list.append(typo_run_folders)

    logger.info("Getting + adding JIRA ticket info for closed seq runs")
    closed_runs_response = get_jira_info(35)
    all_assays_dict, closed_typo_tickets, runs_no_002_proj, cancelled_runs = (
        add_jira_info_closed_issues(
            all_assays_dict, closed_runs_response
        )
    )

    logger.info("Getting + adding JIRA ticket info for open seq runs")
    open_jira_response = get_jira_info(34)
    all_assays_dict, new_runs_list, open_typo_tickets = (
        add_jira_info_open_issues(
            all_assays_dict, open_jira_response
        )
    )

    logger.info("Creating df for all assays")
    all_assays_df = create_all_assays_df(all_assays_dict)
    logger.info("Adding calculation columns")
    add_calculation_columns(all_assays_df)

    all_assays_df.to_csv(
        f'audit_info_{NO_OF_AUDIT_WEEKS}_weeks_{formatted_today_date}.csv',
        float_format='%.3f',
        index=False
    )

    logger.info("Generating objects for each assay")
    CEN_stats, CEN_issues, CEN_fig = create_assay_objects(all_assays_df, 'CEN')
    MYE_stats, MYE_issues, MYE_fig = create_assay_objects(all_assays_df, 'MYE')
    TSO500_stats, TSO500_issues, TSO500_fig = (
        create_assay_objects(all_assays_df, 'TSO500')
    )
    TWE_stats, TWE_issues, TWE_fig = create_assay_objects(all_assays_df, 'TWE')
    SNP_stats, SNP_issues, SNP_fig = create_assay_objects(all_assays_df, 'SNP')

    # Load Jinja2 template
    # Add the charts, tables and issues into the template
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("audit_template.html")

    logger.info("Adding objects into HTML template")
    filename = (
        f"turnaround_times_previous_{NO_OF_AUDIT_WEEKS}"
        f"_weeks_{formatted_today_date}.html"
    )

    # Render all the things to go in the template
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
        runs_to_review_5=SNP_issues,
        new_runs=new_runs_list,
        runs_no_002=runs_no_002_proj,
        open_ticket_typos=open_typo_tickets,
        closed_ticket_typos=closed_typo_tickets,
        typo_folders=typo_folders_list,
        cancelled_runs=cancelled_runs
    )

    logger.info("Writing final report file")
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)


if __name__ == "__main__":
    main()
