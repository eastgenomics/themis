import argparse
import datetime as dt
import dxpy as dx
import json
import Levenshtein
import logging
import numpy as np
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import sys
import time
import warnings

from collections import defaultdict
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from plotly.subplots import make_subplots
from requests.auth import HTTPBasicAuth


warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
ROOT_DIR = Path(__file__).absolute().parents[1]

# Command line args set up for determining audit period
parser = argparse.ArgumentParser(description='Audit settings')

# Add in options to change begin and end date of audit through CLI
parser.add_argument(
    '-s',
    '--start_date',
    type=str,
    help=(
        "Begin date of audit in YYYY-MM-DD format, e.g. 2022-06-25."
        " Defaults to six months before today"
    )
)

parser.add_argument(
    '-e',
    '--end_date',
    type=str,
    help=(
        "End date of audit in YYYY-MM-DD format, e.g. 2022-10-01."
        " Defaults to today's date"
    )
)

parser.add_argument(
    '-f',
    '--font_size',
    nargs='?',
    type=int,
    default=12,
    help=(
        'Font size for headings of subplots within TAT plots in the report. '
        'Default is 12'
    )
)

# Create and configure logger
LOG_FORMAT = (
    "%(asctime)s — %(name)s — %(levelname)s"
    " — %(lineno)d — %(message)s"
)

# Set log level to debug, format with date and time and re-write file each time
logging.basicConfig(
    filename=ROOT_DIR.joinpath('TAT_queries_debug.log'),
    level=logging.INFO,
    format=LOG_FORMAT,
    filemode='w'
)

# Set up logger
logger = logging.getLogger("main log")


def determine_start_and_end_date(no_of_months):
    """
    Determine the start and end dates of the audit based on CLI arguments
    Parameters
    ----------
    no_of_months : int
        no. of months to audit from today by default if no args entered (taken
        from config file)
    Returns
    -------
    audit_begin_date : str
        the begin date of the audit e.g. '2022-06-25'
    audit_end_date : str
        the end date of the audit e.g. '2022-10-01'
    audit_begin_date_obj : dt.date obj
        the begin date as a date obj so can be formatted as different strings
        later
    audit_end_date_obj : dt.date obj
        the end date as a date obj so can be formatted as different strings
        later
    font_size : int
        font size for subplot headings (default is 12)
    """
    # Parse the CLI args
    args = parser.parse_args()

    # Get font size of subplot headings for the main plot
    font_size = args.font_size

    # Work out default dates for audit if none supplied (today - X months)
    today_date = dt.date.today()
    today_str = today_date.strftime('%Y-%m-%d')
    default_begin_date = today_date + relativedelta(months=-int(no_of_months))
    default_begin_date_str = default_begin_date.strftime('%Y-%m-%d')

    # Check both start and end date are entered
    if (not args.start_date) ^ (not args.end_date):
        parser.error('--start_date and --end_date must be given together')

    # Set start and end to be either the default or entered dates
    if not args.start_date and not args.end_date:
        audit_begin_date = default_begin_date_str
        audit_end_date = today_str
    else:
        # Check start is before end date
        if args.end_date < args.start_date:
            parser.error('--start_date must be before --end_date')
        audit_begin_date = args.start_date
        audit_end_date = args.end_date

    # Get the dates as objects so that they can be converted to
    # different str formats later
    audit_begin_date_obj = dt.datetime.strptime(audit_begin_date, '%Y-%m-%d')
    audit_end_date_obj = dt.datetime.strptime(audit_end_date, '%Y-%m-%d')
    # Get day 5 days before and after start date in date obj and string
    # for finding projects when querying DNAnexus
    five_days_before_start = audit_begin_date_obj + relativedelta(days=-5)
    five_days_before = five_days_before_start.strftime("%Y-%m-%d")
    five_days_after_end = audit_end_date_obj + relativedelta(days=+5)
    five_days_after = five_days_after_end.strftime("%Y-%m-%d")

    return (
        audit_begin_date, audit_end_date, audit_begin_date_obj,
        audit_end_date_obj, five_days_before, five_days_after, font_size
    )


def load_credential_info():
    """
    Load the tokens and Jira email from the credentials.json file

    Returns
    -------
    dx_token : str
        auth token for DNAnexus
    jira_email : str
        the email used for Jira
    jira_token : str
        the auth token for Jira
    staging_proj_id : str
        the DNAnexus project ID for Staging_Area52
    default_months : int
        the default number of months to audit previous to today if no CLI args
        are given
    tat_standard : int
        number of days the audit standard is
    assay_types : list
        list of strings, each string being an assay type to audit
    cancelled_statuses : list
        list of Jira ticket statuses where it means the run is cancelled
    open_statuses : list
        list of Jira ticket statuses where it means the run is still being
        processed
    last_jobs : dict
        dict representing the name of the last job to find for each assay
    """
    # The keys to obtain from the credentials.json file
    keys = [
        'DX_TOKEN', 'JIRA_EMAIL', 'JIRA_TOKEN', 'STAGING_AREA_PROJ_ID',
        'DEFAULT_MONTHS', 'TAT_STANDARD_DAYS', 'ASSAYS',
        'CANCELLED_STATUSES', 'OPEN_STATUSES', 'LAST_JOBS'
    ]
    # Get tokens etc from credentials file
    if os.path.exists(ROOT_DIR.joinpath("credentials.json")):
        with open(
            ROOT_DIR.joinpath("credentials.json"), "r", encoding='utf8'
        ) as json_file:
            credentials = json.load(json_file)

        (dx_token, jira_email, jira_token, staging_proj_id, default_months,
        tat_standard, assay_types, cancelled_statuses, open_statuses,
        last_jobs) = list(map(credentials.get, keys))

    else:
        # credentials file doesn't exist, assume credentials are in env
        (dx_token, jira_email, jira_token, staging_proj_id, default_months,
        tat_standard, assay_types, cancelled_statuses, open_statuses,
        last_jobs) = list(map(os.environ.get, keys))

    # Check all are present
    if not all([
        dx_token, jira_email, jira_token, staging_proj_id, default_months,
        tat_standard, assay_types, cancelled_statuses, open_statuses,
        last_jobs
    ]):
        logger.error(
            "Required credentials could not be parsed from "
            "credentials.json or the env"
        )
        sys.exit()

    return (
        dx_token, jira_email, jira_token, staging_proj_id, default_months,
        int(tat_standard), assay_types, cancelled_statuses, open_statuses,
        last_jobs
    )


class QueryPlotFunctions:
    """Class for querying and plotting functions"""
    def __init__(self):
        (self.dx_token,
        self.jira_email,
        self.jira_token,
        self.staging_id,
        self.default_months,
        self.tat_standard,
        self.assay_types,
        self.cancelled_statuses,
        self.open_statuses,
        self.last_jobs) = load_credential_info()
        # Jira API things
        self.auth = HTTPBasicAuth(self.jira_email, self.jira_token)
        self.headers = {"Accept": "application/json"}
        (self.audit_start,
        self.audit_end,
        self.audit_start_obj,
        self.audit_end_obj,
        self.five_days_before_start,
        self.five_days_after,
        self.font_size) = determine_start_and_end_date(
            self.default_months
        )
        self.current_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.pd_current_time = pd.Timestamp(self.current_time)


    def login(self) -> None:
        """
        Logs into DNAnexus
        Parameters
        ----------
        token : str
            authorisation token for DNAnexus, from credentials.json
        Raises
        ------
        Error
            Raised when DNAnexus user authentification check fails
        """
        DX_SECURITY_CONTEXT = {
            "auth_token_type": "Bearer",
            "auth_token": self.dx_token
        }

        dx.set_security_context(DX_SECURITY_CONTEXT)

        try:
            dx.api.system_whoami()
            logger.info("DNAnexus login successful")
        except Exception as err:
            logger.error("Error logging in to DNAnexus")
            sys.exit(1)


    def get_002_projects_within_buffer_period(self):
        """
        Get all the 002 projects ending in the assay types being audited from
        DNAnexus that have been created within the audit period (plus
        a 5 day buffer)

        Returns
        -------
        projects_dx_response : list
            list of dicts, each with info about a project (id and name)
        """
        # Make string for regex joining all assay types to search for
        # at end of project name
        assay_conditional = "|".join(x for x in self.assay_types)

        # Search projs in buffer period starting with 002 that end in
        # relevant assay types
        projects_dx_response = list(dx.find_projects(
            level='VIEW',
            created_before=self.five_days_after,
            created_after=self.five_days_before_start,
            name=f"^002.*({assay_conditional})$",
            name_mode="regexp",
            describe={
                'fields': {
                    'id': True,
                    'name': True
                }
            }
        ))

        return projects_dx_response


    def create_run_dictionary(self, projects_dx_response):
        """
        Add run name, DX project ID and assay type for each run to dict

        Parameters
        ----------
        projects_dx_response : list
            list of dicts from get_002_projects_in_period() function
        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dictionary where key is run name and dict inside has relevant info
            for that run
        Example:
        {
            '240112_A01295_0298_AHW3GTDRX3': {
                'project_id': 'project-XYZ',
                'assay_type': 'TSO500',
            '240111_A01303_0320_BHWYNVDRX3': {
                'project_id': 'project-OPQ',
                'assay_type': 'CEN'
            }
        }
        """
        run_dict = defaultdict(dict)

        # For each proj name, get run name by removing _002 and _assay name
        for project in projects_dx_response:
            project_name = project['describe']['name']
            assay_type = project_name.split('_')[-1]
            run_name = project_name.removeprefix('002_').removesuffix(
                f'_{assay_type}'
            )

            # Check if the date of the run is within audit dates
            # because 002 project may have been made after actual run date
            # Don't capture 002_vaf_checks project for checking VAF
            run_date, first_part_of_name = run_name.split('_')[0:2]
            if (
                run_date >= self.audit_start_obj.strftime('%y%m%d')
                and run_date <= self.audit_end_obj.strftime('%y%m%d')
                and first_part_of_name != "vaf"
            ):
                # Add in DX project ID and assay type to dict
                run_dict[run_name]['project_id'] = project['id']
                run_dict[run_name]['assay_type'] = assay_type

        return run_dict


    def get_staging_folders(self):
        """
        Gets the names of all of the folders in Staging Area to be used for
        matching later.

        Returns
        -------
        staging_folders : list
            list of folder names with prefix removed from beginning
        """
        # Get a list of staging folder names in 001_Staging_Area52
        staging_folder_names = list(
            dx.dxfile_functions.list_subfolders(
                project=self.staging_id,
                path='/',
                recurse=False
            )
        )

        # Remove the '/' from the run folder name for later matching
        staging_folders = [
            name.removeprefix('/') for name in staging_folder_names
        ]

        return staging_folders


    def find_log_file_in_folder(self, run_name):
        """
        Find the log files in the relevant Staging_Area52 folder

        Parameters
        ----------
        run_name: str
            name of the run

        Returns
        -------
        log_file_info : list of dicts
            list response from dxpy of files from that folder
        """
        log_file_info = list(
            dx.find_data_objects(
                project=self.staging_id,
                folder=f'/{run_name}/runs',
                name="*.lane.all.log",
                name_mode='glob',
                classname='file',
                describe={
                    'fields': {
                        'name': True, 'created': True
                    }
                }
            )
        )

        return log_file_info


    def get_log_file_created_time(self, log_file_info):
        """
        Finds the time the log was was created

        Parameters
        ----------
        log_file_info : list
            list of one dict containing log file info

        Returns
        -------
        upload_time : str
            timestamp for created time of the log file
        """
        # Get time in epoch the .lane.all.log file was created
        # then convert to str
        log_time = log_file_info[0]['describe']['created'] / 1000
        upload_time = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(log_time)
        )

        return upload_time


    def add_upload_time(self, staging_folders, run_dict):
        """
        Add upload time for each run and get any typos in 002 project name

        Parameters
        ----------
        staging_folders : list
            list of all the folders in 001_Staging_Area52
        run_dict : collections.defaultdict(dict)
            dictionary where key is run name and dict inside with relevant info

        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dict with each run as key and info as nested dict with upload
            time added
        Example:
        {
            '240124_A01295_0305_AHW725DRX3': {
                'project_id': 'project-Gfk24G84412KXVyf4kVZVv7g',
                'assay_type': 'TSO500',
                'run_folder_name': '240124_A01295_0305_AHW725DRX3',
                'upload_time': '2024-01-25 08:52:27'
            },
             '240122_A01295_0303_AHTNWYDRX3': {
                'project_id': 'project-GfgyZJ84J4xg8jK0Yb8XFxpf',
                'assay_type': 'TWE',
                'run_folder_name': '240122_A01295_0303_AHTNWYDRX3',
                'upload_time': '2024-01-23 16:29:19'
            }
        }
        """
        for run_name in run_dict.keys():
            for folder_name in staging_folders:
                # Get differences between run name and staging folder name
                distance = Levenshtein.distance(folder_name, run_name)
                # If match with less than 2 differences, add run folder
                # name as nested key
                if distance <= 2:
                    run_dict[run_name]['run_folder_name'] = folder_name
                    # Search for log file in folder
                    files_in_folder = self.find_log_file_in_folder(
                        folder_name
                    )
                    # Add log file time as upload time
                    if files_in_folder:
                        upload_time = self.get_log_file_created_time(
                            files_in_folder
                        )
                        run_dict[run_name]['upload_time'] = upload_time

        return run_dict


    def update_run_name(self, run_dict):
        """
        Update each main key in the dict (the run name) to the run name
        according to the folder in the Staging Area. This
        corrects any errors in naming of DNAnexus '002_' projects

        Parameters
        ----------
        run_dict : dict
            dict with each run as key and info as nested dict

        Returns
        -------
        updated_dict : dict
            dict where the run name key is updated if it has a typo
        typo_run_folders : list
            list of dicts representing runs with typos in the 002 project name
        """
        updated_dict = defaultdict(dict)
        typo_run_folders = []
        # For each run, get name of the 001_Staging_Area52 folder if exists.
        # Add new key of folder name and make the value all the existing info
        # for that run
        for run_name, run_info in run_dict.items():
            if run_info.get('run_folder_name'):
                folder_name = run_dict[run_name]['run_folder_name']
                assay_type = run_dict[run_name]['assay_type']

                distance = Levenshtein.distance(folder_name, run_name)
                if distance > 0:
                    # If ticket mismatches, add typo info to list
                    typo_run_folders.append({
                        'assay_type': assay_type,
                        'folder_name': folder_name,
                        'project_name_002': run_name
                    })

                updated_dict[folder_name] = run_dict[run_name]
            # Othereise if no 'run_folder_name' keep key and values as is
            else:
                updated_dict[run_name] = run_dict[run_name]

        return updated_dict, typo_run_folders


    def find_conductor_jobs(self):
        """
        Find eggd_conductor_jobs in the Staging Area

        Returns
        -------
        conductor_jobs : list
            list of dicts with info about conductor jobs in staging area
        """
        # Find the conductor jobs in staging area
        conductor_jobs = list(dx.search.find_jobs(
            project=self.staging_id,
            created_before=self.five_days_after,
            created_after=self.five_days_before_start,
            name='eggd_conductor*',
            name_mode='glob',
            describe={
                'fields': {
                    'id': True,
                    'name': True,
                    'created': True,
                    'originalInput': True
                }
            }
        ))

        return conductor_jobs


    def get_earliest_conductor_job_for_each_run(self, conductor_jobs):
        """
        Get the time the earliest conductor job for each run started

        Parameters
        ----------
        conductor_jobs : list
            list of dicts for eggd_conductor jobs in 001_Staging_Area52

        Returns
        -------
        conductor_job_dict : dict
            dict with run name as key and the earliest conductor job (epoch
            time) as value
        Example:
        {
            '240124_A01295_0305_AHW725DRX3': 1706172760.815,
            '240122_A01295_0303_AHTNWYDRX3': 1706027377.209,
            '240119_A01295_0301_AHW5LGDRX3': 1705752954.959,
        }
        """
        # For each eggd_conductor job in Staging Area
        # get the time it started and the relevant run from the job name
        conductor_job_dict = defaultdict(list)
        for conductor_job in conductor_jobs:
            job_start = conductor_job['describe']['created'] / 1000
            job_name = conductor_job['describe']['name']

            # Try and get the run name from the job name
            try:
                run_name = job_name.split('-')[1:2][0]
            except:
                run_name = job_name
            # Add each time a conductor job started for that run
            conductor_job_dict[run_name].append(job_start)

        # Overwrite the value with the earliest job start time
        for run_name, conductor_jobs in conductor_job_dict.items():
            conductor_job_dict[run_name] = min(conductor_jobs)

        return conductor_job_dict


    def add_first_job_time(self, conductor_job_dict, run_dict):
        """
        Add the time of the first job (the eggd_conductor job) to our dict

        Parameters
        ----------
        conductor_job_dict : dict
            dict with run name as key and the earliest conductor job as value
        run_dict : dict
            dict holding each run and all the info we currently have about it

        Returns
        -------
        run_dict : dict
            dict with each run plus first job time added
        {
            '240124_A01295_0305_AHW725DRX3': {
                'project_id': 'project-Gfk24G84412KXVyf4kVZVv7g',
                'assay_type': 'TSO500',
                'run_folder_name': '240124_A01295_0305_AHW725DRX3',
                'upload_time': '2024-01-25 08:52:27',
                'first_job': '2024-01-25 08:52:40'
            },
             '240122_A01295_0303_AHTNWYDRX3': {
                'project_id': 'project-GfgyZJ84J4xg8jK0Yb8XFxpf',
                'assay_type': 'TWE',
                'run_folder_name': '240122_A01295_0303_AHTNWYDRX3',
                'upload_time': '2024-01-23 16:29:19',
                'first_job': '2024-01-23 16:29:37'
            }
        }
        """
        # Add the time of the eggd_conductor job, making sure the conductor
        # job start time is after the upload time
        for run_name, conductor_start_time in conductor_job_dict.items():
            if run_name in run_dict:
                # Get upload time so we only add a conductor job if it was
                # after the upload time
                upload_time = run_dict[run_name].get('upload_time')
                if upload_time:
                    first_job_start = time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(
                            conductor_start_time
                        )
                    )
                    if upload_time < first_job_start:
                            run_dict[run_name]['first_job'] = first_job_start

        return run_dict


    def get_closest_match_in_dict(self, ticket_name, run_dict):
        """
        Checks for run names in the dict that are only off by 2 characters
        in the Jira ticket name

        Parameters
        ----------
        ticket_name :  str
            the summary name of the Jira ticket (should be run name)
        run_dict : dict
            dict that contains run as key and all audit info as key val pairs

        Returns
        -------
        closest_key_in_dict : str or None
            the key in the dict that either matches the ticket name completely
            or is off by 2. If no relevant key found returns None
        typo_ticket_info : dict or None
            info of a ticket where the ticket + run name mismatch by 1 or 2.
            Otherwise if they match completely or no match returns None
        """
        typo_ticket_info = None
        closest_key = None

        for run_name in run_dict.keys():
            # Get the distance between the names
            # If 1 or 0 get the closest key in the dict
            distance = Levenshtein.distance(ticket_name, run_name)
            if distance <= 2:
                closest_key = run_name
                if distance > 0:
                    typo_ticket_info = {
                        'assay_type': run_dict[closest_key]['assay_type'],
                        'run_name': closest_key,
                        'jira_ticket_name': ticket_name
                    }

        return closest_key, typo_ticket_info


    def query_jira_tickets_in_queue(self, queue_id):
        """
        Get the info from Jira API. As can't change size of response and seems
        to be limited to 50, loop over each page of response to get all tickets
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
            "https://cuhbioinformatics.atlassian.net/rest/servicedeskapi/"
            f"servicedesk/4/queue/{queue_id}/issue"
        )

        response_data = []
        new_data = True
        start = 0
        page_size = 50

        while new_data:
            queue_response = requests.request(
                "GET",
                url=f"{base_url}?start={start}",
                headers=self.headers,
                auth=self.auth
            )
            # Check request response OK, otherwise exit as would be key error
            if queue_response.ok:
                new_data = json.loads(queue_response.text)['values']
                response_data += new_data
                start += page_size
            else:
                logger.error("Issue with Jira response - check credentials")
                sys.exit(1)

        return response_data


    def get_ticket_transition_times(self, ticket_id):
        """
        Get the times of all the Jira ticket transitions to different statuses

        Parameters
        ----------
        ticket_id : int
            the ID of the Jira ticket

        Returns
        -------
        transitions_dict : dict
            dict of the time the ticket transition to a status happened last
            (to account for a ticket being put back to a previous status)
        Example:
        {
            'Data Received': '2024-02-02 09:53:06',
            'Data processed': '2024-02-02 17:09:46',
            'Urgent samples released': '2024-02-05 16:17:08'
        }
        """
        transitions_dict = defaultdict(list)
        url = f"https://cuhbioinformatics.atlassian.net/rest/api/3/issue/{ticket_id}/changelog"

        log_response = requests.request(
            "GET",
            url,
            headers=self.headers,
            auth=self.auth
        )

        change_info = json.loads(log_response.text)['values']

        # Loop over changes, get times the ticket changed to that status
        # add to dict then get the latest time the ticket transitioned
        # to that status
        for change in change_info:
            status_details = [
                x for x in change['items'] if x['field'] == 'status'
            ]

            if status_details:
                status_date, status_time = change['created'].split('T')
                status_time = status_time.split('.', 3)[0]
                status_date_time = f"{status_date} {status_time}"
                new_state = status_details[0]['toString']

                transitions_dict[new_state].append(status_date_time)

        for status_key, status_change_times in transitions_dict.items():
            transitions_dict[status_key] = max(status_change_times)

        return transitions_dict


    def create_jira_info_dict(self, jira_api_response):
        """
        Create a dictionary with only relevant info from all of the Jira
        tickets in a queue

        Parameters
        ----------
        jira_api_response : list
            list of dicts, each dict info about a Jira ticket from a
            specific helpdesk queue
        Example:
        {
            '240130_A01303_0329_BH2HWHDRX5': {
                'ticket_key': 'EBH-2377',
                'ticket_id': '21865',
                'jira_status': 'All samples released',
                'assay_type': 'CEN',
                'date_jira_ticket_created': datetime.datetime(2024, 1, 30, 16, 52, 18)
            },
            '240130_A01303_0330_AHWL32DRX3': {
                'ticket_key': 'EBH-2376',
                'ticket_id': '21864',
                'jira_status': 'All samples released',
                'assay_type': 'MYE',
                'date_jira_ticket_created': datetime.datetime(2024, 1, 30, 16, 49, 38)
            }
        }
        """
        jira_run_dict = defaultdict(dict)
        # Get ticket relevant info
        for issue in jira_api_response:
            ticket_name = issue['fields']['summary']
            start_date, start_time = issue['fields']['created'].split("T")
            start_time = start_time.split(".")[0]
            date_time_created = dt.datetime.strptime(
                f"{start_date} {start_time}", '%Y-%m-%d %H:%M:%S'
            )

            # Get assay type info
            assay_type_field = issue.get('fields').get('customfield_10070')
            if assay_type_field:
                assay_type = assay_type_field[0].get('value')
            else:
                assay_type = 'Unknown'

            # Check the ticket is within 5 days of the audit period and a
            # relevant assay type. If so add ticket info to a dict
            if (
                (assay_type in self.assay_types)
                and (date_time_created >= dt.datetime.strptime(
                    self.five_days_before_start, '%Y-%m-%d'
                ))
                and (date_time_created <= dt.datetime.strptime(
                    self.five_days_after, '%Y-%m-%d'
                ))
            ):
                jira_run_dict[ticket_name]['ticket_key'] = issue['key']
                jira_run_dict[ticket_name]['ticket_id'] = issue['id']
                jira_run_dict[ticket_name]['jira_status'] = (
                    issue['fields']['status']['name']
                )
                jira_run_dict[ticket_name]['assay_type'] = assay_type

                # Convert datetime to string format
                jira_run_dict[ticket_name]['date_jira_ticket_created'] = (
                    date_time_created
                )

        print(
            f"Found {len(jira_run_dict)} Jira tickets within the audit "
            "period (plus a 5 day buffer)")

        return jira_run_dict


    def add_jira_ticket_info(self, run_dict, jira_run_dict):
        """
        Add information about the run from the JIRA ticket to our dict or if
        we're missing it for some reason add it to the relevant list to be
        returned in the report

        Parameters
        ----------
        run_dict : dict
            dict that contains run as key and all audit info as key val pairs
        jira_run_dict : dict
            dict with each Jira ticket summary (run name) and values as
            info about the ticket

        Returns
        -------
        run_dict : dict
            dict where key is run name and vals are audit info, with Jira
            ticket info added
        Example:
        {
            '240124_A01295_0305_AHW725DRX3': {
                'project_id': 'project-Gfk24G84412KXVyf4kVZVv7g',
                'assay_type': 'TSO500',
                'run_folder_name': '240124_A01295_0305_AHW725DRX3',
                'upload_time': '2024-01-25 08:52:27',
                'first_job': '2024-01-25 08:52:40',
                'jira_status': 'All samples released',
                'ticket_key': 'EBH-2079',
                'ticket_id': '21864'
            },
             '240122_A01295_0303_AHTNWYDRX3': {
                'project_id': 'project-GfgyZJ84J4xg8jK0Yb8XFxpf',
                'assay_type': 'TWE',
                'run_folder_name': '240122_A01295_0303_AHTNWYDRX3',
                'upload_time': '2024-01-23 16:29:19',
                'first_job': '2024-01-23 16:29:37',
                'jira_status': 'All samples released',
                'ticket_key': 'EBH-2075',
                'ticket_id': '21862'
            }
        }
        typo_tickets : list
            list of dicts, each representing a run where the ticket name does
            not match the run name (1 or 2 differences)
        runs_no_002_proj: list
            list of dicts, each representing a run where we have a ticket
            at 'All samples released' but haven't found a 002 project
        cancelled_list : list
            list of dicts, each representing a run which was cancelled
        open_runs_list : list
            list of dicts, each representing a run which is still being
            processed but has no 002 project
        """
        typo_tickets = []
        runs_no_002_proj = []
        cancelled_list = []
        open_runs_list = []

        # For each Jira ticket we've saved, get info and compare to all of
        # the runs we're currently storing
        for ticket_name, ticket_info in jira_run_dict.items():
            ticket_id = ticket_info['ticket_id']
            assay_type = ticket_info['assay_type']
            jira_status = ticket_info['jira_status']
            date_time_created = ticket_info['date_jira_ticket_created']
            ticket_key = ticket_info['ticket_key']

            # If this matches run name in our dict (or is off by 2 chars)
            # Get relevant run name key in dict + return any with mismatches
            closest_key, typo_ticket_info = (
                self.get_closest_match_in_dict(ticket_name, run_dict)
            )
            if typo_ticket_info:
                typo_tickets.append(typo_ticket_info)

            # If the run matches a run in our dict (has a 002 project)
            if closest_key:
                run_dict[closest_key]['jira_status'] = jira_status
                run_dict[closest_key]['ticket_key'] = ticket_key
                run_dict[closest_key]['ticket_id'] = ticket_id

                # but is cancelled (has not been changed to a 003 project yet)
                # add to list of cancelled runs
                if jira_status in self.cancelled_statuses:
                    cancelled_list.append({
                        'run_name': ticket_name,
                        'assay_type': assay_type,
                        'date_jira_ticket_created': date_time_created,
                        'jira_status': jira_status
                    })

            else:
                # Otherwise we don't have a 002 project for the ticket
                # Check whether the ticket is for an assay we're auditing and
                # within actual audit period
                if (
                    (assay_type in self.assay_types)
                    and (date_time_created >= self.audit_start_obj)
                    and (date_time_created <= self.audit_end_obj)
                ):
                    # If it's cancelled, add to list of cancelled runs
                    if jira_status in self.cancelled_statuses:
                        # Data was not released, add to cancelled list
                            cancelled_list.append({
                                'run_name': ticket_name,
                                'assay_type': assay_type,
                                'date_jira_ticket_created': date_time_created,
                                'jira_status': jira_status
                            })

                    # If ticket is open, we just don't have a 002 project yet
                    # so add to open runs list
                    elif jira_status in self.open_statuses:
                        open_runs_list.append({
                            'run_name': ticket_name,
                            'assay_type': assay_type,
                            'date_jira_ticket_created': date_time_created,
                            'current_status': jira_status
                        })

                    # Otherwise ticket must be at 'All samples released' - get
                    # an estimated TAT and add to runs with no 002 project
                    else:
                        change_log = self.get_ticket_transition_times(ticket_id)
                        res_time_str = change_log.get('All samples released')
                        if res_time_str:
                            res_time = dt.datetime.strptime(
                                res_time_str, '%Y-%m-%d %H:%M:%S'
                            )
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

        return (
            run_dict, typo_tickets, runs_no_002_proj, cancelled_list,
            open_runs_list
        )


    def add_transition_times(self, run_dict):
        """
        Add the ticket transition times to the run dictionary to the run
        dictionary

        Parameters
        ----------
        run_dict : dict
            dict that contains run as key and all audit info as key val pairs

        Returns
        -------
        run_dict : dict
            dict that contains run as key and all audit info as key val pairs
        Example:
        {
            '240124_A01295_0305_AHW725DRX3': {
                'project_id': 'project-Gfk24G84412KXVyf4kVZVv7g',
                'assay_type': 'TSO500',
                'run_folder_name': '240124_A01295_0305_AHW725DRX3',
                'upload_time': '2024-01-25 08:52:27',
                'first_job': '2024-01-25 08:52:40',
                'jira_status': 'All samples released',
                'ticket_key': 'EBH-2079',
                'ticket_id': '21864',
                'changelog' : {
                    'Data Received': '2024-01-25 09:05:11',
                    'Data processed': '2024-01-26 14:33:30',
                    'All samples released': '2024-01-26 14:57:08'
                },
                'jira_resolved': '2024-01-26 14:57:08'
            },
             '240122_A01295_0303_AHTNWYDRX3': {
                'project_id': 'project-GfgyZJ84J4xg8jK0Yb8XFxpf',
                'assay_type': 'TWE',
                'run_folder_name': '240122_A01295_0303_AHTNWYDRX3',
                'upload_time': '2024-01-23 16:29:19',
                'first_job': '2024-01-23 16:29:37',
                'jira_status': 'All samples released',
                'ticket_key': 'EBH-2075',
                'ticket_id': '21862',
                'changelog' : {
                    'Data Received': '2024-01-23 16:35:11',
                    'Data processed': '2024-01-24 11:33:30',
                    'All samples released': '2024-01-24 14:52:08'
                },
                'jira_resolved': '2024-01-24 14:52:08'
            }
        }
        """
        for run_name, run_info in run_dict.items():
            ticket_id = run_info['ticket_id']
            # Query Jira API with changelog for ticket transition times
            change_log = self.get_ticket_transition_times(ticket_id)

            # Add the dict to the changelog key
            run_dict[run_name]['change_log'] = change_log

            # If ticket is at 'All samples released' add the resolved time
            jira_resolved = change_log.get('All samples released')
            if jira_resolved:
                run_dict[run_name]['jira_resolved'] = jira_resolved

        return run_dict


    def search_for_final_jobs(self, project_id, job_name_to_search):
        """
        In a project, find all of the jobs that are the last job to be run
        for that assay type

        Parameters
        ----------
        project_id : str
            ID of the DNAnexus project
        job_name_to_search : str
            name of the app which is run last in the pipeline for the assay
            type
        """

        final_jobs = list(dx.search.find_jobs(
            project=project_id,
            state='done',
            name=f"*{job_name_to_search}*",
            name_mode='glob',
            describe={
                'fields': {
                    'id': True,
                    'project': True,
                    'name': True,
                    'executableName': True,
                    'stoppedRunning': True
                }
            }
        ))

        return final_jobs


    def get_final_job_before_ticket_resolved(
        self, final_jobs, jira_resolved_timestamp
    ):
        """
        Get the time the final job finished for a run before the
        ticket was resolved (to prevent getting e.g. reanalysis jobs for Dias)
        If ticket not resolved yet, just get the time that the final relevant
        job finished

        Parameters
        ----------
        final_jobs : list
            list of dicts containing info about the final jobs for a run
        jira_resolved_timestamp : str
            timestamp that the Jira ticket for the run was resolved

        Returns
        -------
        excel_completed : str or None
            timestamp the last create excel job finished (or None if no excel
            jobs)
        """
        job_completed = None
        jobs_before_resolution = []
        # Convert time the Jira ticket was resolved from epoch to timestamp
        jira_res_epoch = time.mktime(
            time.strptime(jira_resolved_timestamp, "%Y-%m-%d %H:%M:%S")
        )

        if final_jobs:
            for job in final_jobs:
                # Get the time stopped running in epoch
                finished_running = (
                    job['describe']['stoppedRunning'] / 1000
                )
                # If job finished before the Jira ticket was resolved
                # Add to list
                if finished_running <= jira_res_epoch:
                    jobs_before_resolution.append(finished_running)
            # If any jobs are before Jira ticket resolved, find time
            # last job finished and convert to timestamp
            if jobs_before_resolution:
                job_fin = max(jobs_before_resolution)
                job_completed = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(job_fin)
                )

        return job_completed


    def get_last_job(self, final_jobs):
        """
        Get the time the last job finished for unreleased runs

        Parameters
        ----------
        final_jobs : list
            list of dicts containing info about the final jobs

        Returns
        -------
        job_completed : str or None
            timestamp the last job finished (or None if no jobs)
        """
        job_completed = None

        if final_jobs:
            # Get time last job finished (epoch)
            job_fin = max(
                final_job['describe']['stoppedRunning'] / 1000
                for final_job in final_jobs
            )
            # Convert to timestamp
            job_completed = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(job_fin)
            )

        return job_completed


    def add_last_job_time(self, run_dict):
        """
        Add the time the relevant Excel job finished for the runs to the
        dictionary for CEN and TWE runs

        Parameters
        ----------
        all_assays_dict : dict
            dict containing each run and relevant audit info as nested keys

        Returns
        -------
        all_assays_dict : dict
            dict with processing_finished key added for CEN + TWE runs
            containing time last relevant Excel generation job finished
        Example:
        {
            '240124_A01295_0305_AHW725DRX3': {
                'project_id': 'project-Gfk24G84412KXVyf4kVZVv7g',
                'assay_type': 'TSO500',
                'run_folder_name': '240124_A01295_0305_AHW725DRX3',
                'upload_time': '2024-01-25 08:52:27',
                'first_job': '2024-01-25 08:52:40',
                'jira_status': 'All samples released',
                'ticket_key': 'EBH-2364',
                'ticket_id': '21822',
                'change_log': {
                    'Data Received': '2024-01-26 09:38:42',
                    'Data processed': '2024-01-26 09:38:43',
                    'All samples released': '2024-01-26 11:48:31'
                },
                'jira_resolved': '2024-01-26 11:48:31',
                'processing_finished': '2024-01-26 11:29:14'
            },
        }
        """
        for run, run_info in run_dict.items():
            project_id = run_info.get('project_id')
            assay_type = run_info.get('assay_type')
            # Get the last job to search for that assay type (in config)
            job_to_search = self.last_jobs.get(assay_type)
            jira_resolved = run_info.get('jira_resolved')

            if job_to_search:
                # Search for the specific job type in the project
                final_jobs = self.search_for_final_jobs(
                    project_id, job_to_search
                )
                # If the ticket is resolved, get time the final job was run
                # before the ticket was resolved
                if jira_resolved:
                    job_finished = self.get_final_job_before_ticket_resolved(
                        final_jobs, jira_resolved
                    )

                    # If found the relevant final job, add to dict
                    if job_finished:
                        run_dict[run]['processing_finished'] = (
                            job_finished
                        )
                else:
                    # Jira ticket not resolved, just get last relevant job run
                    # If exists, add the timestamp to dict
                    job_finished = self.get_last_job(final_jobs)
                    if job_finished:
                        run_dict[run]['processing_finished'] = (
                            job_finished
                        )
            else:
                print(
                    "No final job to search for provided in the credentials."
                    f"json file for assay {assay_type}"
                )

        return run_dict


    def add_in_empty_keys(self, run_dict):
        """
        Add in empty keys as None so that we don't have to check every time
        we do anything later that they exist

        Parameters
        ----------
        run_dict : run_dict
            dict with each run and the relevant audit info

        Returns
        -------
        run_dict : run_dict
            dict with each run and the relevant audit info plus any empty
            keys as None
        """
        keys_to_add = [
            'upload_time', 'first_job', 'processing_finished', 'jira_status',
            'jira_resolved', 'change_log', 'ticket_key'
        ]
        for run, run_info in run_dict.items():
            for key in keys_to_add:
                value = run_info.get(key)
                if not value:
                    run_info[key] = None

        return run_dict


    def create_run_df(self, run_dict):
        """
        Creates a df with all the assay types, run names and relevant audit info
        Parameters
        ----------
        run_dict : run_dict
            dict with each run and the relevant audit info
        Returns
        -------
        all_assays_df : pd.DataFrame
            dataframe with a row for each run
        """
        # Convert dict to df with run names as column
        run_df = pd.DataFrame(
            run_dict.values()
        ).assign(run_name=run_dict.keys())

        # Check dataframe is not empty, if it is exit
        if run_df.empty:
            logger.error("No runs were found within the audit period")
            sys.exit(1)

        # Subset to only columns we want
        run_df = run_df[[
            'assay_type', 'run_name', 'upload_time', 'first_job',
            'processing_finished', 'jira_status', 'jira_resolved',
            'change_log', 'ticket_key'
        ]]

        # Convert cols to pandas datetime type
        cols_to_convert = [
            'upload_time', 'first_job', 'processing_finished', 'jira_resolved'
        ]
        run_df[cols_to_convert] = run_df[cols_to_convert].apply(
            pd.to_datetime, format='%Y-%m-%d %H:%M:%S'
        )

        return run_df


    def generate_hyperlink(self, row):
        """
        Create a hyperlink for the run, where the text is the run name but
        it links to the Jira ticket for the run

        Parameters
        ----------
        row : pd object
            pandas object as a row of the dataframe
        Returns
        -------
        url : str
            string representing a hyperlink to the Jira ticket for a run
        """
        url = """<a href="https://cuhbioinformatics.atlassian.net/browse/{}">{}</a>""".format(row['ticket_key'], row['run_name'])

        return url


    def add_jira_ticket_hyperlink(self, run_df):
        """
        Add the Jira ticket hyperlink of a run into the dataframe

        Parameters
        ----------
        run_df : pd.DataFrame
            dataframe with a row for each run

        Returns
        -------
        run_df : pd.DataFrame
            dataframe with a row for each run with extra column
            'ticket_hyperlink'
        """
        # If we have a ticket key generate a hyperlink otherwise keep as
        # run name
        run_df['ticket_hyperlink'] = run_df.apply(
            lambda row: row['run_name'] if pd.isnull(row['ticket_key']) else self.generate_hyperlink(row),
            axis=1
        )

        return run_df


    def add_run_week(self, run_df):
        """
        Add the date of the week start for each run

        Parameters
        ----------
        run_df : pd.DataFrame
            dataframe with a row for each run

        Returns
        -------
        run_df : pd.DataFrame
            dataframe with a row for each run with extra columns 'run_date'
            and 'week_start'
        """
        # Get date of the run from the run name
        run_df['run_date'] = run_df['run_name'].str.split('_').str[0]
        # Convert date column to datetime
        run_df['run_date'] = pd.to_datetime(
            run_df['run_date'], format="%y%m%d"
        )

        # Sort chronologically by date for each assay type
        run_df.sort_values(by='run_date', inplace=True)
        # Get the start date of the week of that run
        run_df['week_start'] = run_df[
            'run_date'
        ].dt.to_period('W').dt.start_time.dt.strftime('%d-%m-%y')

        return run_df


    def create_typo_df(self, typo_list):
        """
        Create table of typos between the Jira ticket and run name

        Parameters
        ----------
        typo_list : list
            list of dicts where each represents a run with a typo
        Returns
        -------
        ticket_typos_html : str
            dataframe of runs with typos in the Jira ticket as html string
        """
        typo_df_html = None

        if typo_list:
            typo_df = pd.DataFrame(typo_list)
            typo_df.rename(
                {
                    'jira_ticket_name': 'Jira ticket name',
                    'run_name': 'Run name',
                    'folder_name': 'Run name',
                    'project_name_002': '002 project name',
                    'assay_type': 'Assay type'
                }, axis=1, inplace=True
            )

            typo_df.sort_values('Assay type', inplace=True)
            typo_df_html = typo_df.to_html(
                index=False,
                classes='table table-striped"',
                justify='left'
            )

        return typo_df_html


    def add_calculation_columns(self, run_df):
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
        # Add time between between upload and first job running
        run_df['upload_to_first_job'] = (
            (run_df['first_job'] - run_df['upload_time'])
            / np.timedelta64(1, 'D')
        )

        # Add time for pipeline running (first job starting to processng
        # finishing)
        run_df['processing_time'] = (
            (
                run_df['processing_finished']
                - run_df['first_job']
            )
            / np.timedelta64(1, 'D')
        )

        # Add time it took us to release after processing finishing
        run_df['processing_end_to_release'] = (
            (
                run_df['jira_resolved']
                - run_df['processing_finished']
            ).where(
                run_df['jira_status'] == "All samples released"
            ) / np.timedelta64(1, 'D')
        )


        # Add new column for time from log file creation to Jira resolution
        run_df['upload_to_release'] = (
            (
                run_df['jira_resolved'] - run_df['upload_time']
            ).where(
                (run_df['jira_status'] == "All samples released")
                & (run_df['upload_to_first_job'] >= 0)
                & (run_df['processing_time'] >= 0)
                & (run_df['processing_end_to_release'] >= 0)
            ) / np.timedelta64(1, 'D')
        )

        # Add the time from last job to now for open tickets with urgents
        # released
        run_df['urgents_time'] = (
            (self.pd_current_time - run_df['processing_finished']).where(
                run_df['jira_status'] == 'Urgent samples released'
            ) / np.timedelta64(1, 'D')
        )

        # Get the time from the most recent processing step by forward filling
        # from columns until 'muliQC_finished' column
        # If this results in a string, convert to NA
        run_df['last_processing_step'] = pd.to_datetime(
            run_df.ffill(axis=1).iloc[:,4], errors='coerce'
        )


        # Add the time since the last processing step which exists to current
        # time for open tickets that are on hold
        run_df['on_hold_time'] = (
            (
                self.pd_current_time - run_df['last_processing_step']
            ).where(
                run_df['jira_status'] == 'On hold'
            ) / np.timedelta64(1, 'D')
        )

        return run_df


    def extract_assay_df(self, all_assays_df, assay_type):
        """
        Gets relevant rows for an assay type from the all assays df
        Parameters
        ----------
        all_assays_df :  pd.DataFrame
            dataframe with a row for each run
        assay_type : str
            e.g. 'CEN'
        Returns
        -------
        assay_df : pd.DataFrame
            dataframe with only rows from that assay type
        """
        # Get df with the rows of the relevant assay type
        assay_df = all_assays_df.loc[
            all_assays_df['assay_type'] == assay_type
        ].reset_index()

        return assay_df


    def create_tat_fig_split_by_week(self, assay_df, assay_type):
        """
        Creates stacked bar for each run of that assay type
        with relevant time periods on bar. Split by week, with empty weeks
        included with no bars

        Parameters
        ----------
        assay_df :  pd.DataFrame
            dataframe with only rows from that assay type
        assay_type : str
            e.g. 'CEN'

        Returns
        -------
        html_fig : str
            Plotly figure as HTML string
        """

        # Remove any cancelled runs from plotting
        assay_df = assay_df[~assay_df.jira_status.isin(self.cancelled_statuses)]
        # Get the start of all possible weeks based on the audit dates
        date_weeks = [
            period.start_time.date().strftime('%d-%m-%y')
            for period in pd.period_range(
                start=self.audit_start,
                end=self.audit_end,
            freq='W'
            )
        ]

        # Work out how many runs are in that week to make normalised widths
        # If no runs in that week make 1 so we can still plot a subplot for
        # that week
        run_totals = []
        for week in date_weeks:
            df = assay_df.loc[assay_df['week_start'] == week]
            df_len = len(df)
            if df_len == 0:
                df_len = 1
            run_totals.append(df_len)

        norm_widths = [float(i)/sum(run_totals) for i in run_totals]

        fig = make_subplots(
            rows=1,
            cols=len(date_weeks),
            shared_yaxes=True,
            subplot_titles=[f"w/c<br>{str(week)}" for week in date_weeks],
            column_widths=norm_widths
        )

        for idx, week in enumerate(date_weeks):
            week_df = assay_df.loc[assay_df['week_start'] == week]

            if len(week_df):
                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['upload_to_first_job'],
                        name='Upload to processing start',
                        marker={'color': '#636EFA'},
                        customdata=week_df['run_name'],
                        legendgroup="group1",
                    ), row=1, col=idx+1
                )

                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['processing_time'],
                        name='Pipeline running',
                        marker={'color': '#EF553B'},
                        customdata=week_df['run_name'],
                        legendgroup='group2'
                    ), row=1, col=idx+1
                )

                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['processing_end_to_release'],
                        name='Pipeline end to all samples released',
                        marker={'color': '#00CC96'},
                        customdata=week_df['run_name'],
                        text=round(week_df['upload_to_release'], 1),
                        legendgroup='group3'
                    ), row=1, col=idx+1
                )

                if "Urgent samples released" in week_df.jira_status.values:
                    fig.append_trace(
                        go.Bar(
                            x=week_df['ticket_hyperlink'],
                            y=week_df['urgents_time'],
                            name=(
                                'Pipeline end to now - urgent samples released'
                            ),
                            marker={'color': '#FFA15A'},
                            customdata=week_df['run_name'],
                            legendgroup='group4'
                        ), row=1, col=idx+1
                    )

                if "On hold" in week_df.jira_status.values:
                    fig.add_trace(
                        go.Bar(
                            x=week_df['ticket_hyperlink'],
                            y=week_df['on_hold_time'],
                            name='Last processing step to now - On hold',
                            marker={'color': '#FECB52'},
                            customdata=week_df['run_name'],
                            legendgroup='group4'
                        ), row=1, col=idx+1
                    )


            else:
                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['processing_end_to_release'],
                        name='Fake data',
                    ), row=1, col=idx+1
                )
                fig.update_xaxes(showticklabels=False, row=1, col=idx+1)

        fig.add_hline(y=self.tat_standard, line_dash="dash")

        fig.update_xaxes(
            tickangle=45, categoryorder='category ascending'
        )

        fig.update_layout(
            barmode='relative',
            title={
                'text': f"{assay_type} Turnaround Times "
                        f"{self.audit_start} - {self.audit_end}",
                'xanchor': 'center',
                'x': 0.5,
                'font_size': 18
            },
            yaxis_title="Number of days",
            width=1100,
            height=700,
            font_family='Helvetica',
            legend_traceorder='reversed'
        )

        fig.update_traces(
            hovertemplate=(
                '<br><b>Run</b>: %{customdata}<br>'
                '<b>Stage</b>: %{data.name}<br>'
                '<b>Days</b>: %{y:.2f}<br>'
                '<extra></extra>'
            ),
            textposition='outside',
            width=0.7
        )

        # Each subplot adds its own legend entry so remove any duplicates
        # from the legend
        names = set()
        fig.for_each_trace(
            lambda trace:
                trace.update(showlegend=False)
                if (trace.name in names) else names.add(trace.name))

        # Update the subplot titles font size using command line arg
        # (default 12)
        fig.update_annotations(font_size=self.font_size)

        # Add x axis title as an annotation because plotly subplots are
        # annoying
        fig.add_annotation(
            x=0.5,
            xanchor='center',
            xref='paper',
            y=0,
            yanchor='top',
            yref='paper',
            showarrow=False,
            text='Run name',
            yshift=-160
        )

        html_fig = fig.to_html(full_html=False, include_plotlyjs=False)

        return html_fig


    def make_stats_table(self, assay_df):
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
        compliance_fraction : str
            fraction of runs compliant with audit standards
        compliance_percentage : float
            percentage of runs compliant with audit standards
        """
        # Count runs to include as compliant that are less than 3 days TAT
        # And don't have any issues in each step timings
        # To be included in overall in compliance
        if assay_df.index.empty:
            stats_df = pd.DataFrame({})
            compliance_fraction = None
            compliance_percentage = None

        else:
            compliant_runs = (
                assay_df.loc[
                    (assay_df['upload_to_release'] <= self.tat_standard)
                    & (assay_df['upload_to_first_job'] >= 0)
                    & (assay_df['processing_time'] >= 0)
                    & (assay_df['processing_end_to_release'] >= 0)
                ]
            ).shape[0]

            relevant_run_count = assay_df.loc[
                (assay_df['upload_to_first_job'] >= 0)
                & (assay_df['processing_time'] >= 0)
                & (assay_df['processing_end_to_release'] >=0)
                & (
                    assay_df['upload_to_release'].notna()
                    | assay_df['urgents_time'].notna()
                )
            ].shape[0]

            if relevant_run_count:
                compliance_percentage = (
                    (compliant_runs / relevant_run_count) * 100
                )

                compliance_fraction = f"({compliant_runs}/{relevant_run_count}) "
                compliance_percentage = round(compliance_percentage, 2)
                compliance_string = (
                        f"{compliance_fraction} "
                        f"{compliance_percentage}%"
                )

                stats_df = pd.DataFrame({
                    'Mean overall TAT': assay_df['upload_to_release'].mean(),
                    'Median overall TAT': (
                        assay_df['upload_to_release'].median()
                    ),
                    'Mean upload to processing start': (
                        assay_df['upload_to_first_job'].mean()
                    ),
                    'Mean pipeline running': assay_df['processing_time'].mean(),
                    'Mean processing end to release': (
                        assay_df['processing_end_to_release'].mean()
                    ),
                    'Compliance with audit standards': compliance_string
                }, index=[assay_df.index.values[-1]]).T.reset_index()

                stats_df.rename(
                    columns={
                        "index": "Metric", stats_df.columns[1]: "Time (days)"
                    }, inplace=True
                )

            # If there are runs but none are relevant to be included in
            # TAT stats (none have all samples released)
            # Set compliance percentage and fraction to zero
            else:
                compliance_percentage = 0.0
                compliance_fraction = "(0/0)"
                stats_df = pd.DataFrame({})

        stats_table = stats_df.to_html(
            index=False,
            float_format='{:.2f}'.format,
            classes='table table-striped"',
            justify='left'
        )

        return stats_table, compliance_fraction, compliance_percentage


    def find_runs_for_manual_review(self, assay_df):
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
            assay_df.loc[(assay_df['jira_status'].isna())]['run_name']
        )

        cancelled_or_open_statuses = (
            self.cancelled_statuses + self.open_statuses
        )

        # If days between log file and first job is negative flag
        # unless it's a cancelled run
        manual_review_dict['first_job_before_log'] = list(
            assay_df.loc[
                assay_df['upload_to_first_job'] < 0
                & ~assay_df['jira_status'].isin(cancelled_or_open_statuses)
            ]['run_name']
        )

        # If upload time was never found, flag unless it's a cancelled run
        # or open run
        manual_review_dict['no_log_file'] = list(
            assay_df.loc[
                (assay_df['upload_time'].isna())
                & ~assay_df['jira_status'].isin(cancelled_or_open_statuses)
            ]['run_name']
        )

        # If no final job was found flag
        # unless it's a cancelled run
        manual_review_dict['no_first_job_found'] = list(
            assay_df.loc[
                (assay_df['first_job'].isna())
                & ~assay_df['jira_status'].isin(cancelled_or_open_statuses)
            ]['run_name']
        )

        # If no final job was found flag unless it's a cancelled run
        manual_review_dict['no_final_job_found'] = list(
            assay_df.loc[
                (assay_df['processing_finished'].isna())
                    & ~assay_df['jira_status'].isin(cancelled_or_open_statuses)
            ]['run_name']
        )

        # If there are runs to be flagged in dict, pass
        # Or if all vals empty pass empty dict so can check in Jinja2 if defined
        # To decide whether to show 'Runs to be manually reviewed' text
        if not any(manual_review_dict.values()):
            manual_review_dict = {}

        return manual_review_dict


    def create_assay_objects(self, all_assays_df, assay_type):
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
            Plotly TAT fig as HTML string
        upload_day_fig : str
            Plotly fig showing upload day of the week vs TAT for that assay
            as HTML string
        assay_no_of_002_runs : int
            the number of runs found for that assay through the number of
            002 projects
        assay_fraction : str
            fraction of runs compliant with audit standards
        assay_percentage : float
            percentage of runs compliant with audit standards
        """
        assay_df = self.extract_assay_df(all_assays_df, assay_type)
        assay_stats, assay_fraction, assay_percentage = self.make_stats_table(
            assay_df
        )
        assay_issues = self.find_runs_for_manual_review(assay_df)
        assay_fig = self.create_tat_fig_split_by_week(assay_df, assay_type)
        upload_day_fig = self.create_upload_day_fig(assay_df, assay_type)
        assay_no_of_002_runs = assay_df.shape[0]

        return (
            assay_stats, assay_issues, assay_fig, upload_day_fig,
            assay_no_of_002_runs, assay_fraction, assay_percentage
        )


    def add_in_cancelled_runs(self, all_assays_df, cancelled_runs):
        """
        Add the cancelled runs captured only from Jira tickets to the
        all_assays_df before converting to csv for completeness

        Parameters
        ----------
        all_assays_df : pd.DataFrame
            dataframe with all runs and timestamps and durations
        cancelled_runs : list
            list of dicts with info for cancelled runs from Jira tickets with
            no 002 project

        Returns
        -------
        all_assays_df: pd.DataFrame
            all_assays_df with cancelled runs added as rows
        """
        # Append the list of dicts as new rows
        all_assays_df = all_assays_df.append(cancelled_runs, ignore_index=True)

        # Remove duplicates if a failed run is still named as a '002' project
        # otherwise both the failed 002 project and failed ticket would be
        # returned
        all_assays_df.drop_duplicates(
            subset=['run_name'], keep='last', inplace=True
        )

        # Create new date column extracted from the run name
        all_assays_df['date'] = all_assays_df['run_name'].str.split('_').str[0]
        # Convert date column to datetime
        all_assays_df['date'] = pd.to_datetime(
            all_assays_df['date'], format="%y%m%d"
        )

        # Sort chronologically by date for each assay type
        all_assays_df.sort_values(by=['assay_type', 'date'], inplace=True)
        # Sort assay types so order matches the report
        custom_dict = {'CEN': 0, 'MYE': 1, 'TSO500': 2, 'TWE': 3}
        all_assays_df = all_assays_df.sort_values(
            by=['assay_type'], key=lambda x: x.map(custom_dict)
        )

        # Remove the date column
        all_assays_df.drop(columns=['date'], inplace=True)

        return all_assays_df


    def create_upload_day_fig(self, assay_df, assay_type):
        """
        Create figure to see if the day of the week for data upload impacts
        turnaround time

        Parameters
        ----------
        assay_df : pd.DataFrame()
            dataframe with rows for an assay type with columns including
            run name, upload timestamp and turnaround time in days
        assay_type : str
            the assay type of interest e.g. 'CEN'

        Returns
        -------
        html_fig : str
            Plotly figure as html string
        """
        if 'upload_to_release' in assay_df.columns:
            number_of_relevant_runs = assay_df['upload_to_release'].count()
        else:
            number_of_relevant_runs = None

        if (len(assay_df) and number_of_relevant_runs):
            # Add df column with names of the day of the week that data were
            # uploaded
            assay_df['upload_day'] = assay_df['upload_time'].dt.day_name()
            # Plot upload day vs TAT, if TAT is <= tat_standard colour in green
            # otherwise colour in red
            fig = px.scatter(
                data_frame=assay_df,
                x='upload_day',
                y='upload_to_release',
                custom_data=['run_name'],
                color=assay_df["upload_to_release"] <= float(self.tat_standard),
                color_discrete_map={
                    True: "green",
                    False: "red"
                },
            )
            # Set days in order
            fig.update_xaxes(
                range=[-0.5, 6.5],
                type='category',
                categoryorder='array',
                categoryarray= [
                    "Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday"
                ]
            )

            fig.update_layout(
                title={
                    'text': f'{assay_type} Upload Day vs Turnaround Time',
                    'xanchor': 'center',
                    'x':0.5
                },
                xaxis_title="Upload day of the week",
                yaxis_title="Turnaround time (days)",
                font_family='Helvetica',
                legend=dict(title='Within standards'),
                width=1000,
                height=500,
            )
            # Add run name to hovertext
            fig.update_traces(
                hovertemplate="Run name: %{customdata[0]} <br> Turnaround time: %{y:.2f} days"
            )
        # If empty show empty plot with message
        else:
            fig = go.Figure()
            fig.update_layout(
                font_family='Helvetica',
                xaxis={"visible": False},
                yaxis={"visible": False},
                annotations = [
                    {
                        "text": "No data",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {
                            "size": 28
                        }
                    }
                ]
            )

        html_fig = fig.to_html(full_html=False, include_plotlyjs=False)

        return html_fig


    def write_to_csv(self, all_assays_df, audit_start, audit_end) -> None:
        """
        Write the dataframe of all runs in the audit period and all of
        the associated info to CSV

        Parameters
        ----------
        all_assays_df : pd.DataFrame
            dataframe with all of the runs in the audit period and
            all of the relevant info
        audit_start : str
            the date of the audit start
        audit_end : str
            the date of the audit end
        """
        # Remove unnecessary columns
        all_assays_df.drop(
            columns=['change_log', 'ticket_hyperlink', 'run_date'],
            inplace=True, errors='ignore'
        )
        all_assays_df.to_csv(
            f'audit_info_{audit_start}_{audit_end}.csv',
            float_format='%.3f',
            index=False
        )


def main():
    """Main function to create html report"""
    tatq = QueryPlotFunctions()
    tatq.login()
    period_audited = f"{tatq.audit_start} to {tatq.audit_end}"
    logger.info("Creating dicts for each assay")

    projects_002 = tatq.get_002_projects_within_buffer_period()
    projects_002_dict = tatq.create_run_dictionary(projects_002)
    no_of_002_runs = len(projects_002_dict)
    print(
        f"Found {no_of_002_runs} sequencing runs with 002 projects in DNAnexus"
        " within the audit period"
    )

    # Get folders in 001_Staging_Area52 to check for ticket/project typos
    staging_folders = tatq.get_staging_folders()

    # Add upload time
    projects_002_dict = tatq.add_upload_time(
        staging_folders, projects_002_dict
    )
    projects_002_dict, typo_002_list = tatq.update_run_name(projects_002_dict)

    logger.info("Finding first job for all runs")
    # Get conductor jobs and add first job time
    conductor_jobs = tatq.find_conductor_jobs()
    conductor_run_dict = tatq.get_earliest_conductor_job_for_each_run(
        conductor_jobs
    )
    projects_002_dict = tatq.add_first_job_time(
        conductor_run_dict, projects_002_dict
    )

    logger.info("Getting + adding JIRA ticket info for closed seq runs")
    # Get info from JIRA from the closed sequencing run queue and open
    # sequencing run queue
    jira_closed_queue_tickets = tatq.query_jira_tickets_in_queue(35)
    jira_open_queue_tickets = tatq.query_jira_tickets_in_queue(34)
    all_jira_tickets = jira_closed_queue_tickets + jira_open_queue_tickets

    jira_ticket_dict = tatq.create_jira_info_dict(all_jira_tickets)
    projects_002_dict, typo_tickets, runs_no_002_proj, cancelled_runs, open_runs_list = tatq.add_jira_ticket_info(
        projects_002_dict, jira_ticket_dict
    )
    projects_002_dict = tatq.add_transition_times(projects_002_dict)

    # ADD final job
    projects_002_dict = tatq.add_last_job_time(projects_002_dict)

    projects_002_dict = tatq.add_in_empty_keys(projects_002_dict)

    # Create df for all runs
    run_df = tatq.create_run_df(projects_002_dict)
    run_df = tatq.add_jira_ticket_hyperlink(run_df)
    run_df = tatq.add_run_week(run_df)

    # Sort out typos
    typo_folders_table = tatq.create_typo_df(typo_002_list)
    typo_tickets_table = tatq.create_typo_df(typo_tickets)

    logger.info("Adding calculation columns")
    run_df = tatq.add_calculation_columns(run_df)

    logger.info("Generating objects for each assay")
    CEN_stats, CEN_issues, CEN_fig, CEN_upload_fig, CEN_runs, CEN_frac, CEN_compl = (
        tatq.create_assay_objects(run_df, 'CEN')
    )
    # tatq.create_tat_fig2(CEN_df, 'CEN')
    MYE_stats, MYE_issues, MYE_fig, MYE_upload_fig, MYE_runs, MYE_frac, MYE_compl = (
        tatq.create_assay_objects(run_df, 'MYE')
    )
    # tatq.create_tat_fig2(MYE_df, 'MYE')
    TSO_stats, TSO_issues, TSO_fig, TSO_upload_fig, TSO_runs, TSO_frac, TSO_compl = (
        tatq.create_assay_objects(run_df, 'TSO500')
    )
    # tatq.create_tat_fig2(TSO_df, 'TSO500')
    TWE_stats, TWE_issues, TWE_fig, TWE_upload_fig, TWE_runs, TWE_frac, TWE_compl = (
        tatq.create_assay_objects(run_df, 'TWE')
    )

    run_df = tatq.add_in_cancelled_runs(run_df, cancelled_runs)
    tatq.write_to_csv(run_df, tatq.audit_start, tatq.audit_end)

    # Load Jinja2 template
    # Add the charts, tables and issues into the template
    environment = Environment(loader=FileSystemLoader(
        ROOT_DIR.joinpath("templates")
    ))
    template = environment.get_template("audit_template.html")

    logger.info("Adding objects into HTML template")
    filename = (
        f"turnaround_times_{tatq.audit_start}_{tatq.audit_end}.html"
    )

    # Render all the things to go in the template
    content = template.render(
        period_audited=period_audited,
        datetime_now=dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        no_of_002_runs=no_of_002_runs,
        no_of_CEN_runs=CEN_runs,
        chart_1=CEN_fig,
        averages_1=CEN_stats,
        CEN_upload=CEN_upload_fig,
        runs_to_review_1=CEN_issues,
        CEN_fraction=CEN_frac,
        CEN_compliance=CEN_compl,
        no_of_MYE_runs=MYE_runs,
        chart_2=MYE_fig,
        averages_2=MYE_stats,
        MYE_upload=MYE_upload_fig,
        runs_to_review_2=MYE_issues,
        MYE_fraction=MYE_frac,
        MYE_compliance=MYE_compl,
        no_of_TSO500_runs=TSO_runs,
        chart_3=TSO_fig,
        averages_3=TSO_stats,
        TSO500_upload=TSO_upload_fig,
        runs_to_review_3=TSO_issues,
        TSO_fraction=TSO_frac,
        TSO_compliance=TSO_compl,
        no_of_TWE_runs=TWE_runs,
        chart_4=TWE_fig,
        averages_4=TWE_stats,
        TWE_upload=TWE_upload_fig,
        runs_to_review_4=TWE_issues,
        TWE_fraction=TWE_frac,
        TWE_compliance=TWE_compl,
        open_runs=open_runs_list,
        runs_no_002=runs_no_002_proj,
        ticket_typos=typo_tickets_table,
        typo_folders=typo_folders_table,
        cancelled_runs=cancelled_runs
    )

    logger.info("Writing final report file")
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)


if __name__ == "__main__":
    main()
