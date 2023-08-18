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
    """
    # Parse the CLI args
    args = parser.parse_args()
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
        audit_end_date_obj, five_days_before, five_days_after
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
    """
    # Get tokens etc from credentials file
    if os.path.exists(ROOT_DIR.joinpath("credentials.json")):
        with open(
            ROOT_DIR.joinpath("credentials.json"), "r", encoding='utf8'
        ) as json_file:
            credentials = json.load(json_file)

        dx_token = credentials.get('DX_TOKEN')
        jira_email = credentials.get('JIRA_EMAIL')
        jira_token = credentials.get('JIRA_TOKEN')
        staging_proj_id = credentials.get('STAGING_AREA_PROJ_ID')
        default_months = credentials.get('DEFAULT_MONTHS')
        tat_standard = int(credentials.get('TAT_STANDARD_DAYS'))

    else:
        # credentials file doesn't exist, assume credentials are in env
        dx_token = os.environ.get('DX_TOKEN')
        jira_email = os.environ.get('JIRA_EMAIL')
        jira_token = os.environ.get('JIRA_TOKEN')
        staging_proj_id = os.environ.get('STAGING_AREA_PROJ_ID')
        default_months = os.environ.get('DEFAULT_MONTHS')
        tat_standard = int(os.environ.get('TAT_STANDARD_DAYS'))

    if not all([
        dx_token, jira_email, jira_token, staging_proj_id, default_months,
        tat_standard
    ]):
        logger.error(
            "Required credentials could not be parsed from "
            "credentials.json or the env"
        )
        sys.exit()

    return dx_token, jira_email, jira_token, staging_proj_id, default_months, tat_standard


class QueryPlotFunctions:
    """Class for querying and plotting functions"""
    def __init__(self):
        (self.dx_token,
        self.jira_email,
        self.jira_token,
        self.staging_id,
        self.default_months,
        self.tat_standard) = load_credential_info()
        self.assay_types = ['TWE', 'CEN', 'MYE', 'TSO500', 'SNP']
        # Jira API things
        self.auth = HTTPBasicAuth(self.jira_email, self.jira_token)
        self.headers = {"Accept": "application/json"}
        (self.audit_start,
        self.audit_end,
        self.audit_start_obj,
        self.audit_end_obj,
        self.five_days_before_start,
        self.five_days_after) = determine_start_and_end_date(
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


    def get_002_projects_in_period(self, assay_type):
        """
        Gets all the 002 projects ending with the relevant assay type from
        DNAnexus that have been created between the audit period dates
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
            created_before=self.five_days_after,
            created_after=self.five_days_before_start,
            name=f"002*{assay_type}",
            name_mode="glob",
            describe={
                'fields': {
                    'id': True, 'name': True, 'created': True
                }
            }
        ))

        return assay_response


    def create_run_dict_add_assay(
        self, assay_type, assay_dx_response
    ):
        """
        Adds the run name, DX project ID and assay type for each run to dict
        Parameters
        ----------
        assay_type : str
            e.g. 'CEN'
        assay_dx_response : list
            list of dicts from get_002_projects_in_period() function
        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dictionary where key is run name and dict inside has relevant info
            for that run
        """
        run_dict = defaultdict(dict)

        # For each proj name, get run name by removing _002 and _assay name
        for project in assay_dx_response:
            run_name = (
                project['describe']['name'].removeprefix('002_').removesuffix(
                    f'_{assay_type}'
                )
            )
            # Check if the date of the run is within audit dates
            # because 002 project may have been made after actual run date
            # Don't capture 002_vaf_checks project for checking VAF
            run_name_date = run_name.split('_')[0]
            first_part_of_name = run_name.split('_')[1]
            if (
                run_name_date >= self.audit_start_obj.strftime('%y%m%d')
                and run_name_date <= self.audit_end_obj.strftime('%y%m%d')
                and first_part_of_name != "vaf"
            ):
                # Add in DX project ID and assay type to dict
                run_dict[run_name]['project_id'] = project['id']
                run_dict[run_name]['assay_type'] = assay_type

        return run_dict


    def get_staging_folders(self):
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


    def get_staging_processed_folders(self):
        """
        Gets the names of all of the folders in Staging Area /processed folder
        where files were uploaded if there was a dx-streaming
        upload bug (to be used for matching later)

        Returns
        -------
        processed_folders : list
            list of folder names with '/processed/' removed from beginning
        """
        processed_folder_names = list(
            dx.dxfile_functions.list_subfolders(
                project=self.staging_id,
                path='/processed',
                recurse=False
            )
        )
        # Remove the '/processed/' from the run folder name for later matching
        processed_folders = [
            name.removeprefix('/processed/') for name in processed_folder_names
        ]

        return processed_folders


    def determine_folder_to_search(self, run_name, assay_type):
        """
        Determine which folder in Staging Area 52 to search based on
        which assay

        Parameters
        ----------
        folder_name : str
            name of the run
        assay_type : str
            e.g. 'CEN'
        Returns
        -------
        file_names : str
            names of files to look for in the folder
        folder_to_search : str
            the folder to search in
        """
        if assay_type == 'SNP':
            # Files on MiSeq are manually uploaded
            # (not with dx-streaming-upload)
            # So for SNP runs the files are within the named folder
            folder_to_search = f'/{run_name}/'
            file_names = "*"
        else:
            # For other assay types
            # Files are within named folder but within sub-folder runs
            folder_to_search = f'/{run_name}/runs'
            file_names = "*.lane.all.log"

        return file_names, folder_to_search


    def find_files_or_log_in_folder(self, file_name, folder_to_search):
        """
        Find the files in the relevant Staging_Area52 folder

        Parameters
        ----------
        file_name : str
            the run name to look for as a folder
        folder_to_search : str
            the staging area folder to search in

        Returns
        -------
        log_file_info : list of dicts
            list response from dxpy of files from that folder
        """
        log_file_info = list(
            dx.find_data_objects(
                project=self.staging_id,
                folder=folder_to_search,
                name=file_name,
                name_mode='glob',
                classname='file',
                describe={
                    'fields': {
                        'name': True,
                        'created': True
                    }
                }
            )
        )

        return log_file_info


    def find_earliest_file_upload(self, files_in_folder):
        """
        Finds the time the earliest file was uploaded in the folder

        Parameters
        ----------
        files_in_folder : list
            list of dicts where each dict represents a file

        Returns
        -------
        upload_time : str
            timestamp the earliest file was uploaded
        """
        min_file_upload = min(
            data['describe']['created']
            for data in files_in_folder
        ) / 1000
        upload_time = time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(min_file_upload)
        )

        return upload_time


    def find_log_file_time(self, log_file_info):
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
        # For non-SNP run, get time in epoch the .lane.all.log file was created
        # convert to str
        log_time = log_file_info[0]['describe']['created'] / 1000
        upload_time = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(log_time)
        )

        return upload_time


    def get_file_info_processed(self, run_name, file_name):
        """
        Get the time the log file was uploaded for runs affected by
        dx-streaming-upload bug in the /processed/ folder

        Parameters
        ----------
        run_name : str
            name of the run

        Returns
        -------
        log_file_info : list
            list of files in the processed folder for that run
        """
        log_file_info = list(
            dx.find_data_objects(
                project=self.staging_id,
                folder=f'/processed/{run_name}/runs',
                name=file_name,
                name_mode='glob',
                classname='file',
                describe={
                    'fields': {
                        'name': True,
                        'created': True
                    }
                }
            )
        )

        return log_file_info


    def add_processed_upload_time(self, run_dict, assay_type):
        """
        Add the upload time for runs affected by dx-streaming-upload bug
        where real data is in the /processed/ folder in Staging Area

        Parameters
        ----------
        run_dict : collections.defaultdict(dict)
            dictionary where key is run name with nested dict containing
            relevant info
        assay_type : str
            assay type e.g. 'CEN'

        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dict with upload time added for runs affected by
            dx-streaming-upload bug
        """
        # Get the names of the folders in /processed/
        processed_folders = self.get_staging_processed_folders()
        # If SNP, get all files in the folder
        # otherwise get info on the log file
        if assay_type == "SNP":
            file_name = '*'
        else:
            file_name = '*.lane.all.log'
        # Compare folder names and run names
        for processed_folder in processed_folders:
            for run_name in run_dict.keys():
                # Get number of differences between run name and processed
                # folder, if less than 2 diffs, get file info in that folder
                distance = self.get_distance(processed_folder, run_name)
                if distance <= 2:
                    file_info = self.get_file_info_processed(
                        processed_folder, file_name
                    )
                    # If SNP, get earliest file upload time
                    # otherwise get time log file uploaded
                    # add upload time to dict
                    if file_info:
                        if assay_type == "SNP":
                            upload_time = self.find_earliest_file_upload(
                                file_info
                            )
                        else:
                            upload_time = self.find_log_file_time(
                                file_info
                            )

                        run_dict[run_name]['upload_time'] = upload_time

        return run_dict


    def add_upload_time(self, run_dict, assay_type):
        """
        Add upload time for runs not affected by upload bug

        Parameters
        ----------
        run_dict : collections.defaultdict(dict)
            dictionary where key is run name and dict inside with relevant info
        assay_type : str
            assay type e.g. 'CEN'

        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dict with each run as key and info as nested dict with upload
            time added
        typo_run_folders : list
            list of runs with typos in the 002 project name
        """
        typo_run_folders = []
        typo_folder_info = None
        staging_folders = self.get_staging_folders()

        # For each run, if it doesn't have upload time from /processed/ folder
        # aka wasn't affected by dx-streaming-upload bug
        for run_name in run_dict.keys():
            if not run_dict[run_name].get('upload_time'):
                for folder_name in staging_folders:
                    # Get no. of differences between each run name and staging
                    # folder name
                    distance = self.get_distance(folder_name, run_name)
                    # If match with less than 2 differences
                    # add nested key with the run folder name
                    if distance <= 2:
                        run_dict[run_name]['run_folder_name'] = folder_name
                        # Work out which folder to search in Staging Area
                        # search for either files or the log file in the folder
                        file_names, folder_to_search = (
                            self.determine_folder_to_search(
                                folder_name, assay_type
                            )
                        )
                        files_in_folder = self.find_files_or_log_in_folder(
                            file_names, folder_to_search
                        )
                        if distance > 0:
                            # If mismatches between names, create dict with info
                            typo_folder_info = {
                                'assay_type': assay_type,
                                'folder_name': folder_name,
                                'project_name_002': run_name
                            }
                            typo_run_folders.append(typo_folder_info)

                        if files_in_folder:
                        # If SNP run, files uploaded manually to staging area
                        # So get time first file was uploaded (sometimes
                        # people add random files later so can't use last file)
                            if assay_type == 'SNP':
                                upload_time = self.find_earliest_file_upload(
                                    files_in_folder
                                )
                                run_dict[run_name]['upload_time'] = upload_time
                            # If not SNP get time the log file was created
                            else:
                                upload_time = self.find_log_file_time(
                                    files_in_folder
                                )
                                run_dict[run_name]['upload_time'] = upload_time

        return run_dict, typo_run_folders


    def update_run_name(self, run_dict):
        """
        Update each main key in the dict representing the run name
        to the run name according to what is in the Staging Area
        so that the correct, non-typo name is used as base and plotted later

        Parameters
        ----------
        run_dict : collections.defaultdict(dict)
            dict with each run as key and info as nested dict

        Returns
        -------
        updated_dict : collections.defaultdict(dict)
            dict where the run name key is updated if it has a typo
        """
        # Create new dict, where the main key for the run name is taken from
        # the run folder, instead of being the run name extracted
        # from the 002 project name
        updated_dict = defaultdict(dict)
        for run_name in run_dict:
            # If the run folder name is found, update the main key in new dict
            if run_dict[run_name].get('run_folder_name'):
                folder_name = run_dict[run_name]['run_folder_name']
                updated_dict[folder_name] = run_dict[run_name]
            # Otherwise just use the original run name from the 002 proj
            else:
                updated_dict[run_name] = run_dict[run_name]

        return updated_dict


    def find_jobs_in_002_project(self, project_id):
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
                    'id': True,
                    'name': True,
                    'created': True
                }
            }
        ))

        return jobs


    def find_staging_demultiplex_jobs(self):
        """
        Find demultiplexing jobs in staging area

        Returns
        -------
        demux_jobs : list
            list of dicts with info about jobs in staging area
        """
        # Find the jobs in staging area in last X weeks that are done
        demux_jobs = list(dx.search.find_jobs(
            project=self.staging_id,
            created_after=self.five_days_before_start,
            name='BCL|bcl',
            name_mode='regexp',
            describe={
                'fields': {
                    'id': True,
                    'name': True,
                    'executableName': True,
                    'created': True,
                    'folder': True,
                    'runInput': True
                }
            }
        ))

        return demux_jobs


    def get_demultiplex_start_time(self, demux_jobs):
        """
        Get the demultiplexing start times for non-TSO500 and non-SNP runs

        Parameters
        ----------
        demux_jobs : list
            list of dicts with info about jobs in staging area

        Returns
        -------
        demux_job_dict : collections.defaultdict(list)
            dict with run name as key and all the matching demux jobs as list
        """
        demux_job_dict = defaultdict(list)
        # for each demultiplexing job in Staging Area
        # get the time it started and the runInput info
        for demux_job in demux_jobs:
            demux_start = demux_job['describe']['created'] / 1000
            sentinel = demux_job['describe']['runInput']
            input_sentinel = sentinel.get('upload_sentinel_record')
            # if there was a sentinel input, its info is in the $dnanexus_link
            # key which can be a string or a nested dict
            if input_sentinel:
                sentinel_info = input_sentinel['$dnanexus_link']
                if isinstance(sentinel_info, str):
                    sentinel_id = sentinel_info
                else:
                    sentinel_id = sentinel_info['id']
                # get the name of the sentinel file input and split it to get
                # the run name
                # append the start times to the list with the run name
                # from the sentinel file as key
                run_name_from_sentinel = dx.describe(
                    sentinel_id
                )['name'].split('.')[1]
                demux_job_dict[run_name_from_sentinel].append(demux_start)

        return demux_job_dict


    def add_demultiplex_start_time(self, demux_job_dict, run_dict):
        """
        Add the time that demultiplexing started to the run dictionary

        Parameters
        ----------
        demux_job_dict : collections.defaultdict(list)
            dict with run name as key and all the matching demux jobs as list
        run_dict : collections.defaultdict(dict)
            dict with each run and relevant audit info

        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dict with each run and relevant audit info with demux time added
        """
        for run_name, start_times in demux_job_dict.items():
            if run_name in run_dict:
                # If the stored assay type is not TSO500 or SNP
                # because these are not demultiplexed by us
                if run_dict[run_name]['assay_type'] not in ['TSO500', 'SNP']:
                    # Get the upload time for the run we stored earlier
                    upload_time = run_dict[run_name].get(
                        'upload_time'
                    )
                    # If we have an upload time, get time the first demux
                    # job started
                    if upload_time:
                        first_job_start = time.strftime(
                            '%Y-%m-%d %H:%M:%S', time.localtime(min(
                                start_times
                            ))
                        )
                        # If the upload time is before the first demux job
                        # add to our dict
                        if upload_time < first_job_start:
                            run_dict[run_name]['first_job'] = first_job_start
                        # otherwise get all the demux start times for that run
                        # find the minimum of those after the upload time
                        # since e.g. sometimes people start demux jobs before
                        # the sentinel file is actually uploaded
                        else:
                            string_start_times = [
                                time.strftime(
                                    '%Y-%m-%d %H:%M:%S', time.localtime(x)
                                ) for x in start_times
                            ]
                            start_times = [
                                x for x in string_start_times
                                if x > upload_time
                            ]
                            # if there is a start time left after the upload
                            # time, add the first job start time of these
                            # to our dict
                            if start_times:
                                first_job_start = time.strftime(
                                    '%Y-%m-%d %H:%M:%S', time.localtime(min(
                                        start_times
                                    ))
                                )
                                run_dict[run_name]['first_job'] = first_job_start

        return run_dict


    def find_earliest_002_job(self, jobs):
        """
        Finds and returns the earliest job created time (as str) from a list of
        job dicts in a 002 project

        Parameters
        ----------
        jobs : list
            list of dicts

        Returns
        -------
        first_job : str
            timestamp of first job in that project
        """
        if jobs:
            min_job = (
                min(data['describe']['created'] for data in jobs) / 1000
            )
            first_job = (
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(min_job))
            )

        else:
            first_job = None

        return first_job


    def add_earliest_002_job(self, run_dict, assay_type):
        """
        Adds the time the earliest job was run in the relevant 002 project for
        that run if TSO500 or SNP, as demultiplexing on instrument
        Parameters
        ----------
        run_dict :  collections.defaultdict(dict)
            dictionary where key is run name and dict inside with relevant info
            for that run
        assay_type : str
            the assay type e.g. 'CEN'
        Returns
        -------
        run_dict : collections.defaultdict(dict)
            dictionary where key is run name with dict inside and earliest_002
            job added
        """
        if assay_type in ['TSO500', 'SNP']:
            # Use proj ID for 002 project and get all the jobs in the proj
            for run in run_dict:
                run_project_id = run_dict[run]['project_id']
                jobs = self.find_jobs_in_002_project(run_project_id)
                first_job = self.find_earliest_002_job(jobs)

                # For key with relevant run name
                # Add first_job key with time of earliest job in datetime
                # format
                if first_job:
                    run_dict[run]['first_job'] = first_job

        return run_dict


    def find_multiqc_jobs(self, project_id):
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
                    'id': True,
                    'project': True,
                    'name': True,
                    'executableName': True,
                    'stoppedRunning': True
                }
            }
        ))

        return multi_qc_jobs


    def get_relevant_multiqc_job(self, multi_qc_jobs, jira_resolved_timestamp):
        """
        Gets the time the successful MultiQC job completed before the
        run was released

        Parameters
        ----------
        multi_qc_jobs : list
            list of dicts of MultiQC executions in that project
        jira_resolved_timestamp : str
            timestamp that the Jira ticket was resolved
        Returns
        -------
        multiQC_completed : str or None
            timestamp the relevant multiQC job completed or None if no MQC job
        last_multiqc : str or None
            the timestamp of the last multiQC job which successfully
            completed or None if there are no MQC jobs
        """
        multiQC_completed = last_multiqc = None
        multi_qc_jobs_before_resolution = []
        # Convert time the Jira ticket was resolved from epoch to timestamp
        jira_res_epoch = time.mktime(
            time.strptime(jira_resolved_timestamp, "%Y-%m-%d %H:%M:%S")
        )
        # If there are MultiQC jobs found, get the time the last one finished
        if multi_qc_jobs:
            last_multiqc_job = (
                max(
                    data['describe']['stoppedRunning']
                    for data in multi_qc_jobs
                ) / 1000
            )

            last_multiqc = (
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(
                    last_multiqc_job
                ))
            )
            # For each job, get time it stopped running in epoch
            for multi_qc_job in multi_qc_jobs:
                finished_running = (
                    multi_qc_job['describe']['stoppedRunning'] / 1000
                )
                # If job finished before the Jira ticket was resolved
                # Add to list
                if finished_running <= jira_res_epoch:
                    multi_qc_jobs_before_resolution.append(finished_running)
            # If any jobs are before Jira ticket resolved, find time
            # last MultiQC job finished of these and convert to timestamp
            if multi_qc_jobs_before_resolution:
                multiQC_fin = max(multi_qc_jobs_before_resolution)
                multiQC_completed = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(multiQC_fin)
                )

        return multiQC_completed, last_multiqc


    def get_last_multiQC_job(self, multiQC_jobs):
        """
        Get the time the last MultiQC job finished for unreleased runs

        Parameters
        ----------
        multiQC_jobs : list
            list of dicts containing info about the MultiQC jobs

        Returns
        -------
        multiQC_completed : str or None
            timestamp the last MultiQC job finished (or None if no MultiQC
            jobs)
        """
        multiQC_completed = None

        if multiQC_jobs:
            multi_fin = max(
                multiQC_job['describe']['stoppedRunning'] / 1000
                for multiQC_job in multiQC_jobs
            )
            multiQC_completed = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(multi_fin)
            )

        return multiQC_completed


    def add_successful_multiqc_time(self, all_assays_dict):
        """
        Adds the time the multiQC job finished in the relevant 002 proj for run

        Parameters
        ----------
        all_assays_dict : collections.defaultdict(dict)
            dictionary where key is run name and dict inside with relevant info
            for that run

        Returns
        -------
        all_assays_dict : collections.defaultdict(dict)
            dictionary where key is run name with dict inside and
            processing_finished key and val added (and last_multiQC_finished)
        """
        # For each run from the dict
        # if not CEN or TWE, where last job is generating workbooks
        # get time the Jira ticket was resolved (if resolved), and the
        # DX project ID for the 002 project for that run
        for run in all_assays_dict:
            if all_assays_dict[run]['assay_type'] not in ['CEN', 'TWE']:
                jira_resolved = all_assays_dict[run].get('jira_resolved')
                project_id = all_assays_dict[run].get('project_id')
                # find executions that include the name *MultiQC*
                # as both eggd_MultiQC and MultiQC_v1.1.2 used
                multi_qc_jobs = self.find_multiqc_jobs(project_id)
                # if all samples are released (Jira resolved)
                # get the last multiqc job before that and add to dict
                # along with any MultiQC jobs after all samples released
                if jira_resolved:
                    multiQC_completed, last_multiQC = self.get_relevant_multiqc_job(
                        multi_qc_jobs, jira_resolved
                    )
                    if multiQC_completed:
                        all_assays_dict[run]['processing_finished'] = (
                            multiQC_completed
                        )
                    if last_multiQC:
                        all_assays_dict[run]['last_multiQC_finished'] = (
                            last_multiQC
                        )
                # If run not resolved, just get the last MultiQC job
                # add to dict
                else:
                    multiQC_completed = self.get_last_multiQC_job(multi_qc_jobs)
                    if multiQC_completed:
                        all_assays_dict[run]['processing_finished'] = (
                            multiQC_completed
                        )

        return all_assays_dict


    def find_excel_jobs(self, project_id):
        """
        Find jobs for eggd_generate_variant_workbook and eggd_vcf2xls_nirvana
        in the 002 project

        Parameters
        ----------
        project_id : str
            str for the project's DX ID

        Returns
        -------
        excel_jobs : list
            list of dicts containing info about the excel jobs
        """
        excel_jobs = list(dx.search.find_jobs(
            project=project_id,
            state='done',
            name='vcf2xls_nirvana|variant',
            name_mode='regexp',
            describe={
                'fields': {
                    'id': True,
                    'name': True,
                    'stoppedRunning': True,
                }
            }
        ))

        return excel_jobs


    def get_relevant_excel_job(self, excel_jobs, jira_resolved_timestamp):
        """
        For CEN and TWE runs which are released, get the time that the
        last generate excel job finished before the Jira ticket was resolved
        (to prevent including reanalysis jobs)

        Parameters
        ----------
        excel_jobs : list
            list of dicts containing info about the excel jobs
        jira_resolved_timestamp : str
            timestamp that the Jira ticket for the run was resolved

        Returns
        -------
        excel_completed : str or None
            timestamp the last create excel job finished (or None if no excel
            jobs)
        """
        excel_completed = None
        excel_jobs_before_resolution = []
        # Convert time the Jira ticket was resolved from epoch to timestamp
        jira_res_epoch = time.mktime(
            time.strptime(jira_resolved_timestamp, "%Y-%m-%d %H:%M:%S")
        )

        if excel_jobs:
            for excel_job in excel_jobs:
                # Get the time stopped running in epoch
                finished_running = (
                    excel_job['describe']['stoppedRunning'] / 1000
                )
                # If job finished before the Jira ticket was resolved
                # Add to list
                if finished_running <= jira_res_epoch:
                    excel_jobs_before_resolution.append(finished_running)
            # If any jobs are before Jira ticket resolved, find time
            # last Excel job finished and convert to timestamp
            if excel_jobs_before_resolution:
                excel_fin = max(excel_jobs_before_resolution)
                excel_completed = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(excel_fin)
                )

        return excel_completed


    def get_last_excel_job(self, excel_jobs):
        """
        Get the time the last Excel job finished for unreleased runs

        Parameters
        ----------
        excel_jobs : list
            list of dicts containing info about the excel jobs

        Returns
        -------
        excel_completed : str or None
            timestamp the last create excel job finished (or None if no excel
            jobs)
        """
        excel_completed = None

        if excel_jobs:
            # Get time last job finished in epoch
            excel_fin = max(
                excel_job['describe']['stoppedRunning'] / 1000
                for excel_job in excel_jobs
            )
            # Convert to timestamp
            excel_completed = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(excel_fin)
            )

        return excel_completed


    def add_excel_finished_time(self, all_assays_dict):
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
        """
        for run in all_assays_dict:
            # If it's a CEN or TWE run
            # Try and get the time Jira ticket was resolved and proj ID
            if all_assays_dict[run]['assay_type'] in ['CEN', 'TWE']:
                jira_resolved = all_assays_dict[run].get('jira_resolved')
                project_id = all_assays_dict[run].get('project_id')
                # Find all the Excel jobs run in that project
                excel_jobs = self.find_excel_jobs(project_id)
                # If the Jira ticket is resolved (All samples released)
                # Get the time final Excel job finished before release
                if jira_resolved:
                    excel_finished = self.get_relevant_excel_job(
                        excel_jobs, jira_resolved
                    )
                    # If found the relevant Excel job, add to dict
                    if excel_finished:
                        all_assays_dict[run]['processing_finished'] = (
                            excel_finished
                        )
                else:
                    # Jira ticket not resolved, just get last Excel job run
                    # If exists, add the timestamp to dict
                    excel_finished = self.get_last_excel_job(excel_jobs)
                    if excel_finished:
                        all_assays_dict[run]['processing_finished'] = (
                            excel_finished
                        )

        return all_assays_dict


    def create_info_dict(self, assay_type):
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
        assay_response = self.get_002_projects_in_period(assay_type)
        assay_run_dict = self.create_run_dict_add_assay(
            assay_type, assay_response
        )
        # Add upload time for runs affected by dx-streaming-upload bug
        assay_run_dict = self.add_processed_upload_time(
            assay_run_dict, assay_type
        )
        # Add upload time for all other runs
        assay_run_dict, typo_run_folders = self.add_upload_time(
            assay_run_dict, assay_type
        )
        # Update the run name in dict according to Staging Area folders
        assay_run_dict = self.update_run_name(assay_run_dict)

        # Add earliest 002 for TSO500 and SNP runs
        assay_run_dict = self.add_earliest_002_job(assay_run_dict, assay_type)

        staging_area_jobs = self.find_staging_demultiplex_jobs()
        # Add demultiplexing time for non-TSO500 and non-SNP runs
        demux_job_dict = self.get_demultiplex_start_time(
            staging_area_jobs
        )
        assay_run_dict = self.add_demultiplex_start_time(
            demux_job_dict, assay_run_dict
        )

        return assay_run_dict, typo_run_folders


    def get_jira_info(self, queue_id):
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


    def get_distance(self, string1, string2):
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


    def get_closest_match_in_dict(self, ticket_name, my_dict):
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
        for run_name in my_dict.keys():
            # Get the distance between the names
            # If 1 or 0 get the closest key in the dict
            distance = self.get_distance(ticket_name, run_name)
            if distance <= 2:
                closest_key_in_dict = run_name
                if distance > 0:
                    typo_ticket_info = {
                        'assay_type': my_dict[closest_key_in_dict][
                            'assay_type'
                        ],
                        'run_name': closest_key_in_dict,
                        'jira_ticket_name': ticket_name
                    }

        return closest_key_in_dict, typo_ticket_info


    def query_specific_ticket(self, ticket_id):
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
            f"https://cuhbioinformatics.atlassian.net/rest/servicedeskapi/"
            f"request/{ticket_id}?expand=changelog"
        )

        ticket_info = requests.request(
            "GET",
            url=url,
            headers=self.headers,
            auth=self.auth
        )

        ticket_data = json.loads(ticket_info.text)

        return ticket_data


    def get_status_change_time(self, ticket_data):
        """
        Queries the individual Jira ticket to get the correct resolution time
        since when querying all tickets in a queue it is not correct
        Parameters
        ----------
        ticket_data : dict
            dict of info about the ticket
        Returns
        -------
        res_time_str :  str
            the time of Jira resolution as e.g. "2022-08-10 12:54"
        """
        status = ticket_data['currentStatus']['status']
        res_time_str = None
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


    def add_jira_info_closed_issues(self, all_assays_dict, closed_response):
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
        not_released_statuses = [
            "Data cannot be processed", "Data cannot be released",
            "Data not received"
        ]
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
            # Try and get the key which stores the assay type
            # If 'SNP Genotyping' change to 'SNP'
            # to check against our list of assays
            # Otherwise if key does not exist, set assay type to Unknown
            assay_type_field = issue.get('fields').get('customfield_10070')
            if assay_type_field:
                assay_type_val = assay_type_field[0].get('value')
                assay_type = assay_type_val.replace(' Genotyping', '')
            else:
                assay_type = 'Unknown'

            # If this matches run name in our dict (or is off by 2 chars)
            # Get relevant run name key in dict + return any with mismatches
            closest_dict_key, typo_ticket_info = (
                self.get_closest_match_in_dict(ticket_name, all_assays_dict)
            )

            if typo_ticket_info:
                typo_tickets.append(typo_ticket_info)

            if closest_dict_key:
                # Jira resolution time is incorrect so query the ticket
                # For accurate info
                ticket_data = self.query_specific_ticket(ticket_id)
                all_assays_dict[closest_dict_key]['jira_status'] = jira_status
                if jira_status in not_released_statuses:
                    cancelled_list.append({
                        'run_name': ticket_name,
                        'assay_type': assay_type,
                        'date_jira_ticket_created': date_time_created,
                        'jira_status': jira_status
                    })
                else:
                    # If resolved add time of resolution in datetime
                    res_time_str = self.get_status_change_time(ticket_data)
                    if res_time_str:
                        all_assays_dict[closest_dict_key]['jira_resolved'] = (
                            res_time_str
                        )
            else:
                # No key in our dict found (no 002 project exists for it)
                # Get relevant info
                # Check it's within audit time + assays we are interested in
                if (
                    assay_type in self.assay_types
                    and start_date >= self.audit_start_obj.strftime('%Y-%m-%d')
                    and start_date <= self.audit_end_obj.strftime('%Y-%m-%d')
                ):
                    # If reports have been released for the run
                    # Get the time ticket changed to 'All samples released'
                    if jira_status == 'All samples released':
                        ticket_data = self.query_specific_ticket(ticket_id)
                        res_time_str = self.get_status_change_time(ticket_data)
                        res_time = dt.datetime.strptime(
                            res_time_str, '%Y-%m-%d %H:%M:%S'
                        )

                        # Get est TAT in days as float
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
                        if jira_status in not_released_statuses:
                            cancelled_list.append({
                                'run_name': ticket_name,
                                'assay_type': assay_type,
                                'date_jira_ticket_created': date_time_created,
                                'jira_status': jira_status
                            })

        return all_assays_dict, typo_tickets, runs_no_002_proj, cancelled_list


    def add_jira_info_open_issues(self, all_assays_dict, open_jira_response):
        """
        Adds the Jira ticket current status for those open and the current time
        to the all_assays_dict
        Parameters
        ----------
        all_assays_dict :  collections.defaultdict(dict)
            dictionary where all the assay types are merged. Each key is run
            name with all the relevant audit info
        jira_response : dict
            API response from Jira
        Returns
        -------
        all_assays_dict :  collections.defaultdict(dict)
            dict with the current Jira status and current time added
        open_runs_list : list
            list of dicts for open runs that don't have a 002 project yet
        typo_tickets : list
            list of dicts with info on proj names which differ to tickets
        """
        open_runs_list = []
        typo_tickets = []
        # Summary of the ticket should be the run name
        for issue in open_jira_response:
            ticket_name = issue['fields']['summary']
            jira_status = issue['fields']['status']['name']

            # If this matches run name in our dict (or is off by 1 char)
            # Get relevant run name key in dict
            closest_dict_key, typo_ticket_info = (
                self.get_closest_match_in_dict(ticket_name, all_assays_dict)
            )
            if typo_ticket_info:
                typo_tickets.append(typo_ticket_info)
            if closest_dict_key:
                all_assays_dict[closest_dict_key]['jira_status'] = (
                    jira_status
                )

            # If not in our dict (i.e. it's a new run with no 002 proj yet)
            else:
                start_time = issue['fields']['created'].split("T")[0]
                assay_type = issue['fields']['customfield_10070'][0]['value']
                run_type = assay_type.replace(' Genotyping', '')
                if (
                    start_time >= self.audit_start_obj.strftime('%Y-%m-%d')
                    and start_time <= self.audit_end_obj.strftime('%Y-%m-%d')
                    and run_type in self.assay_types
                ):
                    open_runs_list.append({
                        'run_name': ticket_name,
                        'assay_type': run_type,
                        'date_jira_ticket_created': start_time,
                        'current_status': jira_status
                    })

        return all_assays_dict, open_runs_list, typo_tickets


    def create_all_assays_df(self, all_assays_dict):
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

        # Check dataframe is not empty, if it is exit
        if all_assays_df.empty:
            logger.error("No runs were found within the audit period")
            sys.exit(1)

        # Reorder columns
        all_assays_df = all_assays_df[[
            'assay_type', 'run_name', 'upload_time', 'first_job',
            'processing_finished', 'last_multiQC_finished',  'jira_status',
            'jira_resolved'
        ]]

        cols_to_convert = [
            'upload_time', 'first_job', 'processing_finished',
            'last_multiQC_finished', 'jira_resolved'
        ]

        # Convert cols to pandas datetime type
        all_assays_df[cols_to_convert] = all_assays_df[cols_to_convert].apply(
            pd.to_datetime, format='%Y-%m-%d %H:%M:%S'
        )

        return all_assays_df


    def add_calculation_columns(self, all_assays_df):
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
        # Add new column for time between log file and first job
        all_assays_df['upload_to_first_job'] = (
            (all_assays_df['first_job'] - all_assays_df['upload_time'])
            / np.timedelta64(1, 'D')
        )

        # Add new column for bioinfx processing time from first job
        # To the time that the last successful MultiQC or excel job finished
        all_assays_df['processing_time'] = (
            (
                all_assays_df['processing_finished']
                - all_assays_df['first_job']
            )
            / np.timedelta64(1, 'D')
        )

        # Add new column for time from MultiQC end or excel job to Jira res
        all_assays_df['processing_end_to_release'] = (
            (
                all_assays_df['jira_resolved']
                - all_assays_df['processing_finished']
            ).where(
                all_assays_df['jira_status'] == "All samples released"
            ) / np.timedelta64(1, 'D')
        )

        # Add new column for time from log file creation to Jira resolution
        all_assays_df['upload_to_release'] = (
            (
                all_assays_df['jira_resolved'] - all_assays_df['upload_time']
            ).where(
                (all_assays_df['jira_status'] == "All samples released")
                & (all_assays_df['upload_to_first_job'] >= 0)
                & (all_assays_df['processing_time'] >= 0)
                & (all_assays_df['processing_end_to_release'] >= 0)
            ) / np.timedelta64(1, 'D')
        )

        # Add the time since MultiQC to now for open tickets with urgents
        # released
        all_assays_df['urgents_time'] = (
            (self.pd_current_time - all_assays_df['processing_finished']).where(
                all_assays_df['jira_status'] == 'Urgent samples released'
            ) / np.timedelta64(1, 'D')
        )

        # Get the time from the most recent processing step by forward filling
        # from columns until 'muliQC_finished' column
        # If this results in a string, convert to NA
        all_assays_df['last_processing_step'] = pd.to_datetime(
            all_assays_df.ffill(axis=1).iloc[:,4], errors='coerce'
        )
        # Add the time since the last processing step which exists to current
        # time for open tickets that are on hold
        all_assays_df['on_hold_time'] = (
            (
                self.pd_current_time - all_assays_df['last_processing_step']
            ).where(
                all_assays_df['jira_status'] == 'On hold'
            ) / np.timedelta64(1, 'D')
        )

        # Add new column for time from last MultiQC end to Jira resolution
        all_assays_df['final_multiqc_to_release'] = (
            (
                all_assays_df['jira_resolved']
                - all_assays_df['last_multiQC_finished']
            ).where(
                all_assays_df['jira_status'] == "All samples released"
            ) / np.timedelta64(1, 'D')
        )

        return all_assays_df


    def extract_assay_df(self, all_assays_df, assay_type):
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


    def create_TAT_fig(self, assay_df, assay_type):
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
        # Only plot uncancelled runs
        cancelled_runs = [
            'Data not received', 'Data cannot be processed',
            'Data cannot be released'
        ]
        assay_df = assay_df[~assay_df.jira_status.isin(cancelled_runs)]
        if len(assay_df):
            # Add trace for Log file to first job
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=assay_df["run_name"],
                    y=assay_df["upload_to_first_job"],
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
                    name="Processing end to all samples released",
                    legendrank=2,
                    text=round(assay_df['upload_to_release'], 1)
                )
            )

            if "Urgent samples released" in assay_df.jira_status.values:
                fig.add_trace(
                    go.Bar(
                        x=assay_df["run_name"],
                        y=assay_df["urgents_time"],
                        name="Processing end to now - Urgent samples released",
                        marker_color='#FFA15A'
                    )
                )

            if "On hold" in assay_df.jira_status.values:
                fig.add_trace(
                    go.Bar(
                        x=assay_df["run_name"],
                        y=assay_df["on_hold_time"],
                        name="Last processing step to now - On hold",
                        marker_color='#FECB52'
                    )
                )

            fig.add_hline(y=self.tat_standard, line_dash="dash")

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
                barmode='relative',
                title={
                    'text': f"{assay_type} Turnaround Times "
                            f"{self.audit_start} - {self.audit_end}",
                    'xanchor': 'center',
                    'x': 0.5,
                    'font_size': 20
                },
                xaxis_title="Run name",
                yaxis_title="Number of days",
                width=1100,
                height=700,
                font_family='Helvetica',
                legend_traceorder="reversed"
            )

        # If empty show empty plot with message
        else:
            fig = go.Figure()
            fig.update_layout(
                font_family='Helvetica',
                xaxis =  { "visible": False },
                yaxis = { "visible": False },
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

        # If days between log file and first job is negative flag
        # unless it's a cancelled run
        manual_review_dict['first_job_before_log'] = list(
            assay_df.loc[
                assay_df['upload_to_first_job'] < 0
                & ~assay_df['jira_status'].isin([
                    'Data cannot be processed',
                    'Data cannot be released',
                    'Data not received'
                ])
            ]['run_name']
        )

        # If days between final multiQC + release is negative flag
        # unless it's a cancelled run
        manual_review_dict['reports_before_multiqc'] = list(
            assay_df.loc[
                (assay_df['final_multiqc_to_release'] < 0)
                & ~assay_df['jira_status'].isin([
                    'Data cannot be processed',
                    'Data cannot be released',
                    'Data not received'
                ])
            ]['run_name']
        )

        # If related log file was never found flag
        # unless it's a cancelled run
        manual_review_dict['no_log_file'] = list(
            assay_df.loc[
                (assay_df['upload_time'].isna())
                & ~assay_df['jira_status'].isin([
                    'Data cannot be processed',
                    'Data cannot be released',
                    'Data not received'
                ])
            ]['run_name']
        )

        # If no 002 job or demux job was found flag
        # unless it's a cancelled run
        manual_review_dict['no_first_job_found'] = list(
            assay_df.loc[
                (assay_df['first_job'].isna())
                & ~assay_df['jira_status'].isin([
                    'Data cannot be processed',
                    'Data cannot be released',
                    'Data not received'
                ])
            ]['run_name']
        )

        # If no MultiQC or excel job was found flag
        # unless it's a cancelled run
        manual_review_dict['no_multiqc_or_excel_found'] = list(
            assay_df.loc[
                (assay_df['processing_finished'].isna())
                    & ~assay_df['jira_status'].isin([
                    'Data cannot be processed',
                    'Data cannot be released',
                    'Data not received'
                ])
            ]['run_name']
        )

        # If there are runs to be flagged in dict, pass
        # Or if all vals empty pass empty dict so can check in Jinja2 if defined
        # To decide whether to show 'Runs to be manually reviewed' text
        if any(manual_review_dict.values()):
            pass
        else:
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
        assay_fig = self.create_TAT_fig(assay_df, assay_type)
        upload_day_fig = self.create_upload_day_fig(assay_df, assay_type)
        assay_no_of_002_runs = assay_df.shape[0]

        return (
            assay_stats, assay_issues, assay_fig, upload_day_fig,
            assay_no_of_002_runs, assay_fraction, assay_percentage
        )


    def create_ticket_typo_df(self, closed_typo_tickets, open_typo_tickets):
        """
        Create table of typos between the Jira ticket and run name

        Parameters
        ----------
        closed_typo_tickets : list
            list of dicts of Jira tickets with typos from closed sequencing
            runs queue
        open_typo_tickets : list
            list of dicts of Jira tickets with typos from open sequencing runs
            queue

        Returns
        -------
        ticket_typos_html : str
            dataframe of runs with typos in the Jira ticket as html string
        """
        ticket_typos_html = None
        all_ticket_typos = closed_typo_tickets + open_typo_tickets

        if all_ticket_typos:
            ticket_typos_df = pd.DataFrame(all_ticket_typos)
            ticket_typos_df.rename(
                {
                    'jira_ticket_name': 'Jira ticket name',
                    'run_name': 'Run name',
                    'assay_type': 'Assay type'
                }, axis=1, inplace=True
            )

            ticket_typos_df.sort_values('Assay type', inplace=True)
            ticket_typos_html = ticket_typos_df.to_html(
                index=False,
                classes='table table-striped"',
                justify='left'
            )

        return ticket_typos_html


    def create_project_typo_df(self, typo_folders_list):
        """
        Create table of typos between Staging Area run folder name and
        the run name extracted from the 002 project name

        Parameters
        ----------
        typo_folders_list : list
            list of dicts representing instances of typos between run folder
            name and 002 project name

        Returns
        -------
        typo_folders_html : str
            dataframe of 002 project names with typos as html string
        """
        typo_folders_html = None
        if typo_folders_list:
            typo_folders_df = pd.DataFrame(typo_folders_list)
            typo_folders_df.rename(
                {
                    'assay_type':'Assay type',
                    'folder_name': 'Run name',
                    'project_name_002': '002 project name'
                }, axis=1, inplace=True
            )
            typo_folders_df.sort_values('Assay type', inplace=True)

            typo_folders_html = typo_folders_df.to_html(
                index=False,
                classes='table table-striped"',
                justify='left'
            )

        return typo_folders_html


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
        custom_dict = {'CEN': 0, 'MYE': 1, 'TSO500': 2, 'TWE': 3, 'SNP': 4}
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
        number_of_relevant_runs = assay_df['upload_to_release'].count()
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
    all_assays_dict = {}
    typo_folders_list = []
    for assay_type in tatq.assay_types:
        assay_run_dict, typo_run_folders = tatq.create_info_dict(assay_type)
        all_assays_dict.update(assay_run_dict)
        if typo_run_folders:
            typo_folders_list = typo_folders_list + typo_run_folders
    typo_folders_table = tatq.create_project_typo_df(typo_folders_list)
    no_of_002_runs = len(all_assays_dict)
    logger.info("Getting + adding JIRA ticket info for closed seq runs")
    closed_runs_response = tatq.get_jira_info(35)
    all_assays_dict, closed_typo_tickets, runs_no_002_proj, cancelled_runs = (
        tatq.add_jira_info_closed_issues(
            all_assays_dict, closed_runs_response
        )
    )

    logger.info("Getting + adding JIRA ticket info for open seq runs")
    open_jira_response = tatq.get_jira_info(34)
    all_assays_dict, open_runs_list, open_typo_tickets = (
        tatq.add_jira_info_open_issues(
            all_assays_dict, open_jira_response
        )
    )
    all_assays_df = tatq.add_excel_finished_time(all_assays_dict)
    all_assays_df = tatq.add_successful_multiqc_time(all_assays_dict)
    all_typos_table = tatq.create_ticket_typo_df(
        closed_typo_tickets, open_typo_tickets
    )
    logger.info("Creating df for all assays")
    all_assays_df = tatq.create_all_assays_df(all_assays_dict)

    logger.info("Adding calculation columns")
    all_assays_df = tatq.add_calculation_columns(all_assays_df)

    logger.info("Generating objects for each assay")
    CEN_stats, CEN_issues, CEN_fig, CEN_upload_fig, CEN_runs, CEN_frac, CEN_compl = (
        tatq.create_assay_objects(all_assays_df, 'CEN')
    )
    MYE_stats, MYE_issues, MYE_fig, MYE_upload_fig, MYE_runs, MYE_frac, MYE_compl = (
        tatq.create_assay_objects(all_assays_df, 'MYE')
    )
    TSO_stats, TSO_issues, TSO_fig, TSO_upload_fig, TSO_runs, TSO_frac, TSO_compl = (
        tatq.create_assay_objects(all_assays_df, 'TSO500')
    )
    TWE_stats, TWE_issues, TWE_fig, TWE_upload_fig, TWE_runs, TWE_frac, TWE_compl = (
        tatq.create_assay_objects(all_assays_df, 'TWE')
    )
    SNP_stats, SNP_issues, SNP_fig, SNP_upload_fig, SNP_runs, SNP_frac, SNP_compl = (
        tatq.create_assay_objects(all_assays_df, 'SNP')
    )

    all_assays_df = tatq.add_in_cancelled_runs(all_assays_df, cancelled_runs)
    tatq.write_to_csv(all_assays_df, tatq.audit_start, tatq.audit_end)

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
        no_of_SNP_runs=SNP_runs,
        chart_5=SNP_fig,
        averages_5=SNP_stats,
        SNP_upload=SNP_upload_fig,
        runs_to_review_5=SNP_issues,
        SNP_fraction=SNP_frac,
        SNP_compliance=SNP_compl,
        open_runs=open_runs_list,
        runs_no_002=runs_no_002_proj,
        ticket_typos=all_typos_table,
        typo_folders=typo_folders_table,
        cancelled_runs=cancelled_runs
    )

    logger.info("Writing final report file")
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)


if __name__ == "__main__":
    main()
