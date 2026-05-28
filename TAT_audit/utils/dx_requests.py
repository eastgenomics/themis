import dxpy as dx
import logging
import Levenshtein
import re
import sys
import time

from collections import defaultdict
from pathlib import Path


ROOT_DIR = Path(__file__).absolute().parents[1]

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


class DXFunctions():
    """
    Functions for searching in DNAnexus
    """

    def login(self, dx_token) -> None:
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
            "auth_token": dx_token
        }

        dx.set_security_context(DX_SECURITY_CONTEXT)

        try:
            dx.api.system_whoami()
            logger.info("DNAnexus login successful")
        except Exception as err:
            logger.error("Error logging in to DNAnexus")
            sys.exit(1)

    def get_002_projects_within_buffer_period(
        self, assay_types, five_days_after, five_days_before_start
    ):
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
        assay_conditional = "|".join(x for x in assay_types)

        # Search projs in buffer period starting with 002 that end in
        # relevant assay types
        projects_dx_response = list(dx.find_projects(
            level='VIEW',
            created_before=five_days_after,
            created_after=five_days_before_start,
            name=f"^002.*({assay_conditional})$",
            name_mode="regexp",
            describe={
                'fields': {
                    'id': True,
                    'name': True
                }
            }
        ))
        logger.debug(projects_dx_response)
        return projects_dx_response

    def get_staging_folders(self, staging_id):
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
                project=staging_id,
                path='/',
                recurse=False
            )
        )

        # Remove the '/' from the run folder name for later matching
        staging_folders = [
            name.removeprefix('/') for name in staging_folder_names
        ]

        return staging_folders

    def find_log_file_in_folder(self, run_name, staging_id):
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
                project=staging_id,
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

    def find_conductor_jobs(
        self, staging_id, five_days_after, five_days_before_start
    ):
        """
        Find eggd_conductor_jobs in the Staging Area

        Returns
        -------
        conductor_jobs : list
            list of dicts with info about conductor jobs in staging area
        """
        # Find the conductor jobs in staging area
        conductor_jobs = list(dx.search.find_jobs(
            project=staging_id,
            created_before=five_days_after,
            created_after=five_days_before_start,
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

    def create_run_dictionary(
        self, projects_dx_response, audit_start_obj, audit_end_obj
    ):
        """
        Add run name, DX project ID and assay type for each run to dict.

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
                'assays': {
                    'MYE': {
                        'project_id': 'project-ABCDE'
                    },
                    'CEN': {
                        'project_id': 'project-FGHIJ'
                    }
                }
            },
            '240111_A01303_0320_BHWYNVDRX3': {
                'assays': {
                    'CEN': {
                        'project_id': 'project-OPQ'
                    }
                }
            }
        }
        """
        run_dict = defaultdict(dict)

        # For each proj name, get run name by removing _002 and _assay name
        for project in projects_dx_response:
            project_name = project['describe']['name']
            assay_type = project_name.split('_')[-1]

            run_name_pattern = (
                r'^002_(\d{6}_[A-Za-z]{1}\d{5}_\d{4}_[A-Za-z0-9]{10})(?:_(37|38))?_[A-Za-z0-9]{3,}$'
            )
            match = re.match(run_name_pattern, project_name)
            if match:
                # select the first group in the regex
                run_name = match.group(1)
                logger.debug("Run name: %s", run_name)
            else:
                logger.error(
                    f"Project name {project_name} does not match the expected"
                    " pattern for a run name"
                )
            # Check if the date of the run is within audit dates
            # because 002 project may have been made after actual run date
            # Don't capture 002_vaf_checks project for checking VAF
            run_date, first_part_of_name = run_name.split('_')[0:2]

            if (
                run_date >= audit_start_obj.strftime('%y%m%d')
                and run_date <= audit_end_obj.strftime('%y%m%d')
                and first_part_of_name != "vaf"
            ):
                # Add in DX project ID and assay type to a nested_dictionary
                # in assays key for each run but first check if the run already
                # has an assay key in the dict, if not create one
                if not run_dict[run_name].get('assays'):
                    run_dict[run_name]['assays'] = {}
                run_dict[run_name]['assays'][f"{assay_type}"] = {
                    'project_id': project['id']
                }
        return run_dict

    def update_run_name(self, run_dict):
        """
        Update each main key in the dict (the run name) to the run name
        according to the folder in the Staging Area. This
        corrects any errors in naming of DNAnexus '002_' projects

        Parameters
        ----------
        run_dict : dict
            dict with each run as key and info as nested dict.

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
        for run_name, run_data in run_dict.items():
            if run_data.get('run_folder_name'):
                folder_name = run_dict[run_name]['run_folder_name']
                assays = run_dict[run_name]['assays']
                assay_list = assays.keys()
                distance = Levenshtein.distance(folder_name, run_name)
                if distance > 0:
                    # If ticket mismatches, add typo info to list
                    for assay in assay_list:
                        typo_run_folders.append({
                            'assay_type': assay,
                            'folder_name': folder_name,
                            'project_name_002': run_name
                        })

                updated_dict[folder_name] = run_dict[run_name]
            # Othereise if no 'run_folder_name' keep key and values as is
            else:
                updated_dict[run_name] = run_dict[run_name]

        return updated_dict, typo_run_folders

    def update_dictionary_structure(self, run_dict):
        """
        Update the structure of the dictionary, where each run_assay
        combination is a main key.
        Parameters
        ----------
        run.dict: dict
            dict with each run as key and info as nested dict with assay type
            nested inside.
        Example:
        {
            '260507_A01295_0123_BHKLV2DRX7': {
                'assays': {
                    'CEN': {
                        'project_id': 'project-ABCDE'
                        },
                    'MYE': {
                        'project_id': 'project-FGHIJ'
                        }
                    },
                'run_folder_name': '260507_A01295_0123_BHKLV2DRX7',
                'upload_time': '2026-03-21 00:53:46',
                'first_job': '2026-03-21 00:54:14'
            },
            '260304_A01303_0456_AHKLFFDRX7': {
                'assays': {
                    'TWE': {
                        'project_id': 'project-KLMNO'
                        }
                    },
                'run_folder_name': '260304_A01303_0456_AHKLFFDRX7',
                'upload_time': '2026-03-19 20:17:41',
                'first_job': '2026-03-19 20:18:03'
            }
        }

        Returns
        -------
        modified_run_dict : dict
            dict with same info but with run_name_and_assay_key as a key in
            the nested dict
        Example:
        {
            '260507_A01295_0123_BHKLV2DRX7_CEN': {
                'project_id': 'project-ABCDE',
                'assay_type': 'CEN',
                'run_folder_name': '260507_A01295_0123_BHKLV2DRX7',
                'upload_time': '2026-03-21 00:53:46',
                'first_job': '2026-03-21 00:54:14'
            },
            '260507_A01295_0123_BHKLV2DRX7_MYE': {
                'project_id': 'project-FGHIJ'
                'assay_type': 'MYE',
                'run_folder_name': '260507_A01295_0123_BHKLV2DRX7',
                'upload_time': '2026-03-21 00:53:46',
                'first_job': '2026-03-21 00:54:14'
            },
            '260304_A01303_0456_AHKLFFDRX7_TWE': {
                'project_id': 'project-KLMNO',
                'assay_type': 'TWE',
                'run_folder_name': '260304_A01303_0456_AHKLFFDRX7',
                'upload_time': '2026-03-19 20:17:41',
                'first_job': '2026-03-19 20:18:03'
            }
        }
        """
        modified_run_dict = defaultdict(dict)

        # Recreate the dict with the run name and assay type as the main key
        for run_name, run_info in run_dict.items():
            assays = run_info.get('assays')
            if assays:
                for assay_type, assay_info in assays.items():
                    new_key = f"{run_name}_{assay_type}"
                    modified_run_dict[new_key] = {
                        'project_id': assay_info.get('project_id'),
                        'assay_type': assay_type,
                        'run_folder_name': run_info.get('run_folder_name'),
                        'upload_time': run_info.get('upload_time'),
                        'first_job': run_info.get('first_job')
                    }
            # Raise an error if no assay was found in the given run
            else:
                logger.error(
                    f"No assay info found for run {run_name} "
                    "when updating dict structure. This should "
                    "have been picked up in earlier steps."
                )
                raise ValueError(
                    f"No assay info found for run {run_name} "
                    "when updating dict structure. This should "
                    "have been picked up in earlier steps."
                )

        return modified_run_dict

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

    def add_upload_time(self, staging_folders, run_dict, staging_id):
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
            '260507_A01295_0123_BHKLV2DRX7': {
                'assays': {
                    'CEN': {
                        'project_id': 'project-ABCDE'
                        },
                    'MYE': {
                        'project_id': 'project-FGHIJ'
                        }
                    },
                'run_folder_name': '260507_A01295_0123_BHKLV2DRX7',
                'upload_time': '2026-03-21 00:53:46'
            },
            '260304_A01303_0456_AHKLFFDRX7': {
                'assays': {
                    'TWE': {
                        'project_id': 'project-KLMNO'
                        }
                    },
                'run_folder_name': '260304_A01303_0456_AHKLFFDRX7',
                'upload_time': '2026-03-19 20:17:41'
            }
        }
        """
        for run_name in run_dict.keys():
            for folder_name in staging_folders:
                # Get differences between run name and staging folder name
                distance = Levenshtein.distance(folder_name, run_name)
                # If match with less than 2 differences, add run folder
                # name as nested key
                if distance < 2:
                    run_dict[run_name]['run_folder_name'] = folder_name
                    # Search for log file in folder
                    files_in_folder = self.find_log_file_in_folder(
                        folder_name, staging_id
                    )
                    # Add log file time as upload time
                    if files_in_folder:
                        upload_time = self.get_log_file_created_time(
                            files_in_folder
                        )
                        run_dict[run_name]['upload_time'] = upload_time

        return run_dict

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
            except IndexError:
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
        modified_dict : dict
            dict with each run plus first job time added
        {
            '240124_A01295_0305_AHW725DRX3_TSO500': {
                'project_id': 'project-Gfk24G84412KXVyf4kVZVv7g',
                'assay_type': 'TSO500',
                'run_folder_name': '240124_A01295_0305_AHW725DRX3',
                'upload_time': '2024-01-25 08:52:27',
                'first_job': '2024-01-25 08:52:40'
            },
             '240122_A01295_0303_AHTNWYDRX3_TWE': {
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

        modified_dict = self.update_dictionary_structure(run_dict)
        return modified_dict

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
        job_completed : str or None
            timestamp of the last job finished (or None if no jobs)
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

    def add_last_job_time(self, run_dict, last_jobs):
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
            job_to_search = last_jobs.get(assay_type)
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
