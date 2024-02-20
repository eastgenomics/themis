import datetime as dt
import json
import Levenshtein
import logging
import requests
import sys

from collections import defaultdict
from pathlib import Path
from requests.auth import HTTPBasicAuth


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


class JiraFunctions():
    """
    Methods for handling Jira things
    """
    def __init__(
        self, jira_email, jira_token, assay_types, cancelled_statuses,
        audit_start_obj, audit_end_obj, open_statuses, five_days_before_start,
        five_days_after
    ):
        self.jira_email = jira_email
        self.jira_token = jira_token
        self.auth = HTTPBasicAuth(jira_email, jira_token)
        self.headers = {"Accept": "application/json"}
        self.assay_types = assay_types
        self.cancelled_statuses = cancelled_statuses
        self.audit_start_obj = audit_start_obj
        self.audit_end_obj = audit_end_obj
        self.open_statuses = open_statuses
        self.five_days_before_start = five_days_before_start
        self.five_days_after = five_days_after

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
        url = (
            "https://cuhbioinformatics.atlassian.net/rest/api/3/issue/"
            f"{ticket_id}/changelog"
        )

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
        Returns
        -------
        jira_run_dict : dict
            dict where the summary name is the key and info about the ticket
            as values
        Example:
        {
            '240130_A01303_0329_BH2HWHDRX5': {
                'ticket_key': 'EBH-2377',
                'ticket_id': '21865',
                'jira_status': 'All samples released',
                'assay_type': 'CEN',
                'date_jira_ticket_created': (
                    datetime.datetime(2024, 1, 30, 16, 52, 18)
                )
            },
            '240130_A01303_0330_AHWL32DRX3': {
                'ticket_key': 'EBH-2376',
                'ticket_id': '21864',
                'jira_status': 'All samples released',
                'assay_type': 'MYE',
                'date_jira_ticket_created': (
                    datetime.datetime(2024, 1, 30, 16, 49, 38)
                )
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
                        change_log = self.get_ticket_transition_times(
                            ticket_id
                        )
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
            ticket_id = run_info.get('ticket_id')
            if ticket_id:
                # Query Jira API with changelog for ticket transition times
                change_log = self.get_ticket_transition_times(ticket_id)

                # Add the dict to the changelog key
                run_dict[run_name]['change_log'] = change_log

                # If ticket is at 'All samples released' add the resolved time
                jira_resolved = change_log.get('All samples released')
                if jira_resolved:
                    run_dict[run_name]['jira_resolved'] = jira_resolved

        return run_dict
