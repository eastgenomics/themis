import datetime as dt
import logging
import numpy as np
import pandas as pd
import sys

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


class GeneralFunctions():
    """
    Functions for creating and manipulating pandas dfs
    """
    def __init__(
        self, tat_standard, cancelled_statuses, open_statuses, audit_start,
        audit_end
    ):
        self.tat_standard = tat_standard
        self.cancelled_statuses = cancelled_statuses
        self.open_statuses = open_statuses
        self.audit_start = audit_start
        self.audit_end = audit_end

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
                    run_dict[run][key] = None

            return run_dict

    def create_run_df(self, run_dict):
        """
        Creates a df with all the assay types, run names and relevant audit
        info

        Parameters
        ----------
        run_dict : run_dict
            dict with each run and the relevant audit info
        Returns
        -------
        run_df : pd.DataFrame
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
        url = """<a href="https://cuhbioinformatics.atlassian.net/browse/{}">{}</a>""".format(
            row['ticket_key'], row['run_name']
        )

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
            lambda row: row['run_name']
            if pd.isnull(row['ticket_key'])
            else self.generate_hyperlink(row),
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
        pd_current_time = pd.Timestamp(
            dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )

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
            (pd_current_time - run_df['processing_finished']).where(
                run_df['jira_status'] == 'Urgent samples released'
            ) / np.timedelta64(1, 'D')
        )

        # Get the time from the most recent processing step by forward filling
        # from columns until 'muliQC_finished' column
        # If this results in a string, convert to NA
        run_df['last_processing_step'] = pd.to_datetime(
            run_df.ffill(axis=1).iloc[:, 4], errors='coerce'
        )

        # Add the time since the last processing step which exists to current
        # time for open tickets that are on hold
        run_df['on_hold_time'] = (
            (pd_current_time - run_df['last_processing_step']).where(
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
                & (assay_df['processing_end_to_release'] >= 0)
                & (
                    assay_df['upload_to_release'].notna()
                    | assay_df['urgents_time'].notna()
                )
            ].shape[0]

            if relevant_run_count:
                compliance_percentage = (
                    (compliant_runs / relevant_run_count) * 100
                )

                compliance_fraction = (
                    f"({compliant_runs}/{relevant_run_count}) "
                )
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
                    'Mean pipeline running': (
                        assay_df['processing_time'].mean()
                    ),
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

        # If there are runs to be flagged in dict, pass or if all vals empty
        # pass empty dict so can check in Jinja2 if defined
        # to decide whether to show 'Runs to be manually reviewed' text
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
        assay_df : pd.DataFrame
            dataframe of just the runs for the specified assay type
        assay_stats : str
            dataframe of TAT stats as HTML string
        assay_issues : dict
            dict with issue as key and any runs with that issue as value
            if dict values empty passes as empty dict to be checked in HTML to
            Decide whether to show 'Runs to be manually reviewed' text
        assay_no_of_002_runs : int
            the number of runs found for that assay through the number of
            002 projects
        assay_fraction : str
            fraction of runs compliant with audit standards
        assay_percentage : float
            percentage of runs compliant with audit standards
        """
        assay_df = self.extract_assay_df(all_assays_df, assay_type)
        assay_no_of_002_runs = assay_df.shape[0]
        assay_stats, assay_fraction, assay_percentage = self.make_stats_table(
            assay_df
        )
        assay_issues = self.find_runs_for_manual_review(assay_df)

        return (
            assay_df, assay_stats, assay_issues, assay_no_of_002_runs,
            assay_fraction, assay_percentage
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

    def write_to_csv(self, all_assays_df) -> None:
        """
        Write the dataframe of all runs in the audit period and all of
        the associated info to CSV

        Parameters
        ----------
        all_assays_df : pd.DataFrame
            dataframe with all of the runs in the audit period and
            all of the relevant info
        """
        # Remove unnecessary columns
        all_assays_df.drop(
            columns=['change_log', 'ticket_hyperlink', 'run_date'],
            inplace=True, errors='ignore'
        )
        all_assays_df.to_csv(
            f'audit_info_{self.audit_start}_{self.audit_end}.csv',
            float_format='%.3f',
            index=False
        )
