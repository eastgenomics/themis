import argparse
import datetime as dt
import json
import logging
import os
import pandas as pd
import sys
import warnings

from ast import literal_eval
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from utils.dx_requests import DXFunctions
from utils.jira_requests import JiraFunctions
from utils.plotting import PlottingFunctions
from utils.utils import GeneralFunctions


warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
ROOT_DIR = Path(__file__).absolute().parents[0]

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


class Arguments():
    """
    Functions for handling and parsing command line arguments
    """
    def __init__(self):
        self.args = self.parse_args()
        (
            self.dx_token,
            self.jira_email,
            self.jira_token,
            self.staging_id,
            self.default_months,
            self.tat_standard,
            self.assay_types,
            self.cancelled_statuses,
            self.open_statuses,
            self.last_jobs
        ) = self.load_credential_info()
        (
            self.audit_start,
            self.audit_end,
            self.audit_start_obj,
            self.audit_end_obj,
            self.five_days_before_start,
            self.five_days_after
        ) = self.determine_start_and_end_date()

    def parse_args(self) -> argparse.Namespace:
        """
        Parse command line arguments

        Returns
        -------
        args : Namespace
            Namespace of passed command line argument inputs
        """
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
                'Font size for headings of subplots within TAT plots in the '
                'report. Default is 12'
            )
        )

        return parser.parse_args()

    def load_credential_info(self):
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
            the default number of months to audit previous to today if no
            CLI args are given
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

        (
            dx_token, jira_email, jira_token, staging_proj_id,
            default_months, tat_standard, assay_types, cancelled_statuses,
            open_statuses, last_jobs
        ) = list(map(os.environ.get, keys))

        # Check all are present
        if not all([
            dx_token, jira_email, jira_token, staging_proj_id, default_months,
            tat_standard, assay_types, cancelled_statuses, open_statuses,
            last_jobs
        ]):
            logger.error(
                "Required credentials could not be parsed from the env"
            )
            sys.exit()

        # Remove leading and trailing single quotes which mess with
        # parsing strings to lists and dicts from .env vars inside Docker
        # containers
        # (single quotes are needed bc trying to export the .env file
        # in a non-Docker environment doesn't work otherwise)
        assay_types = literal_eval(assay_types.strip("'"))
        cancelled_statuses = literal_eval(cancelled_statuses.strip("'"))
        open_statuses = literal_eval(open_statuses.strip("'"))
        last_jobs = literal_eval(last_jobs.strip("'"))

        return (
            dx_token, jira_email, jira_token, staging_proj_id, default_months,
            int(tat_standard), assay_types, cancelled_statuses,
            open_statuses, last_jobs
        )

    def determine_start_and_end_date(self):
        """
        Determine the start and end dates of the audit based on CLI arguments

        Returns
        -------
        audit_begin_date : str
            the begin date of the audit e.g. '2022-06-25'
        audit_end_date : str
            the end date of the audit e.g. '2022-10-01'
        audit_begin_date_obj : dt.date obj
            the begin date as a date obj so can be formatted as different
            strings later
        audit_end_date_obj : dt.date obj
            the end date as a date obj so can be formatted as different strings
            later
        """
        # Work out default dates for audit if none supplied (today - X months)
        today_date = dt.date.today()
        today_str = today_date.strftime('%Y-%m-%d')
        default_begin_date = today_date + relativedelta(
            months=-int(self.default_months)
        )
        default_begin_date_str = default_begin_date.strftime('%Y-%m-%d')

        # Check both start and end date are entered
        if (not self.args.start_date) ^ (not self.args.end_date):
            raise RuntimeError(
                '--start_date and --end_date must be given together'
            )

        # Set start and end to be either the default or entered dates
        if not self.args.start_date and not self.args.end_date:
            audit_begin_date = default_begin_date_str
            audit_end_date = today_str
        else:
            # Check start is before end date
            if self.args.end_date < self.args.start_date:
                raise RuntimeError('--start_date must be before --end_date')
            audit_begin_date = self.args.start_date
            audit_end_date = self.args.end_date

        # Get the dates as objects so that they can be converted to
        # different str formats later
        audit_begin_date_obj = dt.datetime.strptime(
            audit_begin_date, '%Y-%m-%d'
        )
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


def main():
    """Main function to create html report"""
    inputs = Arguments()

    # Log in to DNAnexus
    DXFunctions().login(inputs.dx_token)
    period_audited = f"{inputs.audit_start} to {inputs.audit_end}"

    logger.info("Creating a run dict for all assays")
    projects_002 = DXFunctions().get_002_projects_within_buffer_period(
        inputs.assay_types,
        inputs.five_days_after,
        inputs.five_days_before_start
    )
    projects_002_dict = DXFunctions().create_run_dictionary(
        projects_002,
        inputs.audit_start_obj,
        inputs.audit_end_obj
    )
    no_of_002_runs = len(projects_002_dict)
    print(
        f"Found {no_of_002_runs} sequencing runs with 002 projects in DNAnexus"
        " within the audit period"
    )

    # Get folders in 001_Staging_Area52 to check for ticket/project typos
    staging_folders = DXFunctions().get_staging_folders(inputs.staging_id)

    # Add upload time
    projects_002_dict = DXFunctions().add_upload_time(
        staging_folders,
        projects_002_dict,
        inputs.staging_id
    )
    projects_002_dict, typo_002_list = DXFunctions().update_run_name(
        projects_002_dict
    )

    logger.info("Finding first job for all runs")
    # Get conductor jobs and add first job time
    conductor_jobs = DXFunctions().find_conductor_jobs(
        inputs.staging_id,
        inputs.five_days_after,
        inputs.five_days_before_start
    )
    conductor_run_dict = DXFunctions().get_earliest_conductor_job_for_each_run(
        conductor_jobs
    )
    projects_002_dict = DXFunctions().add_first_job_time(
        conductor_run_dict, projects_002_dict
    )

    logger.info("Getting + adding JIRA ticket info for closed seq runs")
    # Initialise JiraFunctions class with required email and token
    # Get info from JIRA from the closed sequencing run queue and open
    # sequencing run queue
    jira_info = JiraFunctions(
        inputs.jira_email,
        inputs.jira_token,
        inputs.assay_types,
        inputs.cancelled_statuses,
        inputs.audit_start_obj,
        inputs.audit_end_obj,
        inputs.open_statuses,
        inputs.five_days_before_start,
        inputs.five_days_after
    )
    jira_closed_queue_tickets = jira_info.query_jira_tickets_in_queue(35)
    jira_open_queue_tickets = jira_info.query_jira_tickets_in_queue(34)
    all_jira_tickets = jira_closed_queue_tickets + jira_open_queue_tickets

    # Create dict of jira tickets
    jira_ticket_dict = jira_info.create_jira_info_dict(all_jira_tickets)

    # Add Jira ticket info to our dict of runs
    (
        projects_002_dict, typo_tickets, runs_no_002_proj, cancelled_runs,
        open_runs_list
    ) = jira_info.add_jira_ticket_info(projects_002_dict, jira_ticket_dict)
    projects_002_dict = jira_info.add_transition_times(
        projects_002_dict
    )

    # Add final job
    projects_002_dict = DXFunctions().add_last_job_time(
        projects_002_dict,
        inputs.last_jobs
    )

    # Initialise GeneralFunctions class with inputs
    general_functions = GeneralFunctions(
        inputs.tat_standard,
        inputs.cancelled_statuses,
        inputs.open_statuses,
        inputs.audit_start,
        inputs.audit_end
    )

    # Add any keys which don't exist for a run as None so we don't get errors
    # later
    projects_002_dict = general_functions.add_in_empty_keys(projects_002_dict)

    # Create df for all runs and add extra info
    run_df = general_functions.create_run_df(projects_002_dict)
    run_df = general_functions.add_jira_ticket_hyperlink(run_df)
    run_df = general_functions.add_run_week(run_df)

    # # Sort out typos
    typo_folders_table = general_functions.create_typo_df(typo_002_list)
    typo_tickets_table = general_functions.create_typo_df(typo_tickets)

    logger.info("Adding calculation columns")
    run_df = general_functions.add_calculation_columns(run_df)

    logger.info("Generating objects for each assay")
    plotting_functions = PlottingFunctions(
        inputs.cancelled_statuses,
        inputs.audit_start,
        inputs.audit_end,
        inputs.tat_standard,
        inputs.args.font_size
    )
    # CEN
    CEN_df, CEN_stats, CEN_issues, CEN_runs, CEN_frac, CEN_compl = (
        general_functions.create_assay_objects(run_df, 'CEN')
    )
    CEN_fig, CEN_upload_fig = plotting_functions.create_both_figures(
        CEN_df, 'CEN'
    )

    # MYE
    MYE_df, MYE_stats, MYE_issues, MYE_runs, MYE_frac, MYE_compl = (
        general_functions.create_assay_objects(run_df, 'MYE')
    )
    MYE_fig, MYE_upload_fig = plotting_functions.create_both_figures(
        MYE_df, 'MYE'
    )

    # TSO500
    TSO_df, TSO_stats, TSO_issues, TSO_runs, TSO_frac, TSO_compl = (
        general_functions.create_assay_objects(run_df, 'TSO500')
    )
    TSO_fig, TSO_upload_fig = plotting_functions.create_both_figures(
        TSO_df, 'TSO500'
    )

    # TWE
    TWE_df, TWE_stats, TWE_issues, TWE_runs, TWE_frac, TWE_compl = (
        general_functions.create_assay_objects(run_df, 'TWE')
    )
    TWE_fig, TWE_upload_fig = plotting_functions.create_both_figures(
        TWE_df, 'TWE'
    )

    # Add in any cancelled runs to the df
    run_df = general_functions.add_in_cancelled_runs(
        run_df, cancelled_runs
    )
    # Write everything out to CSV
    general_functions.write_to_csv(run_df)

    # Load Jinja2 template
    # Add the charts, tables and issues into the template
    environment = Environment(loader=FileSystemLoader(
        ROOT_DIR.joinpath("templates")
    ))
    template = environment.get_template("audit_template.html")

    logger.info("Adding objects into HTML template")
    filename = (
        f"turnaround_times_{inputs.audit_start}_{inputs.audit_end}.html"
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
