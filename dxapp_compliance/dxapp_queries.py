import sys
from pathlib import Path

# The package's parent must be importable for `from dxapp_compliance... import`
# to resolve when this file is run directly as `python dxapp_queries.py` from
# inside dxapp_compliance/. Keeps the old invocation working during the refactor.
sys.path.insert(0, str(Path(__file__).absolute().parents[1]))

import base64
import json
import logging
from math import ceil

import pandas as pd
import plotly.express as px
import requests
# Imported for its side effect: plotly's trendline="lowess" requires statsmodels
# to be importable. The `sm` name itself is unused - do not "clean this up".
import statsmodels.api as sm  # noqa: F401
# Fastcore extends the python standard library to allow for the use of ghapi.
# This star import is also the only source of `re` and HTTP404NotFoundError in
# this module. Both are imported explicitly in the new package modules.
from fastcore.all import *  # noqa: F401,F403
from ghapi.all import GhApi
from jinja2 import Environment, FileSystemLoader

from dxapp_compliance.checks.registry import NOT_APPLICABLE
from dxapp_compliance.checks.runner import run_all_checks
from dxapp_compliance.config import TEMPLATE_DIR, get_config, setup_logging, today_date
from dxapp_compliance.models import AppEvidence, RepoRecord

# TODO: Add stats to parts of the html report and use bootrap to style it.
# TODO: Make report prettier with bootstrap.
# TODO: Add list of repos without releases to report. (datatables)
# TODO: Add instance type to the report.

# Remove warnings from pandas which aren't relevant.
pd.options.mode.chained_assignment = None
# Set up logger. Handlers are configured in main() via setup_logging(), not at
# import time - see dxapp_compliance/config.py.
logger = logging.getLogger("general log")


def get_template_render(compliance_df, detailed_df, compliance_stats_summary,
                        release_comp_plot, ubuntu_comp_plot,
                        compliance_bycommitdate_plot,
                        ):
    """
    Render jinja2 template with provided variables.
    Parameters
    ----------
    compliance_df (pandas dataframe):
        Dataframe containing compliance information for each app.
    detailed_df (pandas dataframe):
        Dataframe containing detailed information for each app.
    compliance_stats_summary (pandas dataframe):
        Dataframe containing compliance information for each app.
    release_comp_plot (plotly figure):
        Plotly figure of Scatter plot for compliance % by release date
    ubuntu_comp_plot (plotly figure):
        Plotly figure of Scatter plot for compliance % by release date
        coloured by ubuntu version.
    compliance_bycommitdate_plot (plotly figure):
        Plotly figure of Scatter plot for compliance % by last commit date

    Outputs
    -------
    .html
        HTML report of all app compliances.
        Including tables of compliance stats and plots.
    """
    # autoescape is left at Jinja2's default of False deliberately. The plot
    # variables in Report.html are raw plotly HTML and escaping would mangle
    # them - do not add select_autoescape here.
    environment = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = environment.get_template("Report.html")
    filename = f"Audit_{today_date()}.html"
    compliance_html = compliance_df.to_html(table_id="comp",
                                            classes="table table-striped table-hover"
                                            )
    details_html = detailed_df.to_html(table_id="details",
                                       classes="table table-striped table-hover"
                                       )
    # Set conditional formatting for compliance table
    styled_df = compliance_stats_summary.style.apply(
        lambda x: ['background-color: #FFB3BA' if value < 50 else
                   'background-color: #BAFFC9' if value > 80 else
                   'background-color: #FFBF00'
                   for value in x],
        subset=['Compliance %']).hide(axis='index').format(precision=0)
    compliance_stats_summary_html = styled_df.to_html(
        table_attributes="class = 'table table-striped table-hover'",
        table_uuid="compliance_stats_summary",
        bold_headers=True,
        justify="left",
    )

    context = {
        "Compliance_table": compliance_html,
        "Details_table": details_html,
        "compliance_stats_summary": compliance_stats_summary_html,
        "release_comp_plot": release_comp_plot,
        "ubuntu_comp_plot": ubuntu_comp_plot,
        "compliance_bycommitdate_plot": compliance_bycommitdate_plot,
    }
    with open(filename, mode="w", encoding="utf-8") as results:
        results.write(template.render(context))
        print(f"... wrote {filename}")



class audit_class:
    """
    A class for all dx querying functions.
    This collects all the compliance data needed for the audit app.
    """

    def __init__(self):
        # Set config
        self.GITHUB_TOKEN, self.ORGANISATION, self.DEFAULT_REGION = get_config()

    def collect_evidence(self, app, dxjson_content):
        """
        Gathers everything the checks need for one app/applet.

        This is the seam between the GitHub layer and the checks: the checks
        themselves are a pure function of the returned AppEvidence, which is why
        they can be tested without mocking the GitHub API.

        Parameters
        ----------
            app (GithubAPI app object):
                Github API repository object used for extracting app/applet info.
            dxjson_content (dict):
                contents of the dxapp.json file for the app.

        Returns
        -------
            AppEvidence
        """
        repo = RepoRecord.from_api(app)

        # Find source for app/applet
        src_file_contents, last_release_date, latest_commit_date = self.get_src_file(
            app=app,
            dxjson_content=dxjson_content,
            organisation_name=self.ORGANISATION,
            github_token=self.GITHUB_TOKEN)

        (_, _, dependabot_alerts_enabled,
         dependabot_security_updates_set) = self.get_security_advisories(
            repo.name
        )
        requirements_txt_present = self.check_requirements_file_in_python_app(
            repo.name
        )

        entrypoint_path = dxjson_content.get('runSpec', {}).get('file')

        return AppEvidence(
            repo=repo,
            dxapp=dxjson_content,
            # Populated by the git-tree walk once gh_api/contents.py lands.
            file_paths=(),
            scripts={entrypoint_path: src_file_contents}
            if entrypoint_path else {},
            entrypoint_path=entrypoint_path,
            last_release_date=last_release_date,
            latest_commit_date=latest_commit_date,
            dependabot_alerts_enabled=dependabot_alerts_enabled,
            dependabot_security_updates_set=dependabot_security_updates_set,
            requirements_txt_present=requirements_txt_present,
            default_region=self.DEFAULT_REGION,
        )

    def get_list_of_repositories(self, org_username, github_token=None):
        """
        This function gets a list of all visible repositories for a given ORG.

        Parameters
        ----------
            org_username (str):
                the username of the organisation to use
                for getting the list of repositories.
            github_token (str):
                the github token to authenticate with.

        Returns
        -------
            all_repos (list):
                a list of all the repositories for the given organisation.
        """
        # https://api.github.com/orgs/ORG/repos
        api = GhApi(token=github_token)
        org_details = api.orgs.get(org_username)
        logger.info(org_details)
        total_num_repos = org_details['public_repos']
        logger.info(total_num_repos)
        per_page_num = 30
        pages_total = ceil(total_num_repos/per_page_num)
        all_repos = []
        # The API response in paginated, so we need to loop through all pages
        for page in range(1, pages_total+1):
            response = api.repos.list_for_org(org=org_username,
                                              per_page=per_page_num,
                                              page=page)
            response_repos = [repo for repo in response]
            all_repos += response_repos

        return all_repos

    def select_apps(self, list_of_repos, github_token=None):
        """
        Select apps/applets from list of repositories
        and extracts the dxapp.json contents.

        Parameters
        ----------
            list_of_repos (list):
                list of repositories from organisation.
            github_token (str):
                token for secure connection to Github API.

        Returns
        -------
            repos_apps (list):
                list of app/applet dictionaries selected.
            repos_apps_content (list):
                list of app/applet dxapp.json contents selected.

        """
        repos_apps = []
        repos_apps_content = []
        api = GhApi(token=github_token)
        for repo in list_of_repos:

            if repo['archived'] is False:
                owner = 'eastgenomics'
                repo_name = repo['name']
                file_path = 'dxapp.json'
                logger.info(repo_name)
                # print(repo)
                # Checks to find dxapp.json which determines if repo is an app.
                try:
                    contents = api.repos.get_content(
                        owner, repo_name, file_path)
                except HTTP404NotFoundError:
                    logger.error(f'{repo_name} is not an app.')
                    print(f'{repo_name} is not an app.')
                    continue

                # Append app to list of apps
                repos_apps.append(repo)

                # Decode contents using base64 and append to list.
                file_content = contents['content']
                file_content_encoding = contents.get('encoding')
                if file_content_encoding == 'base64':
                    contents_decoded = base64.b64decode(file_content).decode()
                    app_decoded = json.loads(contents_decoded)

                    repos_apps_content.append(app_decoded)

                else:
                    logger.info(
                        f"Other encoding used. {file_content_encoding}")
            elif repo['archived'] is True:
                logger.info(f'{repo["name"]} is archived.')
                continue
            else:
                logger.info(
                    f'{repo["name"]} has unknown archival state see: {repo["archived"]}')
                continue

        logger.info(f"{len(repos_apps)} app repositories found.")

        return repos_apps, repos_apps_content

    def get_src_file(self, app, organisation_name, dxjson_content, github_token=None):
        """
        This function gets the source script for a given app/applet.

        Parameters
        ----------
            app (GithubAPI app object):
                Github API repository object used for extracting app/applet info.
            organisation_name (str):
                the username of the organisation the app/applet is in.
            dxjson_content (dict):
                contents of the dxapp.json file for the app/applet.
            github_token (str, optional):
                Authentication token for github.
                Defaults to None. Therefore showing just public info.

        Returns
        -------
            src_content_decoded (str):
                the source code for the app/applet decoded.
            last_release_date (str):
                the date of the last release for the app/applet.
            latest_commit_date (str):
                the date of the latest commit for the app/applet.
        """
        repos_apps = []
        app_src_file = {}
        src_code_content = ""
        src_content_decoded = ""
        api = GhApi(token=github_token)
        repo_name = app.get('name')
        file_path = dxjson_content.get('runSpec', {}).get('file')

        # Extract src file contents
        try:
            app_src_file = api.repos.get_content(organisation_name,
                                                 repo_name,
                                                 file_path)
        except HTTP404NotFoundError:
            logger.error(
                f'{repo_name} No src file found using dxjson file path')
            # Check if any other src file is in the src/ subfolder
            try:
                file_path = 'src/'
                contents = api.repos.get_content(organisation_name,
                                                 repo_name,
                                                 file_path)
                # Search contents for src file
                for content in contents:
                    filename = content['name']
                    if re.search(r'(.py|.sh)$', filename):
                        try:
                            logger.info("src file found in src/ subfolder."
                                        "src file is named differently in dxapp.json.")
                            file_path = content['path']
                            app_src_file = api.repos.get_content(organisation_name,
                                                                 repo_name,
                                                                 file_path)
                        except HTTP404NotFoundError:
                            logger.info(
                                f'{repo_name} 404 No src file found in src/ subfolder')
                            pass
            except HTTP404NotFoundError:
                logger.error(f'{repo_name} No src folder found.')

        repos_apps.append(dxjson_content)
        src_code_content = app_src_file.get('content', None)
        code_content_encoding = app_src_file.get('encoding', None)
        if src_code_content:
            if code_content_encoding == 'base64':
                src_content_decoded = base64.b64decode(
                    src_code_content
                ).decode()
            else:
                logger.error("Other encoding used.")
        else:
            logger.error("No src file found.")

        # Get the latest release date & commit date
        last_release_date = self.get_latest_release(
            organisation_name, repo_name, github_token
        )
        latest_commit_date = self.get_latest_commit_date(
            organisation_name, repo_name, github_token
        )

        return src_content_decoded, last_release_date, latest_commit_date

    def compliance_stats(self, compliance_df, detailed_df):
        """
        Finds the % compliance with the EastGLH guidelines for each app/applet.

        Parameters
        ----------
            compliance_df (pandas dataframe):
                dataframe of compliance booleans for each app/applet.

        Returns
        -------
            checks_df (pandas dataframe):
                dataframe of compliance booleans for each app/applet.
                with added compliance % column.
            detailed_df (pandas dataframe):
                dataframe of compliance performa details for each app/applet.
        """
        # remove columns that are not compliance checks
        checks_df = compliance_df.copy()
        checks_df.drop(columns=['num_of_region_options'], inplace=True)
        # Find the % overall compliance for each app/applet
        # Set the total performa checks for each app/applet

        checks_df['total_performa'] = checks_df['interpreter'].apply(
            lambda x: 12 if 'bash' in x else 10)
        # Find the number of performa checks passed for each app/applet
        checks_df['compliance_count'] = (checks_df == True).T.sum()
        score_data = round(
            (checks_df['compliance_count'] /
             checks_df['total_performa']) * 100, 2
        )
        checks_df.insert(1, 'compliance_score', score_data)
        detailed_df.insert(1, 'compliance_score', score_data)
        return checks_df, detailed_df

    def get_latest_release(self, organisation_name, repo_name, token):
        """
        Get latest release of app/applet repo.
        Extracts the latest release date from the github API.

        Parameters
        ----------
            organisation_name (str):
                name of organisation of app/applet.
            repo_name (str):
                name of repo to get latest release of.
            token (str):
                github token for accessing repo via API.
        Returns
        -------
            last_release_date (str):
                string of latest release date.
        """
        api = GhApi(token=token)
        # get latest release endpoint json.
        try:
            contents = api.repos.get_latest_release(organisation_name,
                                                    repo_name)
            last_release_date = contents['published_at'].split("T")[0]
        except HTTP404NotFoundError:
            logger.info(f'{repo_name} 404 No release found.')
            last_release_date = None

        return last_release_date

    def get_latest_commit_date(self, organisation_name, repo_name, token):
        """
        Get latest commit of app/applet repo.
        Extracts the latest commit date from the github API.

        Parameters
        ----------
            organisation_name (str):
                name of organisation of app/applet.
            repo_name (str):
                name of repo to get latest commit of.
            token (str):
                github token for accessing repo via API.
        Returns
        -------
            latest_commit_date (str):
                string of latest commit date.
        """
        api = GhApi(token=token)

        # List all branches
        list_of_shas = []
        list_of_branches = api.repos.list_branches(
            organisation_name, repo_name)

        # Find sha for each branch and append to list for api calls.
        list_of_shas = [branch['commit']['sha'] for branch in list_of_branches]

        # For branch in branches find the latest commit date.
        list_of_commit_dates = []
        for branch in list_of_shas:
            try:
                json_reponse = api.repos.list_commits(organisation_name,
                                                      repo_name,
                                                      branch,
                                                      per_page=1,
                                                      page=1)
                commit_date = json_reponse[0]['commit']['committer']['date']
                list_of_commit_dates.append(commit_date)
            except HTTP404NotFoundError:
                logger.info(f'{repo_name} 404 No commits found on {branch}')

        # Find latest commit date
        latest_commit_datetime = max(list_of_commit_dates)
        latest_commit_date = latest_commit_datetime.split("T")[0]

        return latest_commit_date

    def get_security_advisories(self, repo_name):
        """
        Checks if security advisories have been enabled for the GitHub repo using GitHub API.
        Security advisories are taken from the dependabot_alerts and dependabot_security_updates
        fields in the github json following documentation in
        https://docs.github.com/en/rest/code-security/configurations?apiVersion=2022-11-28#get-the-code-security-configuration-associated-with-a-repository

        Parameters
        ----------
            repo_name (str):
                Name of the repository.

        Returns
        -------
            dependabot_alerts_status (str):
                Dependabot status alerts (e.g., "enabled" or "disabled").
            dependabot_security_status (str):
                If dependabot alerts are set or not (e.g., "set" or "not_set").
            dependabot_alerts_enabled (bool):
                True if dependabot alerts are enabled, False otherwise.
            dependabot_security_updates_set (bool):
                True if dependabot security updates are set, False otherwise.
        """

        url = f"https://api.github.com/repos/{self.ORGANISATION}/{repo_name}/code-security-configuration"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"token {self.GITHUB_TOKEN}",
            "X-GitHub-Api-Version": "2022-11-28"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            security_config = response.json()

            dependabot_alerts_status = security_config.get("configuration", {}).get("dependabot_alerts", "N/A")
            dependabot_security_status = security_config.get("configuration", {}).get("dependabot_security_updates", "N/A")

        except requests.RequestException as e:
            print(f"Error checking security advisories for {repo_name}: {str(e)}")
            dependabot_alerts_status = "N/A"
            dependabot_security_status = "N/A"

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {str(e)}")
            dependabot_alerts_status = "N/A"
            dependabot_security_status = "N/A"

        # Create boolean variables based on the string status
        dependabot_alerts_enabled = (dependabot_alerts_status == "enabled")
        dependabot_security_updates_set = (dependabot_security_status == "set")

        return (dependabot_alerts_status,
                dependabot_security_status,
                dependabot_alerts_enabled,
                dependabot_security_updates_set)


    def check_requirements_file_in_python_app(self, repo_name):
        """
        Checks if 'requirements.txt' exists in a Python GitHub repo using GitHub API.

        Parameters
        ----------
        repo_name (str): Name of repo.

        Returns
        -------
        file_exists (bool): Shows if 'requirements.txt' exists in repo
        """

        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"token {self.GITHUB_TOKEN}"
        }

        try:
            # Check language of repo
            lang_url = f"https://api.github.com/repos/{self.ORGANISATION}/{repo_name}/languages"
            lang_response = requests.get(lang_url, headers=headers, timeout=10)
            lang_response.raise_for_status()

            languages = lang_response.json()

            # Check if Python is the main language
            if not languages or 'Python' not in languages:
                return False

            # List contents of repo (root directory)
            contents_url = f"https://api.github.com/repos/{self.ORGANISATION}/{repo_name}/contents"
            contents_response = requests.get(contents_url, headers=headers)
            contents_response.raise_for_status()

            contents = contents_response.json()

            # Search for 'requirements.txt' without case-sensitivity
            file_exists = any(
                item['type'] == 'file' and 'requirements.txt' == item['name'].lower()
                for item in contents
            )

        except requests.RequestException as e:
            logger.error(f"Error checking requirements.txt: {str(e)}")
            file_exists = NOT_APPLICABLE

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing API response: {str(e)}")
            file_exists = NOT_APPLICABLE

        return file_exists


    def orchestrate_app_compliance(self, list_apps, list_of_json_contents):
        """
        This calls the functions to get the compliance and then creates the dfs.


        Parameters
        ----------
            list_apps (list):
                list of dictionaries for app/applet with github repo details.
            list_of_json_contents (list):
                list of json contents of apps/applets


        Returns
        -------
            compliance_rows (list[dict])
                one compliance dict per app/applet.
            detail_rows (list[dict])
                one details dict per app/applet.
        """
        if len(list_apps) != len(list_of_json_contents):
            logger.error("Number of apps and list of json contents do not match.")
            raise AssertionError('List of apps and list of API jsons dont match')

        compliance_rows = []
        detail_rows = []

        for app, dxapp_contents in zip(list_apps, list_of_json_contents):
            # The dependabot and requirements results used to be appended to the
            # dataframe here, after the score denominator had already been
            # written - which is how bash apps ended up able to score 108%. They
            # are now part of the evidence and declared in the registry, so they
            # are counted like every other check.
            evidence = self.collect_evidence(app, dxapp_contents)
            outcome = run_all_checks(evidence)

            compliance_rows.append(outcome.compliance)
            detail_rows.append(outcome.details)

        return compliance_rows, detail_rows


    def compliance_scores_for_each_measure(self, df):
        """
        This function takes the compliance dataframe and
        creates a new dataframe summarising true/false for each measure.
        And then, calcualtes an overall compliance percentage for each measure.

        Parameters
        ----------
            df (pandas dataframe):
                dataframe of compliance booleans and score
                for each app/applet repo.

        Returns
        -------
            summary_df:
                dataframe of compliance scores for each performa.
        """
        available_columns = [
            'authorised_users',
            'authorised_devs',
            'uptodate_ubuntu',
            'timeout_policy',
            'correct_regional_option',
            'set_e',
            'no_manual_compiling',
            'dxapp_boolean',
            'eggd_name_boolean',
            'eggd_title_boolean',
            'dependabot_alerts_status',
            'dependabot_security_status',
            'requirements_file_exists'
        ]

        # Filter the DataFrame to include only the columns that are actually present
        df = df[[col for col in available_columns if col in df.columns]]

        columns_summed = []
        new_col_names = {
            'authorised_users': 'Auth Users',
            'authorised_devs': 'Auth Devs',
            'uptodate_ubuntu': 'Ubuntu 20+',
            'timeout_policy': 'Timeout Policy',
            'correct_regional_option': 'Correct Region',
            'set_e': '`set -e` Present',
            'no_manual_compiling': 'No Manual Compile',
            'dxapp_boolean': 'DNAnexus App',
            'eggd_name_boolean': 'eggd_ name',
            'eggd_title_boolean': 'eggd_ title',
            'dependabot_alerts_status': 'Dependabot alerts set',
            'dependabot_security_status': 'Dependabot security set',
            'requirements_file_exists': 'Requirements file exists'
        }

        for column in df.columns:
            # Get number of true and false values for compliance measures
            no_true = len(df[df[column] == True])
            no_false = len(df[df[column] == False])

            compliance_stats = {
                'Name': new_col_names.get(column, column),
                'No. Compliant / Total': f"{no_true}/{no_true + no_false}",
                'Compliance %': round((no_true / (no_true + no_false))*100, 2)
            }
            columns_summed.append(compliance_stats)

        summary_df = pd.DataFrame(columns_summed)
        summary_df = summary_df.sort_values(by=['Compliance %'])

        return summary_df


    def compliance_df_format(self, compliance_df, detailed_df):
        """
        This function takes the compliance dataframe
        and coerces it to a more readable format for datatables.

        Parameters
        ----------
            compliance_df (pandas dataframe):
                dataframe of compliance booleans and score
            detailed_df (pandas dataframe):
                dataframe of compliance details for each app/applet repo.

        Returns
        -------
            compliance_df (pandas dataframe):
                dataframe of compliance booleans and score
                with columns renamed and minus redundant columns.
            detailed_df (pandas dataframe):
                dataframe of compliance details for each app/applet repo
                with columns renamed.
        """

        # rename columns for displaying in datatables
        compliance_df.rename(columns={
            'compliance_score': 'compliance %',
            'authorised_users': 'Auth Users',
            'authorised_devs': 'Auth Devs',
            'interpreter': 'File Type',
            'uptodate_ubuntu': 'Ubuntu 20+',
            'timeout_policy': 'Timeout Policy',
            'correct_regional_option': 'Correct Region',
            'no_manual_compiling': 'No Manual Compile',
            'dxapp_or_applet': 'App or Applet',
            'eggd_name_boolean': 'eggd_ name',
            'eggd_title_boolean': 'eggd_ title',
            'latest_commit_date': 'Last Commit',
            'dependabot_alerts_status' : 'Dependabot alerts',
            'dependabot_security_status' : 'Dependabot security'
        }, inplace=True)

        detailed_df = detailed_df.rename(columns={
            'compliance_score': 'compliance %',
            'authorised_users': 'Auth Users',
            'authorised_devs': 'Auth Devs',
            'interpreter': 'File Type',
            'dist_version': 'Ubuntu Version',
            'regionalOptions': 'Regions',
            'correct_regional_option': 'Correct Region',
            'num_of_region_options': 'Total Regions',
            'no_manual_compiling': 'No Manual Compile',
            'asset_present': 'Assets',
            'dxapp_or_applet': 'App or Applet',
            'last_release_date': 'Last Release',
            'latest_commit_date': 'Last Commit',
            'timeout_setting': 'Timeout Setting',
            'dependabot_alerts_status' : 'Dependabot alerts',
            'dependabot_security_status' : 'Dependabot security'
        })

        compliance_df.drop(columns=['dxapp_boolean', 'timeout_setting',
                                    'last_release_date', 'total_performa',
                                    'compliance_count',
                                    ], inplace=True)

        detailed_df.drop(columns=['distribution', 'timeout', 'title',
                                  ], inplace=True)

        return compliance_df, detailed_df


class plotting:
    """
    Collection of plotting functions to use plotly to
    create plots of compliance performa for each app/applet repo.
    """

    def __init__(self):
        pass

    def import_csv(self, path_to_dataframe):
        """
        Imports csv files into pandas dataframe for plotting.
        Converts compliance column into float if present.

        Parameters
        ----------
            path_to_dataframe (str): string for absolute/relative path to
                csv file.
        Returns
        -------
            df (pandas dataframe):
                pandas dataframe of csv file with minor changes.
        """
        df = pd.read_csv(path_to_dataframe)

        return df

    def release_date_compliance_plot(self, df):
        """
        Convert date to datetime object.

        Parameters
        ----------
            df (dataframe):
                dataframe of apps/applets with release date and compliance score.

        Returns
        -------
            html_fig (plotly html plot object):
                html plot object of apps/applets with release date and compliance score.
        """
        # Convert release_date to pandas datetime column
        df['last_release_date'] = pd.to_datetime(df['last_release_date'])
        # Convert % column to numeric float column
        df_ordered = df.sort_values(by=['last_release_date'])

        fig = px.scatter(
            data_frame=df_ordered,
            x=df_ordered['last_release_date'],
            y=df_ordered['compliance_score'],
            labels={
                'last_release_date': 'Date of last release',
                'compliance_score': 'Compliance (%)'
            },
        )

        fig.update_layout(
            font=dict(
                size=18,  # Set the font size here
                color="black"
            )
        )

        html_fig = fig.to_html(full_html=True)

        return html_fig

    def compliance_by_latest_activity_plot(self, df):
        """
        Convert date to datetime object.

        Parameters
        ----------
            df (dataframe):
                dataframe of apps/applets with release date and compliance score.

        Returns
        -------
            html fig (plotly html plot):
                plot html object of apps/applets with release date and compliance score.
        """
        # Convert release_date to pandas datetime column
        df['latest_commit_date'] = pd.to_datetime(df['latest_commit_date'])
        # Convert % column to numeric float column
        df_ordered = df.sort_values(by=['latest_commit_date'])

        fig = px.scatter(
            data_frame=df_ordered,
            x=df_ordered['latest_commit_date'],
            y=df_ordered['compliance_score'],
            labels={
                'latest_commit_date': 'Date of last commit',
                'compliance_score': 'Compliance (%)'
            },
            trendline="lowess",
            hover_name="name",
            hover_data=["latest_commit_date"],
        )

        fig.update_layout(
            font=dict(
                size=18,
                color="black"
            )
        )

        html_fig = fig.to_html(full_html=True)

        return html_fig

    def ubuntu_compliance_timeseries(self, df):
        """
        Convert date to datetime object.

        Parameters
        ----------
            df (dataframe):
                dataframe of apps/applets with ubuntu version,
                release date, and compliance score.

        Returns
        -------
            html fig (plotly html plot):
                plot html object of apps/applets with release date,
                ubuntu version, and compliance score.
        """
        # Convert release_date to pandas datetime column
        df = df[df['interpreter'] == 'bash']
        df['last_release_date'] = pd.to_datetime(df['last_release_date'])
        # Convert % column to numeric float column
        df_ordered = df.sort_values(by=['last_release_date'])

        df_ordered['dist_version'] = df_ordered['dist_version'].astype('str')

        fig = px.scatter(
            data_frame=df_ordered,
            x=df_ordered['last_release_date'],
            y=df_ordered['compliance_score'],
            color=df_ordered['dist_version'],
            labels={
                'last_release_date': 'Date of last release',
                'compliance_score': 'Compliance (%)',
                'dist_version': 'Ubuntu version',
            },
            hover_name="name",
            hover_data=["last_release_date",
                        "dist_version"],
        )

        fig.update_layout(
            font=dict(
                size=18,
                color="black"
            )
        )

        html_fig = fig.to_html(full_html=True)

        return html_fig



def main():
    # Configure logging here rather than at import time.
    setup_logging()
    # Initialise class with shorthand
    audit = audit_class()
    plots = plotting()
    # API call to get all apps and check compliance to DNAnexus app standards.
    list_of_repos = audit.get_list_of_repositories(audit.ORGANISATION,
                                                   audit.GITHUB_TOKEN)
    print(f"Number of items: {len(list_of_repos)}")
    list_apps, list_of_json_contents = audit.select_apps(list_of_repos,
                                                         audit.GITHUB_TOKEN)
    compliance_df, detailed_df = audit.orchestrate_app_compliance(list_apps,
                                                                  list_of_json_contents)
    compliance_df, detailed_df = audit.compliance_stats(compliance_df,
                                                        detailed_df)

    # Create tables and plots for html report
    compliance_stats_summary = audit.compliance_scores_for_each_measure(
        compliance_df)
    release_comp_plot = plots.release_date_compliance_plot(compliance_df)
    ubuntu_comp_plot = plots.ubuntu_compliance_timeseries(detailed_df)
    compliance_bycommitdate_plot = plots.compliance_by_latest_activity_plot(
        compliance_df)
    compliance_df, detailed_df = audit.compliance_df_format(compliance_df,
                                                            detailed_df)
    get_template_render(compliance_df, detailed_df, compliance_stats_summary,
                        release_comp_plot, ubuntu_comp_plot,
                        compliance_bycommitdate_plot,
                        )


if __name__ == '__main__':
    main()
