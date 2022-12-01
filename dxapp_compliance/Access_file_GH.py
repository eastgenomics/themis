import base64
import json
import logging
import os
# Fastcore extends the python standard library to allow for the use of ghapi.
from math import ceil
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import requests
import statsmodels.api as sm
from fastcore.all import *
from ghapi.all import GhApi
from jinja2 import Environment, FileSystemLoader

# TODO: Add stats to parts of the html report and use bootrap to style it.
# TODO: Make report prettier with bootstrap.
# TODO: Add assetDepends to the report.
# TODO: Add list of repos without releases to report. (datatables)
# TODO: Add instance type to the report.

# Set-up
# Remove warnings from pandas which aren't relevant.
pd.options.mode.chained_assignment = None
# Set file path for root directory
ROOT_DIR = Path(__file__).absolute().parents[1]
# Create format for logging errors in API queries
LOG_FORMAT = (
    "%(asctime)s — %(name)s — %(levelname)s"
    " — %(lineno)d — %(message)s"
)  # TODO: Change log format
# Set level to debug, format with date and time and re-write file each time
logging.basicConfig(
    filename=ROOT_DIR.joinpath('dxapp_compliance/dx_compliance.log'),
    level=logging.INFO,
    format=LOG_FORMAT,
    filemode='w'
)
# Set up logger
logger = logging.getLogger("general log")


def get_template_render(compliance_df, detailed_df, compliance_stats_summary,
                        release_comp_plot, ubuntu_comp_plot,
                        compliance_bycommitdate_plot,
                        ubuntu_versions_plot
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
    ubuntu_versions_plot (plotly figure):
        Plotly figure of bar plot of counts of ubuntu versions used in apps.

    Outputs
    -------
    .html
        HTML report of all app compliances.
        Including tables of compliance stats and plots.
    """
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("Report.html")
    filename = "Audit_2022_11_30.html"
    compliance_html = compliance_df.to_html(table_id="comp")
    details_html = detailed_df.to_html(table_id="details")

    compliance_stats_summary_html = compliance_stats_summary.to_html(
        justify="left", table_id="compliance_stats_summary",
        classes="table table-success table-striped table-hover", index=False,
    )
    compliance_stats_summary_html = compliance_stats_summary_html.replace('dataframe', '')
    context = {
        "Compliance_table": compliance_html,
        "Details_table": details_html,
        "compliance_stats_summary": compliance_stats_summary_html,
        "release_comp_plot": release_comp_plot,
        "ubuntu_comp_plot": ubuntu_comp_plot,
        "compliance_bycommitdate_plot": compliance_bycommitdate_plot,
        "ubuntu_versions_plot": ubuntu_versions_plot,
        "compliance_stats_dict": compliance_stats_dict,
    }
    with open(filename, mode="w", encoding="utf-8") as results:
        results.write(template.render(context))
        print(f"... wrote {filename}")


def get_config():
    """
    Extracts the config from the json config file.

    Returns:
    github_token (str):
        Github API token for authentication.

    organisation (str):
        Organisation dnanexus username.

    default_region (str):
        Default region for running apps in DNANexus.

    """
    with open('CONFIG.json') as file:
        config = json.load(file)
        github_token = config.get('GITHUB_TOKEN')
        organisation = config.get('organisation')
        default_region = config.get('default_region')

    return github_token, organisation, default_region


class compliance_checks:
    """
    Class for all the checks for the compliance against DNAnexus performa.
    """

    def check_all(self, app=None, dxjson_content="",
                  src_file_contents="",
                  last_release_date=None,
                  latest_commit_date=None,
                  default_region=None,):
        """
        checks all compliance measures for an app
        against Eastgenomics DNAnexus App standards.

        Parameters
        ----------
        app (GithubAPI app object):
                Github API repository object used for extracting app/applet info.
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.
        src_file_contents (str):
                str with the app source code file.
        last_release_date (str):
                the date of the last release for the app/applet.
        latest_commit_date (str):
            the date of the latest commit for the app/applet.
        default_region (str):
                default region to check against.


        Returns:
        -------
            compliance_dict (dict):
                dict of compliance booleans for the app/applet.
            details_dict (dict):
                dict of compliance details for the app/applet.
        """
        # Find compliance for app/applet
        app_boolean, app_or_applet = self.check_app_compliance(
            app, dxjson_content
        )
        name, title, eggd_name_boolean, eggd_title_boolean = self.check_naming_compliance(
            dxjson_content
        )
        interpreter, distribution, dist_version, uptodate_ubuntu = self.check_interpreter_compliance(
            dxjson_content
        )
        region_list, correct_regional_boolean, \
            region_options_num = self.check_region_compliance(
                dxjson_content,
                default_region
            )
        # Convert list of regions to more readable string
        regions = " ".join([x.split(':')[1].rstrip("']") for x in region_list])


        set_e_boolean, no_manual_compiling, asset_present = self.check_src_file_compliance(
            dxjson_content, src_file_contents)
        timeout_policy, timeout_setting = self.check_timeout(
            dxjson_content
        )
        authorised_users, authorised_devs, \
            auth_devs_boolean, auth_users_boolean = self.check_users_and_devs(
                dxjson_content
            )

        # Construct dicts to return data.
        compliance_dict = {'name': name,
                           'authorised_users': auth_users_boolean,
                           'authorised_devs': auth_devs_boolean,
                           'interpreter': interpreter,
                           'uptodate_ubuntu': uptodate_ubuntu,
                           'timeout_policy': timeout_policy,
                           'correct_regional_option': correct_regional_boolean,
                           'num_of_region_options': region_options_num,
                           'set_e': set_e_boolean,
                           'no_manual_compiling': no_manual_compiling,
                           'dxapp_boolean': app_boolean,
                           'dxapp_or_applet': app_or_applet,
                           'eggd_name_boolean': eggd_name_boolean,
                           'eggd_title_boolean': eggd_title_boolean,
                           'last_release_date': last_release_date,
                           'latest_commit_date': latest_commit_date,
                           'timeout_setting': timeout_setting,
                           }

        details_dict = {'name': name,
                        'authorised_users': authorised_users,
                        'authorised_devs': authorised_devs,
                        'interpreter': interpreter,
                        'distribution': distribution,
                        'dist_version': dist_version,
                        'regionalOptions': regions,
                        'title': title,
                        'timeout': timeout_policy,
                        'set_e': set_e_boolean,
                        'no_manual_compiling': no_manual_compiling,
                        'asset_present': asset_present,
                        'dxapp_or_applet': app_or_applet,
                        'last_release_date': last_release_date,
                        'latest_commit_date': latest_commit_date,
                        'timeout_setting': timeout_setting,
                        }

        return compliance_dict, details_dict

    def check_region_compliance(self, dxjson_content, default_region=None):
        """
        Checks compliance for regional settings for DNAnexus app performa.
        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.
            default_region (str):
                default region to check against.


        Returns:
        -------
            regions (list):
                list of regional options for the app/applet.
            correct_regional_boolean (boolean):
                True/False whether only the correct region is selected
            num_regions (int):
                The number of regional options set for dnanexus cloud servers,
                this should be 1 and set to the correct region.
        """
        # Find Region options for cloud servers.
        region = dxjson_content.get('regionalOptions', {})
        region_list = list(region.keys())
        num_regions = len(region_list)

        # regional options compliance info.
        if region_list is [default_region] or 'aws:eu-central-1' in region_list:
            correct_regional_boolean = True
        elif region_list is [] or num_regions == 1:
            correct_regional_boolean = False
            logger.info("Incorrect regional option set.")
        else:
            correct_regional_boolean = False
            logger.info(
                "Incorrect regional option set and multiple regions present.")

        return region_list, correct_regional_boolean, num_regions

    def check_timeout(self, dxjson_content):
        """
        Checks compliance for timeout settings for DNAnexus app performa.
        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.


        Returns:
        -------
        timeout_policy (boolean):
            True/False whether timeout policy is set in dxapp.json.
        timeout_setting (str):
            The timeout setting for the app. i.e. {'hours': 12}.
        """
        data = dxjson_content
        # Timeout policy compliance info.
        timeout_policy_dict = data.get('runSpec', {}).get(
            'timeoutPolicy', {}).get('*', {})
        # If any keys are present then there is a timeout
        # However, this could still be an inappropiate number i.e. 100 hours.
        if not timeout_policy_dict:
            timeout_policy = False
        else:
            timeout_policy = True

        timeout_setting = []
        list_of_time_units = {'days': 'd', 'hours': 'h', 'minutes': 'm'}
        for key in timeout_policy_dict.keys():
            time_nomenclature = str(key)
            time = data.get('runSpec', {}).get(
                'timeoutPolicy', {}).get('*', {}).get(f'{time_nomenclature}')
            # set timeout setting to a string in format of 1d, 30m, 12hrs.
            time_shorthand = list_of_time_units.get(time_nomenclature, ' ')
            timeout_setting.append(f"{time}{time_shorthand}")
        if len(timeout_setting) == 0:
            timeout_setting = None
        elif len(timeout_setting) == 1:
            timeout_setting = timeout_setting[0]
        else:
            timeout_setting = ", ".join(timeout_setting)

        return timeout_policy, timeout_setting

    def check_app_compliance(self, app, dxjson_content):
        """
        Checks if it is an app/applet.
        Parameters
        ----------
            app (GithubAPI app object):
                Github API repository object used for extracting app/applet info.
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.

        Returns:
        -------
            app_boolean (boolean):
                True/False whether the repo is a dnanexus app.
            app_or_applet (str):
                Whether the repo is an 'app' or 'applet'.
        """
        # Find compliance for app
        # Initialise variables - prevents not referenced before assignment error
        app_boolean = app_or_applet = None

        if 'version' in dxjson_content.keys():
            app_or_applet = "app"
            app_boolean = True
            logger.info(f"App: {app.name}")
        elif "_v" in app.get('name'):
            app_or_applet = "applet"
            app_boolean = False
            logger.info(f"Applet: {app.name}")
        else:
            logger.info(f"App or applet not clear. See app/applet here {app}")
            # Likely still applet - So set to applet/false.
            app_or_applet = "applet"
            app_boolean = False

        return app_boolean, app_or_applet

    def check_src_file_compliance(self, dxjson_content, src_file_contents):
        """
        Checks compliance for set -e exit option and manual compiling settings
        for DNAnexus app performa.

        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.
            src_file_contents (str):
                str with the app source code file.

        Returns:
        -------
            set_e_boolean (boolean):
                True/False whether only the set -e option is used.
            no_manual_compliance (boolean):
                True/False whether only the app doesn't manually compile.
        """
        set_e_boolean = no_manual_compiling = None
        interpreter = dxjson_content.get('runSpec', {}).get('interpreter', '')
        # Assets present in dxapp.json
        if dxjson_content.get('assetsDepends', {}):
            asset_present = True
        else:
            asset_present = False
        # Check for set -e option and manual compiling in src file.
        if 'python' in interpreter:
            set_e_boolean = "NA"
            no_manual_compiling = "NA"
        else:
            # Checks for only BASH apps
            # src file compliance info.
            match_set_e = re.search(
                r"set[\ \-exo]+", src_file_contents
            )
            if match_set_e:
                set_e_boolean = True
            else:
                set_e_boolean = False
            match_manual_compiling = re.search(
                r".*make install.*", src_file_contents
            )
            if match_manual_compiling:
                no_manual_compiling = False
            else:
                no_manual_compiling = True

        return set_e_boolean, no_manual_compiling, asset_present

    def check_interpreter_compliance(self, dxjson_content):
        """
        Checks compliance for ubuntu version for bash-based app/applets.

        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.

        Returns:
        -------
            interpreter (str):
                The interpreter used for the app. i.e. bash or python
            distribution (str):
                If the interpreter is bash.
                The ubuntu distribution used for the app. i.e. ubuntu
            dist_version (str):
                The ubuntu version used for the app.
            uptodate_ubuntu (boolean):
                True/False whether the bash app
                uses an up-to-date version of ubuntu.
        """
        data = dxjson_content
        dist_version = None
        # interpreter compliance info.
        interpreter = data.get('runSpec', {}).get('interpreter', '')
        distribution = data.get('runSpec', {}).get('distribution')
        if interpreter == 'bash':
            dist_version = 0
            dist_version = float(data.get('runSpec', {}).get('release', ''))
            if dist_version >= 20:
                uptodate_ubuntu = True
            else:
                uptodate_ubuntu = False
        elif 'python' in interpreter:
            uptodate_ubuntu = "NA"
        else:
            logger.info(f"Interpreter not found. Interpreter: {interpreter}")

        return interpreter, distribution, dist_version, uptodate_ubuntu

    def check_users_and_devs(self, dxjson_content):
        """
        Checks compliance for user and developer settings
        for DNAnexus app performa.

        Currently checks if the user is only for org-emee_1
        but could change to contains the org-emee_1 user.

        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.

        Returns:
        -------
            authorised_users (list):
                List of users who can run the app.
            authorised_devs (list):
                List of users who can develop the app.
            auth_devs_boolean (boolean):
                True/False whether the right developers are set in dxapp.json
            auth_users_boolean (boolean):
                True/False whether the right users are set in dxapp.json
        """
        auth_devs_boolean = auth_users_boolean = None
        # auth devs & users
        authorised_users = dxjson_content.get('authorizedUsers')
        authorised_devs = dxjson_content.get('developers')

        if authorised_users == ['org-emee_1']:
            auth_users_boolean = True
        else:
            auth_users_boolean = False

        if authorised_devs == ['org-emee_1']:
            auth_devs_boolean = True
        else:
            auth_devs_boolean = False

        return authorised_users, authorised_devs, auth_devs_boolean, auth_users_boolean

    def check_naming_compliance(self, dxjson_content):
        """
        Checks compliance for app/applet naming compliance
        against the DNAnexus app performa.

        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.

        Returns:
        -------
            eggd_name_boolean (boolean):
                True/False whether the app/applet name conforms
                to the 'eggd_' prefix criterium.
            eggd_title_boolean (boolean):
                True/False whether the app/applet title conforms
                to the 'eggd_' prefix criterium.
        """
        # get app name and title
        name = dxjson_content.get('name')
        title = dxjson_content.get('title')
        if not name:
            eggd_name_boolean = False
        elif name.startswith('eggd'):
            eggd_name_boolean = True
        else:
            eggd_name_boolean = False
        if not title:
            eggd_title_boolean = False
        elif title.startswith('eggd'):
            eggd_title_boolean = True
        else:
            eggd_title_boolean = False

        return name, title, eggd_name_boolean, eggd_title_boolean


class audit_class:
    """
    A class for all dx querying functions.
    This collects all the compliance data needed for the audit app.
    """

    def __init__(self):
        # Set config
        self.GITHUB_TOKEN, self.ORGANISATION, self.DEFAULT_REGION = get_config()

    def check_file_compliance(self, app, dxjson_content):
        """
        This checks the compliance of each app/applet against the performa guidelines.
        This includes checking compliance using the dxapp.json file.
        (dxapp.json = the app/applet settings file)

        Parameters
        ----------
            app (GithubAPI app object):
                Github API repository object used for extracting app/applet info.
            dxjson_content (dict):
                contents of the dxapp.json file for the app.

        Returns:
            compliance_df (pandas dataframe):
                dataframe of compliance booleans for the app/applet.
            df_details (pandas dataframe):
                dataframe of compliance details for the app/applet.
        """

        # Find source for app/applet and check compliance
        src_file_contents, last_release_date, latest_commit_date = self.get_src_file(
            app=app,
            dxjson_content=dxjson_content,
            organisation_name=self.ORGANISATION,
            github_token=self.GITHUB_TOKEN)
        # Run all compliance checks
        checks = compliance_checks()
        compliance_dict, details_dict = checks.check_all(
            app=app, dxjson_content=dxjson_content,
            src_file_contents=src_file_contents,
            last_release_date=last_release_date,
            latest_commit_date=latest_commit_date,
            default_region=self.DEFAULT_REGION
        )
        # compliance dataframe
        df_compliance = pd.DataFrame.from_dict(compliance_dict,
                                               orient='index')
        df_compliance = df_compliance.transpose()

        # details dataframe
        df_details = pd.DataFrame.from_dict(details_dict,
                                            orient='index')
        df_details = df_details.transpose()
        # transpose fixes value error for length differences

        return df_compliance, df_details

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
        total_num_repos = org_details['public_repos'] + \
            org_details['total_private_repos']
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

        Returns:
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
            lambda x: 10 if 'bash' in x else 7)
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
            compliance_df (dataframe)
                df of apps/applets with compliance stats.
            detailed_df (dataframe)
                df of apps/applets with detailed information.
        """
        compliance_df = detailed_df = None
        if len(list_apps) != len(list_of_json_contents):
            logger.error(
                "Number of apps and list of json contents do not match.")
            raise AssertionError('List of apps and list of API jsons dont match')

        for index, (app, dxapp_contents) in enumerate(zip(list_apps, list_of_json_contents)):
            # The first item creates the dataframe
            if index == 0:
                df_repo, df_repo_details = self.check_file_compliance(
                    app, dxapp_contents)
                compliance_df = df_repo
                detailed_df = df_repo_details
            else:
                df_repo, df_repo_details = self.check_file_compliance(
                    app, dxapp_contents)
                compliance_df = pd.concat([compliance_df,
                                           df_repo], ignore_index=True)
                detailed_df = pd.concat([detailed_df,
                                        df_repo_details], ignore_index=True)

        return compliance_df, detailed_df

    def conditional_formatting(self, val):
        if val < 40:
            color = '#FFB3BA' # Red
        elif val >= 40 and val < 80:
            color = '#FFBF00' # Yellow
        else:
            color = '#BAFFC9' # Green
        return 'background-color: {}'.format(color)


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
        df = df[[
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
        ]]
        columns_summed = []
        new_col_names = {
            'authorised_users': 'Auth Users',
            'authorised_devs': 'Auth Devs',
            'uptodate_ubuntu': 'Ubuntu 20+',
            'timeout_policy': 'Timeout Policy',
            'correct_regional_option': 'Correct Region',
            'set_e': 'Set -e Present',
            'no_manual_compiling': 'No Manual Compile',
            'dxapp_boolean': 'DNAnexus App',
            'eggd_name_boolean': 'eggd_ name',
            'eggd_title_boolean': 'eggd_ title',
        }
        for column in df:
            # Get number of true and false values for compliance measures
            no_true = len(df.query(f'{column} == True'))
            no_false = len(df.query(f'{column} == False'))

            complaince_stats = {
                'Name': new_col_names[column],
                'Compliance %': round((no_true / (no_true + no_false))*100, 1)
            }
            columns_summed.append(complaince_stats)
        summary_df = pd.DataFrame(columns_summed)
        summary_df = summary_df.sort_values(by=['Compliance %'])

        return summary_df, columns_summed

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
                'x': 'Date of last release',
                'y': 'Compliance (%)'
            },
        )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

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
                'x': 'Date of last release',
                'y': 'Compliance (%)'
            },
            trendline="lowess",
        )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

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
                'x': 'Date of last release',
                'y': 'Compliance (%)',
                'color': 'Ubuntu version',
            },
            hover_name="name",
            hover_data=["last_release_date",
                        "dist_version"],
        )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

        return html_fig

    def bash_version(self, df):
        """
        Uses plotly to plot the distribution of bash ubuntu versions.

        Parameters
        ----------
            df (pandas df): dataframe of apps compliance data.

        Returns
        -------
            html_fig (plotly html plot):
                plot of bash and python apps.
        """
        # Find all apps with bash as the interpreter
        dfout = df[df['interpreter'] == 'bash']
        dfout.sort_values(by=['dist_version'])
        dfout["dist_version"] = dfout["dist_version"].values.astype('str')
        dfout = dfout['dist_version'].value_counts().rename_axis(
            'unique_versions').reset_index(name='counts')

        fig = px.bar(dfout, x="unique_versions", y="counts",
                     color="unique_versions",
                     labels={
                         "unique_versions": "Ubuntu version",
                         "counts": "Count",
                     },
                     title="Version of Ubuntu by bash apps",
                     )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

        return html_fig
        # TODO: Add a bar chart for other compliance stats.


def main():
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
    compliance_stats_summary, compliance_stats_dict = audit.compliance_scores_for_each_measure(
        compliance_df)
    release_comp_plot = plots.release_date_compliance_plot(compliance_df)
    ubuntu_comp_plot = plots.ubuntu_compliance_timeseries(detailed_df)
    compliance_bycommitdate_plot = plots.compliance_by_latest_activity_plot(
        compliance_df)
    ubuntu_versions_plot = plots.bash_version(detailed_df)
    compliance_df, detailed_df = audit.compliance_df_format(compliance_df,
                                                            detailed_df)
    get_template_render(compliance_df, detailed_df, compliance_stats_summary,
                        release_comp_plot, ubuntu_comp_plot,
                        compliance_bycommitdate_plot,
                        ubuntu_versions_plot,
                        compliance_stats_dict
                        )


if __name__ == '__main__':
    main()
