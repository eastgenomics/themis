import base64
import json
import requests
import os
from pathlib import Path
from ghapi.all import GhApi
from fastcore.all import *
# Fastcore extends the python standard library to allow for the use of ghapi.
from math import ceil
import pandas as pd
import numpy as np
from jinja2 import Environment, FileSystemLoader
# import plotly.graph_objects as go
import plotly.express as px
import statsmodels.api as sm
import logging

## TODO: Add stats to parts of the html report and use bootrap to style it.
## TODO: Make report prettier with bootstrap.
## TODO: Add assetDepends to the report.
## TODO: Add list of repos without releases to report. (datatables)
## TODO: Add ... to the report.

# Set-up
# Remove warnings from pandas which aren't relevant.
pd.options.mode.chained_assignment = None
# Set file path for root directory
ROOT_DIR = Path(__file__).absolute().parents[1]
# Create format for logging errors in API queries
LOG_FORMAT = (
    "%(asctime)s — %(name)s — %(levelname)s"
    " — %(lineno)d — %(message)s"
) # TODO: Change log format
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
    """
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("Report.html")
    filename = "Audit_2022_11_08.html"
    # Remove old index column
    compliance_df.drop(columns=['Unnamed: 0'], inplace=True)
    detailed_df.drop(columns=['Unnamed: 0'], inplace=True)
    compliance_html = compliance_df.to_html(table_id="comp")
    details_html = detailed_df.to_html(table_id="details")
    compliance_stats_summary_html = compliance_stats_summary.to_html(
        table_id="compliance_stats_summary"
    )
    context = {
        "Compliance_table": compliance_html,
        "Details_table": details_html,
        "compliance_stats_summary": compliance_stats_summary_html,
        "release_comp_plot": release_comp_plot,
        "ubuntu_comp_plot": ubuntu_comp_plot,
        "compliance_bycommitdate_plot": compliance_bycommitdate_plot,
        "ubuntu_versions_plot": ubuntu_versions_plot,
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
        check_all compliance measures.
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
        regions = None
        if len(region_list) == 1:
            region_list = region_list[0]
            regions = region_list.split(":")[1].rstrip("']")
        elif len(region_list) > 1:
            regions = []
            for region in region_list:
                region_str = region.split(":")[1].rstrip("']")
                regions.append(region_str)
        else:
            regions = region_list
        set_e_boolean, no_manual_compiling = self.check_src_file_compliance(dxjson_content, src_file_contents)
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
                        # 'release_version': data.get('properties', {}).get(),
                        'authorised_users': authorised_users,
                        'authorized_devs': authorised_devs,
                        'interpreter': interpreter,
                        'distribution': distribution,
                        'dist_version': dist_version,
                        'regionalOptions': regions,
                        'title': title,
                        'timeout': timeout_policy,
                        'set_e': set_e_boolean,
                        'no_manual_compiling': no_manual_compiling,
                        'dxapp_or_applet': app_or_applet,
                        'last_release_date': last_release_date,
                        'latest_commit_date': latest_commit_date,
                        'timeout_setting': timeout_setting,
                        # 'description': data.get('summary'),
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
            correct_regional_boolean (boolean):
                True/False whether only the correct region is selected
            region_options_num (int):
                The number of regional options set for dnanexus cloud servers,
                this should be 1 and set to the correct region.
        """
        # Find Region options for cloud servers.
        region = dxjson_content.get('regionalOptions', {})
        region_list = list(region.keys())
        num_regions = len(region_list)

        # regional options compliance info.
        if region_list is [default_region]:
            correct_regional_boolean = True
            region_options_num = num_regions
        elif 'aws:eu-central-1' in region_list:
            correct_regional_boolean = True
            region_options_num = num_regions
        elif region_list is []:
            correct_regional_boolean = False
            region_options_num = 0
            logger.info("No regional options set.")
        elif num_regions == 1:
            correct_regional_boolean = False
            region_options_num = num_regions
            logger.info("Incorrect regional option set.")
        else:
            correct_regional_boolean = False
            region_options_num = num_regions
            logger.info("Incorrect regional option set and multiple regions present.")

        return region_list, correct_regional_boolean, region_options_num


    def check_timeout(self, dxjson_content):
        """
        Checks compliance for timeout settings for DNAnexus app performa.
        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.


        Returns:
        -------
            correct_regional_boolean (boolean):
                True/False whether only the correct region is selected
            region_options_num (int):
                The number of regional options set for dnanexus cloud servers,
                this should be 1 and set to the correct region.
        """
        data = dxjson_content
        # Timeout policy compliance info.
        timeout_policy_dict = data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {})
        # If any keys are present then there is a timeout
        # However, this could still be an inappropiate number i.e. 100 hours.
        if timeout_policy_dict is {} or not timeout_policy_dict:
            timeout_policy = False
        else:
            timeout_policy = True

        timeout_setting = {}
        for key in timeout_policy_dict.keys():
            time_nomenclature = str(key)
            timeout_setting[f'{time_nomenclature}'] = data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get(f'{time_nomenclature}')

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
            region_options_num (int):
                The number of regional options set for dnanexus cloud servers,
                this should be 1 and set to the correct region.
        """
        # Find compliance for app
        # Initialise variables - prevents not referenced before assignment error
        app_boolean, app_or_applet = None, None

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
        set_e_boolean, no_manual_compiling = None, None
        interpreter = dxjson_content.get('runSpec', {}).get('interpreter', '')
        if 'python' in interpreter:
            set_e_boolean = "NA"
            no_manual_compiling = "NA"
        else:
            # Checks for only BASH apps
            # src file compliance info.
            if "set -e" in src_file_contents:
                set_e_boolean = True
            else:
                set_e_boolean = False
            if "make install" in src_file_contents:
                no_manual_compiling = False
            else:
                no_manual_compiling = True

        return set_e_boolean, no_manual_compiling


    def check_interpreter_compliance(self, dxjson_content):
        """
        Checks compliance for ubuntu version for bash-based app/applets.

        Parameters
        ----------
            dxjson_content (dict):
                dictionary with all the information on dxapp.json details.

        Returns:
        -------
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
            print("interpreter not found")
            logger.info(f"Interpreter: {interpreter}")

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
            auth_devs_boolean (boolean):
                True/False whether the right developers are set in dxapp.json
            auth_users_boolean (boolean):
                True/False whether the right users are set in dxapp.json
        """
        auth_devs_boolean, auth_users_boolean = None, None
        # auth devs & users
        authorised_users = dxjson_content.get('authorizedUsers')
        authorised_devs = dxjson_content.get('developers')

        if not authorised_users: # authorised_users is None
            auth_users_boolean = False
        elif authorised_users == ['org-emee_1']:
            auth_users_boolean = True
        else:
            auth_users_boolean = False

        if not authorised_devs: # authorised_devs is None
            auth_devs_boolean = False
        elif authorised_devs == ['org-emee_1']:
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
        # name compliance info.
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
        check_file_compliance

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
        # compliance_checks.
        checks = compliance_checks()
        compliance_dict, details_dict = checks.check_all(
            app=app, dxjson_content=dxjson_content,
            src_file_contents=src_file_contents,
            last_release_date=last_release_date,
            latest_commit_date = latest_commit_date,
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
        print(org_details)
        total_num_repos = org_details['public_repos'] + org_details['total_private_repos']
        logger.info(total_num_repos)
        print(total_num_repos)
        per_page_num = 30
        pages_total = ceil(total_num_repos/per_page_num)  # production line
        # pages_total = 1  # testing line
        all_repos = []

        for page in range(1, pages_total+1):
            response = api.repos.list_for_org(org=org_username,
                                              per_page=per_page_num,
                                              page=page)
            response_repos = [repo for repo in response]
            all_repos = all_repos + response_repos

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

                # Checks to find dxapp.json which determines if repo is an app.
                try:
                    contents = api.repos.get_content(owner, repo_name, file_path)
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
                    print(f"Other encoding used. {file_content_encoding}")
            elif repo['archived'] is True:
                logger.info(f'{repo["name"]} is archived.')
                print(f"{repo['name']} is archived.")
                continue
            else:
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
        src_code_content = None
        src_content_decoded = ""
        api = GhApi(token=github_token)
        contents = None
        repo_name = app.get('name')
        file_path = 'src/'

        # Extract src file
        try:
            contents = api.repos.get_content(organisation_name,
                                             repo_name,
                                             file_path)
        except HTTP404NotFoundError:
            logger.info(f'{repo_name} No src file found.')


        repos_apps.append(dxjson_content)
        if contents:
            for content in contents:
                if (content['type'] == 'file' and
                    r'.sh' in content['name'] or
                    r'.py' in content['name']):
                    try:
                        file_path = content['path']
                        app_src_file = api.repos.get_content(organisation_name,
                                                             repo_name,
                                                             file_path)

                    except HTTP404NotFoundError:
                        logger.info(f'{repo_name} 404 No src file found.')
                        pass
                    src_code_content = app_src_file['content']
                    code_content_encoding = app_src_file.get('encoding')
                    if code_content_encoding == 'base64':
                        src_content_decoded = base64.b64decode(
                            src_code_content
                            ).decode()
                    else:
                        logger.error("Other encoding used.")
        logger.info(repo_name)

        # Get the latest release date & commit date
        last_release_date = ""  # None
        last_release_date = self.get_latest_release(
            organisation_name, repo_name, github_token
            )
        latest_commit_date = ""  # None
        latest_commit_date = self.get_latest_commit_date(
            organisation_name, repo_name, github_token
            )

        return src_content_decoded, last_release_date, latest_commit_date


    def summary_compliance_stats(self, compliance_df):
        """
        compliance_stats
        Calculates stats for overall compliance of each app/applet.
        In progress, requires more work.
        Purpose: make a small summary table
        for overall compliance of each category.

        Parameters
        ----------
            compliance_df (dataframe):
                dataframe of app/applet dxapp.json contents.
        Returns
        -------
            None (currently)
        """
        # Find out how many repos have eggd in the name and title
        compliance_df = compliance_df.fillna(value=np.nan)

        print(compliance_df.groupby('timeout'))
        print(compliance_df.groupby('timeout').size())

        print(compliance_df['timeout'].isna().sum())
        total_rows = len(compliance_df)
        print(total_rows)

        # Counts number of repos with each dist_version
        print(compliance_df.groupby(['dist_version'])['dist_version'].count())
        version_complaince = (compliance_df.groupby(['dist_version'])['dist_version'].count()['20.04']/total_rows)*100
        print(version_complaince)

        # Counts number of repos with each dist
        print(compliance_df.groupby(['distribution'])['distribution'].count())

        # Find number with correct name/title compliance
        eggd_match = compliance_df[compliance_df['name'].str.match('eggd')]
        print(eggd_match)
        eggd_stat = round((len(eggd_match)/total_rows)*100, 2)
        print(eggd_stat)
        # Correct version of ubuntu
        compliance_df.info()
        bash_apps = compliance_df[compliance_df['interpreter'].str.match('bash')]
        print(bash_apps)
        ubuntu_version_stat = ((bash_apps.groupby(['dist_version'])['dist_version'].count()['20.04'])/total_rows) * 100
        print(ubuntu_version_stat)
        # Correct regional options - aws:eu-central-1 present.
        selection = ['aws:eu-central-1']
        mask = compliance_df.regionalOptions.apply(lambda x: any(item for item in selection if item in x))
        region_set_apps = compliance_df[mask]
        print(region_set_apps)
        compliance_df = compliance_df['title'].dropna()
        eggd_title_match = compliance_df[compliance_df.str.match('eggd')]
        print(eggd_title_match)
        eggd_stat = round((len(eggd_title_match)/total_rows)*100, 2)
        print(eggd_stat)


    def compliance_stats(self, compliance_df, details_df):
        """
        compliance_stats
        Finds the % compliance with the EastGLH guidelines for each app/applet.

        Parameters
        ----------
            compliance_df (pandas dataframe):
                dataframe of compliance booleans for each app/applet.

        Returns
        -------
            compliance_df (pandas dataframe):
                dataframe of compliance booleans for each app/applet.
                with added compliance % column.
            details_df (pandas dataframe):
                dataframe of compliance performa details for each app/applet.
        """
        list_of_compliance_scores = []
        # remove columns that are not compliance checks
        compliance_checks_df = compliance_df.drop(columns=['name',
                                                           'num_of_region_options',
                                                           'last_release_date',
                                                           'latest_commit_date',
                                                           'timeout_setting',
                                                           ])
        # Find the % overall compliance for each app/applet
        for _, row in compliance_checks_df.iterrows():
            compliance_count = 0
            # Set number of total performa relevant to the repo.
            if 'bash' in row['interpreter']:
                total_performa = 9
            else:
                total_performa = 8
            # Count the number of True values in each row
            for column in row:
                if column is True:
                    compliance_count += 1
                else:
                    pass
            # Calculate the % compliance
            compliance_score = f"{round((compliance_count/total_performa)*100, 1)}%"
            list_of_compliance_scores.append(compliance_score)

        compliance_df.insert(loc=1,
                             column='compliance_score',
                             value=list_of_compliance_scores)
        details_df.insert(loc=1,
                          column='compliance_score',
                          value=list_of_compliance_scores)

        return compliance_df, details_df


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
            last_commit_date (str):
                string of latest commit date.
        """
        api = GhApi(token=token)

        # List all branches
        list_of_shas = []
        try:
            list_of_branches = api.repos.list_branches(organisation_name, repo_name)
        except HTTP404NotFoundError:
            logger.error(f'{repo_name} 404 No branches found.')
            list_of_branches = []

        # Find sha for each branch and append to list for api calls.
        for branch in list_of_branches:
            sha = branch['commit']['sha']
            list_of_shas.append(sha)

        # For branch in branches
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
        orchestrate_app_compliance
        This calls the functions to get the compliance and then creates the dfs.

        Parameters
        ----------
            list_of_json_contents (list):
                list of json contents of apps/applets

        Returns
        -------
            compliance_df (dataframe)
                df of apps/applets with compliance stats.
            details_df (dataframe)
                df of apps/applets with detailed information.
        """
        compliance_df, details_df = None, None
        if len(list_apps) != len(list_of_json_contents):
            print("Error missing repos")
        else:
            pass

        for index, (app, dxapp_contents) in enumerate(zip(list_apps, list_of_json_contents)):
            # The first item creates the dataframe
            if index == 0:
                df_repo, df_repo_details = self.check_file_compliance(app, dxapp_contents)
                compliance_df = df_repo
                details_df = df_repo_details
            else:
                df_repo, df_repo_details = self.check_file_compliance(app, dxapp_contents)
                compliance_df = pd.concat([compliance_df,
                                           df_repo], ignore_index=True)
                details_df = pd.concat([details_df,
                                        df_repo_details], ignore_index=True)

        return compliance_df, details_df


    def compliance_scores_for_each_measure(self, df):
        """
        compliance_scores_for_each_measure
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
        # df.fillna(value=False, inplace=True)
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
        for column in df:
            # Get number of true and false values for compliance measures
            no_true = len(df.query(f'{column} == True'))
            no_false = len(df.query(f'{column} == False'))
            len_df = len(df)

            # Get all non-boolean columns
            non_boolean_rows = df.query(f'{column} != True').query(f'{column} != False')

            # Num_Nas = df[f'{column}'].isna().sum() # Alternative for nas only.
            num_non_boolean_rows = len(non_boolean_rows)
            total = len_df - num_non_boolean_rows
            complaince_stats = {
                'name': column,
                'true': no_true,
                'false': no_false,
                'total': total,
                'compliance_percentage': round((no_true/(no_true + no_false))*100, 2)
            }
            columns_summed.append(complaince_stats)
        summary_df = pd.DataFrame(columns_summed)

        return summary_df

class plotting:
    """
    Collection of plotting functions to use plotly to
    create plots of compliance performa for each app/applet repo.
    """

    def __init__(self):
        pass


    def import_csv(self, path_to_dataframe):
        """
        Imports csv into pandas dataframe for plotting.
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
        if 'compliance_score' in df.columns:
            df['compliance_score'] = df['compliance_score'].str.rstrip("%").astype(float)

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
            plot (plotly plot object):
                plot object of apps/applets with release date and compliance score.
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
            trendline="lowess", # needs module statsmodels
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
            #trendline="lowess",
            #trendline_scope="overall",
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
        dfout["dist_version"]=dfout["dist_version"].values.astype('str')
        dfout = dfout['dist_version'].value_counts().rename_axis('unique_versions').reset_index(name='counts')

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
        #TODO: Add a bar chart for other compliance stats.


def main():
    # Initialise class with shorthand
    audit = audit_class()
    plots = plotting()
    list_of_repos = audit.get_list_of_repositories(audit.ORGANISATION,
                                                   audit.GITHUB_TOKEN)
    print(f"Number of items: {len(list_of_repos)}")
    list_apps, list_of_json_contents = audit.select_apps(list_of_repos,
                                                         audit.GITHUB_TOKEN)
    compliance_df, details_df = audit.orchestrate_app_compliance(list_apps,
                                                     list_of_json_contents)
    compliance_df, details_df = audit.compliance_stats(compliance_df,
                                                       details_df)
    compliance_df.to_csv('compliance_df2.csv')
    details_df.to_csv('details_df2.csv')
    #TODO: Convert to html table and add to report using datatables
    #TODO: Add stats to parts of the html report and use bootrap to style it.
    #TODO: Add logging to the report.
    compliance_df = plots.import_csv('/home/rswilson1/Documents/Programming/Themis/themis/dxapp_compliance/compliance_df2.csv')
    detailed_df = plots.import_csv('/home/rswilson1/Documents/Programming/Themis/themis/dxapp_compliance/details_df2.csv')
    compliance_stats_summary = audit.compliance_scores_for_each_measure(compliance_df)
    release_comp_plot = plots.release_date_compliance_plot(compliance_df)
    ubuntu_comp_plot = plots.ubuntu_compliance_timeseries(detailed_df)
    compliance_bycommitdate_plot = plots.compliance_by_latest_activity_plot(compliance_df)
    ubuntu_versions_plot = plots.bash_version(detailed_df)
    get_template_render(compliance_df, detailed_df, compliance_stats_summary,
                        release_comp_plot, ubuntu_comp_plot,
                        compliance_bycommitdate_plot,
                        ubuntu_versions_plot
                        )


if __name__ == '__main__':
    main()
