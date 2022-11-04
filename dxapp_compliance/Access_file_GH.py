import base64
import json
import requests
import os
from ghapi.all import *
from fastcore.all import *
from math import ceil
import pandas as pd
import numpy as np
from jinja2 import Environment, PackageLoader, select_autoescape
import plotly as py
import plotly.express as px

## TODO: Add logging to the report.
## TODO: Add stats to parts of the html report and use bootrap to style it.
## TODO: Add summary stats table to report


def get_template_render():
    """
    Render jinja2 template with provided variables.
    """
    template = env.get_template("mytemplate.html")
    print(template.render(the="variables", go="here"))

def get_credentials():
    """
    get_credentials

    Extracts the credentials from the json credentials file.

    Returns:
    github_token (str):
        Github API token for authentication.

    organisation (str):
        Organisation dnanexus username.

    """
    with open('CREDENTIALS.json') as f:
        credentials = json.load(f)
        github_token = credentials.get('GITHUB_TOKEN')
        organisation = credentials.get('organisation')
    return github_token, organisation


class app_compliance:
    """
    app_compliance
    A class for all dx querying functions.
    This collects all the compliance data needed for the audit app.
    """

    def __init__(self):
        self.env = Environment(
            loader=PackageLoader("Access_file_GH"),
            autoescape=select_autoescape()
        )
        # Set credentials
        self.GITHUB_TOKEN, self.ORGANISATION = get_credentials()


    def check_file_compliance(self, app, dxjson_content):
        """
        check_file_compliance

        This checks the compliance of each app/applet against the performa guidelines.
        This includes checking compliance using the dxapp.json file.
        (dxapp.json = the app/applet settings file)

        Parameters
        ----------
            dxjson_content (dict):
                contents of the dxapp.json file for the app.

        Returns:
            compliance_df (pandas dataframe):
                dataframe of compliance booleans for the app/applet.
            df_details (pandas dataframe):
                dataframe of compliance details for the app/applet.
        """
        data = dxjson_content

        # Find Region for cloud server
        region = data.get('regionalOptions', {})
        region_list = list(region.keys())

        if region_list == ["aws:eu-central-1"]:
            print(f"Correct only one region - {region_list[0]}")
        else:
            print(f"Incorrect multiple regions - {region_list}")

        # Find source for app/applet and check compliance
        src_file_contents, release_date, release_version = self.get_src_file(
            app,
            dxjson_content=dxjson_content,
            organisation_name=self.ORGANISATION,
            github_token=self.GITHUB_TOKEN)

        # Find compliance for app
        # Initialise variables - prevents not referenced before assignment error
        set_e_boolean, manual_compiling = None, None
        app_boolean, app_or_applet = None, None
        auth_devs_boolean, auth_users_boolean = None, None

        if 'version' in dxjson_content.keys():
            app_or_applet = "app"
            app_boolean = True
            print("app FOUND")
        elif "_v" in app.get('name'):
            app_or_applet = "applet"
            app_boolean = False
            print("applet found")
        else:
            print(f"App or applet not clear. See app/applet here {app}")
            # Likely still applet - So set to applet/false.
            app_or_applet = "applet"
            app_boolean = False

        # src file compliance info.
        if "set -e" in src_file_contents:
            set_e_boolean = True
        else:
            set_e_boolean = False
        if "make install" in src_file_contents:
            no_manual_compiling = False
            # print(src_file_contents)
        else:
            no_manual_compiling = True
        # interpreter compliance info.
        if data.get('runSpec', {}).get('interpreter', '') == 'bash':
            dist_version = float(data.get('runSpec', {}).get('release', ''))
            print(dist_version)
            if dist_version >= 20:
                uptodate_ubuntu = True
            else:
                uptodate_ubuntu = False
        elif 'python' in data.get('runSpec', {}).get('interpreter', ''):
            uptodate_ubuntu = "NA"
        else:
            print("interpreter not found")
            print(data.get('runSpec', {}).get('interpreter', ''))

        # Timeout policy compliance info.
        print(data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get('hours'))
        print(data.get('runSpec', {}).get('timeoutPolicy', {}))
        if data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get('hours') == None:
            timeout_policy = False
        elif data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get('hours') > 0:
            timeout_policy = True
        else:
            timeout_policy = False

        # auth devs & users
        authorised_users = data.get('authorizedUsers')
        authorised_devs = data.get('developers')
        print(authorised_users)
        print(authorised_devs)
        print(data)
        # print(data.get('runSpec', {}))
        if authorised_users is None:
            auth_users_boolean = False
        elif 'org-emee_1' in authorised_users:
            auth_users_boolean = True
        else:
            authorised_users = False
        if authorised_devs is None:
            auth_devs_boolean = False
        elif 'org-emee_1' in authorised_devs:
            auth_devs_boolean = True
        else:
            auth_devs_boolean = False

        # regional options compliance info.
        if region_list == ["aws:eu-central-1"]:
            correct_regional_boolean = True
            region_options_num = len(region_list)
        elif 'aws:eu-central-1' in region_list:
            correct_regional_boolean = True
            region_options_num = len(region_list)
        else:
            correct_regional_boolean = False
            region_options_num = len(region_list)

        # name compliance info.
        name = data.get('name')
        title = data.get('title')
        if name is None:
            eggd_name_boolean = False
        elif name.startswith('eggd'):
            eggd_name_boolean = True
        else:
            eggd_name_boolean = False
        if title is None:
            eggd_title_boolean = False
        elif title.startswith('eggd'):
            eggd_title_boolean = True
        else:
            eggd_title_boolean = False


        compliance_dict = {'name': data.get('name'),
                           'title': data.get('title'),
                           #'description': data.get('summary'),
                           'authorised_users': auth_users_boolean,
                           'authorised_devs': auth_devs_boolean,
                           'interpreter': data.get('runSpec', {}).get('interpreter', ''),
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
                           }

        details_dict = {'name': data.get('name'),
                        'title': data.get('title'),
                        #'description': data.get('summary'),
                        # 'release_version': data.get('properties', {}).get(),
                        'authorised_users': data.get('authorizedUsers'),
                        'authorizedDevs': data.get('developers'),
                        'interpreter': data.get('runSpec', {}).get('interpreter'),
                        'distribution': data.get('runSpec', {}).get('distribution'),
                        'dist_version': data.get('runSpec', {}).get('release'),
                        'regionalOptions': region_list,
                        'timeout': data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get('hours'),
                        'set_e': set_e_boolean,
                        'manual_compiling': manual_compiling,
                        'dxapp_or_applet': app_or_applet,
                        }

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
        self:
            self
        org_username (str):
            the username of the organisation to get the list of repositories for.
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
        print(org_details)
        total_num_repos = org_details['public_repos'] + org_details['total_private_repos']
        print(total_num_repos)
        per_page_num = 30
        pages_total = ceil(total_num_repos/per_page_num)  # production line
        # pages_total = 1  # testing line
        all_repos = []

        for page in range(1, pages_total+1):
            response = api.repos.list_for_org(org=org_username,
                                              per_page = per_page_num,
                                              page = page)
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
            # repo_full_name = repo['full_name']
            # print(repo)
            owner = 'eastgenomics'
            repo_name = repo['name']
            file_path = 'dxapp.json'

            # Checks to find dxapp.json which determines if repo is an app.
            try:
                contents = api.repos.get_content(owner, repo_name, file_path)
            except HTTP404NotFoundError:
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

        print(f"{len(repos_apps)} app repositories found.")

        return repos_apps, repos_apps_content


    def convert_to_dataframe(self, list_of_json_contents):
        """

        Parameters
        ----------
        list_of_json_contents (list of dictionaries):
            list of repositories with json contents

        Returns
        -------
        df (pandas dataframe):
            dataframe of list of repositories with json contents
        """

        dataframe = pd.DataFrame.from_records(list_of_json_contents)

        return dataframe


    def get_src_file(self, app, organisation_name, dxjson_content, github_token=None):
        """
        get_src_file
        This function gets the source script for a given app/applet.

        Parameters
        ----------
            app (GithubAPI app object):
                Github API app object used for extracting app/applet info.
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
        In development:
            returning the last commit date for the app/applet.
            To be used for checking if the app/applet was recently updated.
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
            # log error?
            print("No src file found. ERROR")
            pass

        repos_apps.append(dxjson_content)
        if contents:
            for content in contents:
                if (content['type'] == 'file' and
                    r'.sh' in content['name'] or
                    r'.py' in content['name']):
                    print("okay")
                    try:
                        file_path = content['path']
                        app_src_file = api.repos.get_content(organisation_name,
                                                             repo_name,
                                                             file_path)

                    except HTTP404NotFoundError:
                        # log error
                        print("No src file found. 404 ERROR")
                        pass
                    src_code_content = app_src_file['content']
                    code_content_encoding = app_src_file.get('encoding')
                    if code_content_encoding == 'base64':
                        src_content_decoded = base64.b64decode(src_code_content).decode()
                    else:
                        print("Other encoding used.")
        print(organisation_name)
        print(repo_name)
        print(github_token)
        # In progress - getting latest release date
        last_release_date = None
        last_release_date = self.get_latest_release(organisation_name, repo_name, github_token)

        return src_content_decoded, last_release_date, release_version


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


    def compliance_stats(self, compliance_df):
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
        """
        list_of_compliance_scores = []
        for _, row in compliance_df.iterrows():
            no_compliant = 0
            if 'bash' in row['interpreter']:
                total_performa = 10
            else:
                total_performa = 9
            for column in row:
                if column is Int:
                    if column >= 2:
                        pass
                    if column == 1:
                        no_compliant += 1
                    if column == 0:
                        pass
                if column is True:
                    no_compliant += 1
                else:
                    pass
            compliance_score = f"{round((no_compliant/total_performa)*100, 1)}%"
            list_of_compliance_scores.append(compliance_score)
        compliance_df['compliance_score'] = list_of_compliance_scores
        return compliance_df


    def convert_to_html(self, dataframe):
        """
        Convert dataframe to html table.

        Parameters
        ----------
            dataframe (pandas dataframe):
                dataframe to convert to html table.

        Returns
        -------
            df_html (html table):
                html table of dataframe.
        """

        df_html = pd.DataFrame.to_html(dataframe)

        return df_html


    def get_latest_release(self, organisation_name, repo_name, token):
        """
        Get latest release of app/applet repo.
        In progress - getting latest release date

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
            contents (json):
                json API response of latest release endpoint.
        """
        api = GhApi(token=token)
        # get latest release endpoint json.
        try:
            contents = api.repos.get_latest_release(organisation_name,
                                                    repo_name)
            last_release_date = contents['published_at'].split("T")[0]
        except HTTP404NotFoundError:
            # log error?
            print("No release found. ERROR")
            last_release_date = None

        return last_release_date


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


class plotting:
    """
     In progress.
    """


    def __init__(self):
        pass


    def interpreter_distribution(df):
        """
        interpreter_distribution _summary_

        Args:
            df (pandas df): dataframe of apps compliance data.
        """
        # using Plotly Express directly
        fig2 = px.bar(df.interpreter)
        fig2.show()


    def bash_py(df):
        """
        bash_py: Uses plotly to plot the distribution of bash and python apps.

        Args:
            df (pandas df): dataframe of apps compliance data.
        """
        # using Plotly Express directly
        dfout = df['interpreter'].value_counts()
        print(dfout)
        # print(df.count('interpreter'))

        fig2 = px.bar(dfout)
        fig2.show()


    def bash_version(df):
        """
        bash_py: Uses plotly to plot the distribution of bash and python apps.

        Args:
            df (pandas df): dataframe of apps compliance data.
        """
        # using Plotly Express directly
        dfout = df[df['interpreter'] == 'bash']
        print(df)
        print(dfout)
        dfout = dfout['dist_version'].value_counts().rename_axis('unique_versions').reset_index(name='counts')
        print(dfout)
        print(dfout.columns)

        fig2 = px.bar(dfout, x="unique_versions", y="counts",
                      color="unique_versions",
                      labels={
                              "unique_versions": "Ubuntu version",
                              "counts": "Count",
                              },
                      title="Version of Ubuntu by bash apps"
                      )
        fig2.show()


    def new_plot(df):
        """
        new_plot _summary_

        Args:
            df (pandas df): dataframe of apps compliance data.
        """

        # using Plotly Express directly
        dfout = df[df['interpreter'] == 'bash']
        print(df)
        print(dfout)
        dfout = dfout['dist_version'].value_counts().rename_axis('unique_versions').reset_index(name='counts')
        print(dfout)
        print(dfout.columns)

        fig2 = px.bar(dfout, x="unique_versions", y="counts",
                      color="unique_versions",
                      labels={
                              "unique_versions": "Ubuntu version",
                              "counts": "Count",
                              "unique_versions": "Ubuntu version",
                              },
                      title="Version of Ubuntu by bash apps"
                      )
        fig2.show()
        #TODO: Add a bar chart for other compliance stats.


def docstings_forGITAI():
    """
    check_compliance
    This function checks the compliance of a given app.
    Parameters
    ----------
        src_file (str):
            contents of src file.
        src_file_encoding (str):
            the encoding of the src file.
        dxjson_content (dict):
            contents of the dxapp.json file for the app.
    Returns:
        compliance (boolean):
            boolean of whether the app is compliant.
    """
    """
                app_code_decoded (str):
                    contents of src file decoded
                set_e_boolean (boolean)
                    boolean of whether set -e is used in the src file
                manual_compiling (boolean):
                    boolean of whether manual compiling is used in the src file
                    i.e. "make install".
                dxapp_boolean (boolean):
                    boolean of if the dxapp/applet is an app.
    """


def main():
    # Initialise class with shorthand
    app_c = app_compliance()
    list_of_repos = app_c.get_list_of_repositories(app_c.ORGANISATION,
                                                   app_c.GITHUB_TOKEN)
    print(f"Number of items: {len(list_of_repos)}")
    list_apps, list_of_json_contents = app_c.select_apps(list_of_repos,
                                                         app_c.GITHUB_TOKEN)
    compliance_df, details_df = app_c.orchestrate_app_compliance(list_apps,
                                                     list_of_json_contents)
    print(compliance_df)
    print(compliance_df.columns)
    print(compliance_df.size)
    compliance_df = app_c.compliance_stats(compliance_df)
    compliance_df.to_csv('compliance_df.csv')
    details_df.to_csv('details_df.csv')
    #TODO: Convert to html table and add to report using datatables
    #TODO: Add stats to parts of the html report and use bootrap to style it.
    #TODO: Add logging to the report.


if __name__ == '__main__':
    main()
