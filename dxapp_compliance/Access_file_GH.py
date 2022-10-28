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
    github_token, organisation

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

    def split_filename(self, filename):
        filename_split = filename.split("/")
        print(filename_split)
        file_path = f"{filename_split[-2]}/{filename_split[-1]}"

        return file_path

    def check_file_compliance(self, app, dxjson_content):
        """
        check_file_compliance Checks file contents (dxapp.json)
        for compliance with audit app standards.

        Args:
            file_content (_type_): _description_

        Returns:
            _type_: _description_
        """
        data = dxjson_content

        # Find Region for cloud server
        region = data.get('regionalOptions', {})
        region_list = list(region.keys())

        if region_list == ["aws:eu-central-1"]:
            print(f"Correct only one region - {region_list[0]}")
        else:
            print(f"Incorrect multiple regions - {region_list}")

        # Find source for app and check compliance
        app_code_decoded, set_e_boolean, manual_compiling = self.get_src_file(
            app,
            file_content=dxjson_content,
            organisation_name=self.ORGANISATION,
            github_token=self.GITHUB_TOKEN)

        dict_relevant_cols = {'name': data.get('name'),
                              'title': data.get('title'),
                              'description': data.get('summary'),
                              # 'release_version': data.get('properties', {}).get(),
                              'authorised_users': data.get('authorizedUsers'),
                              'authorizedDevs': data.get('developers'),
                              'interpreter': data.get('runSpec', {}).get('interpreter'),
                              'distribution': data.get('runSpec', {}).get('distribution'),
                              'dist_version': data.get('runSpec', {}).get('release'),
                              'regionalOptions': region_list,
                              'timeout': data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get('hours'),
                              'src_file': app_code_decoded,
                              'set_e': set_e_boolean,
                              'manual_compiling': manual_compiling,
                              }

        df_compliance = pd.DataFrame.from_dict(dict_relevant_cols, orient='index')
        df_compliance = df_compliance.transpose()
        # fixes error with value error for length differences

        return df_compliance

    # Set up new Github API method

    def set_up_GhAPI(self) -> None:
        """
        set_up_GhAPI
        This function creates the connection with
        github API using the fastai ghapi library.
        Parameters
        ----------
            None
        Returns
        -------
            None, but establishes github api connection
            and prints response from github API connection.
        """
        api = GhApi()
        response = api.git.get_ref(owner='fastai', repo='fastcore', ref='heads/master')
        print(response)

    def get_list_of_repositories(self, org_username, github_token=None):
        """
        This function gets a list of all visible repositories for a given ORG.
        Parameters
        ----------
        self:
            self
        org_username: str
            the username of the organisation to get the list of repositories for.
        github_token: str
            the github token to authenticate with.
        Returns
        -------
        all_repos: list
            a list of all the repositories for the given organisation.
        """
        # https://api.github.com/orgs/ORG/repos
        api = GhApi(token=github_token)
        org_details = api.orgs.get(org_username)
        print(org_details)
        total_num_repos = org_details['public_repos'] + org_details['total_private_repos']
        print(total_num_repos)
        per_page_num = 30
        # pages_total = ceil(total_num_repos/per_page_num) #production line
        pages_total = 1  # test line
        all_repos = []

        for page in range(1, pages_total+1):
            response = api.repos.list_for_org(org=org_username,
                                              per_page = per_page_num,
                                              page = page)
            # print(response)
            response_repos = [repo for repo in response]
            all_repos = all_repos + response_repos

        return all_repos


    def select_apps(self, list_of_repos, github_token=None):
        """
        Select apps from list of repositories and extract dxapp.json contents.

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

            #Checks to find dxapp.json which determines if repo is an app.
            try:
                contents = api.repos.get_content(owner, repo_name, file_path)
            except HTTP404NotFoundError:
                print('REPO is not an app. Error: error 404 - not found')
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
                print("Other encoding used.")

        print(len(repos_apps))

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

        df = pd.DataFrame.from_records(list_of_json_contents)

        return df


    def get_src_file(self, app, organisation_name, file_content, github_token=None):
        """
        get_src_file
        This function gets the source script for a given app.

        Args:
            app (GithubAPI app object):
                Github API app object used for extracting app info.
            organisation_name (str):
                the username of the organisation the app is in.
            file_content (dict):
                contents of the dxapp.json file for the app.
            github_token (str, optional):
                Authentication token for github.
                Defaults to None. Therefore showing just public info.

        Returns:
            app_code_decoded (str):
                contents of src file decoded
            set_e_boolean (boolean)
                boolean of whether set -e is used in the src file
            manual_compiling (boolean):
                boolean of whether manual compiling is used in the src file
                i.e. "make install".
        """
        repos_apps = []
        repos_apps_content = []
        set_e_boolean, manual_compiling = None, None
        api = GhApi(token=github_token)
        contents = None
        app_code_decoded = None

        repo_name = app.get('name')
        print(repo_name)
        file_path = 'src/'
        print(file_content)
        try:
            contents = api.repos.get_content(organisation_name,
                                             repo_name,
                                             file_path)
        except HTTP404NotFoundError:
            # log error?
            print("No src file found. ERROR")
            pass

        repos_apps.append(file_content)
        if contents:
            for content in contents:
                # print(content)
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
                        pass
                    code_content = app_src_file['content']
                    code_content_encoding = app_src_file.get('encoding')
                    if code_content_encoding == 'base64':
                        app_code_decoded = base64.b64decode(code_content).decode()
                        # print(f"decoded {app_code_decoded}")
                        if "set -e" in app_code_decoded:
                            print("SET -E FOUND")
                            #TODO: CAPTURE THIS INTO A PANDAS DF OR SOMETHING
                            #TODO: Check if the app is installing everything new or using an existing resources
                            set_e_boolean = True
                        else:
                            set_e_boolean = False
                        if "make install" in app_code_decoded:
                            manual_compiling = True
                            # print(app_code_decoded)
                        else:
                            manual_compiling = False
                    else:
                        print("Other encoding used.")
            # print(contents)
        # if len(list_of_repos) != len(repos_apps_content):
        #     print("Error missing repos") # Add to orchestrator

        return app_code_decoded, set_e_boolean, manual_compiling


    def compliance_stats(self, df):
        """
        compliance_stats
        Calculates stats for overall compliance of each app.

        Parameters
        ----------
            df (dataframe):
                dataframe of app/applet dxapp.json contents.
        Returns
        -------
            None (currently)
        """
        # Find out how many repos have eggd in the name and title
        df = df.fillna(value=np.nan)

        print(df.groupby('timeout'))
        print(df.groupby('timeout').size())

        print(df['timeout'].isna().sum())
        total_rows = len(df)
        print(total_rows)

        # Counts number of repos with each dist_version
        print(df.groupby(['dist_version'])['dist_version'].count())
        version_complaince = (df.groupby(['dist_version'])['dist_version'].count()['20.04']/total_rows)*100
        print(version_complaince)

        # Counts number of repos with each dist
        print(df.groupby(['distribution'])['distribution'].count())

        # Find number with correct name/title compliance
        eggd_match = df[df['name'].str.match('eggd')]
        print(eggd_match)
        eggd_stat = round((len(eggd_match)/total_rows)*100, 2)
        print(eggd_stat)
        df_nona = df['title'].dropna()
        eggd_title_match = df_nona[df_nona.str.match('eggd')]
        print(eggd_title_match)
        eggd_stat = round((len(eggd_title_match)/total_rows)*100, 2)
        print(eggd_stat)
        # Correct version of ubuntu
        df.info()
        bash_apps = df[df['interpreter'].str.match('bash')]
        print(bash_apps)
        ubuntu_version_stat = ((bash_apps.groupby(['dist_version'])['dist_version'].count()['20.04'])/total_rows) * 100
        print(ubuntu_version_stat)
        # Correct regional options - aws:eu-central-1 present.
        selection = ['aws:eu-central-1']
        mask = df.regionalOptions.apply(lambda x: any(item for item in selection if item in x))
        region_set_apps = df[mask]
        print(region_set_apps)


    def convert_to_html(self, df):
        """
        Convert dataframe to html table.

        Parameters
        ----------
            df (pandas dataframe):
                dataframe to convert to html table.

        Returns
        -------
            df_html (html table):
                html table of dataframe.
        """

        df_html = pd.DataFrame.to_html(df)

        return df_html


    def orchestrate_app_compliance(self, list_apps, list_of_json_contents):
        """
        orchestrate_app_compliance _summary_

        Parameters
        ----------
            list_of_json_contents (list):
                list of json contents of apps

        Returns
        -------
            overall_compliance_df (dataframe)
                df of apps with compliance stats.
        """

        for index, (app, dxapp_contents) in enumerate(zip(list_apps, list_of_json_contents)):
            # The first item creates the dataframe
            if index == 0:
                df_repo = self.check_file_compliance(app, dxapp_contents)
                overall_compliance_df = df_repo
            else:
                df_repo = self.check_file_compliance(app, dxapp_contents)
                overall_compliance_df = pd.concat([overall_compliance_df,
                                                   df_repo], ignore_index=True)

        return overall_compliance_df

class plotting:
    def interpreter_distribution(self, df):
        """
        interpreter_distribution _summary_

        Args:
            df (pandas df): dataframe of apps compliance data.
        """
        # using Plotly Express directly
        fig2 = px.bar(df.interpreter)
        fig2.show()

    def bash_py(self, df):
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

    def bash_version(self, df):
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
                              "unique_versions": "Ubuntu version",
                              },
                      title="Version of Ubuntu by bash apps"
                      )
        fig2.show()

    def new_plot(self, df):
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


def main():
    # Initialise class with shorthand
    app_c = app_compliance()
    list_of_repos = app_c.get_list_of_repositories(app_c.ORGANISATION, app_c.GITHUB_TOKEN)
    print(f"Number of items: {len(list_of_repos)}")
    list_apps, list_of_json_contents = app_c.select_apps(list_of_repos, app_c.GITHUB_TOKEN)
    compliance_df = app_c.orchestrate_app_compliance(list_apps, list_of_json_contents)
    print(compliance_df)
    print(compliance_df.columns)
    print(compliance_df.size)
    # plotting.bash_py(compliance_df)
    plotting.bash_version(compliance_df)
    # plotting.interpreter_distribution(compliance_df)
    # direct1, direct2 = app_c.get_src_file(list_of_repos=list_apps,
    #                                       organisation_name=ORGANISATION,
    #                                       github_token=GITHUB_TOKEN)

    # print(direct2)

    # for index, item in enumerate(list_apps):
    #     print(item['name'])

    # print(overall_compliance_df)
    # compliance_stats(overall_compliance_df)
    #TODO: Convert to html table and add to report using datatables
    #TODO: Add stats to parts of the html report and use bootrap to style it.
    #TODO: Add query to find if manually compiling apps or if using existing assets.
    #TODO: Add logging to the report.


if __name__ == '__main__':
    main()
