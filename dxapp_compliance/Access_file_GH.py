import base64
import json
from operator import contains
import requests
import os
from ghapi.all import *
from fastcore.all import *
from math import ceil
import pandas as pd
import numpy as np
from jinja2 import Environment, PackageLoader, select_autoescape

# Set_up_environment
env = Environment(
    loader=PackageLoader("Access_file_GH"),
    autoescape=select_autoescape()
)

def get_template_render():
    template = env.get_template("mytemplate.html")
    print(template.render(the="variables", go="here"))

def get_credentials():
    """
    get_credentials _summary_
    """

    with open('CREDENTIALS.json') as f:
        credentials = json.load(f)
        github_token = credentials.get('GITHUB_TOKEN')
        organisation = credentials.get('organisation')

    return github_token, organisation

# https://api.github.com/repos/{username}/{repository_name}/contents/{file_path}

def split_filename(filename):
    filename_split = filename.split("/")
    print(filename_split)
    file_path = f"{filename_split[-2]}/{filename_split[-1]}"

    return file_path

def check_file_compliance(file_content):
    """
    check_file_compliance Checks file contents (dxapp.json)
    for compliance with audit app standards.

    Args:
        file_content (_type_): _description_

    Returns:
        _type_: _description_
    """
    data = file_content
    print(data)
    # Find Region for cloud server
    region = data.get('regionalOptions', {})
    region_list = list(region.keys())

    if region_list == ["aws:eu-central-1"]:
        print(f"Correct only one region - {region_list[0]}")
    else:
        print(f"Incorrect multiple regions - {region_list}")
    dict_relevant_cols = {'name': data.get('name'),
                          'title': data.get('title'),
                          'description': data.get('summary'),
                        #   'release_version': data.get('properties', {}).get(),
                          'authorised_users': data.get('authorizedUsers'),
                          'authorizedDevs': data.get('developers'),
                          'interpreter': data.get('runSpec', {}).get('interpreter'),
                          'distribution': data.get('runSpec', {}).get('distribution'),
                          'dist_version': data.get('runSpec', {}).get('release'),
                          'regionalOptions': region_list,
                          'timeout': data.get('runSpec', {}).get('timeoutPolicy', {}).get('*', {}).get('hours'),
                          }

    df_compliance = pd.DataFrame.from_dict(dict_relevant_cols, orient='index')
    df_compliance = df_compliance.transpose()
    # fixes error with value error for length differences

    return df_compliance

# Set up new Github API method

def set_up_GhAPI():
    api = GhApi()
    response = api.git.get_ref(owner='fastai', repo='fastcore', ref='heads/master')
    print(response)

def get_list_of_repositories(org_username, github_token=None):
    # https://api.github.com/orgs/ORG/repos
    api = GhApi(token=github_token)
    org_details = api.orgs.get(org_username)
    print(org_details)
    total_num_repos = org_details['public_repos'] + org_details['total_private_repos']
    print(total_num_repos)
    per_page_num = 30
    # pages_total = ceil(total_num_repos/per_page_num) #production line
    pages_total = 1 # test line
    all_repos = []

    for page in range(1, pages_total+1):
        response = api.repos.list_for_org(org=org_username,
                                          per_page = per_page_num,
                                          page = page)
        # print(response)
        response_repos = [repo for repo in response]
        all_repos = all_repos + response_repos

    return all_repos


def select_apps(list_of_repos, github_token=None):
    """_summary_

    Args:
        list_of_repos (_type_): _description_
        github_token (_type_): _description_
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
            print("yes")
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
            print(app_decoded)

            # if json_decoded['authorizedUsers']:
            #     authorised_users = json_decoded['authorizedUsers']
            # else:
            #     authorised_users = ""
            # if json_decoded['developers']:
            #     devs = json_decoded['developers']
            # else:
            #     devs = ""

            # dict_relevant_cols = {'name': json_decoded['name'],
            #                       'title': json_decoded['title'],
            #                       'description': json_decoded['summary'],
            #                       'release_version': json_decoded['properties']['githubRelease'],
            #                       'authorised_users': authorised_users,
            #                       'authorizedDevs': devs,
            #                       'interpreter': json_decoded['runSpec']['interpreter'],
            #                       'regionalOptions': json_decoded['runSpec']['regionalOptions'].keys(),
            #                       'timeout': json_decoded['runSpec']['timeout'],
            #                       'distribuiton': json_decoded['runSpec']['distribution'],
            #                       }

            repos_apps_content.append(app_decoded)

        else:
            print("Other encoding used.")

    print(len(repos_apps))

    return repos_apps, repos_apps_content


def convert_to_dataframe(list_of_json_contents):
    """_summary_

    Args:
        list_of_json_contents (list of dictionaries):
            list of repositories with json contents

    Returns:
        pandas dataframe:
    """

    df = pd.DataFrame.from_records(list_of_json_contents)

    return df


def get_src_file(list_of_repos, organisation_name, github_token=None):
    repos_apps = []
    repos_apps_content = []
    api = GhApi(token=github_token)
    for repo in list_of_repos:
        # owner = 'eastgenomics'
        repo_name = repo['name']
        file_path = 'src/'
        try:
            contents = api.repos.get_content(organisation_name, repo_name, file_path)
        except HTTP404NotFoundError:
            continue
        repos_apps.append(repo)
        # repos_apps_content.append(contents)
        for content in contents:
            if (content['type'] == 'file' and
                r'.sh' in content['name'] or
                r'.py' in content['name']):
                print("yep")
                try:
                    file_path = content['path']
                    code_contents = api.repos.get_content(organisation_name, repo_name, file_path)
                except HTTP404NotFoundError:
                    continue
                repos_apps_content.append(code_contents)
    # print(len(repos_apps))
    # print(len(repos_apps_content))

    if len(list_of_repos) != len(repos_apps_content):
        print("Error missing repos")
    for app in repos_apps_content:
        code_content = app['content']
        code_content_encoding = app.get('encoding')
        if code_content_encoding == 'base64':
            app_code_decoded = base64.b64decode(code_content).decode()
            # print(f"decoded {app_code_decoded}")
            if "set -e" in app_code_decoded:
                print("SET -E FOUND")
                # print(app_code_decoded)
                #TODO: CAPTURE THIS INTO A PANDAS OR SOMETHING
                #TODO: Check if the app is installing everything new or using an existing resources
            else:
                print("NO SET -E FOUND")

    return app_code_decoded, repos_apps, repos_apps_content


def compliance_stats(df):
    """
    compliance_stats _summary_

    Args:
        df (_type_): _description_
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




def convert_to_html(df):
    """
    Convert dataframe to html table.

    Args:
        df (pandas dataframe): dataframe to convert to html table.

    Returns:
        df_html (html table):  html table of dataframe.
    """

    df_html = pd.DataFrame.to_html(df)

    return df_html





def main():
    GITHUB_TOKEN, ORGANISATION = get_credentials()
    list_of_repos = get_list_of_repositories(ORGANISATION, GITHUB_TOKEN)
    print(f"Number of items: {len(list_of_repos)}")
    list_apps, list_of_json_contents = select_apps(list_of_repos, GITHUB_TOKEN)

    direct1, direct2 = get_src_file(list_of_repos=list_apps,
                          organisation_name=ORGANISATION,
                          github_token=GITHUB_TOKEN)

    print(direct2)

    # for index, item in enumerate(list_apps):
    #     print(item['name'])

    # for index, repo in enumerate(list_of_json_contents):
    #     if index == 0:
    #         df = check_file_compliance(repo)
    #         print(df)
    #         overall_compliance_df = df
    #     else:
    #         df = check_file_compliance(repo)
    #         print(f"{overall_compliance_df} - overall_compliance_df")
    #         print(f"{df} - df")
    #         overall_compliance_df = pd.concat([overall_compliance_df, df],
    #                                           ignore_index=True)

    # print(overall_compliance_df)
    # compliance_stats(overall_compliance_df)
    #TODO: Convert to html table and add to report using datatables
    #TODO: Add stats to parts of the html report and use bootrap to style it.
    #TODO: Add query to find if manually compiling apps or if using existing assets.






# Other useful functions:

# def get_variables_for_repos(list_of_repos):
#     """_summary_

#     Args:
#         list_of_repos (_type_): _description_
#         org_username (_type_): _description_
#     """
#     for repo in list_of_repos:
#         # repo_full_name = repo['full_name']
#         owner = repo['owner']
#         repo_name = repo['name']
#         file_path = f'https://api.github.com/repos/{owner}/{repo_name}/contents/'
#         # url = 'https://api.github.com/repos/eastgenomics/eggd_vep/contents/dxapp.json'
#         print(check_if_file_exists(owner, repo_name, "dxapp.json"))
#         if check_if_file_exists(owner, repo_name, "dxapp.json"):
#             print("yes")
#         # file_content = github_search_file(owner, repo_name, file_path)

# def check_if_file_exists(owner, repo_name, file_path):
#     api = GhApi(token=github_token())
#     try:
#         composer_json_file = api.repos.get_content(owner, repo_name, file_path)
#         print(composer_json_file)
#         print(composer_json_file.raise_for_status())
#         return True
#     except Exception as e:
#         print(f'{e} code')
#         return False

if __name__ == '__main__':
    main()
