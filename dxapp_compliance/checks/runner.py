"""Runs every check for one app and assembles the two result dicts.

Moved from ``compliance_checks.check_all``. ``compliance_dict`` holds the
booleans that feed the compliance score; ``details_dict`` holds the
human-readable values for the supplementary table.

The ghapi repo object has been replaced by plain ``repo_name`` and ``html_url``
arguments, so this function is callable with literal values in a test.
"""

import logging

from dxapp_compliance.checks import dxapp_json, scripts

logger = logging.getLogger(__name__)


def run_all_checks(repo_name, html_url, dxjson_content,
                   src_file_contents="",
                   last_release_date=None,
                   latest_commit_date=None,
                   default_region=None):
    """
    Checks all compliance measures for an app
    against Eastgenomics DNAnexus App standards.

    Parameters
    ----------
        repo_name (str):
            name of the app/applet repository.
        html_url (str):
            GitHub URL of the app/applet repository.
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

    Returns
    -------
        compliance_dict (dict):
            dict of compliance booleans for the app/applet.
        details_dict (dict):
            dict of compliance details for the app/applet.
    """
    # Find compliance for app/applet
    app_boolean, app_or_applet = dxapp_json.check_app_compliance(
        repo_name, dxjson_content
    )
    (name, title, eggd_name_boolean,
     eggd_title_boolean) = dxapp_json.check_naming_compliance(dxjson_content)
    (interpreter, distribution, dist_version,
     uptodate_ubuntu) = dxapp_json.check_interpreter_compliance(dxjson_content)
    (region_list, correct_regional_boolean,
     region_options_num) = dxapp_json.check_region_compliance(
        dxjson_content, default_region
    )
    # Convert list of regions to more readable string
    regions = " ".join([x.split(':')[1].rstrip("']") for x in region_list])

    (set_e_boolean, no_manual_compiling,
     asset_present) = scripts.check_src_file_compliance(
        dxjson_content, src_file_contents
    )
    timeout_policy, timeout_setting = dxapp_json.check_timeout(dxjson_content)
    (authorised_users, authorised_devs, auth_devs_boolean,
     auth_users_boolean) = dxapp_json.check_users_and_devs(dxjson_content)

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
                       'URL': html_url,
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
                    'URL': html_url,
                    }

    return compliance_dict, details_dict
