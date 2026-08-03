"""Checks that read only the parsed dxapp.json.

Moved from the ``compliance_checks`` class in dxapp_queries.py. Each function
keeps the original's bare-tuple return so call sites are unchanged; the one
signature change is :func:`check_app_compliance`, which now takes the repository
name as a plain string instead of a ghapi repo object (the original mixed
``app.get('name')`` and ``app.name`` on the same object, which only worked
because ghapi returns a fastcore AttrDict, and made the function untestable with
a plain dict).
"""

import logging

from dxapp_compliance.checks.registry import (
    BASH,
    NOT_APPLICABLE,
    UNKNOWN,
    interpreter_family,
)

logger = logging.getLogger(__name__)


def check_region_compliance(dxjson_content, default_region=None):
    """
    Checks compliance for regional settings for DNAnexus app performa.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.
        default_region (str):
            default region to check against.

    Returns
    -------
        region_list (list):
            list of regional options for the app/applet.
        correct_regional_boolean (boolean):
            True/False whether only the correct region is selected
        num_regions (int):
            The number of regional options set for dnanexus cloud servers,
            this should be 1 and set to the correct region.

    Notes
    -----
    The previous implementation was
    ``if region_list is [default_region] or 'aws:eu-central-1' in region_list``.
    The first clause compared identity against a freshly built list literal, so
    it was always False and the configured default_region never took effect. The
    second clause passed any app that merely *included* eu-central-1, so
    multi-region apps passed - contradicting the documented standard of "only
    aws:eu-central-1 is set". This now requires exactly the default region.
    """
    # Find Region options for cloud servers.
    region = dxjson_content.get('regionalOptions', {})
    region_list = list(region.keys())
    num_regions = len(region_list)

    correct_regional_boolean = region_list == [default_region]

    if not correct_regional_boolean:
        if num_regions > 1:
            logger.info(
                f"Incorrect regional option set and multiple regions present: "
                f"{region_list}"
            )
        else:
            logger.info(f"Incorrect regional option set: {region_list}")

    return region_list, correct_regional_boolean, num_regions


def check_timeout(dxjson_content):
    """
    Checks compliance for timeout settings for DNAnexus app performa.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.

    Returns
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


def check_app_compliance(repo_name, dxjson_content):
    """
    Checks if it is an app/applet.

    Parameters
    ----------
        repo_name (str):
            name of the app/applet repository.
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.

    Returns
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
        logger.info(f"App: {repo_name}")
    elif "_v" in repo_name:
        app_or_applet = "applet"
        app_boolean = False
        logger.info(f"Applet: {repo_name}")
    else:
        logger.info(
            f"App or applet not clear. See app/applet here {repo_name}")
        # Likely still applet - So set to applet/false.
        app_or_applet = "applet"
        app_boolean = False

    return app_boolean, app_or_applet


def check_interpreter_compliance(dxjson_content):
    """
    Checks compliance for ubuntu version for bash-based app/applets.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.

    Returns
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

    Notes
    -----
    Two crashes are fixed here. ``float(get('release', ''))`` raised ValueError
    for a bash app with no ``release`` key, and ``uptodate_ubuntu`` was never
    assigned when the interpreter was neither bash nor python, so the return
    statement raised UnboundLocalError.
    """
    run_spec = dxjson_content.get('runSpec', {})
    dist_version = None
    interpreter = run_spec.get('interpreter', '')
    distribution = run_spec.get('distribution')
    family = interpreter_family(interpreter)

    if family == BASH:
        release = run_spec.get('release')
        try:
            dist_version = float(release)
        except (TypeError, ValueError):
            logger.info(
                f"Bash app has no usable runSpec.release: {release!r}"
            )
            return interpreter, distribution, None, False
        uptodate_ubuntu = dist_version >= 20
    else:
        if family == UNKNOWN:
            logger.info(f"Interpreter not recognised: {interpreter!r}")
        uptodate_ubuntu = NOT_APPLICABLE

    return interpreter, distribution, dist_version, uptodate_ubuntu


def check_users_and_devs(dxjson_content):
    """
    Checks compliance for user and developer settings
    for DNAnexus app performa.

    Currently checks if the user is only for org-emee_1
    but could change to contains the org-emee_1 user.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.

    Returns
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

    # Extract the users and developers from the dxapp.json list
    if authorised_users:
        authorised_users = ', '.join(authorised_users)
    else:
        authorised_users = ""

    if authorised_devs:
        authorised_devs = ', '.join(authorised_devs)
    else:
        authorised_devs = "None"

    return (authorised_users, authorised_devs,
            auth_devs_boolean, auth_users_boolean)


def check_naming_compliance(dxjson_content):
    """
    Checks compliance for app/applet naming compliance
    against the DNAnexus app performa.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.

    Returns
    -------
        name (str):
            The app/applet name from dxapp.json.
        title (str):
            The app/applet title from dxapp.json.
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


def check_network_access(dxjson_content):
    """
    Checks whether the app declares outbound network access.

    Per the DNAnexus I/O and Run Specifications reference, omitting
    ``access.network`` entirely means the executable has no network access. Any
    declared entry - a hostname, a hostname wildcard, a network mask, or "*" -
    grants outbound access, and an app that needs the network at job time is by
    definition not self-contained.

    ``httpsApp`` is noted in the details but does not affect the verdict: it
    permits inbound HTTPS through the platform proxy and neither implies nor
    requires outbound ``access.network``.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.

    Returns
    -------
        no_network_access (boolean):
            True when no outbound network access is declared.
        network_details (str):
            The declared value, for the details table.
    """
    access = dxjson_content.get('access')

    if access is None:
        no_network_access, details = True, "None declared"
    elif not isinstance(access, dict):
        logger.warning(f"dxapp.json access is not a mapping: {type(access)}")
        no_network_access = True
        details = f"access malformed: {type(access).__name__}"
    else:
        network = access.get('network')
        if network is None:
            no_network_access, details = True, "None declared"
        elif isinstance(network, str):
            # Malformed - the schema says array of strings - but it still
            # expresses intent to reach the network.
            no_network_access = False
            details = f'"{network}" (not a list)'
        elif not network:
            no_network_access, details = True, "[] (empty)"
        else:
            no_network_access = False
            details = str(list(network))

        granted = [
            f"{key}: {access[key]}"
            for key in ('project', 'allProjects', 'developer',
                        'projectCreation')
            if key in access
        ]
        if granted:
            details += " || also grants " + ", ".join(granted)

    if dxjson_content.get('httpsApp'):
        details += (" || note: httpsApp declared - inbound HTTPS via the "
                    "platform proxy only, does not imply outbound access")

    return no_network_access, details
