"""Runs every check for one app and assembles the two result dicts.

The two dicts are built by asking the registry what columns exist, rather than
by hand-listing keys as the original ``check_all`` did. That is what keeps the
score denominator, the summary allow-list, the rename maps and the rendered
column order from drifting apart.

Applicability is enforced in one place here: any scored check whose declared
``applies_to`` excludes this app's interpreter family is written as
NOT_APPLICABLE and drops out of the app's denominator.
"""

import logging

from dxapp_compliance.checks import dependencies, dxapp_json, scripts
from dxapp_compliance.checks.registry import (
    COMPLIANCE_COLUMNS,
    DETAIL_COLUMNS,
    NOT_APPLICABLE,
    Role,
    interpreter_family,
)
from dxapp_compliance.models import CheckOutcome

logger = logging.getLogger(__name__)


def _has_python_sources(evidence):
    """Whether the repository contains any Python at all.

    Decides whether the requirements.txt check applies. The original gated on
    GitHub's linguist report and returned a hard False for any repo linguist did
    not label Python - so a bash app shipping a Python helper with unpinned
    dependencies was indistinguishable from a compliant one, and pure-shell apps
    were penalised for lacking a file they have no use for.
    """
    if evidence.dxapp.get('runSpec', {}).get('interpreter', '').startswith(
            'python'):
        return True

    return any(path.endswith('.py') for path in evidence.file_paths)


def run_all_checks(evidence):
    """Run every check for one app.

    Parameters
    ----------
        evidence (AppEvidence):
            Everything already fetched for this app.

    Returns
    -------
        CheckOutcome
    """
    dxapp = evidence.dxapp
    src = evidence.entrypoint_text

    app_boolean, app_or_applet = dxapp_json.check_app_compliance(
        evidence.repo.name, dxapp
    )
    (name, title, eggd_name_boolean,
     eggd_title_boolean) = dxapp_json.check_naming_compliance(dxapp)
    (interpreter, distribution, dist_version,
     uptodate_ubuntu) = dxapp_json.check_interpreter_compliance(dxapp)
    (region_list, correct_regional_boolean,
     region_options_num) = dxapp_json.check_region_compliance(
        dxapp, evidence.default_region
    )
    # Convert list of regions to more readable string
    regions = " ".join([x.split(':')[1].rstrip("']") for x in region_list])

    timeout_policy, timeout_setting = dxapp_json.check_timeout(dxapp)
    (authorised_users, authorised_devs, auth_devs_boolean,
     auth_users_boolean) = dxapp_json.check_users_and_devs(dxapp)

    asset_present = dependencies.check_assets_present(dxapp)

    # Dependency provenance. These read every fetched script, not just the
    # entrypoint, which is why the git-tree walk exists.
    no_network_access, network_details = dxapp_json.check_network_access(
        dxapp, evidence.scripts
    )
    no_remote_packages, package_details = (
        dependencies.evaluate_remote_package_install(
            dxapp, evidence.scripts, evidence.file_paths,
            evidence.entrypoint_path or "",
        )
    )
    pip_local_wheels, pip_details = dependencies.evaluate_pip_provenance(
        dxapp, evidence.scripts, evidence.file_paths,
        evidence.entrypoint_path or "",
    )
    _, exec_depends_rendered = dependencies.describe_exec_depends(dxapp)

    # A truncated git tree means an absent .deb may simply not have been listed,
    # so a pass would be unfounded.
    if evidence.tree_truncated:
        logger.warning(
            f"{evidence.repo.name}: git tree was truncated; reporting "
            f"path-derived package checks as not applicable."
        )
        no_remote_packages = NOT_APPLICABLE
        package_details = "(repository file listing truncated) " + package_details

    # requirements.txt only means something for a repo that ships Python.
    if _has_python_sources(evidence):
        requirements_file_exists = evidence.requirements_txt_present
    else:
        requirements_file_exists = NOT_APPLICABLE

    compliance = {
        # The repository name, not dxapp.json's `name`. The two often differ,
        # and the repo name is what the URL points at and what someone acting
        # on the report will search for. The dxapp.json name is kept in the
        # details table.
        'name': evidence.repo.name,
        # Filled in by report.scoring once every app has been checked.
        'compliance_score': None,
        'authorised_users': auth_users_boolean,
        'authorised_devs': auth_devs_boolean,
        'interpreter': interpreter,
        'uptodate_ubuntu': uptodate_ubuntu,
        'timeout_policy': timeout_policy,
        'correct_regional_option': correct_regional_boolean,
        'set_e': scripts.check_set_e(src),
        'no_manual_compiling': scripts.check_manual_compiling(src),
        'no_network_access': no_network_access,
        'no_remote_package_install': no_remote_packages,
        'pip_uses_local_wheels': pip_local_wheels,
        'dxapp_boolean': app_boolean,
        'dxapp_or_applet': app_or_applet,
        'eggd_name_boolean': eggd_name_boolean,
        'eggd_title_boolean': eggd_title_boolean,
        'dependabot_alerts_status': evidence.dependabot_alerts_enabled,
        'dependabot_security_status':
            evidence.dependabot_security_updates_set,
        'requirements_file_exists': requirements_file_exists,
        'num_of_region_options': region_options_num,
        'timeout_setting': timeout_setting,
        'last_release_date': evidence.last_release_date,
        'latest_commit_date': evidence.latest_commit_date,
        'URL': evidence.repo.html_url,
    }

    compliance = apply_applicability(compliance, interpreter)

    details = {
        'name': evidence.repo.name,
        'dxapp_name': name,
        'compliance_score': None,
        'authorised_users': authorised_users,
        'authorised_devs': authorised_devs,
        'interpreter': interpreter,
        'distribution': distribution,
        'dist_version': dist_version,
        'regionalOptions': regions,
        'title': title,
        'timeout': timeout_policy,
        'timeout_setting': timeout_setting,
        'set_e': compliance['set_e'],
        'no_manual_compiling': compliance['no_manual_compiling'],
        'asset_present': asset_present,
        'network_access': network_details,
        'exec_depends': exec_depends_rendered or "None declared",
        'package_install_details': package_details,
        'pip_install_details': pip_details,
        'dxapp_or_applet': app_or_applet,
        'dependabot_alerts_status': evidence.dependabot_alerts_enabled,
        'dependabot_security_status':
            evidence.dependabot_security_updates_set,
        'requirements_file_exists': requirements_file_exists,
        'last_release_date': evidence.last_release_date,
        'latest_commit_date': evidence.latest_commit_date,
        'URL': evidence.repo.html_url,
    }

    _assert_keys_match(compliance, COMPLIANCE_COLUMNS, "compliance")
    _assert_keys_match(details, DETAIL_COLUMNS, "details")

    return CheckOutcome(compliance=compliance, details=details)


def apply_applicability(compliance, interpreter):
    """Blank out scored checks that do not apply to this app's interpreter.

    Replaces the hardcoded "if python: set -e and manual compiling are NA"
    branch. Declaring applicability in the registry means a new check cannot
    accidentally be scored for an interpreter it makes no sense for.
    """
    family = interpreter_family(interpreter)
    for spec in COMPLIANCE_COLUMNS:
        if spec.role is Role.SCORED and family not in spec.applies_to:
            compliance[spec.key] = NOT_APPLICABLE

    return compliance


def _assert_keys_match(produced, columns, label):
    """Fail loudly if the runner and the registry have drifted apart.

    A missing key used to mean a column silently vanished from the report; an
    undeclared key meant it was computed and then dropped.
    """
    declared = {spec.key for spec in columns}
    actual = set(produced)
    if declared != actual:
        raise AssertionError(
            f"{label} dict does not match the registry. "
            f"Missing: {sorted(declared - actual)}. "
            f"Undeclared: {sorted(actual - declared)}."
        )
