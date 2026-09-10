"""Builds one AppEvidence per app.

This is the only module the checks depend on the *output* of, and it contains no
branching worth testing: every path decision is delegated to ``filepaths.py``,
which is pure and covered.
"""

import logging

from dxapp_compliance import filepaths
from dxapp_compliance.gh_api import activity, contents, security
from dxapp_compliance.models import AppEvidence

logger = logging.getLogger(__name__)


def collect_evidence(client, repo, dxapp, default_region=None, assays=()):
    """Gather everything the checks need for one app.

    Parameters
    ----------
        client (GitHubClient)
        repo (RepoRecord)
        dxapp (dict):
            The parsed dxapp.json.
        default_region (str, optional):
            The region the app is expected to be authorised in.

    Returns
    -------
        AppEvidence
    """
    entries, truncated = contents.list_repo_tree(client, repo)
    paths = tuple(entry['path'] for entry in entries)

    runspec_file = dxapp.get('runSpec', {}).get('file') or ""
    entrypoint = filepaths.resolve_entrypoint(paths, runspec_file)

    scripts = contents.fetch_scripts(client, repo, entries,
                                     entrypoint or runspec_file)

    alerts_enabled, security_set = security.dependabot_status(client, repo.name)

    return AppEvidence(
        repo=repo,
        dxapp=dxapp,
        file_paths=paths,
        scripts=scripts,
        entrypoint_path=entrypoint,
        tree_truncated=truncated,
        last_release_date=activity.latest_release_date(client, repo),
        # No API call - see gh_api/activity.py.
        latest_commit_date=activity.latest_commit_date(repo),
        dependabot_alerts_enabled=alerts_enabled,
        dependabot_security_updates_set=security_set,
        # From the tree listing rather than a separate contents call, and found
        # at any depth: a bash app's Python helper usually keeps its
        # requirements alongside itself under resources/.
        requirements_txt_present=filepaths.has_requirements_txt(paths),
        default_region=default_region,
        assays=tuple(assays),
    )
