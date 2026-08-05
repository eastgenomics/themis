"""Reads a repository's file listing and the contents of selected files.

Replaces the previous approach of fetching exactly one file - the path in
runSpec.file - which meant apt and pip calls in helper scripts or under
resources/ were invisible to the audit.

One recursive git-tree call per repository yields every path. The tree entries
also carry each blob's sha and size, so contents are fetched by sha (one call, no
path escaping, and no 1 MB contents-API cap) and oversized files are skipped
before spending a call at all.
"""

import base64
import logging

from dxapp_compliance import filepaths
from dxapp_compliance.gh_api.client import is_not_found

logger = logging.getLogger(__name__)


def list_repo_tree(client, repo):
    """List every file in a repository with one API call.

    ``default_branch`` is already present on the repo objects returned by the org
    listing, and the tree endpoint accepts a ref name in place of a sha, so no
    extra call is needed to resolve it.

    Parameters
    ----------
        client (GitHubClient)
        repo (RepoRecord)

    Returns
    -------
        tuple: (entries, truncated) where entries is a list of dicts with
        'path', 'sha' and 'size', and truncated is True when GitHub could not
        return the whole tree.
    """
    try:
        tree = client.call(
            client.api.git.get_tree,
            owner=client.organisation,
            repo=repo.name,
            tree_sha=repo.default_branch,
            recursive=1,
        )
    except Exception as error:
        if not is_not_found(error):
            raise
        logger.error(
            f"{repo.name}: could not list the tree for branch "
            f"{repo.default_branch!r}."
        )
        return [], False

    entries = [
        {'path': item['path'],
         'sha': item.get('sha'),
         'size': item.get('size') or 0}
        for item in tree.get('tree', [])
        if item.get('type') == 'blob'
    ]
    truncated = bool(tree.get('truncated'))

    if truncated:
        # A pass on "no bundled .deb found" would be unfounded if the listing is
        # incomplete, so the caller marks path-derived checks NOT_APPLICABLE.
        logger.warning(
            f"{repo.name}: GitHub truncated the tree listing "
            f"({len(entries)} entries returned)."
        )

    return entries, truncated


def fetch_blob_text(client, repo, sha):
    """Fetch and decode one blob by sha.

    Returns
    -------
        str: the decoded text, or "" if it could not be read.
    """
    try:
        blob = client.call(
            client.api.git.get_blob,
            owner=client.organisation,
            repo=repo.name,
            file_sha=sha,
        )
    except Exception as error:
        if not is_not_found(error):
            raise
        logger.error(f"{repo.name}: blob {sha} not found.")
        return ""

    content = blob.get('content')
    if not content:
        return ""

    if blob.get('encoding') != 'base64':
        logger.error(
            f"{repo.name}: unexpected blob encoding {blob.get('encoding')!r}."
        )
        return ""

    try:
        return base64.b64decode(content).decode('utf-8', errors='replace')
    except (ValueError, TypeError) as error:
        logger.error(f"{repo.name}: could not decode blob {sha}: {error}")
        return ""


def fetch_scripts(client, repo, entries, runspec_file=""):
    """Fetch the text of every runtime script in the repository.

    Parameters
    ----------
        client (GitHubClient)
        repo (RepoRecord)
        entries (list[dict]):
            Tree entries from :func:`list_repo_tree`.
        runspec_file (str):
            runSpec.file, which always counts as runtime code.

    Returns
    -------
        dict: path -> decoded text.
    """
    by_path = {entry['path']: entry for entry in entries}
    wanted = filepaths.scannable_scripts(by_path, runspec_file)

    scripts = {}
    for path in wanted:
        entry = by_path[path]
        if entry['size'] > filepaths.MAX_BLOB_BYTES:
            logger.info(
                f"{repo.name}: skipping {path} ({entry['size']} bytes, over "
                f"the {filepaths.MAX_BLOB_BYTES} byte limit)."
            )
            continue
        if not entry['sha']:
            continue
        text = fetch_blob_text(client, repo, entry['sha'])
        if text:
            scripts[path] = text

    return scripts
