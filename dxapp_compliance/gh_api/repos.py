"""Discovers the organisation's app repositories.

A repository is an app if and only if it has a root ``dxapp.json``.
"""

import base64
import json
import logging
from math import ceil

from dxapp_compliance.gh_api.client import is_not_found
from dxapp_compliance.models import RepoRecord

logger = logging.getLogger(__name__)

PER_PAGE = 30


def list_organisation_repos(client):
    """Every non-archived repository in the organisation.

    Note this paginates on ``public_repos``, so private repositories fall off the
    end of the listing. That is a known limitation, accepted for now because all
    app repositories in this organisation are public.

    Returns
    -------
        list: raw repo objects from the API.
    """
    org = client.call(client.api.orgs.get, client.organisation)
    total = org['public_repos']
    logger.info(f"{client.organisation} has {total} public repositories.")

    pages = ceil(total / PER_PAGE)
    all_repos = []
    for page in range(1, pages + 1):
        response = client.call(
            client.api.repos.list_for_org,
            org=client.organisation,
            per_page=PER_PAGE,
            page=page,
        )
        all_repos += list(response)

    return all_repos


def select_apps(client, repos):
    """Keep the repositories that contain a root dxapp.json.

    Returns
    -------
        tuple: (list[RepoRecord], list[dict]) - parallel lists of repository
        records and their parsed dxapp.json contents.
    """
    records = []
    contents = []

    for repo in repos:
        if repo.get('archived'):
            logger.info(f"{repo['name']} is archived; skipping.")
            continue

        record = RepoRecord.from_api(repo)
        dxapp = fetch_dxapp_json(client, record)
        if dxapp is None:
            continue

        records.append(record)
        contents.append(dxapp)

    logger.info(f"{len(records)} app repositories found.")

    return records, contents


def fetch_dxapp_json(client, repo):
    """Fetch and parse a repository's root dxapp.json.

    Returns
    -------
        dict or None: None when the repo is not an app, or the file could not be
        parsed.
    """
    try:
        response = client.call(
            client.api.repos.get_content,
            # The organisation from config, not a hardcoded 'eastgenomics' as
            # before - that silently ignored the configured value.
            client.organisation,
            repo.name,
            'dxapp.json',
        )
    except Exception as error:
        if not is_not_found(error):
            raise
        logger.info(f"{repo.name} is not an app (no dxapp.json).")
        return None

    if response.get('encoding') != 'base64':
        logger.warning(
            f"{repo.name}: unexpected dxapp.json encoding "
            f"{response.get('encoding')!r}."
        )
        return None

    try:
        decoded = base64.b64decode(response['content']).decode()
        return json.loads(decoded)
    except (ValueError, TypeError) as error:
        # A malformed dxapp.json must not abort the whole audit.
        logger.error(f"{repo.name}: could not parse dxapp.json: {error}")
        return None
