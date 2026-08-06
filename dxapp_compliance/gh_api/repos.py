"""Discovers the organisation's app repositories.

A repository is an app if and only if it has a root ``dxapp.json``.
"""

import base64
import json
import logging

from dxapp_compliance.gh_api.client import is_not_found
from dxapp_compliance.models import RepoRecord

logger = logging.getLogger(__name__)

#: 100 is the API maximum, and fewer pages means fewer calls.
PER_PAGE = 100
#: Safety bound so a pagination bug cannot loop forever.
MAX_PAGES = 100

#: The East GLH naming standard for an in-house app repository.
EGGD_PREFIX = 'eggd_'


def has_eggd_prefix(repo_name):
    """Whether a repository name carries the eggd_ prefix.

    Matched case-insensitively: the prefix is a deliberate naming act, so
    ``EGGD_`` is the same intent as ``eggd_``.
    """
    return str(repo_name or "").lower().startswith(EGGD_PREFIX)


def filter_eggd_repos(records, contents):
    """Keep only repositories whose *name* carries the eggd_ prefix.

    Note this is the repository name, which is a different thing from the
    ``eggd_ name`` and ``eggd_ title`` checks - those read dxapp.json, and the
    two frequently disagree (``eggd_nirvana`` the repo contains
    ``nirvana_v2.1.0`` the app). This filter is about which repositories are in
    scope at all, typically to leave out vendor demos and third-party forks that
    were never meant to meet the in-house standard.

    Parameters
    ----------
        records (list[RepoRecord])
        contents (list[dict]):
            Parallel list of parsed dxapp.json contents.

    Returns
    -------
        tuple: (kept_records, kept_contents, skipped_names)
    """
    kept_records, kept_contents, skipped = [], [], []

    for record, dxapp in zip(records, contents):
        if has_eggd_prefix(record.name):
            kept_records.append(record)
            kept_contents.append(dxapp)
        else:
            skipped.append(record.name)

    if skipped:
        # Named, not just counted - an app dropping out of the audit should never
        # be something you have to go looking for.
        logger.info(
            f"Excluding {len(skipped)} repositories without the "
            f"{EGGD_PREFIX!r} prefix: {sorted(skipped)}"
        )

    return kept_records, kept_contents, skipped


def list_organisation_repos(client, repo_type='public'):
    """Every repository of the given type in the organisation.

    Pages until the API returns a short page, rather than computing a page count
    from the organisation's ``public_repos``. That arithmetic was wrong in a way
    that silently lost repositories: the listing endpoint returns every repo the
    token can see, public and private together, so in an org with 318 public and
    52 private repos it fetched ceil(318/30) = 11 pages = 330 of 370 and dropped
    the last 40 - including public app repos such as eggd_cgp-purple. Looping
    until exhaustion cannot be wrong in that way.

    ``repo_type='public'`` also filters server-side, so private repositories are
    excluded by request rather than by accident.

    Parameters
    ----------
        client (GitHubClient)
        repo_type (str):
            'public', 'private', 'all', 'forks', 'sources' or 'member'.

    Returns
    -------
        list: raw repo objects from the API.
    """
    all_repos = []
    page = 1
    while True:
        response = list(client.call(
            client.api.repos.list_for_org,
            org=client.organisation,
            type=repo_type,
            per_page=PER_PAGE,
            page=page,
        ))
        all_repos += response
        if len(response) < PER_PAGE:
            break
        page += 1
        if page > MAX_PAGES:
            logger.error(
                f"Stopped paginating at {MAX_PAGES} pages - the repository "
                f"listing may be incomplete."
            )
            break

    logger.info(
        f"{client.organisation}: {len(all_repos)} {repo_type} repositories "
        f"across {page} page(s)."
    )

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
