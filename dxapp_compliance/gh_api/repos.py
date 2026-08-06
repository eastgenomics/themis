"""Discovers the organisation's app repositories.

A repository is an app if and only if it has a root ``dxapp.json``.
"""

import base64
import json
import logging
from fnmatch import fnmatch
from pathlib import Path

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


def filter_eggd_repos(repos):
    """Keep only repositories whose *name* carries the eggd_ prefix.

    Operates on the raw listing, before dxapp.json is fetched, so an excluded
    repository costs no API call. It also means the two parallel lists of
    records and dxapp.json contents can never be misaligned by a filter, because
    filtering happens before they exist.

    Note this is the repository name, which is a different thing from the
    ``eggd_ name`` and ``eggd_ title`` checks - those read dxapp.json, and the
    two frequently disagree (``eggd_nirvana`` the repo contains
    ``nirvana_v2.1.0`` the app). This filter is about which repositories are in
    scope at all, typically to leave out vendor demos and third-party forks that
    were never meant to meet the in-house standard.

    Parameters
    ----------
        repos (list[dict]):
            Raw repo objects from the listing endpoint.

    Returns
    -------
        tuple: (kept, skipped_names)
    """
    kept, skipped = [], []
    for repo in repos:
        if has_eggd_prefix(repo.get('name')):
            kept.append(repo)
        else:
            skipped.append(repo.get('name'))

    if skipped:
        # Named, not just counted - a repository dropping out of the audit should
        # never be something you have to go looking for.
        logger.info(
            f"Excluding {len(skipped)} repositories without the "
            f"{EGGD_PREFIX!r} prefix: {sorted(skipped)}"
        )

    return kept, skipped


def load_repo_exclusions(path):
    """Read repository names to exclude, one per line.

    Blank lines are skipped and ``#`` starts a comment, so the file can record
    *why* each repository is excluded - which is the difference between a list
    someone can maintain and one nobody dares touch.

    Parameters
    ----------
        path (str or Path)

    Returns
    -------
        list[str]

    Raises
    ------
        FileNotFoundError:
            Rather than silently excluding nothing, which would look like the
            audit ignoring the request.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No repository exclusion file at {file_path}")

    names = []
    for line in file_path.read_text().splitlines():
        entry = line.split('#', 1)[0].strip()
        if entry:
            names.append(entry)

    logger.info(f"Read {len(names)} repository exclusion(s) from {file_path}")

    return names


def matches_any(repo_name, patterns):
    """Whether a repository name matches any exclusion pattern.

    Matching is case-insensitive and glob-aware, so ``ngc_*`` excludes a family
    while a plain name still matches only itself.
    """
    name = str(repo_name or "").lower()

    return any(fnmatch(name, str(pattern).lower()) for pattern in patterns)


def filter_excluded_repos(repos, patterns):
    """Drop repositories whose name matches an exclusion pattern.

    Applied to the raw listing, before dxapp.json is fetched. As well as saving
    a call per excluded repository, this means a pattern naming a repo that is
    not an app still counts as matched - filtering afterwards reported such
    patterns as matching nothing, which read as a typo when it was not.

    Parameters
    ----------
        repos (list[dict]):
            Raw repo objects from the listing endpoint.
        patterns (iterable[str]):
            Names or globs.

    Returns
    -------
        tuple: (kept, skipped_names)
    """
    patterns = [p for p in (patterns or []) if str(p).strip()]
    if not patterns:
        return list(repos), []

    kept, skipped = [], []
    for repo in repos:
        if matches_any(repo.get('name'), patterns):
            skipped.append(repo.get('name'))
        else:
            kept.append(repo)

    unused = [p for p in patterns
              if not any(fnmatch(str(n).lower(), str(p).lower())
                         for n in skipped)]
    if unused:
        # A pattern matching nothing usually means a typo or a renamed repo, and
        # silently excluding nothing looks identical to the audit working.
        logger.warning(
            f"Repository exclusion pattern(s) matched nothing: {sorted(unused)}"
        )

    if skipped:
        logger.info(
            f"Excluding {len(skipped)} named repositories: {sorted(skipped)}"
        )

    return kept, skipped


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
