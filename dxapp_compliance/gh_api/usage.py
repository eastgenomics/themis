"""Discovers which apps are actually referenced by workflows and conductor configs.

The full audit covers every repository with a dxapp.json, which includes apps
that were never wired into anything. This module finds the apps that are
genuinely in use - named by a DNAnexus workflow definition, or by an eggd_conductor
assay config - so the audit can be pointed at what actually runs.

Two reference formats, both real:

``dxworkflow.json``
    ``{"name": "dias_single_v2.16.0", "stages": [{"executable": "app-eggd_fastqc/1.2.1"}]}``

conductor assay config
    ``{"executables": {"app-J6Q1VVQ4Pf3XgF2j1jz53qv9": {"name": "eggd_MultiQC/3.3.0"}}}``

Note the conductor keys are opaque DNAnexus IDs, so the human name has to come
from the ``name`` field. Some conductor entries name a *workflow* rather than an
app; those are resolved through to the workflow's own stages, because an app used
only inside a workflow that conductor launches is still in production.

The parsing functions are pure so they can be tested against literal config
fragments; only ``discover_used_apps`` touches the API.
"""

import base64
import json
import logging
import re

logger = logging.getLogger(__name__)

#: Default repository holding the eggd_conductor assay configs.
CONDUCTOR_CONFIG_REPO = 'eggd_conductor_configs'
#: Where the assay configs live within it.
CONDUCTOR_CONFIG_PREFIX = 'assay_configs/'

#: A DNAnexus object id: 24 alphanumeric characters after the class prefix.
#: Distinguishes 'app-J6Q1VVQ4Pf3XgF2j1jz53qv9' (opaque) from
#: 'app-eggd_fastqc/1.2.1' (named). An id carries no name we can map to a repo.
DNANEXUS_ID = re.compile(r'^[0-9A-Za-z]{24}$')

EXECUTABLE_PREFIXES = ('app-', 'applet-', 'workflow-')

#: A trailing version on a workflow name: _v3.4.0, _3.4.0, _v2.16.0.
TRAILING_VERSION = re.compile(r'_v?\d+(?:\.\d+)*$', re.IGNORECASE)
#: Any version-looking run of numbers, for ordering.
VERSION_NUMBERS = re.compile(r'(\d+(?:\.\d+)*)')


def base_workflow_name(name):
    """A workflow name with its trailing version removed.

    ``uranus_main_workflow_GRCh38_v3.4.0`` -> ``uranus_main_workflow_GRCh38``.
    Also handles a missing 'v' (``gaea_main_workflow_1.0.0``), which is a typo
    that exists in the estate.
    """
    return TRAILING_VERSION.sub('', str(name or "").strip())


def version_key(text):
    """Sort key from the last version-looking number in a string.

    Compared as integer tuples so 3.10 sorts above 3.9, which a string compare
    gets backwards.
    """
    matches = VERSION_NUMBERS.findall(str(text or ""))
    if not matches:
        return ()

    return tuple(int(part) for part in matches[-1].split('.'))


def strip_version(name):
    """Drop a trailing /version from an executable name."""
    return str(name or "").split('/', 1)[0].strip()


def executable_app_name(executable):
    """The app name referenced by an executable string, or None.

    Returns None for anything that does not name an app: opaque DNAnexus ids,
    applets and workflows. Callers that care about workflows should use
    :func:`workflow_reference_name`.

    >>> executable_app_name('app-eggd_fastqc/1.2.1')
    'eggd_fastqc'
    >>> executable_app_name('app-J6Q1VVQ4Pf3XgF2j1jz53qv9') is None
    True
    """
    value = str(executable or "").strip()
    if not value:
        return None

    body = value
    for prefix in EXECUTABLE_PREFIXES:
        if value.startswith(prefix):
            if prefix != 'app-':
                # An applet or workflow is not an app repository.
                return None
            body = value[len(prefix):]
            break

    name = strip_version(body)
    if not name or DNANEXUS_ID.match(name):
        return None

    return name


def workflow_reference_name(executable, display_name=None):
    """The workflow name an executable refers to, or None.

    Conductor entries key on an opaque ``workflow-xxxx`` id and carry the real
    name alongside, so both are consulted.
    """
    value = str(executable or "").strip()
    if value.startswith('workflow-'):
        body = strip_version(value[len('workflow-'):])
        if body and not DNANEXUS_ID.match(body):
            return body
        # Opaque id - fall back to the name recorded beside it.
        return strip_version(display_name) or None

    return None


def parse_workflow_executables(dxworkflow):
    """Executable strings named by a dxworkflow.json's stages.

    Returns
    -------
        list[str]
    """
    if not isinstance(dxworkflow, dict):
        return []

    stages = dxworkflow.get('stages')
    if not isinstance(stages, list):
        return []

    return [stage.get('executable') for stage in stages
            if isinstance(stage, dict) and stage.get('executable')]


def parse_conductor_executables(config):
    """(key, name) pairs from a conductor assay config's executables block.

    Returns
    -------
        list[tuple[str, str]]
    """
    if not isinstance(config, dict):
        return []

    executables = config.get('executables')
    if not isinstance(executables, dict):
        return []

    pairs = []
    for key, value in executables.items():
        name = value.get('name') if isinstance(value, dict) else None
        pairs.append((str(key), str(name or "")))

    return pairs


def _decode(client, repo, path):
    """Fetch and parse a JSON file from a repository, or None."""
    response = client.get(
        f"/repos/{client.organisation}/{repo}/contents/{path}"
    )
    if not response or 'content' not in response:
        return None
    try:
        return json.loads(
            base64.b64decode(response['content']).decode('utf-8', 'replace')
        )
    except (ValueError, TypeError) as error:
        logger.error(f"{repo}/{path}: could not parse: {error}")
        return None


def find_workflow_files(client):
    """Locate every dxworkflow.json in the organisation.

    Returns
    -------
        list[tuple[str, str]]: (repo, path)
    """
    results = client.get(
        f"/search/code?q=org:{client.organisation}+filename:dxworkflow.json"
        f"&per_page=100"
    )
    if not results:
        logger.error("Could not search for workflow definitions.")
        return []

    return [(item['repository']['name'], item['path'])
            for item in results.get('items', [])]


def load_workflows(client):
    """Every workflow definition, keyed by its declared name.

    Returns
    -------
        dict: workflow name -> {'repo': str, 'executables': list[str]}
    """
    workflows = {}
    for repo, path in find_workflow_files(client):
        content = _decode(client, repo, path)
        if not content:
            continue
        workflows[str(content.get('name') or repo)] = {
            'repo': repo,
            'executables': parse_workflow_executables(content),
        }

    logger.info(f"Loaded {len(workflows)} workflow definition(s).")

    return workflows


def find_conductor_configs(client, repo=CONDUCTOR_CONFIG_REPO,
                           prefix=CONDUCTOR_CONFIG_PREFIX):
    """Paths of the conductor assay configs.

    Returns
    -------
        list[str]
    """
    tree = client.get(f"/repos/{client.organisation}/{repo}/git/trees/"
                      f"main?recursive=1")
    if not tree:
        logger.error(f"Could not list {repo}; is the branch named 'main'?")
        return []

    return [item['path'] for item in tree.get('tree', [])
            if item.get('type') == 'blob'
            and item['path'].startswith(prefix)
            and item['path'].endswith('.json')]


def latest_config_per_assay(configs):
    """Keep only the newest conductor config for each assay.

    ``configs`` maps path -> parsed config. An assay accumulates config files
    over time (MYE carries both uranus v4.0.2 and v5.3.0), and counting the
    superseded ones would attribute retired apps to a live assay.

    Returns
    -------
        dict: path -> config, one per assay.
    """
    newest = {}
    for path, config in configs.items():
        if not isinstance(config, dict):
            continue
        assay = str(config.get('assay') or config.get('assay_code') or path)
        # Prefer the config's own version field; fall back to the filename.
        stamp = version_key(config.get('version')) or version_key(path)
        current = newest.get(assay)
        if current is None or stamp > current[1]:
            newest[assay] = (path, stamp)

    superseded = set(configs) - {path for path, _ in newest.values()}
    if superseded:
        logger.info(
            f"Ignoring {len(superseded)} superseded conductor config(s): "
            f"{sorted(superseded)}"
        )

    return {path: configs[path] for path, _ in newest.values()}


def discover_used_apps(client, use_workflows=True, use_conductor=True,
                       conductor_repo=CONDUCTOR_CONFIG_REPO):
    """App names referenced by workflows and/or conductor configs.

    A conductor entry naming a workflow is followed through to that workflow's
    own stages: an app used only inside a workflow that conductor launches is
    still in production.

    Returns
    -------
        tuple: (used, unresolved) where `used` maps a lower-cased app name to
        the set of places referencing it, and `unresolved` holds workflow names
        a conductor config referenced but which no dxworkflow.json declares.
    """
    used = {}
    unresolved = set()

    def record(app_name, source):
        if app_name:
            used.setdefault(app_name.lower(), set()).add(source)

    workflows = load_workflows(client) if (use_workflows or use_conductor) \
        else {}
    # Resolve on the base name with the version stripped, so a config pinning a
    # superseded version still finds the workflow. Conductor configs routinely
    # lag the repo - MYE pins uranus v3.3.0 while the repo declares v3.4.0, and
    # PCAN pins eunomia v1.4.1 against v2.0.0 - and an exact match left those
    # assays' apps unattributed. Where several workflows share a base name the
    # highest version wins, which is what "the latest workflow" means.
    by_base = {}
    for name, info in workflows.items():
        base = base_workflow_name(name).lower()
        current = by_base.get(base)
        if current is None or version_key(name) > version_key(current['name']):
            by_base[base] = dict(info, name=name)

    if use_workflows:
        for name, info in workflows.items():
            for executable in info['executables']:
                record(executable_app_name(executable), name)

    if use_conductor:
        loaded = {}
        for path in find_conductor_configs(client, conductor_repo):
            config = _decode(client, conductor_repo, path)
            if config:
                loaded[path] = config

        # Only the newest config for each assay counts.
        for path, config in latest_config_per_assay(loaded).items():
            label = path.split('/')[-1].replace('.json', '')
            for key, display_name in parse_conductor_executables(config):
                workflow_name = workflow_reference_name(key, display_name)

                if workflow_name:
                    # A workflow entry names a workflow, not an app. Recording
                    # its display name as an app made every workflow look like a
                    # missing repository. Follow it through to its stages
                    # instead - an app used only inside a workflow that conductor
                    # launches is still in production.
                    info = by_base.get(
                        base_workflow_name(workflow_name).lower()
                    )
                    if info:
                        resolved = info['name']
                        if resolved != workflow_name:
                            logger.info(
                                f"{label}: pins {workflow_name!r}; using the "
                                f"current {resolved!r}."
                            )
                        for executable in info['executables']:
                            record(executable_app_name(executable),
                                   f"{label} -> {resolved}")
                    else:
                        unresolved.add(workflow_name)
                    continue

                # An app entry keys on an opaque id, so the name field is the
                # only thing that maps to a repository.
                record(executable_app_name(key), label)
                record(executable_app_name(display_name), label)

    if unresolved:
        # Worth surfacing: a workflow named by a config but absent from GitHub
        # means its apps are invisible to this mode.
        logger.warning(
            f"Conductor configs reference {len(unresolved)} workflow(s) with no "
            f"dxworkflow.json found: {sorted(unresolved)}"
        )

    logger.info(f"{len(used)} distinct apps referenced in use.")

    return used, unresolved


def filter_repos_in_use(repos, used):
    """Keep only repositories whose name is referenced in use.

    Matching is case-insensitive, because an app's declared name and its
    repository name routinely differ in case (eggd_MultiQC).

    Returns
    -------
        tuple: (kept, skipped_names, unmatched_app_names)
    """
    kept, skipped = [], []
    matched = set()

    for repo in repos:
        name = str(repo.get('name') or "").lower()
        if name in used:
            kept.append(repo)
            matched.add(name)
        else:
            skipped.append(repo.get('name'))

    unmatched = sorted(set(used) - matched)
    if unmatched:
        # Usually third-party apps (sentieon-dnaseq, cnvkit_batch) that have no
        # repository here, but a rename would look the same - so it is reported.
        logger.info(
            f"{len(unmatched)} referenced app(s) matched no repository "
            f"(third-party, or renamed): {unmatched}"
        )

    logger.info(f"{len(kept)} repositories are referenced in use.")

    return kept, skipped, unmatched
