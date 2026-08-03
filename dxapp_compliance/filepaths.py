"""Pure logic over a repository's file path list.

Separated from the GitHub layer so every decision it makes - which paths are
scripts, which one is the entrypoint, whether a bundled .deb exists - is
testable without touching the API. That leaves ``gh_api/collect.py`` with no
branching worth testing.

Stdlib only.
"""

import logging

from dxapp_compliance.checks.scanning import (
    RUNTIME_PREFIXES,
    is_runtime_script,
    is_script_path,
)

logger = logging.getLogger(__name__)

#: Never fetch a blob larger than this. Nothing legitimate in an app's runtime
#: path is bigger, and the tree listing gives us the size for free.
MAX_BLOB_BYTES = 256 * 1024

#: Ceiling on blobs fetched per repository, so one vendored directory cannot
#: exhaust the API budget for the whole run.
MAX_SCRIPTS_PER_REPO = 40

#: Paths never worth fetching, whatever their extension.
VENDORED_MARKERS = ('node_modules/', '.venv/', 'site-packages/', '.git/',
                    'vendor/', '.tox/')


def is_vendored(path):
    """Whether a path is third-party code shipped inside the repo."""
    return any(marker in path for marker in VENDORED_MARKERS)


def resolve_entrypoint(paths, runspec_file=""):
    """Determine the app's entrypoint script from the file listing.

    Replaces a cascade of 404-probing API calls. The old fallback looped over
    ``src/`` and *reassigned* on every match, so it kept the last ``.py``/``.sh``
    in the directory rather than the first - non-deterministic in effect. This is
    deterministic, which will change the reported ``set_e`` for a few repos with
    several scripts in ``src/``.

    Parameters
    ----------
        paths (iterable[str]):
            Every path in the repository.
        runspec_file (str):
            The value of runSpec.file from dxapp.json.

    Returns
    -------
        str or None
    """
    paths = list(paths)

    if runspec_file and runspec_file in paths:
        return runspec_file

    if runspec_file:
        logger.info(
            f"runSpec.file {runspec_file!r} is not in the repository; falling "
            f"back to the first script under src/."
        )

    candidates = sorted(
        path for path in paths
        if path.startswith('src/') and is_script_path(path)
        and not is_vendored(path)
    )
    if candidates:
        if len(candidates) > 1:
            logger.info(
                f"Several scripts under src/; using {candidates[0]}. "
                f"Others: {candidates[1:]}"
            )
        return candidates[0]

    logger.warning("No entrypoint script found.")

    return None


def scannable_scripts(paths, runspec_file="", limit=MAX_SCRIPTS_PER_REPO):
    """Paths whose contents the dependency checks should read.

    Only runtime code: the entrypoint plus scripts under src/ and resources/.
    Test, CI, docs and asset-build paths are excluded - a ``pip install`` in a
    vendored library or an asset builder is not the app installing anything, and
    scanning them is the main false-positive risk.

    Returns
    -------
        list[str]: sorted, capped at ``limit``.
    """
    selected = sorted(
        path for path in paths
        if is_script_path(path)
        and not is_vendored(path)
        and is_runtime_script(path, runspec_file)
    )

    if len(selected) > limit:
        logger.warning(
            f"Repository has {len(selected)} runtime scripts; scanning the "
            f"first {limit}. Findings beyond that are not reported."
        )
        selected = selected[:limit]

    return selected


def has_deb_resources(paths):
    """Whether the repo bundles .deb packages under resources/."""
    return bool(deb_resources(paths))


def deb_resources(paths):
    """Bundled .deb packages under resources/."""
    return [path for path in paths
            if path.startswith('resources/') and path.lower().endswith('.deb')]


def wheel_paths(paths):
    """Python wheels anywhere in the repo.

    Supporting evidence only. Wheels frequently arrive via a DNAnexus asset
    rather than the repository, so their absence proves nothing.
    """
    return [path for path in paths if path.lower().endswith('.whl')]


def has_requirements_txt(paths):
    """Whether a requirements.txt exists anywhere in the repo.

    Anywhere, not just the root: a bash app's Python helper usually keeps its
    requirements alongside itself under resources/.
    """
    return any(
        path.split('/')[-1].lower() == 'requirements.txt'
        for path in paths
    )


def runtime_prefixes():
    """Exposed for tests and logging."""
    return RUNTIME_PREFIXES
