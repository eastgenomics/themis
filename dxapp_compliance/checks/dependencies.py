"""Dependency provenance: where an app's packages come from.

This module answers "does this app fetch packages from a remote server while a
job is running?". Such an app is not reproducible - its behaviour depends on the
state of a third-party archive on the day it ran.

The dxapp.json readers live here. The script scanning for apt and pip is added
alongside them.

Schema note, verified against the DNAnexus I/O and Run Specifications reference:
``assetDepends`` and ``bundledDepends`` are nested **under runSpec**, and
``execDepends[].package_manager`` is optional and **defaults to "apt"**. The
previous code read a top-level ``assetsDepends`` - the wrong key *and* the wrong
nesting level - which is why the report's "Assets" column was always False.
"""

import logging

logger = logging.getLogger(__name__)

#: Valid runSpec.execDepends package managers, per the DNAnexus docs.
PACKAGE_MANAGERS = ('apt', 'pip3', 'pip', 'gem', 'cpan', 'cran', 'git')
#: What DNAnexus assumes when package_manager is omitted.
DEFAULT_PACKAGE_MANAGER = 'apt'


def _run_spec(dxjson_content):
    """runSpec as a dict, tolerating a missing or malformed value."""
    run_spec = dxjson_content.get('runSpec') if dxjson_content else None

    return run_spec if isinstance(run_spec, dict) else {}


def read_exec_depends(dxjson_content):
    """Read runSpec.execDepends, tolerating malformed values.

    Returns
    -------
        list: the entries that are mappings. Anything else is discarded with a
        warning rather than raising - a single malformed dxapp.json must not
        abort the whole audit.
    """
    entries = _run_spec(dxjson_content).get('execDepends')
    if not entries:
        return []
    if not isinstance(entries, list):
        logger.warning(f"runSpec.execDepends is not a list: {type(entries)}")
        return []

    usable = [entry for entry in entries if isinstance(entry, dict)]
    if len(usable) != len(entries):
        logger.warning("Discarded non-mapping runSpec.execDepends entries")

    return usable


def describe_exec_depends(dxjson_content):
    """Summarise runSpec.execDepends for the details table.

    Returns
    -------
        tuple: (count, rendered) where rendered is e.g.
        "apt:bcftools, pip3:pandas==1.2". An entry with no package_manager
        renders as apt, because that is what DNAnexus defaults to.
    """
    entries = read_exec_depends(dxjson_content)
    if not entries:
        return 0, ""

    parts = []
    for entry in entries:
        manager = entry.get('package_manager') or DEFAULT_PACKAGE_MANAGER
        name = entry.get('name', '?')
        version = entry.get('version')
        parts.append(f"{manager}:{name}" + (f"=={version}" if version else ""))

    return len(entries), ", ".join(parts)


def describe_asset_depends(dxjson_content):
    """Summarise runSpec.assetDepends for the details table.

    Entries resolve either by record id or by name+project+version, so both
    shapes are handled.

    Returns
    -------
        tuple: (count, rendered)
    """
    entries = _run_spec(dxjson_content).get('assetDepends')
    if not entries or not isinstance(entries, list):
        return 0, ""

    parts = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        # Either shape: {'id': 'record-xxxx'} or {'name', 'project', 'version'}
        parts.append(entry.get('name') or entry.get('id') or '?')

    return len(parts), ", ".join(parts)


def describe_bundled_depends(dxjson_content):
    """Summarise runSpec.bundledDepends for the details table.

    Returns
    -------
        tuple: (count, rendered)
    """
    entries = _run_spec(dxjson_content).get('bundledDepends')
    if not entries or not isinstance(entries, list):
        return 0, ""

    names = [entry.get('name', '?') for entry in entries
             if isinstance(entry, dict)]

    return len(names), ", ".join(names)


def check_assets_present(dxjson_content):
    """Whether the app declares asset or bundled dependencies.

    Both count: each is a way of shipping a pinned artefact with the app rather
    than resolving it from a remote archive at runtime.

    Returns
    -------
        bool
    """
    asset_count, _ = describe_asset_depends(dxjson_content)
    bundled_count, _ = describe_bundled_depends(dxjson_content)

    return bool(asset_count or bundled_count)
