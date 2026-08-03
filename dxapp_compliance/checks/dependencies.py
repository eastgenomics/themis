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
import re
import shlex

from dxapp_compliance.checks.scanning import (
    CMD_START,
    EXE,
    NOISE_LINE,
    NOTHING_FOUND,
    OPTS,
    PREFIX,
    VAREXE,
    format_evidence,
    is_runtime_script,
    iter_logical_lines,
    make_finding,
)

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


# ---------------------------------------------------------------------------
# Remote OS-package installs
# ---------------------------------------------------------------------------

#: `apt-get update` alone contacts the archive but installs nothing. Scored by
#: default; flip this to make it evidence-only.
SCORE_APT_UPDATE_ONLY = True

#: Hosts that only ever serve packages. Used for archive fetches whose URL
#: carries no package extension, e.g. .../pool/main/....
PACKAGE_HOSTS = (
    r"archive\.ubuntu\.com|security\.ubuntu\.com|ppa\.launchpad\.net"
    r"|[\w-]+\.debian\.org|deb\.[\w-]+\.\w+"
)
DOWNLOADERS = r"(?:curl|wget|aria2c|axel)"

APT_PATTERNS = {
    'apt-install': re.compile(
        CMD_START + PREFIX + r"['\"]?(?:apt-get|apt|aptitude)" + OPTS
        + r"\s+install\b(?P<args>[^\n;&|]*)"
    ),
    'apt-update': re.compile(
        CMD_START + PREFIX + r"['\"]?(?:apt-get|apt)" + OPTS + r"\s+update\b"
    ),
    'add-apt-repo': re.compile(
        CMD_START + PREFIX + r"['\"]?(?:add-apt-repository|apt-add-repository)\b"
    ),
    'yum-install': re.compile(
        CMD_START + PREFIX + r"['\"]?(?:yum|dnf|zypper)" + OPTS
        + r"\s+install\b"
    ),
    'conda-install': re.compile(
        CMD_START + PREFIX + r"['\"]?(?:conda|mamba|micromamba)" + OPTS
        + r"\s+install\b"
    ),
    # The narrow curl/wget rule: only a URL that names a package artefact.
    # A .tar.gz is indistinguishable from a reference genome by URL alone, and
    # `wget tarball && make install` is already covered by no_manual_compiling.
    'deb-download': re.compile(
        CMD_START + PREFIX + DOWNLOADERS
        + r"\b[^\n;&|]*?(?:https?|ftp)://\S+\.(?:deb|rpm|udeb)\b"
    ),
    'apt-host-download': re.compile(
        CMD_START + PREFIX + DOWNLOADERS + r"\b[^\n;&|]*?(?:" + PACKAGE_HOSTS
        + r")"
    ),
    'pipe-to-shell': re.compile(
        CMD_START + PREFIX + DOWNLOADERS + r"\b[^\n]*\|\s*" + PREFIX
        + r"(?:ba|z|d)?sh\b"
    ),
    # pip inside `docker run` is not in shell command position, so it needs its
    # own pattern.
    'docker-inline-install': re.compile(
        r"docker\s+(?:run|exec)\b[^\n;&|]*?\s"
        r"(?:(?:apt-get|apt)\s+(?:-{1,2}[\w-]+\s+)*install|pip[23]?\s+install)\b"
    ),
}

#: Local install of an already-present .deb. Evidence that the app is doing the
#: right thing, never a finding. The `-[a-zA-Z]*i[a-zA-Z]*` branch requires a
#: single leading dash so `dpkg -l | grep` and `dpkg --list` do not match.
DPKG_LOCAL_RE = re.compile(
    CMD_START + PREFIX + r"['\"]?dpkg\s+(?:-{1,2}[\w-]+\s+)*"
    r"(?:--install|-[a-zA-Z]*i[a-zA-Z]*)\b"
)


def _apt_install_targets_are_local(args):
    """Whether every target of an apt install is a local .deb path.

    `apt-get install ./foo.deb` really does install a local file, though apt
    still resolves that package's dependencies from the archive - so it is
    reported under its own kind rather than excused.
    """
    try:
        tokens = shlex.split(args, posix=True)
    except ValueError:
        tokens = args.split()
    targets = [t for t in tokens if not t.startswith('-')]

    return bool(targets) and all(t.lower().endswith('.deb') for t in targets)


def scan_for_remote_packages(scripts, runspec_file=""):
    """Find remote OS-package installs across an app's scripts.

    Returns
    -------
        tuple: (scored, unscored, local_evidence) - lists of findings. Hits in
        non-runtime paths land in `unscored` so a reviewer still sees them
        without the app being penalised.
    """
    scored, unscored, local = [], [], []

    for path in sorted(scripts):
        runtime = is_runtime_script(path, runspec_file)
        for line_no, line in iter_logical_lines(scripts[path]):
            if NOISE_LINE.match(line):
                continue

            if DPKG_LOCAL_RE.search(line):
                local.append(make_finding('dpkg-local', path, line_no, line))

            for kind, pattern in APT_PATTERNS.items():
                match = pattern.search(line)
                if not match:
                    continue
                if kind == 'apt-update' and not SCORE_APT_UPDATE_ONLY:
                    continue

                reason = ""
                if kind == 'apt-install':
                    args = match.groupdict().get('args') or ""
                    if _apt_install_targets_are_local(args):
                        kind = 'apt-local-deb'
                        reason = ("local .deb, but apt still resolves its "
                                  "dependencies from the archive")

                finding = make_finding(kind, path, line_no, line, reason)
                (scored if runtime else unscored).append(finding)

    return scored, unscored, local


def evaluate_remote_package_install(dxjson_content, scripts=None,
                                    repo_paths=(), runspec_file=""):
    """Whether the app installs OS packages from a remote server at job time.

    FAIL is dominant: a single remote install falsifies "this app fetches no
    packages while a job runs", however much else is bundled. When the archive is
    unreachable or a version drifts the job dies, so being 90% bundled buys
    nothing. The gradient lives in the evidence string, which reports both sides.

    Returns
    -------
        tuple: (no_remote_package_install, details)
    """
    scripts = scripts or {}
    scored, unscored, local = scan_for_remote_packages(scripts, runspec_file)

    exec_count, exec_rendered = describe_exec_depends(dxjson_content)
    if exec_count:
        scored.append(make_finding(
            'exec-depends', 'dxapp.json', 0, exec_rendered,
            # Accurate rationale: execDepends does NOT require network access -
            # the platform installs from the default Ubuntu and DNAnexus
            # repositories even under restricted networking. The problem is
            # reproducibility.
            "resolved at job start, not pinned to the app release",
        ))

    supporting = _local_package_evidence(dxjson_content, repo_paths, local)

    passed = not scored
    details = format_evidence(scored, supporting=supporting)
    if unscored:
        details += (f" || unscored ({len(unscored)} in non-runtime paths): "
                    f"{unscored[0]['path']}:{unscored[0]['line_no']}")
    if not scripts:
        details = "(no scripts fetched) " + details

    return passed, details


def _local_package_evidence(dxjson_content, repo_paths, local_findings):
    """Human-readable summary of what the app bundles instead."""
    parts = []

    if local_findings:
        first = local_findings[0]
        parts.append(
            f"dpkg -i [{first['path']}:{first['line_no']}]"
            + (f" (+{len(local_findings) - 1} more)"
               if len(local_findings) > 1 else "")
        )

    debs = [p for p in repo_paths
            if p.startswith('resources/') and p.lower().endswith('.deb')]
    if debs:
        parts.append(f"{len(debs)} .deb under resources/")

    asset_count, asset_rendered = describe_asset_depends(dxjson_content)
    if asset_count:
        parts.append(f"runSpec.assetDepends={asset_count} ({asset_rendered})")

    bundled_count, bundled_rendered = describe_bundled_depends(dxjson_content)
    if bundled_count:
        parts.append(
            f"runSpec.bundledDepends={bundled_count} ({bundled_rendered})"
        )

    return ", ".join(parts)


# ---------------------------------------------------------------------------
# pip installs that do not use local wheels
# ---------------------------------------------------------------------------

LOCAL = 'local'
REMOTE = 'remote'

PIP_INSTALL_RE = re.compile(
    CMD_START + PREFIX
    + r"(?:" + EXE + r"(?:pip[23]?(?:\.\d+)?|pipx)['\"]?"
    + r"|(?:" + EXE + r"python[\d.]*['\"]?|" + VAREXE + r")\s+-m\s+pip)"
    + OPTS + r"\s+install\b(?P<args>[^\n;&|]*)"
)

#: Flags that consume the following token as their value.
#:
#: This table is load bearing. Without it `pip install -f /local/wheels pandas`
#: parses `/local/wheels` as a target, sees a path, and reports "local" - the
#: exact opposite of the truth, because pip still falls back to PyPI for pandas.
VALUE_FLAGS = frozenset({
    '-f', '--find-links', '-i', '--index-url', '--extra-index-url',
    '--cache-dir', '--target', '-t', '--prefix', '--root', '--constraint',
    '-c', '-r', '--requirement', '--trusted-host', '--python', '--log',
    '--proxy', '--platform', '--only-binary', '--no-binary', '--build',
    '--upgrade-strategy', '--report', '--config-settings', '-C', '--abi',
    '--implementation', '--python-version', '--src', '--global-option',
})

VCS_URL_RE = re.compile(
    r"^(?:git|hg|svn|bzr)\+|^(?:https?|ftp|file)://|^git@[\w.-]+:"
)
ARTEFACT_SUFFIXES = ('.whl', '.tar.gz', '.tgz', '.zip', '.tar.bz2')


def split_pip_targets(args):
    """Split the tokens after `install` into flags and positional targets.

    Returns
    -------
        tuple: (flags, targets)
    """
    try:
        tokens = shlex.split(args, posix=True)
    except ValueError:
        # Unbalanced quotes - fall back rather than abort the audit.
        tokens = args.split()

    flags, targets = [], []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith('-'):
            flags.append(token)
            if '=' not in token and token in VALUE_FLAGS:
                index += 1
        else:
            targets.append(token)
        index += 1

    return flags, targets


def _is_local_artefact(token):
    """Whether a pip target names a file on disk rather than an index entry."""
    if token.lower().endswith(ARTEFACT_SUFFIXES):
        return True

    return '/' in token or '*' in token


def classify_pip_install(args):
    """Classify one pip invocation as local or remote.

    Returns
    -------
        tuple: (LOCAL or REMOTE, reason)
    """
    flags, targets = split_pip_targets(args)
    flagset = {flag.split('=')[0] for flag in flags}

    if '--no-index' in flagset:
        return LOCAL, "uses --no-index"

    for target in targets:
        if VCS_URL_RE.match(target):
            return REMOTE, f"URL or VCS target {target}"

    if flagset & {'-r', '--requirement'}:
        return REMOTE, "requirements file without --no-index"

    # A source tree, not a built artefact: its dependencies still resolve from
    # PyPI. Checked before the path test, or `-e ./pkg` would look local.
    if '-e' in flagset or '--editable' in flagset:
        return REMOTE, "editable install of a source tree"

    if targets and all(_is_local_artefact(t) for t in targets):
        return LOCAL, "local artefact path"

    if not targets:
        return REMOTE, "no explicit target parsed"

    if targets == ['.']:
        return REMOTE, "installs a source tree, dependencies from PyPI"

    reason = "package name resolved from an index"
    if flagset & {'-i', '--index-url', '--extra-index-url'}:
        reason += " (custom index-url)"

    return REMOTE, reason


def scan_for_pip_installs(scripts, runspec_file=""):
    """Find pip invocations across an app's scripts.

    Returns
    -------
        tuple: (remote, local, unscored) - lists of findings.
    """
    remote, local, unscored = [], [], []

    for path in sorted(scripts):
        runtime = is_runtime_script(path, runspec_file)
        for line_no, line in iter_logical_lines(scripts[path]):
            if NOISE_LINE.match(line):
                continue
            match = PIP_INSTALL_RE.search(line)
            if not match:
                continue

            verdict, reason = classify_pip_install(
                match.groupdict().get('args') or ""
            )
            finding = make_finding('pip-install', path, line_no, line, reason)

            if not runtime:
                unscored.append(finding)
            elif verdict == REMOTE:
                remote.append(finding)
            else:
                local.append(finding)

    return remote, local, unscored


def evaluate_pip_provenance(dxjson_content, scripts=None, repo_paths=(),
                            runspec_file=""):
    """Whether the app's pip installs come from local wheels.

    Returns NOT_APPLICABLE when the app makes no pip calls at all: the check is
    about *how* pip is used, so with no pip it is vacuous and should neither
    reward nor penalise the app. It therefore drops out of the denominator.

    Returns
    -------
        tuple: (pip_uses_local_wheels, details)
    """
    from dxapp_compliance.checks.registry import NOT_APPLICABLE

    scripts = scripts or {}
    remote, local, unscored = scan_for_pip_installs(scripts, runspec_file)

    wheels = [p for p in repo_paths if p.lower().endswith('.whl')]
    supporting_parts = []
    if local:
        supporting_parts.append(f"{len(local)} local pip install(s)")
    if wheels:
        supporting_parts.append(f"{len(wheels)} .whl in repo")
    supporting = ", ".join(supporting_parts)

    exec_pip = [
        entry for entry in read_exec_depends(dxjson_content)
        if (entry.get('package_manager') or "").startswith('pip')
    ]

    if not remote and not local and not unscored:
        details = NOTHING_FOUND
        if exec_pip:
            details += (f" || note: runSpec.execDepends declares "
                        f"{len(exec_pip)} pip package(s) - see Remote Pkg "
                        f"Install")
        if supporting:
            details += f" || local: {supporting}"
        return NOT_APPLICABLE, details

    details = format_evidence(remote, supporting=supporting,
                              noun="remote pip install")
    if unscored:
        details += (f" || unscored ({len(unscored)} in non-runtime paths): "
                    f"{unscored[0]['path']}:{unscored[0]['line_no']}")

    return not remote, details
