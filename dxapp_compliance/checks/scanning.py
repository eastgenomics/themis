"""Shared primitives for finding commands in shell and Python source.

Everything here is regex over text, so it is a triage signal rather than a proof.
The limits are documented in the Readme and in
``dependencies.py``'s module docstring - the evidence string, not the boolean, is
what makes a finding actionable.

The single most important construct is :data:`CMD_START`. Requiring a tool name
to appear in *command position* is what stops ``# pip install x``,
``echo "pip install x"`` and ``grep -q "apt-get install"`` from matching, without
a pile of special cases - while still admitting ``$(...)``, subshells and
``subprocess.run("pip install x")``.
"""

import logging
import re

logger = logging.getLogger(__name__)

# --- shared regex fragments -------------------------------------------------

#: A command must start a line or follow a shell separator. The `(` branch also
#: admits `$( )`, subshells and Python's subprocess.run("...").
CMD_START = r"(?:^|[;&|(){}`]|\bthen\b|\bdo\b|\belse\b)\s*['\"]?\s*"

#: Zero or more command wrappers and inline environment assignments, e.g.
#: `sudo`, `sudo -H`, `DEBIAN_FRONTEND=noninteractive`, `PIP_NO_CACHE_DIR=1`.
PREFIX = (
    r"(?:(?:sudo|env|time|nohup|command|exec|xargs)"
    r"(?:\s+-{1,2}[\w-]+(?:=\S+)?)*\s+"
    r"|[A-Za-z_][A-Za-z0-9_]*=\S*\s+)*"
)

#: Flags between an executable and its subcommand, e.g. `apt -y install`.
#: Deliberately does not consume space-separated flag *values*, so it cannot
#: swallow a package name.
OPTS = r"(?:\s+-{1,2}[\w-]+(?:=\S+)?)*"

#: Optional quote plus optional directory prefix: `/usr/bin/pip3`,
#: `${VENV}/bin/pip`.
EXE = r"['\"]?(?:[\w.${}~+-]*/)*"

#: An interpreter held in a variable: `"$PYTHON" -m pip`, `${PY3} -m pip`.
VAREXE = r"['\"]?\$\{?\w+\}?['\"]?"

#: Whole-line veto for reporting and logging. Without this, CMD_START's `(`
#: branch would match print("pip install pandas") and
#: logger.info("running pip install %s", pkg).
NOISE_LINE = re.compile(
    r"^\s*(?:#|echo\b|printf\b|print\s*\(|pprint\b|logger\.|logging\.|log\."
    r"|sys\.std(?:out|err)\.write|warnings\.warn|raise\b|assert\b"
    r"|parser\.add_argument)"
)

# --- runtime script scoping -------------------------------------------------

#: Paths whose contents run inside a job.
RUNTIME_PREFIXES = ('src/', 'resources/')

#: Directory segments that are never app runtime code.
EXCLUDED_SEGMENTS = frozenset({
    'tests', 'test', 'testing', '.github', 'docs', 'doc',
    'examples', 'example', 'build', 'scripts',
})

#: Filenames that are tooling rather than runtime code. `build_asset*` matters
#: most: such a script exists precisely to run `apt-get install` at *build* time
#: and bake the result into a DNAnexus asset, which is the compliant pattern.
#: Scanning it would fail apps for doing exactly the right thing.
EXCLUDED_NAMES = re.compile(
    r"(?:^|/)(?:test_[^/]*\.py|[^/]*_test\.py|conftest\.py"
    r"|build[_-]?asset[^/]*|setup\.py)$"
)

SCRIPT_SUFFIXES = ('.sh', '.py', '.bash')


def is_script_path(path):
    """Whether a repository path is a shell or Python script.

    Dockerfiles are deliberately excluded - build-time image construction is out
    of scope for this audit, and the omission is asserted in the tests so it
    cannot drift.
    """
    return path.endswith(SCRIPT_SUFFIXES)


def is_runtime_script(path, runspec_file=""):
    """Whether a script's contents can execute inside a job.

    The app entrypoint always qualifies. Otherwise the path must sit under
    ``src/`` or ``resources/`` and must not be test, CI, docs or asset-build
    tooling.
    """
    if not path:
        return False
    if runspec_file and path == runspec_file:
        return True
    if not path.startswith(RUNTIME_PREFIXES):
        return False
    if EXCLUDED_NAMES.search(path):
        return False

    segments = set(path.split('/')[:-1])

    return not (segments & EXCLUDED_SEGMENTS)


# --- line normalisation -----------------------------------------------------

def iter_logical_lines(text):
    """Yield (line_number, logical_line) pairs ready for matching.

    Joins backslash continuations, collapses runs of whitespace to a single
    space, and drops whole-line comments. The reported line number is that of
    the *first* physical line of a continued run, which is the line a reader
    needs to open.

    Heredoc bodies are included. That is deliberate: skipping them would miss
    ``curl ... <<EOF | bash``. The cost is that a heredoc which documents a
    command rather than running it can produce a false positive.
    """
    if not text:
        return []

    results = []
    buffer = ""
    buffer_lineno = None

    for offset, raw in enumerate(text.splitlines(), start=1):
        line = raw.rstrip('\r')
        if buffer_lineno is None:
            buffer_lineno = offset

        stripped = line.strip()
        if stripped.endswith('\\'):
            buffer += stripped[:-1] + " "
            continue

        buffer += stripped
        logical = re.sub(r'\s+', ' ', buffer).strip()
        if logical and not logical.startswith('#'):
            results.append((buffer_lineno, logical))
        buffer = ""
        buffer_lineno = None

    # A trailing continuation with nothing after it must not be lost.
    if buffer.strip():
        logical = re.sub(r'\s+', ' ', buffer).strip()
        if logical and not logical.startswith('#'):
            results.append((buffer_lineno or 1, logical))

    return results


# --- evidence formatting ----------------------------------------------------

SNIPPET_MAX = 100
MAX_FINDINGS = 3
CELL_MAX = 400
#: Rendered when a check passes. An empty cell reads as missing data in a
#: DataTable, which is not the same message.
NOTHING_FOUND = "None detected"


def truncate_snippet(line, limit=SNIPPET_MAX):
    """Collapse whitespace and cut to a length that fits a table cell."""
    collapsed = re.sub(r'\s+', ' ', line or "").strip()
    if len(collapsed) <= limit:
        return collapsed

    # ASCII rather than a single-character ellipsis, so the DataTables CSV and
    # print exports stay clean.
    return collapsed[:limit] + "..."


def make_finding(kind, path, line_no, line, reason=""):
    """One located match."""
    return {
        'kind': kind,
        'path': path,
        'line_no': line_no,
        'snippet': truncate_snippet(line),
        'reason': reason,
    }


def sort_findings(findings):
    """Deterministic, de-duplicated ordering.

    Without this the cell contents follow GitHub's response ordering and every
    month-to-month report diff is noise.
    """
    seen = set()
    unique = []
    for finding in sorted(findings,
                          key=lambda f: (f['kind'], f['path'], f['line_no'])):
        identity = (finding['kind'], finding['path'], finding['line_no'])
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(finding)

    return unique


def _render_finding(finding):
    # line_no 0 means the finding came from a dxapp.json field rather than a line
    # of script, so there is no line to cite.
    location = finding['path']
    if finding['line_no']:
        location += f":{finding['line_no']}"

    rendered = f"{finding['kind']} [{location}] {finding['snippet']}"
    if finding.get('reason'):
        rendered += f" [{finding['reason']}]"

    return rendered


def format_evidence(findings, supporting="", noun="remote install",
                    empty_msg=NOTHING_FOUND, max_findings=MAX_FINDINGS,
                    cell_limit=CELL_MAX):
    """Render findings into one table cell.

    Format::

        <count> <noun>(s): <finding> | <finding> (+N more) || local: <supporting>
    """
    findings = sort_findings(findings or [])

    if findings:
        shown = [_render_finding(f) for f in findings[:max_findings]]
        head = f"{len(findings)} {noun}(s): " + " | ".join(shown)
        if len(findings) > max_findings:
            head += f" (+{len(findings) - max_findings} more)"
    else:
        head = empty_msg

    if supporting:
        head += f" || local: {supporting}"

    if len(head) > cell_limit:
        cut = head[:cell_limit]
        boundary = cut.rfind(" | ")
        head = (cut[:boundary] if boundary > 0 else cut) + " ..."

    return head
