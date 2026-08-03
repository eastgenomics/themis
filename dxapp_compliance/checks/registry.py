"""The single source of truth for what is checked, scored and rendered.

Before this existed, adding a check meant editing five unrelated places, and
missing any one of them failed silently:

* ``compliance_stats`` hardcoded ``total_performa = 12 if 'bash' in x else 10``
  as the score denominator.
* ``compliance_count`` was ``(checks_df == True).T.sum()``, counting every True
  cell in the row rather than the checks.
* ``compliance_scores_for_each_measure`` had an ``available_columns``
  allow-list; a column missing from it was dropped from the summary silently.
* ``new_col_names`` mapped keys to labels for the summary.
* ``compliance_df_format`` had two more rename maps plus two ``drop`` calls.

The consequence was a live bug: three boolean columns were appended after the
denominator was written, giving bash apps 13 possible passes over a denominator
of 12 and a reachable score of 108.33%. Everything above is now derived from the
tuples below, and ``tests/test_registry.py`` asserts the runner produces exactly
the declared keys.
"""

from dataclasses import dataclass, field
from enum import Enum, auto

#: The one "not applicable" marker. The original used both "NA" (in the check
#: functions) and "N/A" (in the GitHub-facing functions); any denominator rule
#: keyed on one silently mishandled the other.
NOT_APPLICABLE = "NA"

BASH = "bash"
PYTHON = "python"
UNKNOWN = "unknown"
ANY_INTERPRETER = frozenset({BASH, PYTHON, UNKNOWN})
BASH_ONLY = frozenset({BASH})


def interpreter_family(interpreter):
    """Map a dxapp.json runSpec.interpreter to a family name.

    Parameters
    ----------
        interpreter (str):
            The raw value, e.g. 'bash', 'python3'.

    Returns
    -------
        str: one of BASH, PYTHON, UNKNOWN.
    """
    if not interpreter:
        return UNKNOWN
    if interpreter == BASH:
        return BASH
    if PYTHON in interpreter:
        return PYTHON

    return UNKNOWN


class Role(Enum):
    """Whether a column contributes to the compliance score."""

    SCORED = auto()
    INFO = auto()


@dataclass(frozen=True)
class ColumnSpec:
    """One column of a result table.

    Attributes
    ----------
        key (str):
            The key the runner emits, and the DataFrame column name.
        display (str):
            The header text in the rendered HTML table and summary.
        role (Role):
            SCORED columns count toward the compliance percentage.
        applies_to (frozenset):
            Interpreter families the check is meaningful for. The runner writes
            NOT_APPLICABLE for any app outside this set, which replaces the
            hand-rolled "if python: set everything to NA" short-circuit.
        in_table (bool):
            Whether to render the column. Some columns are needed in the
            DataFrame but not shown - `last_release_date` feeds the plots,
            `dxapp_boolean` is scored but was dropped from the display.
    """

    key: str
    display: str
    role: Role
    applies_to: frozenset = field(default=ANY_INTERPRETER)
    in_table: bool = True


# Order is rendered order. URL must stay last: the template resolves it by
# header text now, but keeping it last preserves the previous column layout.
COMPLIANCE_COLUMNS = (
    ColumnSpec('name', 'name', Role.INFO),
    ColumnSpec('compliance_score', 'compliance %', Role.INFO),
    ColumnSpec('authorised_users', 'Auth Users', Role.SCORED),
    ColumnSpec('authorised_devs', 'Auth Devs', Role.SCORED),
    ColumnSpec('interpreter', 'File Type', Role.INFO),
    ColumnSpec('uptodate_ubuntu', 'Ubuntu 20+', Role.SCORED,
               applies_to=BASH_ONLY),
    ColumnSpec('timeout_policy', 'Timeout Policy', Role.SCORED),
    ColumnSpec('correct_regional_option', 'Correct Region', Role.SCORED),
    ColumnSpec('set_e', '`set -e` Present', Role.SCORED,
               applies_to=BASH_ONLY),
    ColumnSpec('no_manual_compiling', 'No Manual Compile', Role.SCORED,
               applies_to=BASH_ONLY),
    # Dependency provenance. All three apply to bash and python alike: the pip
    # check matters most for python apps, which the old code excluded wholesale.
    ColumnSpec('no_network_access', 'No Network Access', Role.SCORED),
    ColumnSpec('no_remote_package_install', 'No Remote Pkg Install',
               Role.SCORED),
    ColumnSpec('pip_uses_local_wheels', 'Pip Local Wheels', Role.SCORED),
    ColumnSpec('dxapp_boolean', 'DNAnexus App', Role.SCORED, in_table=False),
    ColumnSpec('dxapp_or_applet', 'App or Applet', Role.INFO),
    ColumnSpec('eggd_name_boolean', 'eggd_ name', Role.SCORED),
    ColumnSpec('eggd_title_boolean', 'eggd_ title', Role.SCORED),
    ColumnSpec('dependabot_alerts_status', 'Dependabot alerts', Role.SCORED),
    ColumnSpec('dependabot_security_status', 'Dependabot security',
               Role.SCORED),
    # Applicability is data-driven rather than interpreter-driven: the runner
    # writes NOT_APPLICABLE when the repo contains no Python at all. Bash apps
    # that ship a Python helper are in scope and are scored.
    ColumnSpec('requirements_file_exists', 'Requirements file exists',
               Role.SCORED),
    # Carried for the plots, not rendered.
    ColumnSpec('num_of_region_options', 'Total Regions', Role.INFO,
               in_table=False),
    ColumnSpec('timeout_setting', 'Timeout Setting', Role.INFO,
               in_table=False),
    ColumnSpec('last_release_date', 'Last Release', Role.INFO,
               in_table=False),
    ColumnSpec('latest_commit_date', 'Last Commit', Role.INFO),
    ColumnSpec('URL', 'URL', Role.INFO),
)

# The details table. A separate tuple because the same key means a different
# thing here: `authorised_users` is a boolean in the compliance dict and a
# comma-joined string in the details dict, and `timeout_policy` appears as
# `timeout`. One keyed registry cannot express that.
DETAIL_COLUMNS = (
    ColumnSpec('name', 'name', Role.INFO),
    ColumnSpec('compliance_score', 'compliance %', Role.INFO),
    ColumnSpec('authorised_users', 'Auth Users', Role.INFO),
    ColumnSpec('authorised_devs', 'Auth Devs', Role.INFO),
    ColumnSpec('interpreter', 'File Type', Role.INFO),
    ColumnSpec('distribution', 'Distribution', Role.INFO, in_table=False),
    ColumnSpec('dist_version', 'Ubuntu Version', Role.INFO),
    ColumnSpec('regionalOptions', 'Regions', Role.INFO),
    ColumnSpec('title', 'Title', Role.INFO, in_table=False),
    ColumnSpec('timeout', 'Timeout', Role.INFO, in_table=False),
    ColumnSpec('timeout_setting', 'Timeout Setting', Role.INFO),
    ColumnSpec('set_e', '`set -e` Present', Role.INFO),
    ColumnSpec('no_manual_compiling', 'No Manual Compile', Role.INFO),
    ColumnSpec('asset_present', 'Assets', Role.INFO),
    # Dependency provenance evidence. These carry the offending file and line so
    # the report is actionable without cloning the repo - which matters because
    # regex detection errs in both directions.
    ColumnSpec('network_access', 'Network Access', Role.INFO),
    ColumnSpec('exec_depends', 'execDepends', Role.INFO),
    ColumnSpec('package_install_details', 'Remote Pkg Evidence', Role.INFO),
    ColumnSpec('pip_install_details', 'Pip Evidence', Role.INFO),
    ColumnSpec('dxapp_or_applet', 'App or Applet', Role.INFO),
    ColumnSpec('dependabot_alerts_status', 'Dependabot alerts', Role.INFO),
    ColumnSpec('dependabot_security_status', 'Dependabot security',
               Role.INFO),
    ColumnSpec('requirements_file_exists', 'Requirements file exists',
               Role.INFO),
    ColumnSpec('last_release_date', 'Last Release', Role.INFO),
    ColumnSpec('latest_commit_date', 'Last Commit', Role.INFO),
    ColumnSpec('URL', 'URL', Role.INFO),
)

#: Header text the report template uses to locate the hyperlink column.
URL_DISPLAY = 'URL'
#: Header text the report template sorts on by default.
SCORE_DISPLAY = 'compliance %'


def compliance_keys():
    """Every key the compliance dict must contain, in order."""
    return tuple(spec.key for spec in COMPLIANCE_COLUMNS)


def detail_keys():
    """Every key the details dict must contain, in order."""
    return tuple(spec.key for spec in DETAIL_COLUMNS)


def scored_specs(family=None):
    """Scored columns, optionally filtered to those applicable to a family.

    Parameters
    ----------
        family (str, optional):
            An interpreter family. When given, only checks applicable to it are
            returned.

    Returns
    -------
        tuple[ColumnSpec, ...]
    """
    specs = tuple(s for s in COMPLIANCE_COLUMNS if s.role is Role.SCORED)
    if family is None:
        return specs

    return tuple(s for s in specs if family in s.applies_to)


def summary_specs():
    """Columns that appear as rows in the per-measure summary table."""
    return scored_specs()


def display_map(columns):
    """Map keys to display labels for a column tuple."""
    return {spec.key: spec.display for spec in columns}


def rendered_columns(columns):
    """Display labels of the columns to render, in order."""
    return [spec.display for spec in columns if spec.in_table]
