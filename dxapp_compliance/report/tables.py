"""Renames and selects columns for rendering, from the registry.

Replaces ``compliance_df_format``, which held two hand-maintained rename maps
and two ``drop(columns=[...])`` calls. Those are now derived, so a new check
appears in the report by virtue of being declared once.
"""

import logging

from dxapp_compliance.checks.registry import display_map, rendered_columns

logger = logging.getLogger(__name__)


def format_table(frame, columns, exclude=()):
    """Rename to display labels and keep only the columns to render.

    Parameters
    ----------
        frame (pandas.DataFrame):
            A frame whose columns are registry keys.
        columns (tuple[ColumnSpec, ...]):
            The registry tuple describing the table.

    Returns
    -------
        pandas.DataFrame: display-labelled, in registry order, carrying only
        the columns marked for rendering.
    """
    renamed = frame.rename(columns=display_map(columns))
    wanted = rendered_columns(columns, exclude=exclude)

    missing = [label for label in wanted if label not in renamed.columns]
    if missing:
        # Should be unreachable: checks.runner asserts its keys match the
        # registry. Report rather than raise so a run still produces a report.
        logger.error(
            f"Columns declared for rendering but absent from the frame: "
            f"{missing}"
        )
        wanted = [label for label in wanted if label in renamed.columns]

    return renamed[wanted]


def split_by_assay(frame, columns, exclude=()):
    """One display-ready table per assay.

    An app referenced by several assays appears in each of their tables, which is
    correct: it really is in use by each. Apps with no assay attribution are
    omitted rather than lumped into a catch-all - the per-assay view is about
    what a given assay runs.

    Parameters
    ----------
        frame (pandas.DataFrame):
            A frame whose columns are registry keys, including 'assays'.
        columns (tuple[ColumnSpec, ...]):
            The registry tuple describing the table.

    Returns
    -------
        dict: assay code -> formatted DataFrame, ordered by assay name.
    """
    if frame.empty or 'assays' not in frame.columns:
        return {}

    per_assay = {}
    for assay in sorted({
        code.strip()
        for value in frame['assays']
        for code in str(value or "").split(',')
        if code.strip()
    }):
        mask = frame['assays'].apply(
            lambda value: assay in [
                c.strip() for c in str(value or "").split(',')
            ]
        )
        subset = frame[mask]
        if not subset.empty:
            per_assay[assay] = format_table(subset, columns, exclude=exclude)

    return per_assay
