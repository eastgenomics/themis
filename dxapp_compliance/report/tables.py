"""Renames and selects columns for rendering, from the registry.

Replaces ``compliance_df_format``, which held two hand-maintained rename maps
and two ``drop(columns=[...])`` calls. Those are now derived, so a new check
appears in the report by virtue of being declared once.
"""

import logging

from dxapp_compliance.checks.registry import display_map, rendered_columns

logger = logging.getLogger(__name__)


def format_table(frame, columns):
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
    wanted = rendered_columns(columns)

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
