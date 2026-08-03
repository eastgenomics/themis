"""Turns result dicts into DataFrames.

The original built a one-row DataFrame per app with
``pd.DataFrame.from_dict(orient='index').transpose()`` and then ``pd.concat``-ed
each new row onto the accumulated frame - quadratic, and it produced an
all-object frame as a side effect of the transpose.

This builds one frame from all the rows. ``dtype=object`` is deliberate and load
bearing: it preserves the previous rendering exactly. Without it pandas infers
``float64`` for a column of numbers and Nones, so a python app's absent
``dist_version`` renders as ``NaN`` where it used to render as ``None``.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def build_frame(rows, columns):
    """Build a DataFrame from result dicts in registry order.

    Parameters
    ----------
        rows (list[dict]):
            One dict per app, as produced by checks.runner.
        columns (tuple[ColumnSpec, ...]):
            The registry tuple describing the table.

    Returns
    -------
        pandas.DataFrame: columns named by registry key, in registry order,
        with dtype=object throughout.
    """
    keys = [spec.key for spec in columns]
    if not rows:
        logger.warning("No apps to report on; building an empty frame.")
        return pd.DataFrame(columns=keys, dtype=object)

    data = [[row.get(key) for key in keys] for row in rows]

    return pd.DataFrame(data, columns=keys, dtype=object)


def build_summary_frame(summary_rows):
    """Build the per-measure summary DataFrame.

    Parameters
    ----------
        summary_rows (list[dict]):
            Rows from report.scoring.summarise_measures.

    Returns
    -------
        pandas.DataFrame
    """
    if not summary_rows:
        logger.warning("No summary measures to report.")
        return pd.DataFrame(
            columns=['Name', 'No. Compliant / Total', 'Compliance %']
        )

    # Not dtype=object here: 'Compliance %' must stay numeric for the Styler's
    # conditional formatting to compare it against its thresholds.
    return pd.DataFrame(summary_rows)
