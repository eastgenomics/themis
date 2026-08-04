"""Plotly figures for the report, as embeddable HTML.

Every function copies its input before touching it. The originals mutated the
caller's dataframe - ``df['last_release_date'] = pd.to_datetime(...)`` operated
on the frame the caller still held and went on to render, so the rendered
"Last Commit" column was a datetime64 that happened to format like the original
string. Copying makes the plots side-effect free.

``plotting.import_csv`` is gone; it was never called.
"""

import logging

import pandas as pd
import plotly.express as px
from plotly.offline import get_plotlyjs_version

logger = logging.getLogger(__name__)

FONT = dict(size=18, color="black")


def plotlyjs_cdn_url():
    """CDN URL for the plotly.js build matching the installed plotly.py.

    Taken from plotly rather than hardcoded, so the bundle can never drift out of
    step with the figures it has to render.
    """
    return f"https://cdn.plot.ly/plotly-{get_plotlyjs_version()}.min.js"


def _figure_html(fig):
    """Render a figure as a fragment, without its own copy of plotly.js.

    Each figure used to be rendered with ``full_html=True``, which inlines the
    whole ~4.8 MB plotly.js bundle - three times over, for a 14 MB report. The
    library is now loaded once from the CDN by the template. The report already
    depends on a CDN for jQuery, bootstrap and DataTables, so this adds no new
    requirement.
    """
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _empty_plot_html(message):
    """Placeholder for when there is nothing to plot."""
    logger.warning(f"Not plotting: {message}")

    return f"<p><em>No data to plot: {message}</em></p>"


def release_date_compliance_plot(df):
    """Compliance percentage against date of last release.

    Parameters
    ----------
        df (pandas.DataFrame):
            Frame with 'last_release_date' and 'compliance_score' columns.

    Returns
    -------
        str: the figure as standalone HTML.
    """
    df = df.copy()
    if df.empty:
        return _empty_plot_html("no apps")

    df['last_release_date'] = pd.to_datetime(df['last_release_date'],
                                             errors='coerce')
    ordered = df.sort_values(by=['last_release_date'])

    fig = px.scatter(
        data_frame=ordered,
        x=ordered['last_release_date'],
        y=ordered['compliance_score'],
        labels={
            'last_release_date': 'Date of last release',
            'compliance_score': 'Compliance (%)',
        },
        hover_name="name",
    )
    fig.update_layout(font=FONT)

    return _figure_html(fig)


def compliance_by_latest_activity_plot(df):
    """Compliance percentage against date of last commit, with a trendline.

    Parameters
    ----------
        df (pandas.DataFrame):
            Frame with 'latest_commit_date' and 'compliance_score' columns.

    Returns
    -------
        str: the figure as standalone HTML.
    """
    df = df.copy()
    if df.empty:
        return _empty_plot_html("no apps")

    df['latest_commit_date'] = pd.to_datetime(df['latest_commit_date'],
                                              errors='coerce')
    ordered = df.sort_values(by=['latest_commit_date'])

    fig = px.scatter(
        data_frame=ordered,
        x=ordered['latest_commit_date'],
        y=ordered['compliance_score'],
        labels={
            'latest_commit_date': 'Date of last commit',
            'compliance_score': 'Compliance (%)',
        },
        # Requires statsmodels to be importable.
        trendline="lowess",
        hover_name="name",
        hover_data=["latest_commit_date"],
    )
    fig.update_layout(font=FONT)

    return _figure_html(fig)


def ubuntu_compliance_timeseries(df):
    """Compliance against release date, coloured by Ubuntu version.

    Bash apps only - python apps have no Ubuntu version to colour by.

    Parameters
    ----------
        df (pandas.DataFrame):
            Details frame with 'interpreter', 'dist_version',
            'last_release_date' and 'compliance_score' columns.

    Returns
    -------
        str: the figure as standalone HTML.
    """
    df = df[df['interpreter'] == 'bash'].copy()
    if df.empty:
        return _empty_plot_html("no bash apps")

    df['last_release_date'] = pd.to_datetime(df['last_release_date'],
                                             errors='coerce')
    ordered = df.sort_values(by=['last_release_date'])
    ordered['dist_version'] = ordered['dist_version'].astype('str')

    fig = px.scatter(
        data_frame=ordered,
        x=ordered['last_release_date'],
        y=ordered['compliance_score'],
        color=ordered['dist_version'],
        labels={
            'last_release_date': 'Date of last release',
            'compliance_score': 'Compliance (%)',
            'dist_version': 'Ubuntu version',
        },
        hover_name="name",
        hover_data=["last_release_date", "dist_version"],
    )
    fig.update_layout(font=FONT)

    return _figure_html(fig)
