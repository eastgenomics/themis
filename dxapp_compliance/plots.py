import plotly.express as px
import pandas as pd


class plotting:
    """
    Collection of plotting functions to use plotly to
    create plots of compliance performa for each app/applet repo.
    """

    def __init__(self):
        pass

    def import_csv(self, path_to_dataframe):
        """
        Imports csv into pandas dataframe for plotting.
        Converts compliance column into float if present.

        Parameters
        ----------
            path_to_dataframe (str): string for absolute/relative path to
                csv file.
        Returns
        -------
            df (pandas dataframe):
                pandas dataframe of csv file with minor changes.
        """
        df = pd.read_csv(path_to_dataframe)
        if 'compliance_score' in df.columns:
            df['compliance_score'] = df['compliance_score'].str.rstrip(
                "%").astype(float)

        return df

    def release_date_compliance_plot(self, df):
        """
        Convert date to datetime object.

        Parameters
        ----------
            df (dataframe):
                dataframe of apps/applets with release date and compliance score.

        Returns
        -------
            html_fig (plotly html plot object):
                html plot object of apps/applets with release date and compliance score.
        """
        # Convert release_date to pandas datetime column
        df['last_release_date'] = pd.to_datetime(df['last_release_date'])
        # Convert % column to numeric float column
        df_ordered = df.sort_values(by=['last_release_date'])

        fig = px.scatter(
            data_frame=df_ordered,
            x=df_ordered['last_release_date'],
            y=df_ordered['compliance_score'],
            labels={
                'x': 'Date of last release',
                'y': 'Compliance (%)'
            },
        )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

        return html_fig

    def compliance_by_latest_activity_plot(self, df):
        """
        Convert date to datetime object.

        Parameters
        ----------
            df (dataframe):
                dataframe of apps/applets with release date and compliance score.

        Returns
        -------
            html fig (plotly html plot):
                plot html object of apps/applets with release date and compliance score.
        """
        # Convert release_date to pandas datetime column
        df['latest_commit_date'] = pd.to_datetime(df['latest_commit_date'])
        # Convert % column to numeric float column
        df_ordered = df.sort_values(by=['latest_commit_date'])

        fig = px.scatter(
            data_frame=df_ordered,
            x=df_ordered['latest_commit_date'],
            y=df_ordered['compliance_score'],
            labels={
                'x': 'Date of last release',
                'y': 'Compliance (%)'
            },
            trendline="lowess",  # needs module statsmodels
        )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

        return html_fig

    def ubuntu_compliance_timeseries(self, df):
        """
        Convert date to datetime object.

        Parameters
        ----------
            df (dataframe):
                dataframe of apps/applets with ubuntu version,
                release date, and compliance score.

        Returns
        -------
            html fig (plotly html plot):
                plot html object of apps/applets with release date,
                ubuntu version, and compliance score.
        """
        # Convert release_date to pandas datetime column
        df = df[df['interpreter'] == 'bash']
        df['last_release_date'] = pd.to_datetime(df['last_release_date'])
        # Convert % column to numeric float column
        df_ordered = df.sort_values(by=['last_release_date'])

        df_ordered['dist_version'] = df_ordered['dist_version'].astype('str')

        fig = px.scatter(
            data_frame=df_ordered,
            x=df_ordered['last_release_date'],
            y=df_ordered['compliance_score'],
            color=df_ordered['dist_version'],
            # trendline="lowess",
            # trendline_scope="overall",
            labels={
                'x': 'Date of last release',
                'y': 'Compliance (%)',
                'color': 'Ubuntu version',
            },
            hover_name="name",
            hover_data=["last_release_date",
                        "dist_version"],
        )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

        return html_fig

    def bash_version(self, df):
        """
        Uses plotly to plot the distribution of bash ubuntu versions.

        Parameters
        ----------
            df (pandas df): dataframe of apps compliance data.

        Returns
        -------
            html_fig (plotly html plot):
                plot of bash and python apps.
        """
        # Find all apps with bash as the interpreter
        dfout = df[df['interpreter'] == 'bash']
        dfout.sort_values(by=['dist_version'])
        dfout["dist_version"] = dfout["dist_version"].values.astype('str')
        dfout = dfout['dist_version'].value_counts().rename_axis(
            'unique_versions').reset_index(name='counts')

        fig = px.bar(dfout, x="unique_versions", y="counts",
                     color="unique_versions",
                     labels={
                         "unique_versions": "Ubuntu version",
                         "counts": "Count",
                     },
                     title="Version of Ubuntu by bash apps",
                     )
        html_fig = fig.to_html(full_html=True, include_plotlyjs=True)

        return html_fig
        # TODO: Add a bar chart for other compliance stats.
