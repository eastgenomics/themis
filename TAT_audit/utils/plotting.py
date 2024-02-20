import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from plotly.subplots import make_subplots


class PlottingFunctions():
    """
    Functions related to plotting
    """
    def __init__(
        self, cancelled_statuses, audit_start, audit_end, tat_standard,
        font_size
    ):
        self.cancelled_statuses = cancelled_statuses
        self.audit_start = audit_start
        self.audit_end = audit_end
        self.tat_standard = tat_standard
        self.font_size = font_size


    def create_tat_fig_split_by_week(self, assay_df, assay_type):
        """
        Creates stacked bar for each run of that assay type
        with relevant time periods on bar. Split by week, with empty weeks
        included with no bars

        Parameters
        ----------
        assay_df :  pd.DataFrame
            dataframe with only rows from that assay type
        assay_type : str
            e.g. 'CEN'

        Returns
        -------
        html_fig : str
            Plotly figure as HTML string
        """

        # Remove any cancelled runs from plotting
        assay_df = assay_df[
            ~assay_df.jira_status.isin(self.cancelled_statuses)
        ]
        # Get the start of all possible weeks based on the audit dates
        date_weeks = [
            period.start_time.date().strftime('%d-%m-%y')
            for period in pd.period_range(
                start=self.audit_start,
                end=self.audit_end,
            freq='W'
            )
        ]

        # Work out how many runs are in that week to make normalised widths
        # If no runs in that week make 1 so we can still plot a subplot for
        # that week
        run_totals = []
        for week in date_weeks:
            df = assay_df.loc[assay_df['week_start'] == week]
            df_len = len(df)
            if df_len == 0:
                df_len = 1
            run_totals.append(df_len)

        norm_widths = [float(i)/sum(run_totals) for i in run_totals]

        # Make subplots for each possible week
        fig = make_subplots(
            rows=1,
            cols=len(date_weeks),
            shared_yaxes=True,
            subplot_titles=[f"w/c<br>{str(week)}" for week in date_weeks],
            column_widths=norm_widths
        )

        for idx, week in enumerate(date_weeks):
            week_df = assay_df.loc[assay_df['week_start'] == week]

            if len(week_df):
                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['upload_to_first_job'],
                        name='Upload to processing start',
                        marker={'color': '#636EFA'},
                        customdata=week_df['run_name'],
                        legendgroup="group1",
                    ), row=1, col=idx+1
                )

                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['processing_time'],
                        name='Pipeline running',
                        marker={'color': '#EF553B'},
                        customdata=week_df['run_name'],
                        legendgroup='group2'
                    ), row=1, col=idx+1
                )

                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['processing_end_to_release'],
                        name='Pipeline end to all samples released',
                        marker={'color': '#00CC96'},
                        customdata=week_df['run_name'],
                        text=round(week_df['upload_to_release'], 1),
                        legendgroup='group3'
                    ), row=1, col=idx+1
                )

                if "Urgent samples released" in week_df.jira_status.values:
                    fig.append_trace(
                        go.Bar(
                            x=week_df['ticket_hyperlink'],
                            y=week_df['urgents_time'],
                            name=(
                                'Pipeline end to now - urgent samples released'
                            ),
                            marker={'color': '#FFA15A'},
                            customdata=week_df['run_name'],
                            legendgroup='group4'
                        ), row=1, col=idx+1
                    )

                if "On hold" in week_df.jira_status.values:
                    fig.add_trace(
                        go.Bar(
                            x=week_df['ticket_hyperlink'],
                            y=week_df['on_hold_time'],
                            name='Last processing step to now - On hold',
                            marker={'color': '#FECB52'},
                            customdata=week_df['run_name'],
                            legendgroup='group4'
                        ), row=1, col=idx+1
                    )


            else:
                fig.append_trace(
                    go.Bar(
                        x=week_df['ticket_hyperlink'],
                        y=week_df['processing_end_to_release'],
                        name='Fake data',
                    ), row=1, col=idx+1
                )
                fig.update_xaxes(showticklabels=False, row=1, col=idx+1)

        fig.add_hline(y=self.tat_standard, line_dash="dash")

        fig.update_xaxes(
            tickangle=45, categoryorder='category ascending'
        )

        fig.update_layout(
            barmode='relative',
            title={
                'text': f"{assay_type} Turnaround Times "
                        f"{self.audit_start} - {self.audit_end}",
                'xanchor': 'center',
                'x': 0.5,
                'font_size': 18
            },
            yaxis_title="Number of days",
            width=1100,
            height=700,
            font_family='Helvetica',
            legend_traceorder='reversed'
        )

        fig.update_traces(
            hovertemplate=(
                '<br><b>Run</b>: %{customdata}<br>'
                '<b>Stage</b>: %{data.name}<br>'
                '<b>Days</b>: %{y:.2f}<br>'
                '<extra></extra>'
            ),
            textposition='outside',
            width=0.7
        )

        # Each subplot adds its own legend entry so remove any duplicates
        # from the legend
        names = set()
        fig.for_each_trace(
            lambda trace:
                trace.update(showlegend=False)
                if (trace.name in names) else names.add(trace.name))

        # Update the subplot titles font size using command line arg
        # (default 12)
        fig.update_annotations(font_size=self.font_size)

        # Add x axis title as an annotation because plotly subplots are
        # annoying
        fig.add_annotation(
            x=0.5,
            xanchor='center',
            xref='paper',
            y=0,
            yanchor='top',
            yref='paper',
            showarrow=False,
            text='Run name',
            yshift=-160
        )

        # Convert to HTML string
        html_fig = fig.to_html(full_html=False, include_plotlyjs=False)

        return html_fig


    def create_upload_day_fig(self, assay_df, assay_type):
        """
        Create figure to see if the day of the week for data upload impacts
        turnaround time

        Parameters
        ----------
        assay_df : pd.DataFrame()
            dataframe with rows for an assay type with columns including
            run name, upload timestamp and turnaround time in days
        assay_type : str
            the assay type of interest e.g. 'CEN'

        Returns
        -------
        html_fig : str
            Plotly figure as html string
        """
        if 'upload_to_release' in assay_df.columns:
            number_of_relevant_runs = assay_df['upload_to_release'].count()
        else:
            number_of_relevant_runs = None

        if (len(assay_df) and number_of_relevant_runs):
            # Add df column with names of the day of the week that data were
            # uploaded
            assay_df['upload_day'] = assay_df['upload_time'].dt.day_name()
            # Plot upload day vs TAT, if TAT is <= tat_standard colour in green
            # otherwise colour in red
            fig = px.scatter(
                data_frame=assay_df,
                x='upload_day',
                y='upload_to_release',
                custom_data=['run_name'],
                color=assay_df["upload_to_release"] <= float(self.tat_standard),
                color_discrete_map={
                    True: "green",
                    False: "red"
                },
            )
            # Set days in order
            fig.update_xaxes(
                range=[-0.5, 6.5],
                type='category',
                categoryorder='array',
                categoryarray= [
                    "Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday"
                ]
            )

            fig.update_layout(
                title={
                    'text': f'{assay_type} Upload Day vs Turnaround Time',
                    'xanchor': 'center',
                    'x':0.5
                },
                xaxis_title="Upload day of the week",
                yaxis_title="Turnaround time (days)",
                font_family='Helvetica',
                legend=dict(title='Within standards'),
                width=1000,
                height=500,
            )
            # Add run name to hovertext
            fig.update_traces(
                hovertemplate="Run name: %{customdata[0]} <br> Turnaround time: %{y:.2f} days"
            )
        # If empty show empty plot with message
        else:
            fig = go.Figure()
            fig.update_layout(
                font_family='Helvetica',
                xaxis={"visible": False},
                yaxis={"visible": False},
                annotations = [
                    {
                        "text": "No data",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {
                            "size": 28
                        }
                    }
                ]
            )

        # Convert to HTML string
        html_fig = fig.to_html(full_html=False, include_plotlyjs=False)

        return html_fig


    def create_both_figures(self, assay_df, assay_type):
        """
        Create the two figures required for the assay

        Parameters
        ----------
        assay_df : pd.DataFrame
            dataframe of all runs for the specific assay type
        assay_type : str
            the assay type, e.g. 'CEN'

        Returns
        -------
        assay_fig : str
            Plotly TAT fig as HTML string
        assay_upload_fig : str
            Plotly fig showing upload day of the week vs TAT for that assay
            as HTML string
        """
        assay_fig = self.create_tat_fig_split_by_week(
            assay_df,
            assay_type,
        )

        assay_upload_fig = self.create_upload_day_fig(
            assay_df,
            assay_type,
        )

        return assay_fig, assay_upload_fig
