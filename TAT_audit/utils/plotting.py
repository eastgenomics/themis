import plotly.express as px
import plotly.graph_objects as go


class PlotFunctions():
    """
    Methods for plotting things
    """
    def __init__(
        self, tat_standard, cancelled_statuses, audit_start, audit_end
    ):
        self.tat_standard = tat_standard
        self.cancelled_statuses = cancelled_statuses
        self.audit_start = audit_start
        self.audit_end = audit_end

    def create_TAT_fig(self, assay_df, assay_type):
        """
        Creates stacked bar for each run of that assay type
        with relevant time periods on bar
        Parameters
        ----------
        assay_df :  pd.DataFrame()
            dataframe with only rows from that assay type
        assay_type : str
            e.g. 'CEN'
        Returns
        -------
        html_fig : str
            Plotly figure as HTML string
        """
        assay_df = assay_df[~assay_df.jira_status.isin(self.cancelled_statuses)]

        if len(assay_df):
            fig = go.Figure()

            # Add trace for Log file to first job
            if not assay_df['upload_to_first_job'].isnull().all():
                fig.add_trace(
                    go.Bar(
                        x=assay_df['ticket_hyperlink'],
                        y=assay_df["upload_to_first_job"],
                        name="Upload to processing start",
                        customdata=assay_df['run_name'],
                        legendrank=4
                    )
                )

            # Add trace for bioinformatics run time
            if not assay_df['processing_time'].isnull().all():
                fig.add_trace(
                    go.Bar(
                        x=assay_df["ticket_hyperlink"],
                        y=assay_df["processing_time"],
                        name="Pipeline running",
                        customdata=assay_df['run_name'],
                        legendrank=3
                    )
                )

            # Add trace for release, only add full TAT above bar if we have
            # Upload to release time
            if not assay_df['processing_end_to_release'].isnull().all():
                fig.add_trace(
                    go.Bar(
                        x=assay_df["ticket_hyperlink"],
                        y=assay_df["processing_end_to_release"],
                        name="Processing end to all samples released",
                        customdata=assay_df['run_name'],
                        legendrank=2,
                        text=round(assay_df['upload_to_release'], 1)
                    )
                )

            if "Urgent samples released" in assay_df.jira_status.values:
                fig.add_trace(
                    go.Bar(
                        x=assay_df["ticket_hyperlink"],
                        y=assay_df["urgents_time"],
                        customdata=assay_df['run_name'],
                        name="Processing end to now - Urgent samples released",
                        marker_color='#FFA15A'
                    )
                )

            if "On hold" in assay_df.jira_status.values:
                fig.add_trace(
                    go.Bar(
                        x=assay_df["ticket_hyperlink"],
                        y=assay_df["on_hold_time"],
                        customdata=assay_df['run_name'],
                        name="Last processing step to now - On hold",
                        marker_color='#FECB52'
                    )
                )

            if len(fig.data) > 0:
                fig.add_hline(y=self.tat_standard, line_dash="dash")

                fig.update_xaxes(
                    tickangle=45, categoryorder='category ascending'
                )

                fig.update_traces(
                    hovertemplate=(
                        '<br><b>Run</b>: %{customdata}<br>'
                        '<b>Stage</b>: %{data.name}<br>'
                        '<b>Days</b>: %{y:.2f}<br>'
                        '<extra></extra>'
                    ),
                    textposition='outside'
                )

                # Update relevant aspects of chart
                fig.update_layout(
                    barmode='relative',
                    title={
                        'text': f"{assay_type} Turnaround Times "
                                f"{self.audit_start} - {self.audit_end}",
                        'xanchor': 'center',
                        'x': 0.5,
                        'font_size': 20
                    },
                    xaxis_title="Run name",
                    yaxis_title="Number of days",
                    width=1100,
                    height=700,
                    font_family='Helvetica',
                    legend_traceorder="reversed"
                )
                if len(fig.data) == 1:
                    fig['data'][0]['showlegend'] = True
            else:
                fig.update_layout(
                    font_family='Helvetica',
                    xaxis =  { "visible": False },
                    yaxis = { "visible": False },
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

        # If empty show empty plot with message
        else:
            fig = go.Figure()
            fig.update_layout(
                font_family='Helvetica',
                xaxis =  { "visible": False },
                yaxis = { "visible": False },
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

        html_fig = fig.to_html(full_html=False, include_plotlyjs=False)

        return html_fig
