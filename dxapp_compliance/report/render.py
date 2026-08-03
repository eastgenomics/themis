"""Renders the HTML report.

Two changes from the original beyond the move. Tables are rendered with
``index=False``: the pandas index was emitted as a leading column and then
hidden by a DataTables rule, and that phantom column was the origin of every
off-by-one in the template's column indices. And ``release_comp_plot`` was
computed on every run but never referenced by the template - it is now shown.
"""

import logging

from jinja2 import Environment, FileSystemLoader

from dxapp_compliance.config import TEMPLATE_DIR, today_date

logger = logging.getLogger(__name__)

TABLE_CLASSES = "table table-striped table-hover"

#: Thresholds for the summary table's red/amber/green conditional formatting.
POOR_BELOW = 50
GOOD_ABOVE = 80


def _summary_html(summary_df):
    """Style and render the per-measure summary table."""
    if summary_df.empty:
        return "<p><em>No measures to summarise.</em></p>"

    styled = summary_df.style.apply(
        lambda column: [
            'background-color: #FFB3BA' if value < POOR_BELOW else
            'background-color: #BAFFC9' if value > GOOD_ABOVE else
            'background-color: #FFBF00'
            for value in column
        ],
        subset=['Compliance %'],
    ).hide(axis='index').format(precision=0)

    return styled.to_html(
        table_attributes=f"class = '{TABLE_CLASSES}'",
        table_uuid="compliance_stats_summary",
        bold_headers=True,
        justify="left",
    )


def render_report(compliance_df, detailed_df, summary_df, plots,
                  output_dir=None):
    """Render the report and write it to disk.

    Parameters
    ----------
        compliance_df (pandas.DataFrame):
            Display-labelled compliance table.
        detailed_df (pandas.DataFrame):
            Display-labelled details table.
        summary_df (pandas.DataFrame):
            Per-measure summary.
        plots (dict):
            Figure HTML keyed by template variable name.
        output_dir (str or Path, optional):
            Where to write the report. Defaults to the working directory, as
            before.

    Returns
    -------
        str: the path written.
    """
    # autoescape is left at Jinja2's default of False deliberately: the plot
    # variables are raw plotly HTML and escaping mangles them. Do not add
    # select_autoescape here.
    environment = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = environment.get_template("Report.html")

    filename = f"Audit_{today_date()}.html"
    path = f"{output_dir}/{filename}" if output_dir else filename

    context = {
        # index=False: the hidden pandas index column is what made every
        # index-based DataTables rule fragile.
        "Compliance_table": compliance_df.to_html(
            table_id="comp", classes=TABLE_CLASSES, index=False
        ),
        "Details_table": detailed_df.to_html(
            table_id="details", classes=TABLE_CLASSES, index=False
        ),
        "compliance_stats_summary": _summary_html(summary_df),
    }
    context.update(plots)

    with open(path, mode="w", encoding="utf-8") as results:
        results.write(template.render(context))

    logger.info(f"Wrote {path}")
    print(f"... wrote {path}")

    return path
