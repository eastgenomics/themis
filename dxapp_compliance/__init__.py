"""Compliance auditing for East Genomics DNAnexus app repositories.

Audits every repository in the configured GitHub organisation that contains a
root ``dxapp.json`` against the East GLH DNAnexus app standards, and renders an
HTML report.

Run with::

    uv run python -m dxapp_compliance.main

The package is layered, and the layering is enforced by
``tests/test_layering.py``:

``checks/``
    Pure functions over already-fetched content. Import no pandas, no
    ``gh_api``, no ``requests``/``ghapi`` - which is what makes them testable
    without mocking the GitHub API.
``gh_api/``
    Everything that talks to GitHub. Collects an ``AppEvidence`` for each repo.
``report/``
    Scoring, table formatting, plots and rendering.
"""
