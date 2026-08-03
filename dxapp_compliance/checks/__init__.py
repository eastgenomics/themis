"""Compliance checks.

Every module in this package is a pure function of already-fetched content:
a parsed ``dxapp.json`` dict, decoded script text, and a list of repository file
paths. Nothing here talks to GitHub, and nothing here imports pandas.

That is what makes the checks testable without mocking: a test constructs the
input as literal Python and asserts on the returned value. The repository has no
mock infrastructure and its existing tests use none, so this constraint is
enforced by ``tests/test_layering.py`` rather than left to convention.
"""
