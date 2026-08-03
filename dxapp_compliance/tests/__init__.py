"""Tests for dxapp_compliance.

Fixtures are inline Python constants in ``fixtures.py`` rather than files on
disk. Two reasons:

1. The repository's ``.gitignore`` has a bare ``*.json`` rule, so any fixture
   ``dxapp.json`` would be silently untracked.
2. It matches the pattern already used by ``TAT_audit/tests``, which pastes API
   payloads in as class-level attributes.

Run with ``uv run pytest dxapp_compliance/tests -v`` from the repository root.
No ``sys.path`` manipulation is needed: with ``__init__.py`` present here and in
the parent package, pytest's prepend import mode puts the repository root on
``sys.path`` and ``from dxapp_compliance... import ...`` resolves directly.
"""
