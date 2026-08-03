"""Everything that talks to GitHub.

Named ``gh_api`` rather than ``github`` on purpose: ``github`` is the import name
of the PyPI ``PyGithub`` package, and a local ``github/`` directory would shadow
it for anything run from that working directory.

This layer's whole job is to build one ``AppEvidence`` per app. Every decision it
makes about paths is delegated to ``filepaths.py``, which is pure and tested, so
there is no branching logic here that needs mocking to exercise.
"""
