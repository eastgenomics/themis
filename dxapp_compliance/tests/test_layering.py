"""Guards the package layering that makes the checks testable without mocks.

The checks are only mocklessly testable for as long as they stay free of the
GitHub client and pandas. That is easy to erode with one convenient import, so
it is asserted here rather than left to convention.

Uses ``ast`` rather than importing the modules, so these tests pass without any
third-party package installed.
"""

import ast
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parents[1]


def _module_paths(subpackage):
    """Every .py file in a subpackage, excluding tests."""
    return sorted((PACKAGE_DIR / subpackage).rglob("*.py"))


def _imported_roots(path):
    """Top-level module names imported by a file, e.g. 'pandas', 'requests'."""
    tree = ast.parse(path.read_text(), filename=str(path))
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            # Relative imports have no module root to police.
            if node.level == 0 and node.module:
                roots.add(node.module.split('.')[0])
    return roots


def _imported_dotted(path):
    """Fully dotted module names imported by a file."""
    tree = ast.parse(path.read_text(), filename=str(path))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                names.add(node.module)
    return names


class TestChecksLayerIsPure():
    forbidden = {'pandas', 'numpy', 'requests', 'ghapi', 'fastcore', 'plotly',
                 'jinja2', 'statsmodels'}

    def test_no_third_party_imports(self):
        offenders = {}
        for path in _module_paths("checks"):
            bad = _imported_roots(path) & self.forbidden
            if bad:
                offenders[path.name] = sorted(bad)
        assert offenders == {}, (
            "checks/ must stay free of third-party imports so it is testable "
            f"without mocking or installing them; found {offenders}"
        )

    def test_no_gh_api_imports(self):
        offenders = {}
        for path in _module_paths("checks"):
            bad = {name for name in _imported_dotted(path)
                   if 'gh_api' in name}
            if bad:
                offenders[path.name] = sorted(bad)
        assert offenders == {}, (
            "checks/ must not import the GitHub layer - checks receive already "
            f"fetched content as arguments; found {offenders}"
        )


class TestReportLayer():
    def test_scoring_has_no_pandas(self):
        """Scoring must operate on dicts, not frames.

        `1 == True` is True in pandas, so a frame-based numerator counts any
        integer column as a pass, and a numpy.bool_ cell fails `is True`.
        Keeping pandas out of this module makes both mistakes unavailable.
        """
        path = PACKAGE_DIR / "report" / "scoring.py"
        roots = _imported_roots(path)
        assert 'pandas' not in roots and 'numpy' not in roots, (
            f"report/scoring.py must stay pandas-free; it imports {roots}"
        )

    def test_report_does_not_import_the_github_layer(self):
        offenders = {}
        for path in _module_paths("report"):
            bad = {name for name in _imported_dotted(path) if 'gh_api' in name}
            if bad:
                offenders[path.name] = sorted(bad)
        assert offenders == {}, (
            f"report/ renders already-collected results; found {offenders}"
        )


class TestNoStarImports():
    def test_no_star_imports_in_package_modules(self):
        """`from x import *` is what hid `re` and HTTP404NotFoundError.

        In dxapp_queries.py `re` had twelve call sites and never appeared as an
        import, arriving only via `from fastcore.all import *`. New modules must
        import explicitly. dxapp_queries.py itself is exempt until it is reduced
        to a shim.
        """
        offenders = []
        for path in sorted(PACKAGE_DIR.rglob("*.py")):
            if path.name == "dxapp_queries.py":
                continue
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    if any(a.name == '*' for a in node.names):
                        offenders.append(f"{path.name}:{node.lineno}")
        assert offenders == [], (
            f"Star imports hide where names come from; found at {offenders}"
        )
