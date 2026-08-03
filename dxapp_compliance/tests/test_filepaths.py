"""Tests for the pure path logic behind the fetch layer.

These cover every decision gh_api/collect.py makes, which is what leaves that
module with nothing needing a mock.
"""

from dxapp_compliance import filepaths


class TestResolveEntrypoint():
    paths = ('dxapp.json', 'src/code.sh', 'src/helper.py', 'README.md')

    def test_runspec_file_preferred(self):
        assert filepaths.resolve_entrypoint(self.paths, 'src/code.sh') == \
            'src/code.sh', "runSpec.file should win when it exists"

    def test_falls_back_to_first_src_script(self):
        """Deterministic. The old fallback reassigned on every match, so it kept
        the LAST script in src/, not the first."""
        assert filepaths.resolve_entrypoint(self.paths, 'src/missing.sh') == \
            'src/code.sh', (
            "With runSpec.file absent, the alphabetically first src/ script "
            "should be chosen deterministically"
        )

    def test_stable_across_input_order(self):
        forward = filepaths.resolve_entrypoint(self.paths, '')
        backward = filepaths.resolve_entrypoint(tuple(reversed(self.paths)), '')
        assert forward == backward, (
            "Entrypoint resolution must not depend on API response ordering"
        )

    def test_none_when_no_scripts(self):
        assert filepaths.resolve_entrypoint(('dxapp.json', 'README.md'), '') \
            is None, "No script means no entrypoint, not an exception"

    def test_entrypoint_outside_src(self):
        assert filepaths.resolve_entrypoint(('code.sh', 'dxapp.json'),
                                            'code.sh') == 'code.sh', (
            "runSpec.file may sit at the repository root"
        )


class TestScannableScripts():
    paths = (
        'dxapp.json',
        'src/code.sh',
        'src/helper.py',
        'resources/home/dnanexus/wrap.sh',
        'tests/test_helper.py',
        'build_asset.sh',
        '.github/workflows/ci.sh',
        'docs/example.sh',
        'setup.py',
        'resources/.venv/lib/site-packages/requests/api.py',
        'Dockerfile',
        'README.md',
    )

    def test_runtime_scripts_selected(self):
        selected = filepaths.scannable_scripts(self.paths, 'src/code.sh')
        assert 'src/code.sh' in selected, "The entrypoint must be scanned"
        assert 'src/helper.py' in selected, "src/ scripts run in the job"
        assert 'resources/home/dnanexus/wrap.sh' in selected, (
            "resources/ scripts ship with the app and run"
        )

    def test_asset_builder_excluded(self):
        """The important exclusion: build_asset.sh runs apt-get install at build
        time to bake an asset, which is the compliant pattern."""
        selected = filepaths.scannable_scripts(self.paths, 'src/code.sh')
        assert 'build_asset.sh' not in selected, (
            "Scanning an asset builder would fail apps for doing the right thing"
        )

    def test_tooling_paths_excluded(self):
        selected = filepaths.scannable_scripts(self.paths, 'src/code.sh')
        for path in ('tests/test_helper.py', '.github/workflows/ci.sh',
                     'docs/example.sh', 'setup.py'):
            assert path not in selected, f"{path} is not app runtime code"

    def test_vendored_code_excluded(self):
        selected = filepaths.scannable_scripts(self.paths, 'src/code.sh')
        assert not any('site-packages' in path for path in selected), (
            "A pip install inside a vendored library is not the app installing"
        )

    def test_dockerfile_never_selected(self):
        """Out of scope by decision; asserted so it cannot drift."""
        selected = filepaths.scannable_scripts(self.paths, 'src/code.sh')
        assert not any('Dockerfile' in path for path in selected), (
            "Dockerfiles are deliberately not scanned"
        )

    def test_capped_with_a_warning(self):
        many = tuple(f'src/script{i:03d}.sh' for i in range(100))
        selected = filepaths.scannable_scripts(many, 'src/script000.sh',
                                               limit=5)
        assert len(selected) == 5, (
            f"Should cap at the limit, got {len(selected)}"
        )

    def test_deterministic(self):
        first = filepaths.scannable_scripts(self.paths, 'src/code.sh')
        second = filepaths.scannable_scripts(tuple(reversed(self.paths)),
                                             'src/code.sh')
        assert first == second, (
            "Script selection must not depend on API response ordering"
        )


class TestBundledArtefacts():
    def test_deb_under_resources_detected(self):
        paths = ('resources/packages/samtools.deb', 'src/code.sh')
        assert filepaths.has_deb_resources(paths) is True, (
            "A bundled .deb is supporting evidence for the package check"
        )

    def test_deb_elsewhere_not_counted(self):
        assert filepaths.has_deb_resources(('build/samtools.deb',)) is False, (
            "Only resources/ ships with the app"
        )

    def test_wheels_found_anywhere(self):
        paths = ('packages/foo-1.0-py3-none-any.whl', 'src/code.sh')
        assert filepaths.wheel_paths(paths) == \
            ['packages/foo-1.0-py3-none-any.whl'], (
            "Wheels are evidence wherever they sit in the repo"
        )

    def test_no_artefacts(self):
        assert filepaths.deb_resources(('src/code.sh',)) == []
        assert filepaths.wheel_paths(('src/code.sh',)) == []


class TestRequirementsDetection():
    def test_found_at_root(self):
        assert filepaths.has_requirements_txt(
            ('requirements.txt', 'src/code.py')
        ) is True

    def test_found_at_depth(self):
        """A bash app's Python helper keeps its requirements alongside itself.
        The old check looked only at the repository root."""
        assert filepaths.has_requirements_txt(
            ('resources/home/dnanexus/requirements.txt', 'src/code.sh')
        ) is True, "requirements.txt should be found at any depth"

    def test_case_insensitive(self):
        assert filepaths.has_requirements_txt(('Requirements.TXT',)) is True

    def test_absent(self):
        assert filepaths.has_requirements_txt(('src/code.sh',)) is False
