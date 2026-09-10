"""Tests for discovering which apps are actually in use.

Fixtures are real fragments from eastgenomics workflow and conductor configs,
so the parsers are exercised against the shapes they will actually meet.
"""

from dxapp_compliance.gh_api import usage


class TestExecutableAppName():
    def test_named_app_with_version(self):
        assert usage.executable_app_name('app-eggd_fastqc/1.2.1') == \
            'eggd_fastqc'

    def test_named_app_without_version(self):
        assert usage.executable_app_name('app-eggd_vep') == 'eggd_vep'

    def test_hyphenated_third_party_app(self):
        assert usage.executable_app_name('app-sentieon-dnaseq/5.1.0') == \
            'sentieon-dnaseq'

    def test_opaque_dnanexus_id_yields_nothing(self):
        """A conductor key is an id, not a name - it maps to no repository, and
        guessing from it would be inventing data."""
        assert usage.executable_app_name(
            'app-J6Q1VVQ4Pf3XgF2j1jz53qv9'
        ) is None

    def test_applet_is_not_an_app(self):
        assert usage.executable_app_name('applet-Gj2jP6j4PKbJpVgjK417J4X7') \
            is None

    def test_workflow_is_not_an_app(self):
        assert usage.executable_app_name('workflow-J9pzKv84jBJzBZfz33GzFV7q') \
            is None

    def test_bare_name_from_a_conductor_name_field(self):
        """Conductor's `name` carries 'eggd_MultiQC/3.3.0' with no prefix."""
        assert usage.executable_app_name('eggd_MultiQC/3.3.0') == 'eggd_MultiQC'

    def test_empty_and_none(self):
        assert usage.executable_app_name('') is None
        assert usage.executable_app_name(None) is None


class TestWorkflowReferenceName():
    def test_opaque_id_falls_back_to_the_display_name(self):
        assert usage.workflow_reference_name(
            'workflow-J9pzKv84jBJzBZfz33GzFV7q', 'dias_single_v2.16.0'
        ) == 'dias_single_v2.16.0', (
            "The conductor key is opaque, so the name beside it is the only "
            "thing that identifies the workflow"
        )

    def test_version_stripped_from_display_name(self):
        assert usage.workflow_reference_name(
            'workflow-Gj2jP6j4PKbJpVgjK417J4X7', 'dias_multi_v2.2.0/1.0.0'
        ) == 'dias_multi_v2.2.0'

    def test_an_app_is_not_a_workflow(self):
        assert usage.workflow_reference_name('app-eggd_vep/1.0.0') is None


class TestParseWorkflowExecutables():
    # Real shape from eggd_dias_single_workflow/dxworkflow.json.
    workflow = {
        'name': 'dias_single_v2.16.0',
        'stages': [
            {'id': 'stage-fastQC', 'executable': 'app-eggd_fastqc/1.2.1'},
            {'id': 'stage-sentieon_dnaseq',
             'executable': 'app-sentieon-dnaseq/5.1.0'},
            {'id': 'stage-verifybamid',
             'executable': 'app-eggd_verifybamid/2.3.0', 'input': {}},
        ],
    }

    def test_every_stage_executable(self):
        assert usage.parse_workflow_executables(self.workflow) == [
            'app-eggd_fastqc/1.2.1',
            'app-sentieon-dnaseq/5.1.0',
            'app-eggd_verifybamid/2.3.0',
        ]

    def test_malformed_inputs_do_not_raise(self):
        for value in ({}, {'stages': None}, {'stages': 'x'}, None, []):
            assert usage.parse_workflow_executables(value) == [], (
                f"Malformed workflow {value!r} should yield nothing, not raise"
            )

    def test_stage_without_an_executable_skipped(self):
        assert usage.parse_workflow_executables(
            {'stages': [{'id': 'stage-1'}, {'executable': 'app-a/1.0'}]}
        ) == ['app-a/1.0']


class TestParseConductorExecutables():
    # Real shape from eggd_conductor_dias_CEN_config_v3.2.2.json.
    config = {
        'assay': 'CEN',
        'executables': {
            'workflow-J9pzKv84jBJzBZfz33GzFV7q':
                {'name': 'dias_single_v2.16.0', 'analysis': 'analysis_1'},
            'app-J6Q1VVQ4Pf3XgF2j1jz53qv9':
                {'name': 'eggd_MultiQC/3.3.0', 'analysis': 'analysis_3'},
        },
    }

    def test_key_and_name_pairs(self):
        pairs = dict(usage.parse_conductor_executables(self.config))
        assert pairs['app-J6Q1VVQ4Pf3XgF2j1jz53qv9'] == 'eggd_MultiQC/3.3.0'
        assert pairs['workflow-J9pzKv84jBJzBZfz33GzFV7q'] == \
            'dias_single_v2.16.0'

    def test_malformed_inputs_do_not_raise(self):
        for value in ({}, {'executables': None}, {'executables': []}, None):
            assert usage.parse_conductor_executables(value) == [], (
                f"Malformed config {value!r} should yield nothing, not raise"
            )

    def test_entry_without_a_name(self):
        pairs = usage.parse_conductor_executables(
            {'executables': {'app-eggd_vep/1.0.0': {}}}
        )
        assert pairs == [('app-eggd_vep/1.0.0', '')], (
            "A missing name is fine - the key itself still names the app here"
        )


class TestFilterReposInUse():
    repos = [
        {'name': 'eggd_fastqc'},
        {'name': 'eggd_MultiQC'},
        {'name': 'eggd_never_wired_up'},
        {'name': 'eggd_verifybamid'},
    ]

    def test_keeps_only_referenced_repos(self):
        used = {'eggd_fastqc': {'dias_single'},
                'eggd_verifybamid': {'dias_single'}}
        kept, skipped, _ = usage.filter_repos_in_use(self.repos, used)
        assert [r['name'] for r in kept] == ['eggd_fastqc',
                                             'eggd_verifybamid']
        assert 'eggd_never_wired_up' in skipped

    def test_case_insensitive_match(self):
        """An app's declared name and its repo name routinely differ in case."""
        kept, _, _ = usage.filter_repos_in_use(
            self.repos, {'eggd_multiqc': {'CEN'}}
        )
        assert [r['name'] for r in kept] == ['eggd_MultiQC']

    def test_third_party_apps_reported_as_unmatched(self):
        """sentieon-dnaseq is referenced but has no repo here. Reporting it is
        what distinguishes 'not ours' from 'we renamed it and lost track'."""
        _, _, unmatched = usage.filter_repos_in_use(
            self.repos, {'eggd_fastqc': {'w'}, 'sentieon-dnaseq': {'w'}}
        )
        assert unmatched == ['sentieon-dnaseq']

    def test_nothing_used_keeps_nothing(self):
        kept, skipped, _ = usage.filter_repos_in_use(self.repos, {})
        assert kept == [] and len(skipped) == 4, (
            "An empty used-set must not silently fall back to auditing all"
        )


class FakeUsageClient():
    """Serves fixed workflow and conductor content, hand-written not mocked."""

    def __init__(self, workflows, configs):
        self.organisation = 'eastgenomics'
        self.workflows = workflows      # repo -> dxworkflow dict
        self.configs = configs          # path -> config dict

    def get(self, path, **kw):
        import base64
        import json
        if path.startswith('/search/code'):
            return {'items': [{'repository': {'name': r},
                               'path': 'dxworkflow.json'}
                              for r in self.workflows]}
        if '/git/trees/' in path:
            return {'tree': [{'type': 'blob', 'path': p}
                             for p in self.configs]}
        for repo, content in self.workflows.items():
            if f'/{repo}/contents/dxworkflow.json' in path:
                return {'content': base64.b64encode(
                    json.dumps(content).encode()).decode()}
        for cfg, content in self.configs.items():
            if path.endswith(cfg):
                return {'content': base64.b64encode(
                    json.dumps(content).encode()).decode()}
        return None


class TestDiscoverUsedApps():
    workflows = {
        'eggd_dias_single_workflow': {
            'name': 'dias_single_v2.16.0',
            'stages': [{'executable': 'app-eggd_fastqc/1.2.1'},
                       {'executable': 'app-sentieon-dnaseq/5.1.0'}],
        },
    }
    configs = {
        'assay_configs/CEN/conductor_CEN_v1.json': {
            'executables': {
                'workflow-J9pzKv84jBJzBZfz33GzFV7q':
                    {'name': 'dias_single_v2.16.0'},
                'app-J6Q1VVQ4Pf3XgF2j1jz53qv9':
                    {'name': 'eggd_MultiQC/3.3.0'},
            },
        },
    }

    def _discover(self, **kw):
        client = FakeUsageClient(self.workflows, self.configs)
        return usage.discover_used_apps(client, **kw)

    def test_workflow_stages_collected(self):
        used, _ = self._discover(use_conductor=False)
        assert 'eggd_fastqc' in used and 'sentieon-dnaseq' in used

    def test_conductor_app_entry_uses_the_name_field(self):
        used, _ = self._discover(use_workflows=False)
        assert 'eggd_multiqc' in used, (
            "The conductor key is an opaque id; the name field is the only "
            "thing that maps to a repository"
        )

    def test_workflow_reference_resolved_transitively(self):
        """An app used only inside a workflow that conductor launches is still
        in production."""
        used, _ = self._discover(use_workflows=False)
        assert 'eggd_fastqc' in used, (
            "conductor -> workflow -> app should reach eggd_fastqc"
        )
        assert any('->' in s for s in used['eggd_fastqc']), (
            f"The source should record the path taken: {used['eggd_fastqc']}"
        )

    def test_workflow_name_is_not_recorded_as_an_app(self):
        """Regression: recording a workflow entry's display name as an app made
        every workflow show up as a missing repository."""
        used, _ = self._discover()
        assert 'dias_single_v2.16.0' not in used, (
            f"A workflow name is not an app name: {sorted(used)}"
        )

    def test_workflow_lookup_is_case_insensitive(self):
        """A conductor config and the workflow's own dxworkflow.json do not
        always agree on case."""
        client = FakeUsageClient(
            {'w': {'name': 'uranus_main_workflow_GRCh38_v3.3.0',
                   'stages': [{'executable': 'app-eggd_x/1.0'}]}},
            {'assay_configs/a.json': {'executables': {
                'workflow-J9pzKv84jBJzBZfz33GzFV7q':
                    {'name': 'uranus_main_workflow_grch38_v3.3.0'}}}},
        )
        used, unresolved = usage.discover_used_apps(client,
                                                    use_workflows=False)
        assert 'eggd_x' in used and unresolved == set(), (
            f"Case should not prevent resolution; unresolved={unresolved}"
        )

    def test_unresolvable_workflow_is_reported(self):
        client = FakeUsageClient(
            {}, {'assay_configs/a.json': {'executables': {
                'workflow-J9pzKv84jBJzBZfz33GzFV7q':
                    {'name': 'retired_workflow_v9.0.0'}}}},
        )
        _, unresolved = usage.discover_used_apps(client, use_workflows=False)
        assert unresolved == {'retired_workflow_v9.0.0'}, (
            "A workflow named by a config but absent from GitHub means its apps "
            "are invisible to this mode, so it must be surfaced"
        )

    def test_sources_are_recorded(self):
        used, _ = self._discover(use_conductor=False)
        assert used['eggd_fastqc'] == {'dias_single_v2.16.0'}, (
            "Knowing which workflow pulls an app in is what makes the filter "
            "auditable"
        )
