"""Tests for the check runner, including the behaviour fixes it enables."""

from dxapp_compliance.checks.registry import NOT_APPLICABLE
from dxapp_compliance.checks.runner import run_all_checks
from dxapp_compliance.tests.fixtures import (
    COMPLIANT_BASH_DXAPP,
    COMPLIANT_PYTHON_SRC,
    PYTHON_DXAPP,
    make_evidence,
)


class TestRunAllChecks():
    def test_compliant_bash_app_passes_everything(self):
        outcome = run_all_checks(make_evidence())
        failed = {
            key: value for key, value in outcome.compliance.items()
            if value is False
        }
        assert failed == {}, (
            f"The compliant bash fixture should fail nothing; failed: {failed}"
        )

    def test_keys_match_the_registry(self):
        """run_all_checks raises if it drifts from the registry, so this passing
        is itself the assertion. Kept explicit so the intent is visible."""
        outcome = run_all_checks(make_evidence())
        assert outcome.compliance and outcome.details, (
            "Both result dicts should be populated"
        )

    def test_python_app_skips_bash_only_checks(self):
        evidence = make_evidence(
            dxapp=PYTHON_DXAPP,
            scripts={'src/code.py': COMPLIANT_PYTHON_SRC},
            repo_name='eggd_python_app',
        )
        outcome = run_all_checks(evidence)
        for key in ('set_e', 'no_manual_compiling', 'uptodate_ubuntu'):
            assert outcome.compliance[key] == NOT_APPLICABLE, (
                f"{key} is bash-only and should read NA for a python app, "
                f"got {outcome.compliance[key]!r}"
            )

    def test_python_app_without_release_does_not_raise(self):
        """Regression: float(runSpec.release) raised ValueError when absent."""
        evidence = make_evidence(
            dxapp=PYTHON_DXAPP,
            scripts={'src/code.py': COMPLIANT_PYTHON_SRC},
        )
        outcome = run_all_checks(evidence)
        assert outcome.details['dist_version'] is None, (
            "A python app has no Ubuntu version and should report None"
        )

    def test_bash_app_without_release_does_not_raise(self):
        """Regression: the same crash for a bash app missing runSpec.release."""
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['runSpec'] = {k: v for k, v in dxapp['runSpec'].items()
                            if k != 'release'}
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.compliance['uptodate_ubuntu'] is False, (
            "A bash app with no release cannot be shown up to date, and must "
            "fail rather than raise ValueError"
        )

    def test_unknown_interpreter_does_not_raise(self):
        """Regression: uptodate_ubuntu was unbound for a non-bash non-python
        interpreter, raising UnboundLocalError on return."""
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['runSpec'] = dict(dxapp['runSpec'], interpreter='rscript')
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.compliance['uptodate_ubuntu'] == NOT_APPLICABLE, (
            "An unrecognised interpreter should read NA, not raise"
        )


class TestRegionStrictness():
    def test_single_correct_region_passes(self):
        outcome = run_all_checks(make_evidence())
        assert outcome.compliance['correct_regional_option'] is True, (
            "Exactly the default region should pass"
        )

    def test_multi_region_now_fails(self):
        """The old check passed any app merely *including* eu-central-1."""
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['regionalOptions'] = {'aws:eu-central-1': {},
                                    'aws:us-east-1': {}}
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.compliance['correct_regional_option'] is False, (
            "An app authorised in more than one region should fail"
        )

    def test_wrong_region_fails(self):
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['regionalOptions'] = {'aws:us-east-1': {}}
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.compliance['correct_regional_option'] is False, (
            "An app in the wrong region should fail"
        )

    def test_no_regions_fails(self):
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['regionalOptions'] = {}
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.compliance['correct_regional_option'] is False, (
            "An app with no regionalOptions should fail"
        )

    def test_config_default_region_is_honoured(self):
        """Regression: `region_list is [default_region]` was always False, so
        CONFIG.json's default_region never took effect."""
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['regionalOptions'] = {'aws:us-east-1': {}}
        outcome = run_all_checks(
            make_evidence(dxapp=dxapp, default_region='aws:us-east-1')
        )
        assert outcome.compliance['correct_regional_option'] is True, (
            "The configured default_region should determine what passes"
        )


class TestRequirementsApplicability():
    def test_pure_bash_app_is_not_applicable(self):
        """A pure shell app has no use for requirements.txt and used to be
        penalised with a hard False."""
        evidence = make_evidence(file_paths=('src/code.sh', 'dxapp.json'),
                                 requirements_txt_present=False)
        outcome = run_all_checks(evidence)
        assert outcome.compliance['requirements_file_exists'] == NOT_APPLICABLE, (
            "A repo with no Python should report NA for requirements.txt"
        )

    def test_bash_app_shipping_python_is_scored(self):
        """The case that motivated moving off GitHub's linguist gate: a bash app
        that runs a Python helper should be required to pin its dependencies."""
        evidence = make_evidence(
            file_paths=('src/code.sh', 'resources/home/dnanexus/helper.py'),
            requirements_txt_present=False,
        )
        outcome = run_all_checks(evidence)
        assert outcome.compliance['requirements_file_exists'] is False, (
            "A bash app shipping a Python helper with no requirements.txt "
            "should fail, not be excused"
        )

    def test_python_app_is_scored(self):
        evidence = make_evidence(
            dxapp=PYTHON_DXAPP,
            scripts={'src/code.py': COMPLIANT_PYTHON_SRC},
            requirements_txt_present=True,
        )
        outcome = run_all_checks(evidence)
        assert outcome.compliance['requirements_file_exists'] is True, (
            "A python app with a requirements.txt should pass"
        )


class TestProvenanceEndToEnd():
    def test_python_app_is_scored_on_pip(self):
        """The regression that motivated removing the python short-circuit.

        Every source check used to be forced to "NA" for python apps, which would
        have exempted them from the pip check entirely - the apps it matters most
        for.
        """
        evidence = make_evidence(
            dxapp=PYTHON_DXAPP,
            scripts={'src/code.py':
                     'subprocess.run("pip install pandas", shell=True)\n'},
            entrypoint_path='src/code.py',
        )
        outcome = run_all_checks(evidence)
        assert outcome.compliance['pip_uses_local_wheels'] is False, (
            "A python app installing from PyPI must be scored, not excused"
        )
        assert 'src/code.py' in outcome.details['pip_install_details'], (
            "The evidence should name the offending file"
        )

    def test_network_access_scored_for_both_interpreters(self):
        for dxapp, entry, src in (
            (COMPLIANT_BASH_DXAPP, 'src/code.sh', 'set -e\n'),
            (PYTHON_DXAPP, 'src/code.py', 'pass\n'),
        ):
            spec = dict(dxapp, access={'network': ['*']})
            outcome = run_all_checks(make_evidence(
                dxapp=spec, scripts={entry: src}, entrypoint_path=entry
            ))
            assert outcome.compliance['no_network_access'] is False, (
                f"Network access must be scored for {spec['runSpec']['interpreter']}"
            )

    def test_apt_install_flows_through_to_the_verdict(self):
        evidence = make_evidence(
            scripts={'src/code.sh': "set -e\nsudo apt-get install -y jq\n"},
        )
        outcome = run_all_checks(evidence)
        assert outcome.compliance['no_remote_package_install'] is False, (
            "A shell apt install must fail the package provenance check"
        )
        assert 'apt-install' in outcome.details['package_install_details'], (
            "The evidence should name the finding kind"
        )

    def test_truncated_tree_reports_not_applicable(self):
        """A truncated listing means an absent .deb may just not be listed, so a
        pass would be unfounded."""
        evidence = make_evidence(tree_truncated=True)
        outcome = run_all_checks(evidence)
        assert outcome.compliance['no_remote_package_install'] == NOT_APPLICABLE, (
            "A truncated file listing must not produce a confident pass"
        )
        assert 'truncated' in outcome.details['package_install_details'], (
            "The reason should be visible in the report"
        )

    def test_exec_depends_surfaced_in_details(self):
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['runSpec'] = dict(dxapp['runSpec'],
                                execDepends=[{'name': 'bcftools'}])
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.details['exec_depends'] == 'apt:bcftools', (
            f"execDepends should render with its defaulted package manager, "
            f"got {outcome.details['exec_depends']!r}"
        )


class TestAssetDepends():
    def test_runspec_nested_asset_depends_detected(self):
        outcome = run_all_checks(make_evidence())
        assert outcome.details['asset_present'] is True, (
            "runSpec.assetDepends should be detected - the old code read a "
            "top-level 'assetsDepends', so this column was always False"
        )

    def test_legacy_typo_key_is_not_honoured(self):
        """Guard against reintroducing the old misspelling."""
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['runSpec'] = {k: v for k, v in dxapp['runSpec'].items()
                            if k != 'assetDepends'}
        dxapp['assetsDepends'] = [{'name': 'eggd_asset'}]
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.details['asset_present'] is False, (
            "A top-level 'assetsDepends' is not the real field and must not "
            "count as a declared asset"
        )

    def test_bundled_depends_also_counts(self):
        dxapp = dict(COMPLIANT_BASH_DXAPP)
        dxapp['runSpec'] = dict(
            {k: v for k, v in dxapp['runSpec'].items() if k != 'assetDepends'},
            bundledDepends=[{'name': 'samtools.tar.gz',
                             'id': {'$dnanexus_link': 'file-Gxxxx'}}],
        )
        outcome = run_all_checks(make_evidence(dxapp=dxapp))
        assert outcome.details['asset_present'] is True, (
            "runSpec.bundledDepends is also a pinned artefact shipped with the "
            "app and should count"
        )
