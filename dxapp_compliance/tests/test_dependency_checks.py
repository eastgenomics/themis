"""Tests for the dependency-provenance checks.

Regex detection errs in both directions, so the negative cases matter as much as
the positive ones. The cases flagged "Regression" encode a mistake that a naive
implementation makes.
"""

from dxapp_compliance.checks import dependencies, dxapp_json
from dxapp_compliance.checks.dependencies import LOCAL, REMOTE
from dxapp_compliance.checks.registry import NOT_APPLICABLE


def _classify(command):
    """Classify a full pip command line, as the scanner would."""
    match = dependencies.PIP_INSTALL_RE.search(command)
    assert match, f"pip pattern did not match: {command!r}"

    return dependencies.classify_pip_install(match.group('args'))[0]


def _pip_fires(command):
    return bool(dependencies.PIP_INSTALL_RE.search(command))


def _apt_fires(command):
    return any(pattern.search(command)
               for pattern in dependencies.APT_PATTERNS.values())


class TestNetworkAccess():
    def test_no_access_key_passes(self):
        ok, details = dxapp_json.check_network_access({})
        assert ok is True, "An app with no access block has no network access"
        assert details == "None declared", details

    def test_access_without_network_passes(self):
        ok, _ = dxapp_json.check_network_access(
            {'access': {'project': 'CONTRIBUTE'}}
        )
        assert ok is True, "access without a network key grants no network"

    def test_empty_network_list_passes(self):
        ok, details = dxapp_json.check_network_access({'access': {'network': []}})
        assert ok is True, "An empty network list grants nothing"
        assert "empty" in details, details

    def test_wildcard_fails(self):
        ok, details = dxapp_json.check_network_access(
            {'access': {'network': ['*']}}
        )
        assert ok is False, "Unrestricted network access must fail"
        assert "*" in details, details

    def test_named_host_also_fails(self):
        """The chosen standard: any declared network access fails."""
        ok, details = dxapp_json.check_network_access(
            {'access': {'network': ['dnanexus.com']}}
        )
        assert ok is False, (
            "A scoped allowlist is still outbound network access and fails"
        )
        assert "dnanexus.com" in details, details

    def test_wildcard_host_and_netmask_fail(self):
        ok, _ = dxapp_json.check_network_access(
            {'access': {'network': ['*.ensembl.org', '10.0.0.0/8']}}
        )
        assert ok is False, (
            "Hostname wildcards and network masks are both valid entry forms "
            "and both grant access"
        )

    def test_malformed_string_network_fails(self):
        ok, details = dxapp_json.check_network_access(
            {'access': {'network': '*'}}
        )
        assert ok is False, (
            "A bare string is malformed per the schema but still expresses "
            "intent to reach the network"
        )
        assert "not a list" in details, details

    def test_malformed_access_does_not_raise(self):
        ok, details = dxapp_json.check_network_access({'access': ['network']})
        assert ok is True, "A malformed access block should not fail the app"
        assert "malformed" in details, details

    def test_https_app_noted_but_does_not_fail(self):
        ok, details = dxapp_json.check_network_access(
            {'httpsApp': {'ports': [443]}}
        )
        assert ok is True, (
            "httpsApp is inbound via the platform proxy and must not fail the "
            "outbound network check"
        )
        assert "httpsApp" in details, details

    def test_other_permissions_surfaced(self):
        _, details = dxapp_json.check_network_access(
            {'access': {'allProjects': 'VIEW'}}
        )
        assert "allProjects" in details, (
            "Other granted permissions are worth a reviewer's eye"
        )


class TestExecDepends():
    def test_omitted_package_manager_defaults_to_apt(self):
        """Per the DNAnexus docs, package_manager defaults to apt."""
        count, rendered = dependencies.describe_exec_depends(
            {'runSpec': {'execDepends': [{'name': 'bcftools'}]}}
        )
        assert (count, rendered) == (1, 'apt:bcftools'), (
            f"An entry with no package_manager is an apt install, got {rendered}"
        )

    def test_version_and_manager_rendered(self):
        _, rendered = dependencies.describe_exec_depends(
            {'runSpec': {'execDepends': [
                {'name': 'pandas', 'package_manager': 'pip3',
                 'version': '1.2'},
                {'name': 'jq'},
            ]}}
        )
        assert rendered == 'pip3:pandas==1.2, apt:jq', rendered

    def test_exec_depends_fails_the_check(self):
        ok, details = dependencies.evaluate_remote_package_install(
            {'runSpec': {'execDepends': [{'name': 'bcftools'}]}}, {}
        )
        assert ok is False, "Any execDepends resolves packages at job start"
        assert 'exec-depends' in details, details

    def test_rationale_is_about_reproducibility_not_network(self):
        """execDepends does NOT require network access - the platform installs
        from the default repos even under restricted networking."""
        _, details = dependencies.evaluate_remote_package_install(
            {'runSpec': {'execDepends': [{'name': 'bcftools'}]}}, {}
        )
        assert 'not pinned' in details, (
            f"The reported reason should be reproducibility, got: {details}"
        )

    def test_malformed_exec_depends_does_not_raise(self):
        for value in ({}, ['bcftools'], None, 'apt'):
            count, _ = dependencies.describe_exec_depends(
                {'runSpec': {'execDepends': value}}
            )
            assert count == 0, (
                f"Malformed execDepends {value!r} should yield nothing, not raise"
            )


class TestAptDetection():
    fires = [
        "apt-get install -y bcftools tabix",
        "sudo apt-get install -y --no-install-recommends libz-dev",
        "sudo apt -y install jq",
        "DEBIAN_FRONTEND=noninteractive apt-get install -qq foo",
        "aptitude install foo",
        "sudo apt-get update && sudo apt-get install -y foo",
        "apt-get update",
        "sudo add-apt-repository ppa:deadsnakes/ppa",
        "yum install -y foo",
        "conda install -c bioconda samtools",
        "if ! command -v jq; then sudo apt-get install -y jq; fi",
        'os.system("apt-get install -y jq")',
        'subprocess.check_call("sudo apt-get install -y jq", shell=True)',
        "docker exec ctr apt-get install -y jq",
        "wget https://example.com/foo_1.0_amd64.deb",
        "curl -sSL -o foo.rpm https://example.com/pkgs/foo.rpm",
        "curl -fsSL https://get.docker.com | sudo bash",
    ]
    quiet = [
        'echo "now running apt-get install"',
        'grep -q "apt-get install" file',
        'sed -i "s/apt-get install/#/" script.sh',
        "make install",
        "tar xzf foo.tar.gz && ./configure && make && sudo make install",
        "dpkg -l | grep foo",
        "dx download file-xxxx",
        "docker load -i image.tar.gz",
        "wget ftp://ftp.ensembl.org/pub/release-105/GRCh38.fa.gz",
        "wget https://github.com/samtools/samtools/releases/download/1.15/"
        "samtools-1.15.tar.gz",
        "curl -s $URL | jq -r .id",
    ]

    def test_remote_installs_detected(self):
        missed = [cmd for cmd in self.fires if not _apt_fires(cmd)]
        assert missed == [], f"Should have been flagged: {missed}"

    def test_non_installs_not_flagged(self):
        false_positives = [cmd for cmd in self.quiet if _apt_fires(cmd)]
        assert false_positives == [], (
            f"Should NOT have been flagged: {false_positives}"
        )

    def test_source_tarball_download_not_flagged(self):
        """A .tar.gz URL is indistinguishable from a reference genome, and
        `wget tarball && make install` is no_manual_compiling's job."""
        assert not _apt_fires(
            "wget https://example.com/samtools-1.15.tar.gz"
        ), "Source tarball downloads must not be flagged as package installs"

    def test_dpkg_local_is_evidence_not_a_finding(self):
        scripts = {'src/code.sh': "sudo dpkg -i /home/dnanexus/pkgs/*.deb\n"}
        ok, details = dependencies.evaluate_remote_package_install(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True, "Installing a bundled .deb is the compliant pattern"
        assert 'dpkg -i' in details, details

    def test_dpkg_list_not_treated_as_install(self):
        scripts = {'src/code.sh': "dpkg -l | grep samtools\n"}
        ok, details = dependencies.evaluate_remote_package_install(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True and 'dpkg -i' not in details, (
            f"`dpkg -l` is a query, not an install: {details}"
        )

    def test_apt_install_of_a_local_deb_is_labelled(self):
        scripts = {'src/code.sh': "apt-get install -y ./local.deb\n"}
        ok, details = dependencies.evaluate_remote_package_install(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is False, (
            "apt still resolves the deb's dependencies from the archive"
        )
        assert 'apt-local-deb' in details, (
            f"It should be labelled so a reviewer can see it is arguable: "
            f"{details}"
        )


class TestFailDominance():
    def test_remote_install_fails_despite_bundled_debs(self):
        scripts = {'src/code.sh': (
            "sudo dpkg -i /home/dnanexus/pkgs/*.deb\n"
            "sudo apt-get install -y jq\n"
        )}
        ok, details = dependencies.evaluate_remote_package_install(
            {}, scripts, ('resources/pkgs/foo.deb',), 'src/code.sh'
        )
        assert ok is False, (
            "A runtime apt call still happens regardless of what is bundled"
        )
        assert 'apt-install' in details and 'local:' in details, (
            f"Both sides should appear in the evidence: {details}"
        )

    def test_remote_install_fails_despite_asset_depends(self):
        dxapp = {'runSpec': {'assetDepends': [{'name': 'eggd_asset'}]}}
        scripts = {'src/code.sh': "sudo apt-get install -y jq\n"}
        ok, _ = dependencies.evaluate_remote_package_install(
            dxapp, scripts, (), 'src/code.sh'
        )
        assert ok is False, "An asset does not undo a runtime apt install"

    def test_clean_app_passes(self):
        scripts = {'src/code.sh': "set -e\nsamtools view -c in.bam\n"}
        ok, details = dependencies.evaluate_remote_package_install(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True, "An app that installs nothing remotely passes"
        assert details.startswith("None detected"), details


class TestPipDetection():
    fires = [
        "pip install pandas",
        "pip3 install pandas==1.2.3",
        "python -m pip install pandas",
        "python3 -m pip install --break-system-packages pandas",
        "sudo -H pip3 install numpy",
        "/home/dnanexus/venv/bin/pip install foo",
        "${VENV}/bin/pip install foo",
        '"$PYTHON" -m pip install foo',
        "pipx install black",
        "mkdir -p x && sudo pip install pandas || exit 1",
        "cd /home/dnanexus && pip install ./wheels/foo.whl",
        'subprocess.run("pip install pandas", shell=True)',
    ]
    quiet = [
        "# pip install pandas",
        'echo "pip install pandas" >> log',
        'print("pip install pandas")',
        'logger.info("running pip install %s", pkg)',
        'parser.add_argument("--pip-install", help="pip install x")',
        "mark_set=$(pip list)",
    ]

    def test_invocations_detected(self):
        missed = [cmd for cmd in self.fires if not _pip_fires(cmd)]
        assert missed == [], f"pip pattern should match: {missed}"

    def test_reporting_lines_not_flagged(self):
        from dxapp_compliance.checks.scanning import NOISE_LINE
        false_positives = [
            cmd for cmd in self.quiet
            if _pip_fires(cmd) and not NOISE_LINE.match(cmd)
        ]
        assert false_positives == [], (
            f"Should NOT be treated as pip installs: {false_positives}"
        )


class TestPipClassification():
    def test_no_index_is_local(self):
        assert _classify(
            "pip install --no-index -f /home/dnanexus/wheels pandas"
        ) == LOCAL, "--no-index is a definitive offline marker"

    def test_find_links_without_no_index_is_remote(self):
        """Regression: the mistake a naive path regex makes.

        `-f /local/wheels` does not stop pip falling back to PyPI, and without a
        table of value-taking flags `/local/wheels` parses as a local target -
        reporting the exact opposite of the truth.
        """
        assert _classify("pip install -f /local/wheels pandas") == REMOTE, (
            "--find-links alone still permits a PyPI fallback"
        )

    def test_find_links_equals_form_is_remote(self):
        assert _classify(
            "pip install --find-links=file:///home/x pandas"
        ) == REMOTE, "The =-joined form must be handled too"

    def test_bare_package_is_remote(self):
        assert _classify("pip install pandas") == REMOTE

    def test_pinned_bare_package_is_remote(self):
        assert _classify("pip3 install pandas==1.2 numpy>=1.0") == REMOTE

    def test_requirements_file_is_remote(self):
        assert _classify("pip install -r requirements.txt") == REMOTE, (
            "A requirements file with no --no-index resolves from PyPI"
        )

    def test_requirements_with_no_index_is_local(self):
        assert _classify(
            "pip install --no-index -f packages/ -r requirements.txt"
        ) == LOCAL

    def test_vcs_target_is_remote(self):
        assert _classify(
            "pip install git+https://github.com/x/y.git@v1"
        ) == REMOTE

    def test_https_wheel_is_remote(self):
        """A .whl served over HTTPS is still a network fetch."""
        assert _classify(
            "pip install https://files.pythonhosted.org/foo-1.0.whl"
        ) == REMOTE

    def test_local_wheel_glob_is_local(self):
        assert _classify("pip install packages/*.whl") == LOCAL

    def test_absolute_wheel_path_is_local(self):
        assert _classify(
            "pip install /home/dnanexus/foo-1.0-py3-none-any.whl"
        ) == LOCAL

    def test_bare_wheel_filename_is_local(self):
        assert _classify("pip install foo-1.0-py3-none-any.whl") == LOCAL

    def test_variable_path_wheel_is_local(self):
        assert _classify("pip install $HOME/wheels/foo.whl") == LOCAL

    def test_quoted_local_sdist_is_local(self):
        assert _classify("pip install './dist/foo-1.0.tar.gz'") == LOCAL

    def test_mixed_targets_are_remote(self):
        assert _classify("pip install ./foo.whl pandas") == REMOTE, (
            "One remote target is enough to make the invocation remote"
        )

    def test_source_tree_is_remote(self):
        assert _classify("pip install .") == REMOTE, (
            "A source tree still resolves its dependencies from PyPI"
        )

    def test_editable_is_remote(self):
        assert _classify("pip install -e .") == REMOTE

    def test_editable_path_is_remote(self):
        assert _classify("pip install -e ./mypkg") == REMOTE, (
            "-e must be checked before the path test or this looks local"
        )

    def test_upgrade_pip_itself_is_remote(self):
        assert _classify(
            "python -m pip install --upgrade pip setuptools wheel"
        ) == REMOTE

    def test_unbalanced_quote_does_not_raise(self):
        verdict = _classify('pip install "pandas')
        assert verdict == REMOTE, (
            "shlex raises on unbalanced quotes; the fallback must still return "
            "a verdict rather than aborting the audit"
        )


class TestPipProvenanceCheck():
    def test_remote_pip_fails(self):
        scripts = {'src/code.sh': "pip install pandas\n"}
        ok, details = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is False, "A PyPI install must fail the check"
        assert 'src/code.sh:1' in details, (
            f"Evidence should locate the offending line: {details}"
        )

    def test_local_pip_passes(self):
        scripts = {'src/code.sh': "pip install packages/foo.whl\n"}
        ok, _ = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True, "A local wheel install passes"

    def test_no_pip_at_all_is_not_applicable(self):
        scripts = {'src/code.sh': "samtools view -c in.bam\n"}
        verdict, details = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert verdict == NOT_APPLICABLE, (
            "With no pip the check is vacuous and must not award a free pass"
        )
        assert details.startswith("None detected"), details

    def test_wheel_in_repo_is_evidence_not_a_requirement(self):
        """Wheels often arrive via a DNAnexus asset, so their absence from the
        repo must not fail an otherwise-compliant app."""
        scripts = {'src/code.sh': "pip install --no-index -f pkgs/ -r req.txt\n"}
        ok, _ = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True, (
            "An offline install with no .whl in the repo should still pass"
        )

    def test_exec_depends_pip_cross_referenced(self):
        dxapp = {'runSpec': {'execDepends': [
            {'name': 'pandas', 'package_manager': 'pip3'}
        ]}}
        verdict, details = dependencies.evaluate_pip_provenance(
            dxapp, {'src/code.sh': "echo hi\n"}, (), 'src/code.sh'
        )
        assert verdict == NOT_APPLICABLE, (
            "execDepends is scored by the package check, not this one"
        )
        assert 'Remote Pkg Install' in details, (
            f"It should cross-reference where it IS scored: {details}"
        )

    def test_continued_line_reported_at_first_line(self):
        scripts = {'src/code.sh': (
            "set -e\n"
            "pip install \\\n"
            "    --no-index \\\n"
            "    -f packages/ \\\n"
            "    -r requirements.txt\n"
        )}
        ok, _ = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True, (
            "A backslash-continued offline install must be joined before "
            "matching, or --no-index is invisible"
        )

    def test_commented_out_pip_does_not_fire(self):
        scripts = {'src/code.sh': "# pip install pandas\nsamtools view in.bam\n"}
        verdict, _ = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert verdict == NOT_APPLICABLE, (
            "A commented-out install is not an install"
        )


class TestRuntimeScoping():
    def test_build_asset_script_is_unscored(self):
        """The severe false positive this scoping exists to prevent.

        build_asset.sh runs apt-get install at *build* time to bake a DNAnexus
        asset - the compliant pattern. Failing an app for it would discredit the
        report.
        """
        scripts = {'build_asset.sh': "sudo apt-get install -y bcftools\n"}
        ok, details = dependencies.evaluate_remote_package_install(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is True, (
            "An asset-build script must not fail the app at runtime"
        )
        assert 'unscored' in details, (
            f"But a reviewer should still see it: {details}"
        )

    def test_test_file_is_unscored(self):
        scripts = {'tests/test_helper.py': "pip install pytest\n"}
        verdict, details = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert verdict is True, "A test file is not app runtime code"
        assert 'unscored' in details, details

    def test_entrypoint_outside_src_still_scored(self):
        scripts = {'code.sh': "sudo apt-get install -y jq\n"}
        ok, _ = dependencies.evaluate_remote_package_install(
            {}, scripts, (), 'code.sh'
        )
        assert ok is False, (
            "runSpec.file is runtime code wherever it sits in the repo"
        )

    def test_resources_script_is_scored(self):
        scripts = {'resources/home/dnanexus/wrap.sh': "pip install pandas\n"}
        ok, _ = dependencies.evaluate_pip_provenance(
            {}, scripts, (), 'src/code.sh'
        )
        assert ok is False, "Scripts under resources/ ship with the app and run"

    def test_dockerfile_is_not_a_script(self):
        """Dockerfiles are out of scope by decision; asserted so it cannot
        drift."""
        from dxapp_compliance.checks.scanning import is_script_path
        assert not is_script_path('Dockerfile'), (
            "Dockerfiles must not be fetched or scanned"
        )
        assert not is_script_path('docker/Dockerfile.build'), (
            "Dockerfile variants must not be scanned either"
        )


class TestEvidenceFormatting():
    def test_deterministic_regardless_of_input_order(self):
        """Without this the cell follows API response order and every
        month-to-month report diff is noise."""
        from dxapp_compliance.checks.scanning import format_evidence, make_finding
        findings = [
            make_finding('apt-install', 'src/b.sh', 4, 'apt-get install b'),
            make_finding('apt-install', 'src/a.sh', 2, 'apt-get install a'),
            make_finding('apt-update', 'src/a.sh', 1, 'apt-get update'),
        ]
        first = format_evidence(findings)
        second = format_evidence(list(reversed(findings)))
        assert first == second, (
            f"Evidence must be order-independent:\n{first}\n{second}"
        )

    def test_truncates_with_a_count(self):
        from dxapp_compliance.checks.scanning import format_evidence, make_finding
        findings = [
            make_finding('apt-install', f'src/f{i}.sh', i, f'apt-get install p{i}')
            for i in range(7)
        ]
        rendered = format_evidence(findings)
        assert '(+4 more)' in rendered, rendered
        assert len(rendered) <= 400, f"Cell too long ({len(rendered)})"

    def test_long_line_snippet_truncated(self):
        from dxapp_compliance.checks.scanning import truncate_snippet
        snippet = truncate_snippet("pip install " + " ".join(
            f"package{i}" for i in range(60)
        ))
        assert len(snippet) == 103, len(snippet)
        assert snippet.endswith("..."), snippet

    def test_passing_check_is_not_an_empty_cell(self):
        ok, details = dependencies.evaluate_remote_package_install(
            {}, {'src/code.sh': "echo hi\n"}, (), 'src/code.sh'
        )
        assert ok and details, (
            "An empty cell reads as missing data in a DataTable, which is a "
            "different message from 'nothing found'"
        )
