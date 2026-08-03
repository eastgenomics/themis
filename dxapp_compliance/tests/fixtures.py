"""Fixture data as Python constants.

Not files on disk: the repository's .gitignore has a bare `*.json` rule, so any
fixture dxapp.json would be silently untracked. This also matches the pattern in
TAT_audit/tests, which pastes API payloads in as class attributes.
"""

from dxapp_compliance.models import AppEvidence, RepoRecord

#: A bash app that passes every check.
COMPLIANT_BASH_DXAPP = {
    'name': 'eggd_compliant',
    'title': 'eggd_compliant',
    'version': '1.0.0',
    'authorizedUsers': ['org-emee_1'],
    'developers': ['org-emee_1'],
    'regionalOptions': {'aws:eu-central-1': {}},
    'runSpec': {
        'interpreter': 'bash',
        'distribution': 'Ubuntu',
        'release': '20.04',
        'file': 'src/code.sh',
        'timeoutPolicy': {'*': {'hours': 12}},
        'assetDepends': [{'name': 'eggd_samtools_asset',
                          'project': 'project-Gxxxx',
                          'version': '1.0.0'}],
    },
}

#: A python app. No release key, which used to raise ValueError.
PYTHON_DXAPP = {
    'name': 'eggd_python_app',
    'title': 'eggd_python_app',
    'version': '1.0.0',
    'authorizedUsers': ['org-emee_1'],
    'developers': ['org-emee_1'],
    'regionalOptions': {'aws:eu-central-1': {}},
    'runSpec': {
        'interpreter': 'python3',
        'file': 'src/code.py',
        'timeoutPolicy': {'*': {'hours': 6}},
    },
}

COMPLIANT_BASH_SRC = """#!/bin/bash
set -exo pipefail

main() {
    dx download "$input_bam"
    sudo dpkg -i /home/dnanexus/packages/*.deb
    samtools view -c input.bam
}
"""

COMPLIANT_PYTHON_SRC = """import subprocess


def main():
    subprocess.run(["samtools", "view", "-c", "in.bam"], check=True)
"""


def make_evidence(dxapp=None, scripts=None, entrypoint_path=None,
                  file_paths=(), repo_name='eggd_compliant',
                  default_region='aws:eu-central-1', **kwargs):
    """Build an AppEvidence for a test, with compliant defaults.

    Keyword arguments are passed through to AppEvidence, so a test can override
    exactly the one field it is about and leave the rest passing.
    """
    dxapp = COMPLIANT_BASH_DXAPP if dxapp is None else dxapp
    if entrypoint_path is None:
        entrypoint_path = dxapp.get('runSpec', {}).get('file')
    if scripts is None:
        scripts = {entrypoint_path: COMPLIANT_BASH_SRC} if entrypoint_path else {}

    defaults = {
        'last_release_date': '2026-01-15',
        'latest_commit_date': '2026-02-01',
        'dependabot_alerts_enabled': True,
        'dependabot_security_updates_set': True,
        'requirements_txt_present': True,
    }
    defaults.update(kwargs)

    return AppEvidence(
        repo=RepoRecord(
            name=repo_name,
            html_url=f'https://github.com/eastgenomics/{repo_name}',
            default_branch='main',
            pushed_at='2026-02-01T09:00:00Z',
        ),
        dxapp=dxapp,
        file_paths=tuple(file_paths),
        scripts=scripts,
        entrypoint_path=entrypoint_path,
        default_region=default_region,
        **defaults,
    )
