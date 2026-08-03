"""Data carried between the GitHub layer and the checks.

``AppEvidence`` is the seam that makes the checks testable without mocking: the
GitHub layer's only job is to build one, and every check is then a pure function
of it. A test constructs one from literal Python - no ``GhApi``, no ``requests``,
no monkeypatching, no fixture files on disk.

Stdlib only, so it sits below both ``checks/`` and ``gh_api/`` and neither has to
import the other.
"""

from dataclasses import dataclass, field
from typing import Mapping, Optional, Tuple, Union


@dataclass(frozen=True)
class RepoRecord:
    """A GitHub repository, normalised at the ghapi boundary.

    Exists because the original passed ghapi's fastcore ``AttrDict`` straight
    into the checks and accessed it inconsistently - ``app.get('name')`` in one
    line and ``app.name`` two lines later. That worked only for AttrDict, so the
    checks could not be called with a plain dict in a test.
    """

    name: str
    html_url: str = ""
    archived: bool = False
    default_branch: str = "main"
    #: The repo's last push time, from the org listing. Used instead of walking
    #: every branch and requesting its latest commit.
    pushed_at: Optional[str] = None

    @classmethod
    def from_api(cls, repo):
        """Build from a ghapi repo object or a plain dict."""
        return cls(
            name=repo['name'],
            html_url=repo.get('html_url', ""),
            archived=bool(repo.get('archived', False)),
            default_branch=repo.get('default_branch') or "main",
            pushed_at=repo.get('pushed_at'),
        )


@dataclass(frozen=True)
class AppEvidence:
    """Everything fetched for one app. Checks read only this.

    Attributes
    ----------
        repo (RepoRecord):
            Identity and activity metadata.
        dxapp (dict):
            The parsed dxapp.json.
        file_paths (tuple[str, ...]):
            Every path in the repository, from one recursive git-tree call.
            Empty when the tree could not be listed.
        scripts (Mapping[str, str]):
            Decoded text of the *.sh and *.py files that were fetched, keyed by
            repository path.
        entrypoint_path (str, optional):
            The resolved runSpec.file.
        tree_truncated (bool):
            True when GitHub truncated the tree listing. Path-derived checks
            must report NOT_APPLICABLE rather than a false pass, since an
            absent .deb may simply not have been listed.
    """

    repo: RepoRecord
    dxapp: dict = field(default_factory=dict)
    file_paths: Tuple[str, ...] = ()
    scripts: Mapping[str, str] = field(default_factory=dict)
    entrypoint_path: Optional[str] = None
    tree_truncated: bool = False

    last_release_date: Optional[str] = None
    latest_commit_date: Optional[str] = None
    dependabot_alerts_enabled: Union[bool, str] = False
    dependabot_security_updates_set: Union[bool, str] = False
    requirements_txt_present: Union[bool, str] = False
    default_region: Optional[str] = None

    @property
    def entrypoint_text(self):
        """Text of the app's entrypoint script, or "" if it was not fetched."""
        if not self.entrypoint_path:
            return ""

        return self.scripts.get(self.entrypoint_path, "")


@dataclass(frozen=True)
class CheckOutcome:
    """The two result dicts for one app.

    ``compliance`` holds the booleans that feed the score; ``details`` holds the
    human-readable values for the supplementary table.
    """

    compliance: dict
    details: dict
