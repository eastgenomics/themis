# DX App Compliance

This repo contains the script to generate an audit summary report for bioinformatics DNAnexus apps in GitHub.

## **Installation**

Dependencies are managed with [uv](https://docs.astral.sh/uv/), from the
`pyproject.toml` at the repository root. From the root:

```
uv sync
```

That creates `.venv/` and installs the exact versions recorded in `uv.lock`.
`uv.lock` is tracked deliberately — an audit that flags other people's
unpinned dependency installs should pin its own.

`requirements.txt` is kept only for anyone not using uv, and is a **derived
artefact** — regenerate it rather than editing it by hand:

```
uv export --no-hashes --format requirements-txt > dxapp_compliance/requirements.txt
```

Config variables should be passed in a CONFIG.json file. This should be placed within dxapp_compliance. i.e. themis/dxapp_compliance/CONFIG.json

    {
    "GITHUB_TOKEN": "XXX",
    "organisation": "eastgenomics",
    "default_region": "aws:eu-central-1"
    }

## **Layout**

```
dxapp_compliance/
  main.py       entrypoint
  config.py     paths, config loading, logging
  models.py     RepoRecord, AppEvidence - what the GitHub layer hands the checks
  filepaths.py  pure logic over a repository's file listing
  checks/       pure checks over already-fetched content
  gh_api/       everything that talks to GitHub
  report/       scoring, table formatting, plots, rendering
  tests/
```

The `checks/` package imports no pandas, no `requests`/`ghapi` and nothing from
`gh_api/`. Every check is a pure function of an `AppEvidence`, so the tests
construct their input as literal Python and need no mocking. That property is
asserted by `tests/test_layering.py` rather than left to convention.

Which columns exist, which are scored, which interpreters each applies to and
what header each renders under are all declared once in `checks/registry.py`.
Scoring, the summary table, the rename maps and the rendered column order are
derived from it, and `checks/runner.py` raises if it produces a key the registry
does not declare.

## **Description**

The script works by:

Querying github's API using the ghapi package.
First all the repositories from the organisation are returned. Then, only repos with `dxapp.json` are kept.
For each app, one recursive git-tree call lists every file in the repository, and
the runtime scripts (`src/`, `resources/`, plus `runSpec.file`) are fetched by
blob sha. Everything the checks need is gathered into an `AppEvidence`, and the
checks are then a pure function of it.

Each app's score is the percentage of *applicable* checks it passed. A check that
does not apply - a bash-only check on a Python app, or a check that could not be
evaluated - reads `NA` and is excluded from that app's denominator rather than
counting as a failure.

For each standard we check the compliance by:

- **Prefixed with eggd_**

for app title and name
For this we extract the title/name from `dxapp.json` and use regex to confirm the correct prefix.

- **Apps and not applets**

All apps have a version in the dxapp.json so by checking for the presence of this you can confirm if it is an app or applet.

- **Authorised developers**

set to only org-emee_1
Extracting the auth_devs field from `dxapp.json` and checking for only 'org-emee_1' present.

- **Authorised users**

set to only org-emee_1
Extracting the auth_users field from `dxapp.json` and checking for only 'org-emee_1' present.

- **Using Ubuntu 20 (for bash apps)**

Using regex to confirm if the src file is bash, we then check the version from `dxapp.json`
if it's greater the 20 then it passes.

- **Using region to aws:eu-central-1**

For this we extract `regionalOptions` from `dxapp.json` and require that it
contains *exactly* the configured `default_region` and nothing else. An app
authorised in more than one region fails. (Previously the check passed any app
that merely included `aws:eu-central-1` alongside others, and the configured
`default_region` had no effect.)

- **Time out policy set**

For this we extract the `timeout_policy` from `dxapp.json` and check only a timeout is set to a non-zero amount of time.

- **Using assets over manually compiling in-app**

For this, we check the src file using regex for `make install`, which suggests
manual compiling. Note this is narrower than "any `make`", which is what this
document used to claim.

- **Bash scripts using a minimum of set -e**

For this, we check the src file using regex for any `set -e` or set -e derivatives present such as set -exo. This ensures a proper erroring policy so apps don't run for longer than needed if they error out.

A HTML file is then created, which has interactive datatables for viewing complaince for each app and interactive plots.

- **Dependabot security configurations are enabled**

For this, we check the security advisories have been enabled for the GitHub repo using the GitHub REST API

- **Checks if 'requirements.txt' exists**

A check that `requirements.txt` exists (case insensitive) in any repository
that ships Python, found at any depth rather than only at the repository root.
Repositories with no Python at all read `NA` rather than failing - previously a
pure-shell app was marked down for lacking a file it has no use for, while a bash
app shipping an unpinned Python helper looked identical to a compliant one.

### Dependency provenance

Three checks about **where an app's dependencies come from at job runtime**. An
app that reaches out to an apt mirror or PyPI while a job runs is not
reproducible: its behaviour depends on the state of a third-party server on the
day it ran, and it silently requires outbound network access.

- **No network access declared**

`access.network` absent or empty passes. Any entry fails — a hostname, a hostname
wildcard, a network mask or `"*"`. Per the DNAnexus *I/O and Run Specifications*
reference, omitting `access.network` means the executable has no network access,
so anything declared means the app is not self-contained. The declared value is
recorded in the details table.

`httpsApp` is noted in the details but does not affect the verdict: it permits
inbound HTTPS through the platform proxy and neither implies nor requires
outbound access.

- **No remote OS-package installs**

Fails on either source of remote installation:

  - `runSpec.execDepends`, for any `package_manager` (which is optional and
    **defaults to `apt`**, so an entry with no `package_manager` is an apt
    install). Note the reason is *reproducibility*, not network access — the
    platform installs these from the default Ubuntu and DNAnexus repositories
    even under restricted networking. The problem is that the version is resolved
    at job start rather than pinned to the app release.
  - Shell installs: `apt-get install`, `apt install`, `aptitude install`,
    `add-apt-repository`, `yum`/`dnf`/`zypper install`, `conda`/`mamba install`,
    a `curl`/`wget` of a `.deb`/`.rpm`/`.udeb` URL or of a known package archive
    host, `curl … | bash`, and `docker run … apt-get install`.

Bundling instead — `dpkg -i` on a local path, `.deb` files under `resources/`,
`runSpec.assetDepends` or `runSpec.bundledDepends` — is recorded as supporting
evidence.

**Any remote install fails the check regardless of what else is bundled.** A
runtime apt call still happens, and when the archive is unreachable or a version
drifts the job dies; being 90% bundled buys nothing. The gradient lives in the
evidence column, which reports both sides.

Deliberately *not* flagged: `wget` of a `.tar.gz`, `.zip` or `.gz`. By URL alone a
source tarball is indistinguishable from a reference genome, both are ubiquitous
in genomics apps, and the false-positive cost outweighs the signal.
`wget tarball && make install` is already covered by the manual-compiling check.

- **pip installs use local wheels**

Fails on a bare package name, a `git+`/URL target, or `-r requirements.txt`
without `--no-index`. Passes on a local `.whl`/`.tar.gz` path target, or any
invocation using `--no-index`.

Note `pip install -f /local/wheels pandas` **fails**: `--find-links` alone does
not stop pip falling back to PyPI. `pip install .` and `pip install -e .` also
fail — a source tree still resolves its dependencies from PyPI.

Reads `NA` when the app makes no pip calls at all: the check is about *how* pip is
used, so with no pip it is vacuous and neither rewards nor penalises the app.
`.whl` files in the repository are supporting evidence only, never required —
wheels commonly arrive via a DNAnexus asset.

#### What is scanned, and what these checks will get wrong

Only app runtime code: `runSpec.file` plus scripts under `src/` and `resources/`.
Findings in `tests/`, `.github/`, `docs/`, vendored directories, `setup.py` or
**`build_asset.sh`** are reported as `unscored` and do not fail the app. That last
exclusion matters most: an asset-build script exists precisely to run `apt-get
install` at *build* time and bake the result into a DNAnexus asset, which is the
compliant pattern.

Detection is regex over text, so it is a triage signal, not a proof. **The
evidence column — file, line number and matched line — is the product, not the
boolean.**

It will **miss**:

  - variable indirection: `PIP_CMD="python3 -m pip"; $PIP_CMD install pandas`
  - list-form `subprocess.run(["pip", "install", "pandas"])`
  - installs in a `Makefile`, an `.R` file, or any extension not fetched
  - `pip --cache-dir /tmp install pandas` (a space-separated *global* option)
  - `uv pip install`, `poetry install`, `pdm sync`, `pip download`
  - **anything in a Dockerfile.** Dockerfiles are deliberately out of scope, since
    their installs happen at image build time and are frozen into the image. An
    app that moves its `apt-get install` lines into a Dockerfile will therefore
    score as compliant. This is a real and predictable gap.

It will **over-report**:

  - an install inside a branch that never executes
    (`if [[ $DEBUG == true ]]; then pip install ipdb; fi`) — there is no
    control-flow analysis
  - a heredoc body that documents rather than runs a command. Heredocs are scanned
    because `curl … <<EOF | bash` must be caught, and this is the price
  - `apt-get install ./local.deb`, reported under its own `apt-local-deb` label
    since apt still resolves that package's dependencies from the archive
  - an internal PyPI mirror via `--index-url`, reported as remote — arguably
    correct, since it is still a network fetch

## **Running**

From the repository root:

`uv run python -m dxapp_compliance.main`

`uv run` activates the environment for you, so there is no separate activation
step.

The script will create a HTML file in the directory you're currently in. If the script is run twice for the same period, if a summary report has been previously generated this will be replaced.

Note: This requires a GitHub access token with the correct permissions to access all the repositories in the organisation.

Options:

| Flag | Effect |
|---|---|
| `--config PATH` | Use a different CONFIG.json |
| `--output-dir DIR` | Write the report somewhere other than the working directory |
| `--org NAME` | Override the organisation from CONFIG.json |
| `--limit N` | Audit only the first N apps, for development |
| `--verbose` | Log at DEBUG level |

`python dxapp_queries.py` still works via a deprecation shim, but will be removed
in the next release.

## **Tests**

```
uv run pytest dxapp_compliance/tests -v
```

No mocking and no fixture files: the checks are pure functions of an
`AppEvidence`, which the tests build from literal Python in `tests/fixtures.py`.
(Fixtures are inline rather than on disk partly because the repository's
`.gitignore` has a bare `*.json` rule that would silently untrack any fixture
`dxapp.json`.)

## **Backlog**

Carried over from TODO comments that used to sit at the top of `dxapp_queries.py`,
kept here now that the file is only a deprecation shim:

- Add summary statistics to parts of the HTML report, and style it with bootstrap.
- Make the report prettier with bootstrap.
- Add a list of repos without releases to the report (as a datatable).
- Add instance type to the report.
- Revisit the log format (`LOG_FORMAT` in `config.py`).

Done since: *"Add assetDepends to the report"* — `runSpec.assetDepends` is now read
at the correct path and surfaced in both the **Assets** and **execDepends**
columns. It had never worked, because the code read a top-level `assetsDepends`.

## **Known limitations**

- Only **public** repositories are audited. `list_organisation_repos` paginates on
  the organisation's `public_repos` count, so private repositories fall off the
  end of the listing.
- **The report needs an internet connection to view.** jQuery, bootstrap,
  DataTables and plotly.js are all loaded from CDNs. plotly.js is loaded once for
  all three figures rather than inlined into each; inlining made the report ~14 MB
  instead of ~50 KB. If an offline-viewable report is ever needed, change
  `_figure_html` in `report/plots.py` to `include_plotlyjs=True` for one figure -
  and accept the size.
- Dockerfiles are not scanned — see the dependency-provenance section above for
  what that means for the apt and pip checks.