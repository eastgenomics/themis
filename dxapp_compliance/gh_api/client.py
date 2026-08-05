"""GitHub API client construction and rate-limit awareness."""

import logging
import time

import requests
from ghapi.all import GhApi

logger = logging.getLogger(__name__)

REST_ROOT = "https://api.github.com"
API_VERSION = "2022-11-28"


def error_status(error):
    """HTTP status carried by a ghapi exception, or None.

    ghapi 1.x raised typed fastcore exceptions (HTTP404NotFoundError); ghapi 2.x
    raises a single fastspec.errors.APIError carrying status_code. Reading the
    status rather than catching a class keeps this working across both, and means
    a future reshuffle of the exception hierarchy cannot silently stop a 404 from
    being recognised - which matters because "404 on dxapp.json" is how the audit
    decides a repository is not an app.
    """
    for attribute in ('status_code', 'status', 'code'):
        value = getattr(error, attribute, None)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)

    # fastcore's typed exceptions encode the status in the class name.
    name = type(error).__name__
    if name.startswith('HTTP') and name[4:7].isdigit():
        return int(name[4:7])

    return None


def is_not_found(error):
    """Whether an exception represents a GitHub 404."""
    return error_status(error) == 404


def is_rate_limited(error):
    """Whether an exception represents a rate-limit refusal."""
    return error_status(error) in (403, 429)


class GitHubClient:
    """Wraps GhApi plus a requests session for the endpoints ghapi lacks.

    Also counts API calls, so a run can report whether it is near the 5000/hour
    ceiling rather than failing opaquely part-way through.
    """

    def __init__(self, token, organisation):
        self.token = token
        self.organisation = organisation
        # sync=True is required, not optional. ghapi 2.x defaults to sync=False,
        # which makes every endpoint a coroutine - calling one without awaiting
        # it returns a coroutine object, so the first attribute access fails with
        # "'coroutine' object is not subscriptable" and a "was never awaited"
        # warning. ghapi 1.x, which this code was originally written against, was
        # synchronous by default.
        self.api = GhApi(token=token, sync=True)
        self.calls = 0

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "Authorization": f"token {token}",
            "X-GitHub-Api-Version": API_VERSION,
        })

    def call(self, func, *args, **kwargs):
        """Invoke a ghapi endpoint, retrying once on a rate-limit refusal.

        A 403 from GitHub is usually a secondary rate limit rather than a
        permissions problem, and it otherwise surfaces mid-run as a partial
        report with no obvious cause.
        """
        self.calls += 1
        try:
            return func(*args, **kwargs)
        except Exception as error:
            if not is_rate_limited(error):
                raise
            wait = 60
            logger.warning(
                f"{error_status(error)} from GitHub (likely a secondary rate "
                f"limit); waiting {wait}s and retrying once."
            )
            time.sleep(wait)
            self.calls += 1
            return func(*args, **kwargs)

    def get(self, path, **kwargs):
        """GET a REST path, returning the parsed JSON or None on failure."""
        self.calls += 1
        url = f"{REST_ROOT}{path}"
        try:
            response = self.session.get(url, timeout=10, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            logger.error(f"GET {path} failed: {error}")
            return None
        except ValueError as error:
            logger.error(f"GET {path} returned unparseable JSON: {error}")
            return None

    def rate_limit_remaining(self):
        """Remaining core API quota, or None if it could not be read."""
        data = self.get("/rate_limit")
        if not data:
            return None

        return data.get('resources', {}).get('core', {}).get('remaining')
