"""GitHub API client construction and rate-limit awareness."""

import logging
import time

import requests
from fastcore.net import HTTP403ForbiddenError
from ghapi.all import GhApi

logger = logging.getLogger(__name__)

REST_ROOT = "https://api.github.com"
API_VERSION = "2022-11-28"


class GitHubClient:
    """Wraps GhApi plus a requests session for the endpoints ghapi lacks.

    Also counts API calls, so a run can report whether it is near the 5000/hour
    ceiling rather than failing opaquely part-way through.
    """

    def __init__(self, token, organisation):
        self.token = token
        self.organisation = organisation
        self.api = GhApi(token=token)
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
        except HTTP403ForbiddenError:
            wait = 60
            logger.warning(
                f"403 from GitHub (likely a secondary rate limit); waiting "
                f"{wait}s and retrying once."
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
