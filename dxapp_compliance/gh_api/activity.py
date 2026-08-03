"""Release and activity dates for a repository."""

import logging

from fastcore.net import HTTP404NotFoundError

logger = logging.getLogger(__name__)


def latest_release_date(client, repo):
    """Date of the repository's most recent release, or None.

    Returns
    -------
        str or None: 'YYYY-MM-DD'.
    """
    try:
        release = client.call(
            client.api.repos.get_latest_release,
            client.organisation,
            repo.name,
        )
    except HTTP404NotFoundError:
        logger.info(f"{repo.name}: no releases.")
        return None

    published = release.get('published_at')

    return published.split("T")[0] if published else None


def latest_commit_date(repo):
    """Date of the repository's most recent push. No API call.

    ``pushed_at`` is already on the repo object from the organisation listing.
    The previous implementation listed every branch and then requested the latest
    commit of each - an N+1 pattern per repository, and by far the largest
    consumer of the hourly API budget. It is what paid for the new git-tree and
    blob calls.

    This reports the most recent push across the repository rather than the
    maximum commit date over all branches. The values can differ slightly: a
    branch carrying a back-dated or rebased commit could previously report a date
    that no push corresponds to.

    Returns
    -------
        str or None: 'YYYY-MM-DD'.
    """
    if not repo.pushed_at:
        return None

    return repo.pushed_at.split("T")[0]
