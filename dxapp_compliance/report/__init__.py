"""Scoring, table formatting, plotting and rendering.

``scoring.py`` deliberately contains no pandas: it scores plain dicts. Two
reasons, both of which were live bugs in the original.

``1 == True`` is True in pandas, so the old ``(checks_df == True).T.sum()``
counted any integer-valued column as a pass - it only avoided that by
remembering to drop ``num_of_region_options`` first. And a ``numpy.bool_`` cell
fails an ``is True`` check, so the frame is the wrong substrate for a strict
comparison. Scoring dicts with ``is True`` removes both hazards by construction,
and makes the scoring functions testable with literal input.
"""
