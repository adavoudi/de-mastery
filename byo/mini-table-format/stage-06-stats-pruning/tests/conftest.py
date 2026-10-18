"""Put the submission dir on sys.path so tests can import the PROVIDED
``objectstore`` primitive directly (the learner never edits it). The submission
module itself is still loaded via the ``submission`` fixture.
"""
import os
import sys

_sub = os.environ.get("DE_SUBMISSION_DIR")
if _sub and _sub not in sys.path:
    sys.path.insert(0, _sub)
