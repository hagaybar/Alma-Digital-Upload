"""
Matching strategy modules for Alma Digital Upload.

This package provides different strategies for matching local files
to Alma bibliographic records:

- Marc907eStrategy: Matches files using MARC 907$e field values
- MmsIdFilenameStrategy: Matches files by MMS ID in filename
"""

from strategies.base import MatchStrategy
from strategies.marc_907e_strategy import Marc907eStrategy
from strategies.mms_id_filename_strategy import MmsIdFilenameStrategy

__all__ = [
    "MatchStrategy",
    "Marc907eStrategy",
    "MmsIdFilenameStrategy",
]
