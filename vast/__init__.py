import logging

logger = logging.getLogger(__name__)

class VastException(Exception):
    pass

from .vast import Vast, vast_cmd

