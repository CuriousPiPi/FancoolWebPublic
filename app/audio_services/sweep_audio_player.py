"""Compatibility import for the generalized audio generator."""

import sys

from app.audio_services import audio_player

sys.modules[__name__] = audio_player
