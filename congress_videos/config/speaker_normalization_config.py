"""
Speaker normalization configuration constants.

Controls fuzzy-match threshold, AI model selection, and feature toggle for
the speaker_normalization module.  Mirrors the pattern used by other config
modules in this package (Python constants only — no YAML loader).
"""

from utils.llm_config import LLM_CHEAP

# Minimum fuzzy similarity score (0-1) for a candidate to proceed to AI
# verification.  Mirrors WIKIDATA_FUZZY_THRESHOLD from constants.py.
FUZZY_THRESHOLD: float = 0.90

# OpenAI model used for AI-assisted speaker identity verification.
# Alias for the LLM_CHEAP tier (utils/llm_config.py); keeps this module's
# public shape unchanged for existing importers.
AI_MODEL: str = LLM_CHEAP

# Master on/off switch.  When False, the t_normalize_speakers DAG task
# short-circuits and the DAG completes without modifying any chapter.
ENABLED: bool = True

# When True, the AI verification prompt includes a snippet of the chapter
# timeline as additional context for the speaker match decision.
CONTEXT_ENABLED: bool = True
