"""Shared structured failure type for model-sidecar HTTP wrappers.

Raised by both ``speaker_turns_api`` and ``trim_proposals_api`` on any
transport or parse failure (connection refused, timeout, non-200 response,
or malformed JSON). Callers can catch the specific type or the ``RuntimeError``
base to handle all sidecar failures uniformly.
"""

from __future__ import annotations


class SidecarApiError(RuntimeError):
    """Raised when a model-sidecar call fails.

    Covers: connection refused, request timeout, non-200 HTTP response,
    and malformed JSON in the response body. The error message always
    includes the failing URL and, for non-200 responses, the HTTP status code.
    """
