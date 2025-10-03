"""
AI Helper utilities for OpenAI integration.

This module provides reusable functions for working with OpenAI API across all projects.
Includes text generation, JSON response parsing, and error handling utilities.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

try:
    import openai

    # OpenAI Configuration - gets API key from environment
    openai.api_key = os.getenv("OPENAI_API_KEY")
except ImportError:
    openai = None
    logging.warning(
        "OpenAI module not installed. AI functions will not work. Install with: pip install openai"
    )


def generate_chat_completion(
    system_prompt: str,
    user_prompt: str,
    model: str = "gpt-3.5-turbo",
    temperature: float = 0.7,
    max_tokens: int = 500,
) -> Dict[str, Any]:
    """
    Generate a chat completion using OpenAI API.

    Args:
        system_prompt: System message defining AI behavior
        user_prompt: User message with the actual request
        model: OpenAI model to use (default: gpt-3.5-turbo)
        temperature: Sampling temperature 0-1 (default: 0.7)
        max_tokens: Maximum tokens in response (default: 500)

    Returns:
        Dict with:
        - content: Generated text content
        - error: Error message if generation failed, None otherwise
    """
    if not openai:
        return {
            "content": None,
            "error": "OpenAI module not installed",
        }

    try:
        response = openai.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )

        content = response.choices[0].message.content.strip()
        logging.debug(f"OpenAI response generated successfully (model: {model})")

        return {
            "content": content,
            "error": None,
        }

    except Exception as e:
        error_msg = f"Error generating chat completion: {str(e)}"
        logging.error(error_msg)
        return {
            "content": None,
            "error": error_msg,
        }


def parse_json_response(response_text: str) -> Dict[str, Any]:
    """
    Parse JSON from OpenAI response, handling markdown code blocks.

    Args:
        response_text: Raw response text from OpenAI that may contain JSON

    Returns:
        Dict with:
        - data: Parsed JSON data (dict) if successful, None otherwise
        - error: Error message if parsing failed, None otherwise
    """
    try:
        # Remove markdown code blocks if present
        cleaned_text = response_text.strip()
        if cleaned_text.startswith("```"):
            # Extract content between code fences
            parts = cleaned_text.split("```")
            if len(parts) >= 2:
                cleaned_text = parts[1]
                # Remove language identifier if present
                if cleaned_text.startswith("json"):
                    cleaned_text = cleaned_text[4:]
                cleaned_text = cleaned_text.strip()

        # Parse JSON
        data = json.loads(cleaned_text)
        return {
            "data": data,
            "error": None,
        }

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse JSON: {str(e)}"
        logging.warning(f"{error_msg}. Response text: {response_text}")
        return {
            "data": None,
            "error": error_msg,
        }


def generate_json_completion(
    system_prompt: str,
    user_prompt: str,
    model: str = "gpt-4o-mini",
    temperature: float = 0.3,
    max_tokens: int = 500,
) -> Dict[str, Any]:
    """
    Generate a JSON completion using OpenAI API with automatic parsing.

    This is a convenience function that combines generate_chat_completion
    and parse_json_response for JSON-based responses.

    Args:
        system_prompt: System message defining AI behavior
        user_prompt: User message with the actual request
        model: OpenAI model to use (default: gpt-4o-mini)
        temperature: Sampling temperature 0-1 (default: 0.3 for more deterministic JSON)
        max_tokens: Maximum tokens in response (default: 500)

    Returns:
        Dict with:
        - data: Parsed JSON data (dict) if successful, None otherwise
        - raw_content: Raw text content from OpenAI
        - error: Error message if generation or parsing failed, None otherwise
    """
    # Generate completion
    completion_result = generate_chat_completion(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    if completion_result["error"]:
        return {
            "data": None,
            "raw_content": None,
            "error": completion_result["error"],
        }

    # Parse JSON from response
    raw_content = completion_result["content"]
    parse_result = parse_json_response(raw_content)

    return {
        "data": parse_result["data"],
        "raw_content": raw_content,
        "error": parse_result["error"],
    }


def clamp_value(value: int, min_value: int, max_value: int) -> int:
    """
    Clamp a value between min and max bounds.

    Args:
        value: Value to clamp
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Returns:
        Clamped value
    """
    return max(min_value, min(max_value, value))


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate text to maximum length, adding suffix if truncated.

    Args:
        text: Text to truncate
        max_length: Maximum length (including suffix)
        suffix: Suffix to add if truncated (default: "...")

    Returns:
        Truncated text with suffix if necessary
    """
    if len(text) <= max_length:
        return text

    return text[: max_length - len(suffix)] + suffix
