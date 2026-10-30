"""Offline regression for Vast background-tab jobs polling."""

from javascript_checks import run_javascript

from pathlib import Path


def test_hidden_tab_pauses_jobs_polling_and_resumes_on_return():
    """No jobs request or timer survives a hidden tab; return refreshes once."""
    script = Path(__file__).with_name("vast_visibility_poll.browser.js")
    run_javascript([str(script)], check=True, capture_output=True, text=True)
