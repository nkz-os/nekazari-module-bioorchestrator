"""The canonicalize_trial_sites CLI must default to dry-run (never mutate without --execute)."""

from __future__ import annotations

from scripts.canonicalize_trial_sites import parse_args


def test_defaults_to_dry_run():
    assert parse_args([]).execute is False


def test_execute_flag_enables_mutation():
    assert parse_args(["--execute"]).execute is True
