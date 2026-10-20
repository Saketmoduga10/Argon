"""Failure lab scenarios exercised in process. The API server runs
them in a subprocess to protect its own event loop; a test loop has
nothing else to protect, so here they run directly."""

import asyncio

import pytest

from argon.lab import FAMILIES, run_scenario


@pytest.mark.parametrize("family", sorted(FAMILIES))
def test_every_scenario_family_recovers(family, tmp_path):
    result = asyncio.run(run_scenario(family, 0, tmp_path))
    assert result["family"] == family
    assert result["passed"] is True, result["detail"]


@pytest.mark.parametrize("index", [1, 2, 3])
def test_crash_after_append_variants(index, tmp_path):
    result = asyncio.run(run_scenario("crash_after_append", index, tmp_path))
    assert result["passed"] is True
    assert result["crashed_mid_run"] is True
