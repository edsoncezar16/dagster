"""Scenarios where the set of asset definitions changes between ticks."""

from dagster_tests.definitions_tests.auto_materialize_tests.base_scenario import (
    AssetReconciliationScenario,
)
from dagster_tests.definitions_tests.auto_materialize_tests.scenarios.basic_scenarios import (
    one_asset,
    two_assets_in_sequence,
)
from dagster_tests.definitions_tests.auto_materialize_tests.scenarios.partition_scenarios import (
    one_asset_one_partition,
)

definition_change_scenarios = {
    "asset_removed": AssetReconciliationScenario(
        assets=[],
        unevaluated_runs=[],
        expected_run_requests=[],
        cursor_from=AssetReconciliationScenario(
            assets=one_asset,
            unevaluated_runs=[],
        ),
    ),
    "downstream_asset_removed": AssetReconciliationScenario(
        assets=one_asset,
        unevaluated_runs=[],
        expected_run_requests=[],
        cursor_from=AssetReconciliationScenario(
            assets=two_assets_in_sequence,
            unevaluated_runs=[],
        ),
    ),
    "partitioned_asset_removed": AssetReconciliationScenario(
        assets=[],
        unevaluated_runs=[],
        expected_run_requests=[],
        cursor_from=AssetReconciliationScenario(
            assets=one_asset_one_partition,
            unevaluated_runs=[],
        ),
    ),
}
