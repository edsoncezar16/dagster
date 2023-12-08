import datetime
import json
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
)

import dagster._check as check
from dagster._core.definitions.auto_materialize_rule_evaluation import (
    AutoMaterializeAssetEvaluation,
)
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.time_window_partitions import (
    TimeWindowPartitionsDefinition,
    TimeWindowPartitionsSubset,
)
from dagster._serdes.serdes import deserialize_value, serialize_value

from .asset_graph import AssetGraph
from .asset_subset import AssetSubset
from .partition import PartitionsSubset

if TYPE_CHECKING:
    from .asset_automation_evaluator import ConditionEvaluation


class AssetDaemonAssetCursor(NamedTuple):
    """Convenience class to represent the state of an individual asset being handled by the daemon.
    In the future, this will be serialized as part of the cursor.
    """

    asset_key: AssetKey
    latest_storage_id: Optional[int]
    latest_evaluation_timestamp: Optional[float]
    latest_evaluation: Optional["ConditionEvaluation"]
    materialized_requested_or_discarded_subset: AssetSubset


class AssetDaemonCursor(NamedTuple):
    """State that's saved between reconciliation evaluations.

    Attributes:
        latest_storage_id:
            The latest observed storage ID across all assets. Useful for finding out what has
            happened since the last tick.
        handled_root_asset_keys:
            Every entry is a non-partitioned asset with no parents that has been requested by this
            sensor, discarded by this sensor, or has been materialized (even if not by this sensor).
        handled_root_partitions_by_asset_key:
            Every key is a partitioned root asset. Every value is the set of that asset's partitions
            that have been requested by this sensor, discarded by this sensor,
            or have been materialized (even if not by this sensor).
        last_observe_request_timestamp_by_asset_key:
            Every key is an observable source asset that has been auto-observed. The value is the
            timestamp of the tick that requested the observation.
    """

    latest_storage_id: Optional[int]
    handled_root_asset_keys: AbstractSet[AssetKey]
    handled_root_partitions_by_asset_key: Mapping[AssetKey, PartitionsSubset]
    evaluation_id: int
    last_observe_request_timestamp_by_asset_key: Mapping[AssetKey, float]
    latest_evaluation_by_asset_key: Mapping[AssetKey, AutoMaterializeAssetEvaluation]
    latest_evaluation_timestamp: Optional[float]

    def was_previously_handled(self, asset_key: AssetKey) -> bool:
        return asset_key in self.handled_root_asset_keys

    def asset_cursor_for_key(
        self, asset_key: AssetKey, asset_graph: AssetGraph
    ) -> AssetDaemonAssetCursor:
        from .asset_automation_evaluator import ConditionEvaluation

        partitions_def = asset_graph.get_partitions_def(asset_key)
        handled_partitions_subset = self.handled_root_partitions_by_asset_key.get(asset_key)
        if handled_partitions_subset is not None:
            handled_subset = AssetSubset(asset_key=asset_key, value=handled_partitions_subset)
        elif asset_key in self.handled_root_asset_keys:
            handled_subset = AssetSubset(asset_key=asset_key, value=True)
        else:
            handled_subset = AssetSubset.empty(asset_key, partitions_def)
        condition = (
            check.not_none(asset_graph.get_auto_materialize_policy(asset_key))
            .to_auto_materialize_policy_evaluator()
            .condition
        )
        return AssetDaemonAssetCursor(
            asset_key=asset_key,
            latest_storage_id=self.latest_storage_id,
            latest_evaluation_timestamp=self.latest_evaluation_timestamp,
            latest_evaluation=ConditionEvaluation.from_evaluation(
                condition=condition,
                evaluation=self.latest_evaluation_by_asset_key.get(asset_key),
                asset_graph=asset_graph,
            ),
            materialized_requested_or_discarded_subset=handled_subset,
        )

    def with_updates(
        self,
        latest_storage_id: Optional[int],
        evaluation_id: int,
        newly_observe_requested_asset_keys: Sequence[AssetKey],
        observe_request_timestamp: float,
        evaluations: Sequence[AutoMaterializeAssetEvaluation],
        evaluation_time: datetime.datetime,
        asset_cursors: Sequence[AssetDaemonAssetCursor],
    ) -> "AssetDaemonCursor":
        """Returns a cursor that represents this cursor plus the updates that have happened within the
        tick.
        """
        result_last_observe_request_timestamp_by_asset_key = {
            **self.last_observe_request_timestamp_by_asset_key
        }
        for asset_key in newly_observe_requested_asset_keys:
            result_last_observe_request_timestamp_by_asset_key[
                asset_key
            ] = observe_request_timestamp

        if latest_storage_id and self.latest_storage_id:
            check.invariant(
                latest_storage_id >= self.latest_storage_id,
                "Latest storage ID should be >= previous latest storage ID",
            )

        latest_evaluation_by_asset_key = {
            evaluation.asset_key: evaluation
            for evaluation in evaluations
            # don't bother storing empty evaluations on the cursor
            if not evaluation.is_empty
        }

        return AssetDaemonCursor(
            latest_storage_id=latest_storage_id or self.latest_storage_id,
            handled_root_asset_keys={
                cursor.asset_key
                for cursor in asset_cursors
                if not cursor.materialized_requested_or_discarded_subset.is_partitioned
                and cursor.materialized_requested_or_discarded_subset.bool_value
            },
            handled_root_partitions_by_asset_key={
                cursor.asset_key: cursor.materialized_requested_or_discarded_subset.subset_value
                for cursor in asset_cursors
                if cursor.materialized_requested_or_discarded_subset.is_partitioned
            },
            evaluation_id=evaluation_id,
            last_observe_request_timestamp_by_asset_key=result_last_observe_request_timestamp_by_asset_key,
            latest_evaluation_by_asset_key=latest_evaluation_by_asset_key,
            latest_evaluation_timestamp=evaluation_time.timestamp(),
        )

    @classmethod
    def empty(cls) -> "AssetDaemonCursor":
        return AssetDaemonCursor(
            latest_storage_id=None,
            handled_root_partitions_by_asset_key={},
            handled_root_asset_keys=set(),
            evaluation_id=0,
            last_observe_request_timestamp_by_asset_key={},
            latest_evaluation_by_asset_key={},
            latest_evaluation_timestamp=None,
        )

    @classmethod
    def from_serialized(cls, cursor: str, asset_graph: AssetGraph) -> "AssetDaemonCursor":
        data = json.loads(cursor)

        if isinstance(data, list):  # backcompat
            check.invariant(len(data) in [3, 4], "Invalid serialized cursor")
            (
                latest_storage_id,
                serialized_handled_root_asset_keys,
                serialized_handled_root_partitions_by_asset_key,
            ) = data[:3]

            evaluation_id = data[3] if len(data) == 4 else 0
            serialized_last_observe_request_timestamp_by_asset_key = {}
            serialized_latest_evaluation_by_asset_key = {}
            latest_evaluation_timestamp = 0
        else:
            latest_storage_id = data["latest_storage_id"]
            serialized_handled_root_asset_keys = data["handled_root_asset_keys"]
            serialized_handled_root_partitions_by_asset_key = data[
                "handled_root_partitions_by_asset_key"
            ]
            evaluation_id = data["evaluation_id"]
            serialized_last_observe_request_timestamp_by_asset_key = data.get(
                "last_observe_request_timestamp_by_asset_key", {}
            )
            serialized_latest_evaluation_by_asset_key = data.get(
                "latest_evaluation_by_asset_key", {}
            )
            latest_evaluation_timestamp = data.get("latest_evaluation_timestamp", 0)

        handled_root_partitions_by_asset_key = {}
        for (
            key_str,
            serialized_subset,
        ) in serialized_handled_root_partitions_by_asset_key.items():
            key = AssetKey.from_user_string(key_str)
            if key not in asset_graph.materializable_asset_keys:
                continue

            partitions_def = asset_graph.get_partitions_def(key)
            if partitions_def is None:
                continue

            try:
                # in the case that the partitions def has changed, we may not be able to deserialize
                # the corresponding subset. in this case, we just use an empty subset
                subset = partitions_def.deserialize_subset(serialized_subset)
                # this covers the case in which the start date has changed for a time-partitioned
                # asset. in reality, we should be using the can_deserialize method but because we
                # are not storing the serializable unique id, we can't do that.
                if (
                    isinstance(subset, TimeWindowPartitionsSubset)
                    and isinstance(partitions_def, TimeWindowPartitionsDefinition)
                    and any(
                        time_window.start < partitions_def.start
                        for time_window in subset.included_time_windows
                    )
                ):
                    subset = partitions_def.empty_subset()
            except:
                subset = partitions_def.empty_subset()
            handled_root_partitions_by_asset_key[key] = subset

        latest_evaluation_by_asset_key = {}
        for key_str, serialized_evaluation in serialized_latest_evaluation_by_asset_key.items():
            key = AssetKey.from_user_string(key_str)
            evaluation = check.inst(
                deserialize_value(serialized_evaluation), AutoMaterializeAssetEvaluation
            )
            latest_evaluation_by_asset_key[key] = evaluation

        return cls(
            latest_storage_id=latest_storage_id,
            handled_root_asset_keys={
                AssetKey.from_user_string(key_str) for key_str in serialized_handled_root_asset_keys
            },
            handled_root_partitions_by_asset_key=handled_root_partitions_by_asset_key,
            evaluation_id=evaluation_id,
            last_observe_request_timestamp_by_asset_key={
                AssetKey.from_user_string(key_str): timestamp
                for key_str, timestamp in serialized_last_observe_request_timestamp_by_asset_key.items()
            },
            latest_evaluation_by_asset_key=latest_evaluation_by_asset_key,
            latest_evaluation_timestamp=latest_evaluation_timestamp,
        )

    @classmethod
    def get_evaluation_id_from_serialized(cls, cursor: str) -> Optional[int]:
        data = json.loads(cursor)
        if isinstance(data, list):  # backcompat
            check.invariant(len(data) in [3, 4], "Invalid serialized cursor")
            return data[3] if len(data) == 4 else None
        else:
            return data["evaluation_id"]

    def serialize(self) -> str:
        serializable_handled_root_partitions_by_asset_key = {
            key.to_user_string(): subset.serialize()
            for key, subset in self.handled_root_partitions_by_asset_key.items()
        }
        serialized = json.dumps(
            {
                "latest_storage_id": self.latest_storage_id,
                "handled_root_asset_keys": [
                    key.to_user_string() for key in self.handled_root_asset_keys
                ],
                "handled_root_partitions_by_asset_key": (
                    serializable_handled_root_partitions_by_asset_key
                ),
                "evaluation_id": self.evaluation_id,
                "last_observe_request_timestamp_by_asset_key": {
                    key.to_user_string(): timestamp
                    for key, timestamp in self.last_observe_request_timestamp_by_asset_key.items()
                },
                "latest_evaluation_by_asset_key": {
                    key.to_user_string(): serialize_value(evaluation)
                    for key, evaluation in self.latest_evaluation_by_asset_key.items()
                },
                "latest_evaluation_timestamp": self.latest_evaluation_timestamp,
            }
        )
        return serialized
