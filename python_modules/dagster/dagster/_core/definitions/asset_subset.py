import datetime
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, AbstractSet, Generic, Optional, TypeVar, Union, cast

from typing_extensions import Self

import dagster._check as check
from dagster._core.definitions.asset_key import AssetGraphEntityKey
from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey
from dagster._core.definitions.partition import (
    AllPartitionsSubset,
    PartitionsDefinition,
    PartitionsSubset,
)
from dagster._core.definitions.time_window_partitions import BaseTimeWindowPartitionsSubset
from dagster._serdes.serdes import DataclassSerializer, whitelist_for_serdes

if TYPE_CHECKING:
    from dagster._core.definitions.declarative_automation.legacy.valid_asset_subset import (
        ValidAssetSubset,
    )
    from dagster._core.instance import DynamicPartitionsStore


T = TypeVar("T", bound=AssetGraphEntityKey)
AssetGraphEntitySubsetValue = Union[bool, PartitionsSubset]


@dataclass(frozen=True)
class AssetGraphEntitySubset(Generic[T]):
    """Generic base class for representing a subset of an AssetGraphEntity."""

    key: T
    value: AssetGraphEntitySubsetValue

    @property
    def is_partitioned(self) -> bool:
        return not isinstance(self.value, bool)

    @property
    def bool_value(self) -> bool:
        check.invariant(isinstance(self.value, bool))
        return cast(bool, self.value)

    @property
    def subset_value(self) -> PartitionsSubset:
        check.invariant(isinstance(self.value, PartitionsSubset))
        return cast(PartitionsSubset, self.value)

    @property
    def size(self) -> int:
        if not self.is_partitioned:
            return int(self.bool_value)
        else:
            return len(self.subset_value)

    @property
    def is_empty(self) -> bool:
        if self.is_partitioned:
            return self.subset_value.is_empty
        else:
            return not self.bool_value


class AssetSubsetSerializer(DataclassSerializer):
    """Ensures that the inner PartitionsSubset is converted to a serializable form if necessary."""

    def get_storage_name(self) -> str:
        # override this method so all ValidAssetSubsets are serialzied as AssetSubsets
        return "AssetSubset"

    def before_pack(self, value: "AssetSubset") -> "AssetSubset":
        if value.is_partitioned:
            return replace(value, value=value.subset_value.to_serializable_subset())
        return value


@whitelist_for_serdes(serializer=AssetSubsetSerializer, storage_field_names={"key": "asset_key"})
@dataclass(frozen=True)
class AssetSubset(AssetGraphEntitySubset[AssetKey]):
    """Represents a set of AssetKeyPartitionKeys for a given AssetKey. For partitioned assets, this
    class uses a PartitionsSubset to represent the set of partitions, enabling lazy evaluation of the
    underlying partition keys. For unpartitioned assets, this class uses a bool to represent whether
    the asset is present or not.
    """

    key: AssetKey
    value: AssetGraphEntitySubsetValue

    @property
    def asset_key(self) -> AssetKey:
        return self.key

    @property
    def asset_partitions(self) -> AbstractSet[AssetKeyPartitionKey]:
        if not self.is_partitioned:
            return {AssetKeyPartitionKey(self.asset_key)} if self.bool_value else set()
        else:
            return {
                AssetKeyPartitionKey(self.asset_key, partition_key)
                for partition_key in self.subset_value.get_partition_keys()
            }

    def is_compatible_with_partitions_def(
        self, partitions_def: Optional[PartitionsDefinition]
    ) -> bool:
        if self.is_partitioned:
            # for some PartitionSubset types, we have access to the underlying partitions
            # definitions, so we can ensure those are identical
            if isinstance(self.value, (BaseTimeWindowPartitionsSubset, AllPartitionsSubset)):
                return self.value.partitions_def == partitions_def
            else:
                return partitions_def is not None
        else:
            return partitions_def is None

    def _is_compatible_with_subset(self, other: "AssetSubset") -> bool:
        if isinstance(other.value, (BaseTimeWindowPartitionsSubset, AllPartitionsSubset)):
            return self.is_compatible_with_partitions_def(other.value.partitions_def)
        else:
            return self.is_partitioned == other.is_partitioned

    def as_valid(self, partitions_def: Optional[PartitionsDefinition]) -> "ValidAssetSubset":
        """Converts this AssetSubset to a ValidAssetSubset by returning a replace of this AssetSubset
        if it is compatible with the given PartitionsDefinition, otherwise returns an empty subset.
        """
        from dagster._core.definitions.declarative_automation.legacy.valid_asset_subset import (
            ValidAssetSubset,
        )

        if self.is_compatible_with_partitions_def(partitions_def):
            return ValidAssetSubset(key=self.asset_key, value=self.value)
        else:
            return ValidAssetSubset.empty(self.asset_key, partitions_def)

    @classmethod
    def all(
        cls,
        asset_key: AssetKey,
        partitions_def: Optional[PartitionsDefinition],
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
        current_time: Optional[datetime.datetime] = None,
    ) -> Self:
        if partitions_def is None:
            return cls(key=asset_key, value=True)
        else:
            if dynamic_partitions_store is None or current_time is None:
                check.failed(
                    "Must provide dynamic_partitions_store and current_time for partitioned assets."
                )
            return cls(
                key=asset_key,
                value=AllPartitionsSubset(partitions_def, dynamic_partitions_store, current_time),
            )

    @classmethod
    def empty(cls, asset_key: AssetKey, partitions_def: Optional[PartitionsDefinition]) -> Self:
        if partitions_def is None:
            return cls(key=asset_key, value=False)
        else:
            return cls(key=asset_key, value=partitions_def.empty_subset())

    @classmethod
    def from_asset_partitions_set(
        cls,
        asset_key: AssetKey,
        partitions_def: Optional[PartitionsDefinition],
        asset_partitions_set: AbstractSet[AssetKeyPartitionKey],
    ) -> Self:
        return (
            cls.from_partition_keys(
                asset_key=asset_key,
                partitions_def=partitions_def,
                partition_keys={
                    ap.partition_key for ap in asset_partitions_set if ap.partition_key is not None
                },
            )
            if partitions_def
            else cls(key=asset_key, value=bool(asset_partitions_set))
        )

    @classmethod
    def from_partition_keys(
        cls,
        asset_key: AssetKey,
        partitions_def: PartitionsDefinition,
        partition_keys: AbstractSet[str],
    ) -> Self:
        return cls(key=asset_key, value=partitions_def.subset_with_partition_keys(partition_keys))

    def __contains__(self, item: AssetKeyPartitionKey) -> bool:
        if not self.is_partitioned:
            return (
                item.asset_key == self.asset_key and item.partition_key is None and self.bool_value
            )
        else:
            return item.asset_key == self.asset_key and item.partition_key in self.subset_value
