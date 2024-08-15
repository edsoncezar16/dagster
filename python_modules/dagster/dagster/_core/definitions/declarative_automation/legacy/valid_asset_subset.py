import datetime
import operator
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable, Optional

from dagster._core.definitions.asset_subset import AssetSubset, AssetSubsetSerializer
from dagster._core.definitions.partition import PartitionsDefinition
from dagster._serdes.serdes import whitelist_for_serdes

if TYPE_CHECKING:
    from dagster._core.instance import DynamicPartitionsStore


@whitelist_for_serdes(serializer=AssetSubsetSerializer)
class ValidAssetSubset(AssetSubset):
    """Legacy construct used for doing operations over AssetSubsets that are known to be valid. This
    functionality is subsumed by AssetSlice.
    """

    def inverse(
        self,
        partitions_def: Optional[PartitionsDefinition],
        current_time: Optional[datetime.datetime] = None,
        dynamic_partitions_store: Optional["DynamicPartitionsStore"] = None,
    ) -> "ValidAssetSubset":
        """Returns the AssetSubset containing all asset partitions which are not in this AssetSubset."""
        if partitions_def is None:
            return replace(self, value=not self.bool_value)
        else:
            value = partitions_def.subset_with_partition_keys(
                self.subset_value.get_partition_keys_not_in_subset(
                    partitions_def,
                    current_time=current_time,
                    dynamic_partitions_store=dynamic_partitions_store,
                )
            )
            return replace(self, value=value)

    def _oper(self, other: "ValidAssetSubset", oper: Callable[..., Any]) -> "ValidAssetSubset":
        value = oper(self.value, other.value)
        return replace(self, value=value)

    def __sub__(self, other: AssetSubset) -> "ValidAssetSubset":
        """Returns an AssetSubset representing self.asset_partitions - other.asset_partitions."""
        valid_other = self.get_valid(other)
        if not self.is_partitioned:
            return replace(self, value=self.bool_value and not valid_other.bool_value)
        return self._oper(valid_other, operator.sub)

    def __and__(self, other: AssetSubset) -> "ValidAssetSubset":
        """Returns an AssetSubset representing self.asset_partitions & other.asset_partitions."""
        return self._oper(self.get_valid(other), operator.and_)

    def __or__(self, other: AssetSubset) -> "ValidAssetSubset":
        """Returns an AssetSubset representing self.asset_partitions | other.asset_partitions."""
        return self._oper(self.get_valid(other), operator.or_)

    def get_valid(self, other: AssetSubset) -> "ValidAssetSubset":
        """Creates a ValidAssetSubset from the given AssetSubset by returning a replace of the given
        AssetSubset if it is compatible with this AssetSubset, otherwise returns an empty subset.
        """
        if isinstance(other, ValidAssetSubset):
            return other
        elif self._is_compatible_with_subset(other):
            return ValidAssetSubset(key=other.asset_key, value=other.value)
        else:
            return replace(
                self,
                # unfortunately, this is the best way to get an empty partitions subset of an unknown
                # type if you don't have access to the partitions definition
                value=(self.subset_value - self.subset_value) if self.is_partitioned else False,
            )
