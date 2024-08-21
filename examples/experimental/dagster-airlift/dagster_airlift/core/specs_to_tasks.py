from typing import Sequence, Union

from dagster import AssetsDefinition, AssetSpec, multi_asset
from dagster._core.definitions.asset_key import CoercibleToAssetKey
from dagster._core.definitions.definitions_class import Definitions
from typing_extensions import TypeAlias

from .utils import DAG_ID_TAG, TASK_ID_TAG

CoercibleToAssetSpec: TypeAlias = Union[AssetSpec, CoercibleToAssetKey]


def defs_from_task(
    *, task_id: str, dag_id: str, assets: Sequence[CoercibleToAssetSpec]
) -> Definitions:
    asset_specs = [
        asset if isinstance(asset, AssetSpec) else AssetSpec(key=asset) for asset in assets
    ]

    @multi_asset(
        op_tags={DAG_ID_TAG: dag_id, TASK_ID_TAG: task_id},
        specs=asset_specs,
        name=f"{dag_id}__{task_id}",
        compute_kind="airflow",
    )
    def task_asset():
        pass

    return Definitions(assets=[task_asset])


def combine_defs(*defs: Union[AssetsDefinition, Definitions]) -> Definitions:
    """Combine a set of definitions into a single definitions object."""
    assets = [the_def for the_def in defs if isinstance(the_def, AssetsDefinition)]

    return Definitions.merge(
        *[the_def for the_def in defs if isinstance(the_def, Definitions)],
        Definitions(assets=assets),
    )
