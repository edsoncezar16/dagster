from pathlib import Path
from typing import Dict, Literal, Optional, Sequence, Set, Tuple, Union

import dagster._check as check
import polars as pl
from dagster import AssetDep, AssetKey, AssetOut, Nothing
from dagster._record import IHaveNew, record_custom

from .asset_utils import dagster_name_fn
from .constants import (
    DEFAULT_SDF_WORKSPACE_ENVIRONMENT,
    SDF_INFORMATION_SCHEMA_TABLES_STAGE_COMPILE,
    SDF_INFORMATION_SCHEMA_TABLES_STAGE_PARSE,
    SDF_TARGET_DIR,
)
from .dagster_sdf_translator import DagsterSdfTranslator


def get_info_schema_dir(target_dir: Path, environment: str) -> Path:
    return target_dir.joinpath(
        SDF_TARGET_DIR, environment, "data", "system", "information_schema::sdf"
    )


@record_custom(checked=False)
class SdfInformationSchema(IHaveNew):
    """A class to represent the SDF information schema.

    The information schema is a set of tables that are generated by the sdf cli on compilation.
    It can be queried directly via the sdf cli, or by reading the parquet files that live in the
    `sdftarget` directory.

    This class specifically interfaces with the tables and columns tables, which contain metadata
    on their upstream and downstream dependencies, as well as their schemas, descriptions, classifiers,
    and other metadata.

    Read more about the information schema here: https://docs.sdf.com/reference/sdf-information-schema#sdf-information-schema

    Args:
        workspace_dir (Union[Path, str]): The path to the workspace directory.
        target_dir (Union[Path, str]): The path to the target directory.
        environment (str, optional): The environment to use. Defaults to "dbg".
    """

    information_schema_dir: Path
    information_schema: Dict[str, pl.DataFrame]

    def __new__(
        cls,
        workspace_dir: Union[Path, str],
        target_dir: Union[Path, str],
        environment: str = DEFAULT_SDF_WORKSPACE_ENVIRONMENT,
    ):
        check.inst_param(workspace_dir, "workspace_dir", (str, Path))
        check.inst_param(target_dir, "target_dir", (str, Path))
        check.str_param(environment, "environment")

        workspace_dir = Path(workspace_dir)
        target_dir = Path(target_dir)

        information_schema_dir = get_info_schema_dir(target_dir, environment)
        check.invariant(
            information_schema_dir.exists(),
            f"Information schema directory {information_schema_dir} does not exist.",
        )

        return super().__new__(
            cls,
            information_schema_dir=information_schema_dir,
            information_schema={},
        )

    def read_table(
        self,
        table_name: Literal["tables", "columns", "table_lineage", "column_lineage", "table_deps"],
    ) -> pl.DataFrame:
        check.invariant(
            table_name
            in SDF_INFORMATION_SCHEMA_TABLES_STAGE_COMPILE
            + SDF_INFORMATION_SCHEMA_TABLES_STAGE_PARSE,
            f"Table `{table_name}` is not valid information schema table."
            f" Select from one of {SDF_INFORMATION_SCHEMA_TABLES_STAGE_COMPILE + SDF_INFORMATION_SCHEMA_TABLES_STAGE_PARSE}.",
        )

        return self.information_schema.setdefault(
            table_name, pl.read_parquet(self.information_schema_dir.joinpath(table_name))
        )

    def build_sdf_multi_asset_args(
        self, io_manager_key: Optional[str], dagster_sdf_translator: DagsterSdfTranslator
    ) -> Tuple[Sequence[AssetDep], Dict[str, AssetOut], Dict[str, Set[AssetKey]]]:
        deps: Sequence[AssetDep] = []
        table_id_to_dep: Dict[str, AssetKey] = {}
        table_id_to_upstream: Dict[str, Set[AssetKey]] = {}
        outs: Dict[str, AssetOut] = {}
        internal_asset_deps: Dict[str, Set[AssetKey]] = {}

        # Step 0: Filter out system and external-system tables
        table_deps = self.read_table("table_deps").filter(
            ~pl.col("purpose").is_in(["system", "external-system"])
        )

        # Step 1: Build Dagster Asset Deps
        for table_row in table_deps.rows(named=True):
            # Iterate over the meta column to find the dagster-asset-key
            for meta_map in table_row["meta"]:
                # If the meta_map has a key of dagster-asset-key, add it to the deps
                if meta_map["keys"] == "dagster-asset-key":
                    dep_asset_key = meta_map["values"]
                    deps.append(AssetDep(asset=dep_asset_key))
                    table_id_to_dep[table_row["table_id"]] = AssetKey(dep_asset_key)
                elif meta_map["keys"] == "dagster-depends-on-asset-key":
                    dep_asset_key = meta_map["values"]
                    deps.append(AssetDep(asset=dep_asset_key))
                    # Currently, we only support one upstream asset
                    table_id_to_upstream.setdefault(table_row["table_id"], set()).add(
                        AssetKey(dep_asset_key)
                    )

        # Step 2: Build Dagster Asset Outs and Internal Asset Deps
        for table_row in table_deps.rows(named=True):
            asset_key = dagster_sdf_translator.get_asset_key(table_row["table_id"])
            output_name = dagster_name_fn(table_row["table_id"])
            # If the table is a annotated as a dependency, we don't need to create an output for it
            if table_row["table_id"] not in table_id_to_dep:
                outs[output_name] = AssetOut(
                    key=asset_key,
                    dagster_type=Nothing,
                    io_manager_key=io_manager_key,
                    description=table_row["description"],
                    is_required=False,
                )
                internal_asset_deps[output_name] = {
                    table_id_to_dep[dep]
                    if dep
                    in table_id_to_dep  # If the dep is a dagster dependency, use the meta asset key
                    else dagster_sdf_translator.get_asset_key(
                        dep
                    )  # Otherwise, use the translator to get the asset key
                    for dep in table_row["depends_on"]
                }.union(table_id_to_upstream.get(table_row["table_id"], set()))

        return deps, outs, internal_asset_deps

    def is_compiled(self) -> bool:
        for table in SDF_INFORMATION_SCHEMA_TABLES_STAGE_COMPILE:
            if not any(self.information_schema_dir.joinpath(table).iterdir()):
                return False
        return True

    def is_parsed(self) -> bool:
        for table in SDF_INFORMATION_SCHEMA_TABLES_STAGE_PARSE:
            if not any(self.information_schema_dir.joinpath(table).iterdir()):
                return False
        return True
