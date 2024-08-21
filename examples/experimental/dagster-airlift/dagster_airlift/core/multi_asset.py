from dataclasses import dataclass
from typing import Callable, List, Optional

from dagster import AssetSpec, Definitions, multi_asset

from .def_factory import DefsFactory


@dataclass
class PythonDefs(DefsFactory):
    specs: List[AssetSpec]
    name: str
    group: Optional[str] = None
    python_fn: Optional[Callable] = None

    def build_defs(self) -> Definitions:
        @multi_asset(
            specs=self.specs,
            name=self.name,
            group_name=self.group,
        )
        def _multi_asset():
            if self.python_fn:
                self.python_fn()

        return Definitions(assets=[_multi_asset])


def defs_from_python_task(
    *,
    task_id: str,
    dag_id: str,
    assets: List[AssetSpec],
    python_fn: Optional[Callable] = None,
    group: Optional[str] = None,
) -> Definitions:
    return PythonDefs(
        specs=assets, name=f"{dag_id}__{task_id}", group=group, python_fn=python_fn
    ).build_defs()
