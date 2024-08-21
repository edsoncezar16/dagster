from typing import Optional

from dagster import (
    AssetsDefinition,
    _check as check,
)

MIGRATED_TAG = "airlift/task_migrated"
DAG_ID_TAG = "airlift/dag_id"
TASK_ID_TAG = "airlift/task_id"


def get_task_id_from_asset(assets_def: AssetsDefinition) -> Optional[str]:
    # First, we attempt to infer the task_id from the tags fo the asset.
    task_id_from_tags = None
    if any(TASK_ID_TAG in spec.tags for spec in assets_def.specs):
        task_id = None
        for spec in assets_def.specs:
            if task_id is None:
                task_id = spec.tags[TASK_ID_TAG]
            else:
                if spec.tags.get(TASK_ID_TAG) is None:
                    check.failed(
                        f"Missing {TASK_ID_TAG} tag in spec {spec.key} for {assets_def.node_def.name}"
                    )
                check.invariant(
                    task_id == spec.tags[TASK_ID_TAG],
                    f"Task ID mismatch within same AssetsDefinition: {task_id} != {spec.tags[TASK_ID_TAG]}",
                )
        task_id_from_tags = task_id
    # Then, check for the airlift/task_id tag within op tags.
    task_id_from_op_tags = None
    if assets_def.node_def.tags and TASK_ID_TAG in assets_def.node_def.tags:
        task_id_from_op_tags = assets_def.node_def.tags[TASK_ID_TAG]
    # Finally, check within the name of the node_def.
    task_id_from_name = None
    if len(assets_def.node_def.name.split("__")) == 2:
        task_id_from_name = assets_def.node_def.name.split("__")[1]
    # If we have multiple task_ids, we check if they are all the same.
    if task_id_from_tags and task_id_from_op_tags:
        check.invariant(
            task_id_from_tags == task_id_from_op_tags,
            f"Task ID mismatch between asset tags and op tags: {task_id_from_tags} != {task_id_from_op_tags}",
        )
    if task_id_from_tags and task_id_from_name:
        check.invariant(
            task_id_from_tags == task_id_from_name,
            f"Task ID mismatch between tags and name: {task_id_from_tags} != {task_id_from_name}",
        )
    if task_id_from_op_tags and task_id_from_name:
        check.invariant(
            task_id_from_op_tags == task_id_from_name,
            f"Task ID mismatch between op tags and name: {task_id_from_op_tags} != {task_id_from_name}",
        )
    # If we have a task_id, we return it.
    return task_id_from_tags or task_id_from_op_tags or task_id_from_name


def get_dag_id_from_asset(assets_def: AssetsDefinition) -> Optional[str]:
    dag_id_from_tags = None
    if any(DAG_ID_TAG in spec.tags for spec in assets_def.specs):
        dag_id = None
        for spec in assets_def.specs:
            if dag_id is None:
                dag_id = spec.tags[DAG_ID_TAG]
            else:
                if spec.tags.get(DAG_ID_TAG) is None:
                    check.failed(
                        f"Missing {DAG_ID_TAG} tag in spec {spec.key} for {assets_def.node_def.name}"
                    )
                check.invariant(
                    dag_id == spec.tags[DAG_ID_TAG],
                    f"Task ID mismatch within same AssetsDefinition: {dag_id} != {spec.tags[DAG_ID_TAG]}",
                )
        dag_id_from_tags = dag_id
    dag_id_from_op_tags = None
    if assets_def.node_def.tags and DAG_ID_TAG in assets_def.node_def.tags:
        dag_id_from_op_tags = assets_def.node_def.tags[DAG_ID_TAG]
    dag_id_from_name = None
    if len(assets_def.node_def.name.split("__")) == 2:
        dag_id_from_name = assets_def.node_def.name.split("__")[0]
    if dag_id_from_tags and dag_id_from_op_tags:
        check.invariant(
            dag_id_from_tags == dag_id_from_op_tags,
            f"Dag ID mismatch between asset tags and op tags: {dag_id_from_tags} != {dag_id_from_op_tags}",
        )
    if dag_id_from_tags and dag_id_from_name:
        check.invariant(
            dag_id_from_tags == dag_id_from_name,
            f"Dag ID mismatch between tags and name: {dag_id_from_tags} != {dag_id_from_name}",
        )
    if dag_id_from_op_tags and dag_id_from_name:
        check.invariant(
            dag_id_from_op_tags == dag_id_from_name,
            f"Dag ID mismatch between op tags and name: {dag_id_from_op_tags} != {dag_id_from_name}",
        )
    return dag_id_from_tags or dag_id_from_op_tags or dag_id_from_name
