from typing import TYPE_CHECKING, Optional

from dagster._core.definitions.auto_materialize_rule import AutoMaterializeRule
from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
    AutomationResult,
)
from dagster._record import record
from dagster._serdes.serdes import whitelist_for_serdes
from dagster._utils.security import non_secure_md5_hash_str

if TYPE_CHECKING:
    from ..automation_context import AutomationContext


@whitelist_for_serdes
@record
class RuleCondition(AutomationCondition):
    """This class represents the condition that a particular AutoMaterializeRule is satisfied."""

    rule: AutoMaterializeRule
    label: Optional[str] = None

    def get_unique_id(self, *, parent_unique_id: Optional[str], index: Optional[str]) -> str:
        # preserves old (bad) behavior of not including the parent_unique_id to avoid inavlidating
        # old serialized information
        parts = [self.rule.__class__.__name__, self.description]
        return non_secure_md5_hash_str("".join(parts).encode())

    @property
    def description(self) -> str:
        return self.rule.description

    def evaluate(self, context: "AutomationContext") -> AutomationResult:
        context.log.debug(f"Evaluating rule: {self.rule.to_snapshot()}")
        # Allow for access to legacy context in legacy rule evaluation
        evaluation_result = self.rule.evaluate_for_asset(context)
        context.log.debug(
            f"Rule returned {evaluation_result.true_subset.size} partitions "
            f"({evaluation_result.end_timestamp - evaluation_result.start_timestamp:.2f} seconds)"
        )
        return evaluation_result
