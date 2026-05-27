"""One sensor that evaluates every asset's AutomationCondition.

Drives the whole graph: ``on_cron("@monthly")`` refreshes the raw roots on the
1st, and ``eager()`` cascades the refresh through silver -> gold -> report.
"""

import dagster as dg


@dg.definitions
def automation() -> dg.Definitions:
    return dg.Definitions(
        sensors=[
            dg.AutomationConditionSensorDefinition(
                "default_automation_condition_sensor",
                target=dg.AssetSelection.all(),
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
        ]
    )
