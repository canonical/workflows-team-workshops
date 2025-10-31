#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Sample Temporal Worker."""

import asyncio
import dataclasses
import datetime
import logging
import os

import concurrent.futures

import temporalio.activity
import temporalio.client
import temporalio.runtime
import temporalio.worker

logger = logging.getLogger(__name__)


class CustomInterceptor(temporalio.worker.Interceptor):
    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return CustomScheduleToStartInterceptor(next)


class CustomScheduleToStartInterceptor(temporalio.worker.ActivityInboundInterceptor):
    async def execute_activity(self, input: temporalio.worker.ExecuteActivityInput):
        schedule_to_start = (
            temporalio.activity.info().started_time
            - temporalio.activity.info().current_attempt_scheduled_time
        )
        meter = temporalio.activity.metric_meter()
        histogram = meter.create_histogram_timedelta(
            "custom_activity_schedule_to_start_latency",
            description="Time between activity scheduling and start",
            unit="duration",
        )
        histogram.record(
            schedule_to_start, {"workflow_type": temporalio.activity.info().workflow_type}
        )
        return await self.next.execute_activity(input)


@dataclasses.dataclass
class HelloWorldInput:
    greeted: str


# Basic activity that logs and does string concatenation
@temporalio.activity.defn(name="compose_hello_world")
async def compose_hello_world(arg: HelloWorldInput) -> str:
    temporalio.activity.logger.info("Running activity with parameter %s" % arg)

    return f"Hello world to {arg.greeted} in python!"


# Basic workflow that logs and invokes an activity
@temporalio.workflow.defn(name="HelloWorldWorkflow")
class HelloWorldWorkflow:
    @temporalio.workflow.run
    async def run(self, name: str) -> str:
        temporalio.workflow.logger.info("Running HelloWorld workflow with parameter %s" % name)
        return await temporalio.workflow.execute_activity(
            compose_hello_world,
            HelloWorldInput(greeted=name),
            start_to_close_timeout=datetime.timedelta(seconds=10),
        )


async def run_worker():
    """Connect Temporal worker to Temporal server."""
    target_host = os.environ.get("TEMPORAL_HOST")
    namespace = os.environ.get("TEMPORAL_NAMESPACE")
    task_queue = os.environ.get("TEMPORAL_QUEUE")

    runtime = temporalio.runtime.Runtime(
        telemetry=temporalio.runtime.TelemetryConfig(
            metrics=temporalio.runtime.PrometheusConfig(bind_address="0.0.0.0:9000")
        )
    )

    client = await temporalio.client.Client.connect(
        target_host, namespace=namespace, runtime=runtime
    )

    worker = temporalio.worker.Worker(
        client=client,
        workflows=[HelloWorldWorkflow],
        activities=[compose_hello_world],
        activity_executor=concurrent.futures.ThreadPoolExecutor(1),
        max_concurrent_activities=1,
        task_queue=task_queue,
        interceptors=[CustomInterceptor()],
    )

    await worker.run()


if __name__ == "__main__":  # pragma: nocover
    asyncio.run(run_worker())
