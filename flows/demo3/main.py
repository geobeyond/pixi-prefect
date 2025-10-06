"""Prefect flows that demonstrate GPU acquisition using Prefect concurrency limits.

This example shows how GPU access can be managed in the prefect cluster.

It assumes that the prefect server has been set up with concurrency limits in
such a way as to have one concurrency limit per GPU per worker. For example,
if a worker has 2 GPUs, and a WORKER_ID of "worker-1", then the following
concurrency limits should be created in the Prefect server:

- gpu-worker-1-0 with a limit of 1
- gpu-worker-1-1 with a limit of 1

In order to run appropriately, the prefect worker must be also have the
following environment variables set:

- WORKER_ID: Unique identifier of the worker which was used when creating the
  concurrency limits. In the example above, this would be "worker-1".
- WORKER_GPU_IDS: Comma-separated list of GPU IDs that the worker has access to.
  In the example above, this would be "0,1".

The crucial part of this example is the `acquire_gpu()` context manager, which
tries to acquire one of the GPUs available on the worker by using Prefect's
concurrency limits. If a GPU is successfully acquired, its ID is yielded to
the caller, which can then use it to run GPU-intensive tasks. If no GPU is
available, an error is raised.

Note that this example does not implement any retry logic for acquiring
GPUs - since the logic for the GPU id acquisition makes use of the prefect
builtin concurrency limits feature, retrying can also be handled by prefect by
using the `retries` and `retry_delay_seconds` parameters of the `@task` decorator.

The example contains two flows that demonstrate different usage patterns:

- `task_acquisition_prediction_flow`: Each task acquires a GPU before running.
- `single_acquisition_prediction_flow`: A GPU is acquired at the start of the
  flow and used for all tasks.
"""

import contextlib
import logging
import os
import random
import time
from collections.abc import Generator

from prefect import flow, task
from prefect.concurrency.sync import concurrency
from prefect.concurrency.asyncio import (
    AcquireConcurrencySlotTimeoutError,
    ConcurrencySlotAcquisitionError,
)
from prefect.logging import get_run_logger


def _simulate_work(gpu_id, work_for_seconds: int, logger: logging.Logger) -> int:
    """Does not actually use any GPU, just simulates work by sleeping."""
    logger.info(
        f"Simulating GPU intensive work on GPU {gpu_id} "
        f"for {work_for_seconds} seconds..."
    )
    result = 0
    for _ in range(work_for_seconds):
        result += random.randint(0, 10)
        time.sleep(1)
    return result


def get_worker_gpu_ids() -> list[int]:
    if not (raw_gpu_ids := os.environ.get("WORKER_GPU_IDS", "")):
        raise ValueError("WORKER_GPU_IDS environment variable is not set.")
    return [int(id_) for id_ in raw_gpu_ids.split(",")]


@contextlib.contextmanager
def acquire_gpu(logger: logging.Logger) -> Generator[int, None, None]:
    """Try to acquire a GPU by using Prefect concurrency limits.

    If a GPU is acquired, its ID is yielded. If no GPU is available,
    an error is raised.

    This function does not implement any sort of retry
    logic intentionally, as it is expected that the caller prefect task will
    let prefect handle retries.
    """
    try:
        worker_id = os.environ["WORKER_ID"]
    except KeyError as err:
        raise ValueError(
            "WORKER_ID environment variable is not set - cannot acquire GPU"
        ) from err

    existing_gpu_ids = get_worker_gpu_ids()
    prefect_concurrency_limit_names = [
        (gpu_id, f"gpu-{worker_id}-{gpu_id}")
        for gpu_id in existing_gpu_ids
    ]
    logger.info(f"Worker {worker_id} has GPUs: {existing_gpu_ids}")
    logger.info(
        f"Prefect concurrency limit names to try to "
        f"acquire: {prefect_concurrency_limit_names}"
    )

    for gpu_id, prefect_limit_name in prefect_concurrency_limit_names:
        try:
            with concurrency(prefect_limit_name, occupy=1, timeout_seconds=0.1):
                logger.info(f"Acquired GPU {gpu_id} on worker {worker_id}")
                yield gpu_id
                logger.info(f"Released GPU {gpu_id} on worker {worker_id}")
                return
        except (AcquireConcurrencySlotTimeoutError, ConcurrencySlotAcquisitionError):
            logger.info(f"GPU {gpu_id} is busy, trying another one...")

    raise RuntimeError(f"No available GPUs on worker {worker_id}")


@task
def pre_processing(gpu_id: int, simulate_work_for_seconds: int = 30) -> int:
    """
    Simulate a pre-processing step on image.
    """
    logger = get_run_logger()
    logger.info(f"Started pre_processing on GPU {gpu_id}")
    result = _simulate_work(gpu_id, simulate_work_for_seconds, logger)
    logger.info(f"Finished pre_processing on GPU {gpu_id}")
    return result


@task
def predict(gpu_id: int, data: int, simulate_work_for_seconds: int = 30) -> str:
    """
    Simulate a Deep-Learning prediction on a specific GPU.
    """
    logger = get_run_logger()
    logger.info(f"Started prediction on GPU {gpu_id}")
    _simulate_work(gpu_id, simulate_work_for_seconds, logger)
    logger.info(f"Finished prediction on GPU {gpu_id}")
    return random.choice(['Cat', 'Dog', 'Sheep', 'Duck', 'Owl', 'Squirrel'])


@task
def post_processing(gpu_id: int, pred: str, simulate_work_for_seconds: int = 30) -> str:
    """
    Simulate a post-processing step on the DL predictions.
    """
    logger = get_run_logger()
    logger.info(f"Started post_processing on GPU {gpu_id}")
    _simulate_work(gpu_id, simulate_work_for_seconds, logger)
    logger.info(f"Finished post_processing on GPU {gpu_id}")
    print(pred.upper())


@flow()
def task_acquisition_prediction_flow():
    """Simulates usage pattern where each task acquires a GPU before running."""
    logger = get_run_logger()
    with acquire_gpu(logger) as gpu_id:
        data = pre_processing(gpu_id)
    with acquire_gpu(logger) as gpu_id:
        prediction = predict(gpu_id, data)
    with acquire_gpu(logger) as gpu_id:
        post_processing(gpu_id, prediction)


@flow(retries=3, retry_delay_seconds=[30, 60, 120, 240])
def single_acquisition_prediction_flow():
    """Simulates usage pattern where a GPU is acquired at the start of the
    flow and used for all tasks."""
    logger = get_run_logger()
    with acquire_gpu(logger) as gpu_id:
        data = pre_processing(gpu_id)
        prediction = predict(gpu_id, data)
        post_processing(gpu_id, prediction)
