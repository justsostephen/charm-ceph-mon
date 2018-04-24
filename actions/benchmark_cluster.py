#!/usr/bin/env python3

"""
# benchmark_cluster.py

Benchmark a storage cluster using `rados bench`.

Copyright 2018 Canonical Ltd

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from subprocess import (
    CalledProcessError,
    check_call,
    check_output,
)
import sys

sys.path.append("hooks")

from charmhelpers.contrib.storage.linux.ceph import pool_exists
from charmhelpers.core.hookenv import (
    action_fail,
    action_get,
    action_set,
    log,
)


def benchmark_storage_cluster(pool, benchmark_mode, benchmark_duration,
                              threads, object_size):
    """Call `rados bench`."""
    command = ["rados", "bench", str(benchmark_duration), benchmark_mode,
               "-p", pool, "-t", str(threads)]
    # Specifying object size is only valid in write mode.  Clean up is handled
    # by `clean_up_data()`.
    if benchmark_mode == "write":
        command += ["-b", str(object_size), "--no-cleanup"]
    try:
        benchmark_results = check_output(command, universal_newlines=True)
    except CalledProcessError as error:
        message = ("Benchmarking failed with the following error: {}"
                   .format(error))
        action_fail(message)
        log("ceph-mon benchmark-cluster: {}".format(message))
        if benchmark_mode != "write":
            action_set({"note": "Read benchmarks will fail if a write "
                                "benchmark is not performed first."})
    else:
        message = "{} benchmark successfully completed.".format(benchmark_mode)
        action_set({"mode": message, "statistics": benchmark_results})
        log("ceph-mon benchmark-cluster: {}".format(message))


def clean_up_data(pool):
    """Call `rados cleanup`."""
    command = ["rados", "cleanup", "-p", pool]
    try:
        check_call(command)
    except CalledProcessError as error:
        message = "Cleanup failed with the following error: {}".format(error)
        action_fail(message)
        log("ceph-mon benchmark-cluster: {}".format(message))
    else:
        message = "Benchmark data successfully removed."
        action_set({"cleanup": message})
        log("ceph-mon benchmark-cluster: {}".format(message))


def main():
    """Benchmark cluster and/or clean up data."""
    action_params = action_get()
    pool = action_params["pool"]  # type: str
    if "mode" in action_params:
        benchmark_mode = action_params["mode"]  # type: str
    else:
        benchmark_mode = None
    benchmark_duration = action_params["seconds"]  # type: int
    threads = action_params["threads"]  # type: int
    object_size = action_params["size"]  # type: int
    clean_up_write_data = action_params["cleanup"]  # type: bool

    if pool_exists(service="admin", name=pool):
        if benchmark_mode is not None:
            benchmark_storage_cluster(
                pool, benchmark_mode, benchmark_duration, threads, object_size
            )
            if not clean_up_write_data:
                action_set({"hint": "Run this action with `cleanup=true` to "
                                    "remove written benchmark data."})
        if clean_up_write_data:
            clean_up_data(pool)
        if benchmark_mode is None and not clean_up_write_data:
            message = ('"mode" must be specified and/or "cleanup" must be '
                       '"true" in order to perform an operation.')
            action_fail(message)
            log("ceph-mon benchmark-cluster: {}".format(message))
    else:
        message = 'Pool "{}" does not exist.'.format(pool)
        action_fail('{} You can create a pool for benchmarking purposes with '
                    'the "create-pool" action.'.format(message))
        log("ceph-mon benchmark-cluster: {}".format(message))


if __name__ == "__main__":
    main()
