"""

Taken from: https://github.com/rapidsai/dask-cuda/blob/branch-0.18/dask_cuda/benchmarks/local_cudf_merge.py

This is the cuDF merge benchmarking application. It has been 
modified to work for the MVAPICH2-based (http://mvapich.cse.ohio-state.edu) 
communication backend for the Dask Distributed library using the 
dask-mpi package.

"""

import math
from collections import defaultdict
from time import perf_counter as clock

import numpy

from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.distributed import Client, performance_report, wait
from dask.utils import format_bytes, format_time, parse_bytes
from dask_mpi import initialize

import argparse
import os

from dask.distributed import SSHCluster

#adjust as per compute system
GPUS_PER_NODE       =   1    # number of GPUs in the system  
DASK_INTERFACE      = 'ib0'  # interface to use for communication
THREADS_PER_NODE    =  28    # number of threads per node.

rank = os.environ['MV2_COMM_WORLD_LOCAL_RANK']
device_id = int(rank) % GPUS_PER_NODE
os.environ["CUDA_VISIBLE_DEVICES"]=str(device_id)

#from dask_cuda.local_cuda_cluster import LocalCUDACluster

#from dask_cuda import explicit_comms
#from utils import (
#    get_cluster_options,
#    get_scheduler_workers,
#    parse_benchmark_args,
#    setup_memory_pool,
#)

# Benchmarking cuDF merge operation based on
# <https://gist.github.com/rjzamora/0ffc35c19b5180ab04bbf7c793c45955>

def generate_chunk(i_chunk, local_size, num_chunks, chunk_type, frac_match, gpu):
    #print("generate_chunk")
    # Setting a seed that triggers max amount of comm in the two-GPU case.
    if gpu:
        import cupy as xp

        import cudf as xdf
    else:
        import numpy as xp
        import pandas as xdf

    xp.random.seed(2 ** 32 - 1)

    chunk_type = chunk_type or "build"
    frac_match = frac_match or 1.0
    if chunk_type == "build":
        # Build dataframe
        #
        # "key" column is a unique sample within [0, local_size * num_chunks)
        #
        # "shuffle" column is a random selection of partitions (used for shuffle)
        #
        # "payload" column is a random permutation of the chunk_size

        start = local_size * i_chunk
        stop = start + local_size

        parts_array = xp.arange(num_chunks, dtype="int64")
        suffle_array = xp.repeat(parts_array, math.ceil(local_size / num_chunks))
        
        df = xdf.DataFrame(
            {
                "key": xp.arange(start, stop=stop, dtype="int64"),
                "shuffle": xp.random.permutation(suffle_array)[:local_size],
                "payload": xp.random.permutation(xp.arange(local_size, dtype="int64")),
            }
        )
    else:
        #print("chunk type is other")
        # Other dataframe
        #
        # "key" column matches values from the build dataframe
        # for a fraction (`frac_match`) of the entries. The matching
        # entries are perfectly balanced across each partition of the
        # "base" dataframe.
        #
        # "payload" column is a random permutation of the chunk_size

        # Step 1. Choose values that DO match
        sub_local_size = local_size // num_chunks
        sub_local_size_use = max(int(sub_local_size * frac_match), 1)
        arrays = []
        for i in range(num_chunks):
            bgn = (local_size * i) + (sub_local_size * i_chunk)
            end = bgn + sub_local_size
            ar = xp.arange(bgn, stop=end, dtype="int64")
            arrays.append(xp.random.permutation(ar)[:sub_local_size_use])
        key_array_match = xp.concatenate(tuple(arrays), axis=0)

        # Step 2. Add values that DON'T match
        missing_size = local_size - key_array_match.shape[0]
        start = local_size * num_chunks + local_size * i_chunk
        stop = start + missing_size
        key_array_no_match = xp.arange(start, stop=stop, dtype="int64")

        # Step 3. Combine and create the final dataframe chunk (dask_cudf partition)
        key_array_combine = xp.concatenate(
            (key_array_match, key_array_no_match), axis=0
        )

        df = xdf.DataFrame(
            {
                "key": xp.random.permutation(key_array_combine),
                "payload": xp.random.permutation(xp.arange(local_size, dtype="int64")),
            }
        )
    return df


def get_random_ddf(chunk_size, num_chunks, frac_match, chunk_type, args):

    parts = [chunk_size for i in range(num_chunks)]
    device_type = True if args.type == "gpu" else False
    meta = generate_chunk(0, 4, 1, chunk_type, None, device_type)
    divisions = [None] * (len(parts) + 1)

    name = "generate-data-" + tokenize(chunk_size, num_chunks, frac_match, chunk_type)

    graph = {
        (name, i): (
            generate_chunk,
            i,
            part,
            len(parts),
            chunk_type,
            frac_match,
            device_type,
        )
        for i, part in enumerate(parts)
    }

    ddf = new_dd_object(graph, name, meta, divisions)

    if chunk_type == "build":
        if not args.no_shuffle:
            divisions = [i for i in range(num_chunks)] + [num_chunks]
            return ddf.set_index("shuffle", divisions=tuple(divisions))
        else:
            del ddf["shuffle"]

    return ddf


def merge(args, ddf1, ddf2, write_profile):
    #print("merge called")
    # Lazy merge/join operation
    ddf_join = ddf1.merge(ddf2, on=["key"], how="inner")
    if args.set_index:
        ddf_join = ddf_join.set_index("key")

    # Execute the operations to benchmark
    if write_profile is not None:
        with performance_report(filename=args.profile):
            t1 = clock()
            wait(ddf_join.persist())
            took = clock() - t1
    else:
        t1 = clock()
        wait(ddf_join.persist())
        took = clock() - t1
    return took


#def merge_explicit_comms(args, ddf1, ddf2):
#    t1 = clock()
#    wait(explicit_comms.dataframe_merge(ddf1, ddf2, on="key").persist())
#    took = clock() - t1
#    return took


def run(client, args, n_workers, write_profile=None):
    # Generate random Dask dataframes
    ddf_base = get_random_ddf(
        args.chunk_size, n_workers, args.frac_match, "build", args
    ).persist()

    ddf_other = get_random_ddf(
        args.chunk_size, n_workers, args.frac_match, "other", args
    ).persist()

    wait(ddf_base)
    wait(ddf_other)
    client.wait_for_workers(n_workers)

    assert len(ddf_base.dtypes) == 2
    assert len(ddf_other.dtypes) == 2
    data_processed = len(ddf_base) * sum([t.itemsize for t in ddf_base.dtypes])
    data_processed += len(ddf_other) * sum([t.itemsize for t in ddf_other.dtypes])

    if args.backend == "dask":
        took = merge(args, ddf_base, ddf_other, write_profile)
    else:
        took = None#merge_explicit_comms(args, ddf_base, ddf_other)

    return (data_processed, took)


def main(args):
    cluster_options = get_cluster_options(args)
    Cluster = cluster_options["class"]
    cluster_args = cluster_options["args"]
    cluster_kwargs = cluster_options["kwargs"]
    scheduler_addr = cluster_options["scheduler_addr"]

    if args.sched_addr:
        initialize(
            interface=DASK_INTERFACE,
            protocol=args.protocol,
            nanny=False,
            nthreads=THREADS_PER_NODE
        )
        import time
        time.sleep(5)

        client = Client()
        print(client)
    else:
        cluster = Cluster(*cluster_args, **cluster_kwargs)
        if args.multi_node:
            import time

            # Allow some time for workers to start and connect to scheduler
            # TODO: make this a command-line argument?
            time.sleep(15)

        client = Client(scheduler_addr if args.multi_node else cluster)

    if args.type == "gpu":
        client.run(setup_memory_pool, disable_pool=args.no_rmm_pool)
        # Create an RMM pool on the scheduler due to occasional deserialization
        # of CUDA objects. May cause issues with InfiniBand otherwise.
        client.run_on_scheduler(setup_memory_pool, 1e9, disable_pool=args.no_rmm_pool)

    scheduler_workers = client.run_on_scheduler(get_scheduler_workers)
    n_workers = len(scheduler_workers)

    took_list = []
    for _ in range(args.runs - 1):
        took_list.append(run(client, args, n_workers, write_profile=None))
    took_list.append(
        run(client, args, n_workers, write_profile=args.profile)
    )  # Only profiling the last run

    # Collect, aggregate, and print peer-to-peer bandwidths
    incoming_logs = client.run(lambda dask_worker: dask_worker.incoming_transfer_log)
    bandwidths = defaultdict(list)
    total_nbytes = defaultdict(list)
    for k, L in incoming_logs.items():
        for d in L:
            if d["total"] >= args.ignore_size:
                bandwidths[k, d["who"]].append(d["bandwidth"])
                total_nbytes[k, d["who"]].append(d["total"])
    bandwidths = {
        (scheduler_workers[w1].name, scheduler_workers[w2].name): [
            "%s/s" % format_bytes(x) for x in numpy.quantile(v, [0.25, 0.50, 0.75])
        ]
        for (w1, w2), v in bandwidths.items()
    }
    total_nbytes = {
        (scheduler_workers[w1].name, scheduler_workers[w2].name,): format_bytes(sum(nb))
        for (w1, w2), nb in total_nbytes.items()
    }

    t_runs = numpy.empty(len(took_list))
    if args.markdown:
        print("```")
    print("Merge benchmark")
    print("-------------------------------")
    print(f"backend        | {args.backend}")
    print(f"merge type     | {args.type}")
    print(f"rows-per-chunk | {args.chunk_size}")
    print(f"protocol       | {args.protocol}")
    print(f"device(s)      | {args.devs}")
    print(f"rmm-pool       | {(not args.no_rmm_pool)}")
    print(f"frac-match     | {args.frac_match}")
    if args.protocol == "ucx":
        print(f"tcp            | {args.enable_tcp_over_ucx}")
        print(f"ib             | {args.enable_infiniband}")
        print(f"nvlink         | {args.enable_nvlink}")
    print(f"data-processed | {format_bytes(took_list[0][0])}")
    print("===============================")
    print("Wall-clock     | Throughput")
    print("-------------------------------")
    for idx, (data_processed, took) in enumerate(took_list):
        throughput = int(data_processed / took)
        m = format_time(took)
        m += " " * (15 - len(m))
        print(f"{m}| {format_bytes(throughput)}/s")
        t_runs[idx] = float(format_bytes(throughput).split(" ")[0])
    print("===============================")
    if args.markdown:
        print("\n```")

    #if args.plot is not None:
    #    plot_benchmark(t_runs, args.plot, historical=True)

    if args.backend == "dask":
        if args.markdown:
            print("<details>\n<summary>Worker-Worker Transfer Rates</summary>\n\n```")
        print("(w1,w2)     | 25% 50% 75% (total nbytes)")
        print("-------------------------------")
        for (d1, d2), bw in sorted(bandwidths.items()):
            fmt = (
                "(%s,%s)     | %s %s %s (%s)"
                if args.multi_node or args.sched_addr
                else "(%02d,%02d)     | %s %s %s (%s)"
            )
            print(fmt % (d1, d2, bw[0], bw[1], bw[2], total_nbytes[(d1, d2)]))
        if args.markdown:
            print("```\n</details>\n")

    if args.multi_node:
        client.shutdown()
        client.close()


def parse_args():
    special_args = [
        {
            "name": ["-b", "--backend",],
            "choices": ["dask", "explicit-comms"],
            "default": "dask",
            "type": str,
            "help": "The backend to use.",
        },
        {
            "name": ["-t", "--type",],
            "choices": ["cpu", "gpu"],
            "default": "gpu",
            "type": str,
            "help": "Do merge with GPU or CPU dataframes",
        },
        {
            "name": ["-c", "--chunk-size",],
            "default": 1_000_000,
            "metavar": "n",
            "type": int,
            "help": "Chunk size (default 1_000_000)",
        },
        {
            "name": "--ignore-size",
            "default": "1 MiB",
            "metavar": "nbytes",
            "type": parse_bytes,
            "help": "Ignore messages smaller than this (default '1 MB')",
        },
        {
            "name": "--frac-match",
            "default": 0.3,
            "type": float,
            "help": "Fraction of rows that matches (default 0.3)",
        },
        {
            "name": "--no-shuffle",
            "action": "store_true",
            "help": "Don't shuffle the keys of the left (base) dataframe.",
        },
        {
            "name": "--markdown",
            "action": "store_true",
            "help": "Write output as markdown",
        },
        {"name": "--runs", "default": 3, "type": int, "help": "Number of runs",},
        {
            "name": ["-s", "--set-index",],
            "action": "store_true",
            "help": "Call set_index on the key column to sort the joined dataframe.",
        },
    ]

    return parse_benchmark_args(
        description="Distributed merge (dask/cudf) benchmark", args_list=special_args
    )

def parse_benchmark_args(description="Generic dask-cuda Benchmark", args_list=[]):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-d", "--devs", default="0", type=str, help='GPU devices to use (default "0").'
    )
    parser.add_argument(
        "-p",
        "--protocol",
        choices=["tcp", "ucx", "mpi"],
        default="tcp",
        type=str,
        help="The communication protocol to use.",
    )
    parser.add_argument(
        "--profile",
        metavar="PATH",
        default=None,
        type=str,
        help="Write dask profile report (E.g. dask-report.html)",
    )
    parser.add_argument(
        "--no-rmm-pool", action="store_true", help="Disable the RMM memory pool"
    )
    parser.add_argument(
        "--enable-tcp-over-ucx",
        action="store_true",
        dest="enable_tcp_over_ucx",
        help="Enable tcp over ucx.",
    )
    parser.add_argument(
        "--enable-infiniband",
        action="store_true",
        dest="enable_infiniband",
        help="Enable infiniband over ucx.",
    )
    parser.add_argument(
        "--enable-nvlink",
        action="store_true",
        dest="enable_nvlink",
        help="Enable NVLink over ucx.",
    )
    parser.add_argument(
        "--disable-tcp-over-ucx",
        action="store_false",
        dest="enable_tcp_over_ucx",
        help="Disable tcp over ucx.",
    )
    parser.add_argument(
        "--disable-infiniband",
        action="store_false",
        dest="enable_infiniband",
        help="Disable infiniband over ucx.",
    )
    parser.add_argument(
        "--disable-nvlink",
        action="store_false",
        dest="enable_nvlink",
        help="Disable NVLink over ucx.",
    )
    parser.add_argument(
        "--ucx-net-devices",
        default=None,
        type=str,
        help="The device to be used for UCX communication, or 'auto'. "
        "Ignored if protocol is 'tcp'",
    )
    parser.add_argument(
        "--single-node",
        action="store_true",
        dest="multi_node",
        help="Runs a single-node cluster on the current host.",
    )
    parser.add_argument(
        "--multi-node",
        action="store_true",
        dest="multi_node",
        help="Runs a multi-node cluster on the hosts specified by --hosts.",
    )
    parser.add_argument(
        "--scheduler-address",
        default="Not Needed", # MPI4Dask related modification
        type=str,
        dest="sched_addr",
        help="Scheduler Address -- assumes cluster is created outside of benchmark.",
    )
    parser.add_argument(
        "--hosts",
        default=None,
        type=str,
        help="Specifies a comma-separated list of IP addresses or hostnames. "
        "The list begins with the host where the scheduler will be launched "
        "followed by any number of workers, with a minimum of 1 worker. "
        "Requires --multi-node, ignored otherwise. "
        "Usage example: --multi-node --hosts 'dgx12,dgx12,10.10.10.10,dgx13' . "
        "In the example, the benchmark is launched with scheduler on host "
        "'dgx12' (first in the list), and workers on three hosts being 'dgx12', "
        "'10.10.10.10', and 'dgx13'. "
        "Note: --devs is currently ignored in multi-node mode and for each host "
        "one worker per GPU will be launched.",
    )

    for args in args_list:
        name = args.pop("name")
        if not isinstance(name, list):
            name = [name]
        parser.add_argument(*name, **args)

    parser.set_defaults(
        enable_tcp_over_ucx=True, enable_infiniband=True, enable_nvlink=True
    )
    args = parser.parse_args()

    if args.protocol == "tcp":
        args.enable_tcp_over_ucx = False
        args.enable_infiniband = False
        args.enable_nvlink = False

    if args.multi_node and len(args.hosts.split(",")) < 2:
        raise ValueError("--multi-node requires at least 2 hosts")

    return args


def get_cluster_options(args):
    #print("get_cluster_options")

    if args.multi_node is True:

        #print("get_cluster_options: args.multi_node")
        
        Cluster = SSHCluster
        cluster_args = [args.hosts.split(",")]
        scheduler_addr = args.protocol + "://" + cluster_args[0][0] + ":8786"

        worker_options = {}

        # This looks counterintuitive but adding the variable name with
        # an empty string is how we can enable CLI booleans currently,
        # note that SSHCluster uses the dask-cuda-worker CLI.
        if args.enable_tcp_over_ucx:
            worker_options["enable_tcp_over_ucx"] = ""
        if args.enable_nvlink:
            worker_options["enable_nvlink"] = ""
        if args.enable_infiniband:
            worker_options["enable_infiniband"] = ""

        if args.ucx_net_devices:
            worker_options["ucx_net_devices"] = args.ucx_net_devices

        cluster_kwargs = {
            "connect_options": {"known_hosts": None},
            "scheduler_options": {"protocol": args.protocol},
            "worker_module": "dask_cuda.dask_cuda_worker",
            "worker_options": worker_options,
            # "n_workers": len(args.devs.split(",")),
            # "CUDA_VISIBLE_DEVICES": args.devs,
        }
    else:
        #print("get_cluster_options: else")
        #Cluster = LocalCUDACluster
        Cluster = None
        scheduler_addr = None
        cluster_args = []
        cluster_kwargs = {
            "protocol": args.protocol,
            "n_workers": len(args.devs.split(",")),
            "CUDA_VISIBLE_DEVICES": args.devs,
            "ucx_net_devices": args.ucx_net_devices,
            "enable_tcp_over_ucx": args.enable_tcp_over_ucx,
            "enable_infiniband": args.enable_infiniband,
            "enable_nvlink": args.enable_nvlink,
        }

    #print("returning cluster object")
    return {
        "class": Cluster,
        "args": cluster_args,
        "kwargs": cluster_kwargs,
        "scheduler_addr": scheduler_addr,
    }


def get_scheduler_workers(dask_scheduler=None):
    return dask_scheduler.workers


def setup_memory_pool(pool_size=None, disable_pool=False):
    import cupy

    os.environ['RMM_NO_INITIALIZE'] = 'True'
    import rmm

    rmm.reinitialize(
        pool_allocator=not disable_pool, devices=0, initial_pool_size=pool_size,
    )
    cupy.cuda.set_allocator(rmm.rmm_cupy_allocator)

if __name__ == "__main__":
    main(parse_args())

