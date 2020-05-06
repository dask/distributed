from distributed.deploy.old_ssh import SSHCluster
from distributed.cli.dask_worker import worker_options
from distributed.cli.dask_scheduler import scheduler_options
from distributed.cli.utils import prepare_dask_ssh_options
import click

from distributed.cli.utils import check_python_3

dask_ssh_options = [
    click.Option(
        ["--scheduler"],
        default=None,
        type=str,
        help="Specify scheduler node.  Defaults to first address.",
    ),
    click.Option(
        ["--scheduler-port"],
        default=8786,
        show_default=True,
        type=int,
        help="Specify scheduler port number.",
    ),
    click.Option(
        ["--nthreads"],
        default=0,
        type=int,
        help=(
            "Number of threads per worker process. "
            "Defaults to number of cores divided by the number of "
            "processes per host."
        ),
    ),
    click.Option(
        ["--nprocs"],
        default=1,
        show_default=True,
        type=int,
        help="Number of worker processes per host.",
    ),
    click.Option(
        ["--hostfile"],
        default=None,
        type=click.Path(exists=True),
        help="Textfile with hostnames/IP addresses",
    ),
    click.Option(
        ["--ssh-username"],
        default=None,
        type=str,
        help="Username to use when establishing SSH connections.",
    ),
    click.Option(
        ["--ssh-port"],
        default=22,
        type=int,
        show_default=True,
        help="Port to use for SSH connections.",
    ),
    click.Option(
        ["--ssh-private-key"],
        default=None,
        type=str,
        help="Private key file to use for SSH connections.",
    ),
    click.Option(
        ["--nohost"], is_flag=True, help="Do not pass the hostname to the worker."
    ),
    click.Option(
        ["--log-directory"],
        default=None,
        type=click.Path(exists=True),
        help=(
            "Directory to use on all cluster nodes for the output of "
            "dask-scheduler and dask-worker commands."
        ),
    ),
    click.Option(
        ["--local-directory"],
        default=None,
        type=click.Path(exists=True),
        help="Directory to use on all cluster nodes to place workers files.",
    ),
    click.Option(
        ["--remote-python"],
        default=None,
        type=str,
        help="Path to Python on remote nodes.",
    ),
    click.Option(
        ["--memory-limit"],
        default="auto",
        show_default=True,
        help="Bytes of memory that the worker can use. "
        "This can be an integer (bytes), "
        "float (fraction of total system memory), "
        "string (like 5GB or 5000M), "
        "'auto', or zero for no memory management",
    ),
    click.Option(
        ["--worker-port"],
        default=None,
        help="Serving computation port, defaults to random. "
        "When creating multiple workers with --nprocs, a sequential range of "
        "worker ports may be used by specifying the first and last available "
        "ports like <first-port>:<last-port>. For example, --worker-port=3000:3026 "
        "will use ports 3000, 3001, ..., 3025, 3026.",
    ),
    click.Option(
        ["--nanny-port"],
        default=None,
        help="Serving nanny port, defaults to random. "
        "When creating multiple nannies with --nprocs, a sequential range of "
        "nanny ports may be used by specifying the first and last available "
        "ports like <first-port>:<last-port>. For example, --nanny-port=3000:3026 "
        "will use ports 3000, 3001, ..., 3025, 3026.",
    ),
    click.Option(
        ["--remote-dask-worker"],
        default="distributed.cli.dask_worker",
        show_default=True,
        type=str,
        help="Worker to run.",
    ),
    click.Argument(["hostnames"], nargs=-1, type=str),
]

dask_ssh_opt_names = [
    opt.name for opt in dask_ssh_options if opt.param_type_name == "option"
]
ignored_options = ["version", "pid_file", "port"]
options_dict = []

for opt in scheduler_options:
    if opt.name in ignored_options:
        continue
    if opt.param_type_name == "option" and opt.name not in dask_ssh_opt_names:
        opt = prepare_dask_ssh_options("scheduler", opt)
        options_dict.append(opt)

for opt in worker_options:
    if opt.name in ignored_options:
        continue
    if opt.param_type_name == "option" and opt.name not in dask_ssh_opt_names:
        opt = prepare_dask_ssh_options("worker", opt)
        options_dict.append(opt)


class DaskSSHCommand(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = dask_ssh_options + options_dict
        self.help = (
            "Launch a distributed cluster over SSH. A 'dask-scheduler' process will run on the"
            "first host specified in [HOSTNAMES] or in the hostfile (unless --scheduler is specified"
            "explicitly). One or more 'dask-worker' processes will be run each host in [HOSTNAMES] or"
            "in the hostfile. Use command line flags to adjust how many dask-worker process are run on"
            "each host (--nprocs) and how many cpus are used by each dask-worker process (--nthreads)."
        )


@click.version_option()
@click.command(cls=DaskSSHCommand)
@click.pass_context
def main(
    ctx,
    scheduler,
    scheduler_port,
    hostnames,
    hostfile,
    nthreads,
    nprocs,
    ssh_username,
    ssh_port,
    ssh_private_key,
    nohost,
    log_directory,
    remote_python,
    memory_limit,
    worker_port,
    nanny_port,
    remote_dask_worker,
    local_directory,
    **kwargs,
):
    try:
        hostnames = list(hostnames)
        if hostfile:
            with open(hostfile) as f:
                hosts = f.read().split()
            hostnames.extend(hosts)

        if not scheduler:
            scheduler = hostnames[0]

    except IndexError:
        print(ctx.get_help())
        exit(1)

    c = SSHCluster(
        scheduler,
        scheduler_port,
        hostnames,
        nthreads,
        nprocs,
        ssh_username,
        ssh_port,
        ssh_private_key,
        nohost,
        log_directory,
        remote_python,
        memory_limit,
        worker_port,
        nanny_port,
        remote_dask_worker,
        local_directory,
        **kwargs,
    )

    import distributed

    print("\n---------------------------------------------------------------")
    print(
        "                 Dask.distributed v{version}\n".format(
            version=distributed.__version__
        )
    )
    print("Worker nodes:".format(n=len(hostnames)))
    for i, host in enumerate(hostnames):
        print("  {num}: {host}".format(num=i, host=host))
    print("\nscheduler node: {addr}:{port}".format(addr=scheduler, port=scheduler_port))
    print("---------------------------------------------------------------\n\n")

    # Monitor the output of remote processes.  This blocks until the user issues a KeyboardInterrupt.
    c.monitor_remote_processes()

    # Close down the remote processes and exit.
    print("\n[ dask-ssh ]: Shutting down remote processes (this may take a moment).")
    c.shutdown()
    print("[ dask-ssh ]: Remote processes have been terminated. Exiting.")


def go():
    check_python_3()
    main()


if __name__ == "__main__":
    go()
