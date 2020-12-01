import datetime

import click
from tornado.ioloop import IOLoop

from dask.utils import format_bytes, format_time_ago
from distributed.cli.utils import check_python_3, install_signal_handlers, format_table
from distributed.deploy.discovery import (
    discover_clusters,
    list_discovery_methods,
    scale_cluster,
    delete_cluster,
)


loop = IOLoop.current()
install_signal_handlers(loop)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("discovery", type=str, required=False)
def list(discovery=None):
    async def _list():
        headers = [
            "Name",
            "Address",
            "Type",
            "Workers",
            "Threads",
            "Memory",
            "Created",
            "Status",
        ]
        output = []
        async for cluster in discover_clusters(discovery=discovery):

            threads = sum(
                w["nthreads"] for w in cluster.scheduler_info["workers"].values()
            )
            memory = format_bytes(
                sum(
                    [
                        w["memory_limit"]
                        for w in cluster.scheduler_info["workers"].values()
                    ]
                )
            )
            try:
                created = format_time_ago(
                    datetime.datetime.fromtimestamp(
                        float(cluster.scheduler_info["time_started"])
                    )
                )
            except KeyError:
                created = "Unknown"
            output.append(
                [
                    cluster.name,
                    cluster.scheduler_address,
                    type(cluster).__name__,
                    len(cluster.scheduler_info["workers"]),
                    threads,
                    memory,
                    created,
                    cluster.status.name.title(),
                ]
            )
        format_output(headers, output)

    loop.run_sync(lambda: _list())


@cli.command()
def list_discovery():
    async def _list_discovery():
        dm = list_discovery_methods()
        format_output(
            ["name", "package", "version", "path"],
            [[m, dm[m]["package"], dm[m]["version"], dm[m]["path"]] for m in dm],
        )

    loop.run_sync(lambda: _list_discovery())


@cli.command()
@click.argument("name")
@click.argument("n-workers", type=int)
def scale(name, n_workers):
    scale_cluster(name, n_workers)
    click.echo(f"Scaled cluster {name} to {n_workers} workers.")


@cli.command()
@click.argument("name")
def delete(name):
    try:
        delete_cluster(name)
    except Exception as e:
        click.echo(e)


def format_output(headers, output):
    click.echo(format_table(output, headers=[h.upper() for h in headers]))


def go():
    check_python_3()
    cli()


if __name__ == "__main__":
    go()
