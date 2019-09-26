import asyncio
import click
import json
import yaml

from distributed.deploy.spec import run_workers


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("scheduler", type=str, required=False)
@click.option("--text", type=str, default="", help="")
@click.option("--file", type=str, default=None, help="")
@click.version_option()
def main(scheduler: str, text: str, file: str):
    spec = {}
    if file:
        with open(file) as f:
            spec.update(yaml.safe_load(f))

    if text:
        spec.update(json.loads(text))

    if "cls" in spec:  # single worker spec
        spec = {spec["opts"].get("name", 0): spec}

    async def run():
        workers = await run_workers(scheduler, spec)
        try:
            await asyncio.gather(*[w.finished() for w in workers.values()])
        except KeyboardInterrupt:
            await asyncio.gather(*[w.close() for w in workers.values()])

    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    main()
