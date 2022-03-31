from __future__ import annotations

import argparse
import html
import io
import os
import re
import shelve
import zipfile
from collections.abc import Iterator
from typing import Any

import altair
import altair_saver
import junitparser
import pandas
import requests

TOKEN = os.environ.get("GITHUB_TOKEN")

# Mapping between a symbol (pass, fail, skip) and a color
COLORS = {
    "✓": "#acf2a5",
    "x": "#f2a5a5",
    "s": "#f2ef8f",
}


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--repo",
        default="dask/distributed",
        help="github repository",
    )
    parser.add_argument(
        "--branch",
        default="main",
        help="git branch",
    )
    parser.add_argument(
        "--events",
        nargs="+",
        default=["push", "schedule"],
        help="github events",
    )
    parser.add_argument(
        "--days",
        "-d",
        type=int,
        default=90,
        help="Number of days to look back from now",
    )
    parser.add_argument(
        "--nfails",
        "-n",
        type=int,
        default=1,
        help="Show test if it failed more than this many times",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="test_report.html",
        help="Output file name",
    )
    return parser.parse_args(argv)


def get_from_github(url: str, params: dict[str, Any]) -> requests.Response:
    """
    Make an authenticated request to the GitHub REST API.
    """
    r = requests.get(url, params=params, headers={"Authorization": f"token {TOKEN}"})
    r.raise_for_status()
    return r


def maybe_get_next_page_path(response: requests.Response) -> str | None:
    """
    If a response is paginated, get the url for the next page.
    """
    link_regex = re.compile(r'<([^>]*)>;\s*rel="([\w]*)\"')
    link_headers = response.headers.get("Link")
    next_page_path = None
    if link_headers:
        links = {}
        matched = link_regex.findall(link_headers)
        for match in matched:
            links[match[1]] = match[0]
        next_page_path = links.get("next", None)

    return next_page_path


def get_workflow_listing(repo: str, branch: str, event: str, days: int):
    """
    Get a list of workflow runs from GitHub actions.
    """
    since = (pandas.Timestamp.now(tz="UTC") - pandas.Timedelta(days=days)).date()
    params = {"per_page": 100, "branch": branch, "event": event, "created": f">{since}"}
    r = get_from_github(
        f"https://api.github.com/repos/{repo}/actions/runs", params=params
    )
    workflows = r.json()["workflow_runs"]
    next_page = maybe_get_next_page_path(r)
    while next_page:
        r = get_from_github(next_page, params)
        workflows += r.json()["workflow_runs"]
        next_page = maybe_get_next_page_path(r)

    return workflows


def get_artifacts_for_workflow(run_id: str, repo: str) -> list:
    """
    Get a list of artifacts from GitHub actions
    """
    params = {"per_page": 100}
    r = get_from_github(
        f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/artifacts",
        params=params,
    )
    artifacts = r.json()["artifacts"]
    next_page = maybe_get_next_page_path(r)
    while next_page:
        r = get_from_github(next_page, params=params)
        artifacts += r.json()["artifacts"]
        next_page = maybe_get_next_page_path(r)

    return artifacts


def suite_from_name(name: str) -> str:
    """
    Get a test suite name from an artifact name. The artifact
    can have matrix partitions, pytest marks, etc. Basically,
    just lop off the front of the name to get the suite.
    """
    return "-".join(name.split("-")[:3])


def download_and_parse_artifact(url: str):
    """
    Download the artifact at the url parse it.
    """
    try:
        r = get_from_github(url, params={})
        f = zipfile.ZipFile(io.BytesIO(r.content))
        run = junitparser.JUnitXml.fromstring(f.read(f.filelist[0].filename))
        return run
    except Exception:
        print(f"Failed to download/parse {url}")
        return None


def dataframe_from_jxml(run: list) -> pandas.DataFrame:
    """
    Turn a parsed JXML into a pandas dataframe
    """
    fname = []
    tname = []
    status = []
    message = []
    for suite in run:
        for test in suite:
            fname.append(test.classname)
            tname.append(test.name)
            s = "✓"
            result = test.result

            if len(result) == 0:
                status.append(s)
                message.append("")
                continue
            result = result[0]
            m = result.message if result and hasattr(result, "message") else ""
            if isinstance(result, junitparser.Error):
                s = "x"
            elif isinstance(result, junitparser.Failure):
                s = "x"
            elif isinstance(result, junitparser.Skipped):
                s = "s"
            else:
                s = "x"
            status.append(s)
            message.append(html.escape(m))
    df = pandas.DataFrame(
        {"file": fname, "test": tname, "status": status, "message": message}
    )

    # There are sometimes duplicate tests in the report for some unknown reason.
    # If that is the case, concatenate the messages and prefer to show errors.
    def dedup(group):
        if len(group) > 1:
            if "message" in group.name:
                return group.str.cat(sep="")
            else:
                if (group == "x").any(axis=0):
                    return "x"
                else:
                    return group.iloc[0]
        else:
            return group

    return df.groupby(["file", "test"]).agg(dedup)


def download_and_parse_artifacts(
    repo: str, branch: str, events: list[str], days: int
) -> Iterator[pandas.DataFrame]:

    print("Getting workflows list...")
    workflows = []
    for event in events:
        workflows += get_workflow_listing(
            repo=repo, branch=branch, event=event, days=days
        )

    # Filter the workflows listing to be in the retention period,
    # and only be test runs (i.e., no linting) that completed.
    workflows = [
        w
        for w in workflows
        if (
            pandas.to_datetime(w["created_at"])
            > pandas.Timestamp.now(tz="UTC") - pandas.Timedelta(days=days)
            and w["conclusion"] != "cancelled"
            and w["name"].lower() == "tests"
        )
    ]
    print(f"Found {len(workflows)} workflows")
    # Each workflow processed takes ~10-15 API requests. To avoid being
    # rate limited by GitHub (1000 requests per hour) we choose just the
    # most recent N runs. This also keeps the viz size from blowing up.
    workflows = sorted(workflows, key=lambda w: w["created_at"])[-50:]
    print(f"Fetching artifact listing for the {len(workflows)} most recent workflows")

    for w in workflows:
        artifacts = get_artifacts_for_workflow(w["id"], repo=repo)
        # We also upload timeout reports as artifacts, but we don't want them here.
        w["artifacts"] = [
            a
            for a in artifacts
            if "timeouts" not in a["name"] and "cluster_dumps" not in a["name"]
        ]

    nartifacts = sum(len(w["artifacts"]) for w in workflows)
    ndownloaded = 0
    print(f"Downloading and parsing {nartifacts} artifacts...")

    with shelve.open("test_report") as cache:
        for w in workflows:
            w["dfs"] = []
            for a in w["artifacts"]:
                url = a["archive_download_url"]
                df: pandas.DataFrame | None
                try:
                    df = cache[url]
                except KeyError:
                    xml = download_and_parse_artifact(url)
                    if xml:
                        df = dataframe_from_jxml(xml)
                        # Note: we assign a column with the workflow timestamp rather
                        # than the artifact timestamp so that artifacts triggered under
                        # the same workflow can be aligned according to the same trigger
                        # time.
                        df = df.assign(
                            name=a["name"],
                            suite=suite_from_name(a["name"]),
                            date=w["created_at"],
                            url=w["html_url"],
                        )
                    else:
                        df = None
                    cache[url] = df

                if df is not None:
                    yield df

                ndownloaded += 1
                if ndownloaded and not ndownloaded % 20:
                    print(f"{ndownloaded}... ", end="")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if not TOKEN:
        raise RuntimeError("Failed to find a GitHub Token")

    # Note: we drop **all** tests which did not have at least <nfails> failures.
    # This is because, as nice as a block of green tests can be, there are
    # far too many tests to visualize at once, so we only want to look at
    # flaky tests. If the test suite has been doing well, this chart should
    # dwindle to nothing!
    dfs = list(
        download_and_parse_artifacts(
            repo=args.repo,
            branch=args.branch,
            events=args.events,
            days=args.days,
        )
    )

    total = pandas.concat(dfs, axis=0)
    grouped = (
        total.groupby(total.index)
        .filter(lambda g: (g.status == "x").sum() >= args.nfails)
        .reset_index()
        .assign(test=lambda df: df.file + "." + df.test)
        .groupby("test")
    )
    overall = {name: grouped.get_group(name) for name in grouped.groups}

    # Get all of the workflow timestamps that we wound up with, which we can use
    # below to align the different groups.
    times = set()
    for df in overall.values():
        times.update(df.date.unique())

    print("Making chart...")
    altair.data_transformers.disable_max_rows()
    charts = []
    for name, df in overall.items():
        # Don't show this suite if it has passed all tests recently.
        if not len(df):
            continue

        # Create an aggregated form of the suite with overall pass rate
        # over the time in question.
        df_agg = (
            df[df.status != "x"]
            .groupby("suite")
            .size()
            .truediv(df.groupby("suite").size(), fill_value=0)
            .to_frame(name="Pass Rate")
            .reset_index()
        )

        # Create a grid with hover tooltip for error messages
        charts.append(
            altair.Chart(df)
            .mark_rect(stroke="gray")
            .encode(
                x=altair.X("date:O", scale=altair.Scale(domain=sorted(list(times)))),
                y=altair.Y("suite:N", title=None),
                href=altair.Href("url:N"),
                color=altair.Color(
                    "status:N",
                    scale=altair.Scale(
                        domain=list(COLORS.keys()),
                        range=list(COLORS.values()),
                    ),
                ),
                tooltip=["suite:N", "date:O", "status:N", "message:N", "url:N"],
            )
            .properties(title=name)
            | altair.Chart(df_agg.assign(_="_"))
            .mark_rect(stroke="gray")
            .encode(
                y=altair.Y("suite:N", title=None, axis=altair.Axis(labels=False)),
                x=altair.X("_:N", title=None),
                color=altair.Color(
                    "Pass Rate:Q",
                    scale=altair.Scale(
                        range=[COLORS["x"], COLORS["✓"]], domain=[0.0, 1.0]
                    ),
                ),
                tooltip=["suite:N", "Pass Rate:Q"],
            )
        )

    # Concat the sub-charts and output to file
    chart = (
        altair.vconcat(*charts)
        .configure_axis(labelLimit=1000)  # test names are long
        .configure_title(anchor="start")
        .resolve_scale(x="shared")  # enforce aligned x axes
    )
    altair_saver.save(
        chart,
        args.output,
        embed_options={
            "renderer": "svg",  # Makes the text searchable
            "loader": {"target": "_blank"},  # Open hrefs in a new window
        },
    )


if __name__ == "__main__":
    main()
