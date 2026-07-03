from __future__ import annotations

import argparse
import contextlib
import html
import io
import os
import re
import shelve
import subprocess
import sys
import zipfile
from collections.abc import Generator, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor
from typing import Any, cast

import altair
import junitparser
import pandas
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TOKEN: str | None = None

# The output format of tests.yaml changed substantially when CI was migrated to
# pixi (dask/distributed#9276, merged 2026-06-03; dask/dask#12389, merged
# 2026-05-21). Workflow runs older than this date cannot be parsed.
CUTOFF = pandas.Timestamp("2026-06-04", tz="UTC")

# Mapping between a symbol (pass, fail, skip) and a color
COLORS = {
    "✓": "#acf2a5",
    "x": "#f2a5a5",
    "s": "#f2ef8f",
}


def get_token() -> str:
    """Read the GitHub API token from the GITHUB_TOKEN environment variable,
    falling back to the gh CLI if the variable is not set.
    """
    if token := os.environ.get("GITHUB_TOKEN"):
        return token
    try:
        proc = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, check=True
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        raise RuntimeError(
            "Failed to find a GitHub Token. Either set the GITHUB_TOKEN "
            "environment variable or log into the gh CLI (`gh auth login`)."
        ) from None
    return proc.stdout.strip()


def cache_name(prefix: str, repo: str) -> str:
    """Name of the local cache database, e.g. test_report_dask__distributed"""
    return f"{prefix}_{repo.replace('/', '__')}"


@contextlib.contextmanager
def get_session() -> Generator[requests.Session]:
    retry_strategy = Retry(
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=0.2,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    with requests.Session() as session:
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        yield session


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
        "--max-days",
        "-d",
        type=int,
        default=90,
        help="Maximum number of days to look back from now",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=50,
        help="Maximum number of workflow runs to fetch",
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
    parser.add_argument("--title", "-t", default="Test Report", help="Report title")
    return parser.parse_args(argv)


def get_from_github(
    url: str, params: dict[str, Any], session: requests.Session
) -> requests.Response:
    """
    Make an authenticated request to the GitHub REST API.
    """
    r = session.get(url, params=params, headers={"Authorization": f"token {TOKEN}"})
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


def get_jobs(run, session, repo):
    with shelve.open(cache_name("test_report_jobs", repo)) as cache:
        url = run["jobs_url"]
        try:
            jobs = cache[url]
        except KeyError:
            params = {"per_page": 100}
            r = get_from_github(run["jobs_url"], params, session=session)
            jobs = r.json()["jobs"]
            while next_page := maybe_get_next_page_path(r):
                r = get_from_github(next_page, params=params, session=session)
                jobs.extend(r.json()["jobs"])
            cache[url] = jobs

    df_jobs = pandas.DataFrame.from_records(jobs)
    df_jobs = df_jobs[df_jobs.name != "Event File"]

    # Reconstruct the artifact name from the job name, so that it can later be
    # joined with the JXML results. Job names are space-separated, e.g.
    #     ubuntu-latest py310 test-ci not ci1
    # whereas the matching artifact is named after $TEST_ID (see the
    # `Set $TEST_ID` step in tests.yaml), which is dash-separated and with the
    # space removed from the partition name:
    #     ubuntu-latest-py310-test-ci-notci1
    # dask/dask job names are the same, minus the trailing partition.
    df_jobs["suite_name"] = df_jobs.name.str.replace(" not ci1", " notci1").str.replace(
        " ", "-"
    )
    return df_jobs


def get_workflow_run_listing(
    repo: str, branch: str, event: str, days: int, session: requests.Session
) -> list[dict]:
    """
    Get a list of workflow runs from GitHub actions.
    """
    since = (pandas.Timestamp.now(tz="UTC") - pandas.Timedelta(days=days)).date()
    since = max(since, CUTOFF.date())
    params = {
        "per_page": 100,
        "branch": branch,
        "event": event,
        "created": f">={since}",
    }
    r = get_from_github(
        f"https://api.github.com/repos/{repo}/actions/runs",
        params=params,
        session=session,
    )
    runs = r.json()["workflow_runs"]
    next_page = maybe_get_next_page_path(r)
    while next_page:
        r = get_from_github(next_page, params, session=session)
        runs += r.json()["workflow_runs"]
        next_page = maybe_get_next_page_path(r)

    return runs


def get_artifacts_for_workflow_run(
    run_id: str, repo: str, session: requests.Session
) -> list:
    """
    Get a list of artifacts from GitHub actions
    """
    params = {"per_page": 100}
    r = get_from_github(
        f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/artifacts",
        params=params,
        session=session,
    )
    artifacts = r.json()["artifacts"]
    next_page = maybe_get_next_page_path(r)
    while next_page:
        r = get_from_github(next_page, params=params, session=session)
        artifacts += r.json()["artifacts"]
        next_page = maybe_get_next_page_path(r)

    return artifacts


def suite_from_name(name: str) -> str:
    """
    Get a test suite name from an artifact name. This is the artifact name
    minus the trailing pytest partition (dask/distributed only), so that the
    ci1 and notci1 partitions of the same test suite are shown on one row.
    """
    return re.sub(r"-(not)?ci1$", "", name)


def download_and_parse_artifact(
    url: str, repo: str, session: requests.Session
) -> junitparser.JUnitXml | None:
    """
    Download the artifact at the url parse it.
    """
    with shelve.open(cache_name("test_report", repo)) as cache:
        try:
            xml_raw = cache[url]
        except KeyError:
            r = get_from_github(url, params={}, session=session)
            f = zipfile.ZipFile(io.BytesIO(r.content))
            cache[url] = xml_raw = f.read(f.filelist[0].filename)
    try:
        return junitparser.JUnitXml.fromstring(xml_raw)
    except Exception:
        # e.g. truncated XML from a job that hit the timeout
        return None


def dataframe_from_jxml(run: Iterable) -> pandas.DataFrame:
    """
    Turn a parsed JXML into a pandas dataframe
    """
    fname = []
    tname = []
    status = []
    message = []
    sname = []
    for suite in run:
        for test in suite:
            sname.append(suite.name)
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
        {
            "file": fname,
            "test": tname,
            "status": status,
            "message": message,
            "suite_name": sname,
        }
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

    return df.groupby(["file", "test"], as_index=False).agg(dedup)


def download_and_parse_artifacts(
    repo: str, branch: str, events: list[str], max_days: int, max_runs: int
) -> Iterator[pandas.DataFrame]:
    print("Getting list of workflow runs...")
    runs = []
    with get_session() as session:
        for event in events:
            runs += get_workflow_run_listing(
                repo=repo, branch=branch, event=event, days=max_days, session=session
            )

        # Filter the workflow runs listing to be in the retention period,
        # and only be test runs (i.e., no linting) that completed.
        runs = [
            r
            for r in runs
            if (
                pandas.to_datetime(r["created_at"])
                > pandas.Timestamp.now(tz="UTC") - pandas.Timedelta(days=max_days)
                and pandas.to_datetime(r["created_at"]) >= CUTOFF
                and r["status"] == "completed"
                and r["conclusion"] != "cancelled"
                and r["name"].lower() == "tests"
            )
        ]
        print(f"Found {len(runs)} workflow runs")
        # Each workflow run processed takes ~10-15 API requests. To avoid being
        # rate limited by GitHub (1000 requests per hour) we choose just the
        # most recent N runs. This also keeps the viz size from blowing up.
        runs = sorted(runs, key=lambda r: r["created_at"])[-max_runs:]
        print(
            f"Fetching artifact listing for the {len(runs)} most recent workflow runs"
        )

        for r in runs:
            artifacts = get_artifacts_for_workflow_run(
                r["id"], repo=repo, session=session
            )
            # Skip artifacts that don't contain a JUnit XML report
            r["artifacts"] = [
                a
                for a in artifacts
                if not a["expired"]
                and a["name"] != "Event File"
                and "cluster_dumps" not in a["name"]
            ]

        nartifacts = sum(len(r["artifacts"]) for r in runs)
        ndownloaded = 0
        print(f"Downloading and parsing {nartifacts} artifacts...")

        for r in runs:
            jobs_df = get_jobs(r, session=session, repo=repo)
            r["dfs"] = []
            for a in r["artifacts"]:
                url = a["archive_download_url"]
                df: pandas.DataFrame | None
                xml = download_and_parse_artifact(url, repo=repo, session=session)
                if xml is None:
                    continue
                df = dataframe_from_jxml(cast(Iterable, xml))

                # Note: we assign a column with the workflow run timestamp rather
                # than the artifact timestamp so that artifacts triggered under
                # the same workflow run can be aligned according to the same trigger
                # time.
                html_url = jobs_df[jobs_df["suite_name"] == a["name"]].html_url.unique()
                assert len(html_url) == 1, (
                    f"Artifact suite name {a['name']} did not match any jobs dataframe:\n{jobs_df['suite_name'].unique()}"
                )
                html_url = html_url[0]
                assert html_url is not None
                df2 = df.assign(
                    name=a["name"],
                    suite=suite_from_name(a["name"]),
                    date=r["created_at"],
                    html_url=html_url,
                )

                if df2 is not None:
                    yield df2

                ndownloaded += 1
                if ndownloaded and not ndownloaded % 20:
                    print(f"{ndownloaded}... ", end="")


def make_chart(name, df, times):
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
    return altair.Chart(df).mark_rect(stroke="gray").encode(
        x=altair.X("date:O", scale=altair.Scale(domain=sorted(list(times)))),
        y=altair.Y("suite:N", title=None),
        href=altair.Href("html_url:N"),
        color=altair.Color(
            "status:N",
            scale=altair.Scale(
                domain=list(COLORS.keys()),
                range=list(COLORS.values()),
            ),
        ),
        tooltip=["suite:N", "date:O", "status:N", "message:N", "html_url:N"],
    ).properties(title=name) | altair.Chart(df_agg.assign(_="_")).mark_rect(
        stroke="gray"
    ).encode(
        y=altair.Y("suite:N", title=None, axis=altair.Axis(labels=False)),
        x=altair.X("_:N", title=None),
        color=altair.Color(
            "Pass Rate:Q",
            scale=altair.Scale(range=[COLORS["x"], COLORS["✓"]], domain=[0.0, 1.0]),
        ),
        tooltip=["suite:N", "Pass Rate:Q"],
    )


def main(argv: list[str] | None = None) -> None:
    global TOKEN

    args = parse_args(argv)
    TOKEN = get_token()

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
            max_days=args.max_days,
            max_runs=args.max_runs,
        )
    )
    total = pandas.concat(dfs, axis=0)
    # Reduce the size of the DF since the entire thing is encoded in the vega spec
    required_columns = [
        "test",
        "date",
        "suite",
        "file",
        "html_url",
        "status",
        "message",
    ]
    total = total[required_columns]
    grouped = (
        total.groupby([total.file, total.test])
        .filter(lambda g: (g.status == "x").sum() >= args.nfails)
        .reset_index()
        .assign(test=lambda df: df.file + "." + df.test)
        .groupby("test")
    )
    overall = {name: grouped.get_group(name) for name in grouped.groups}

    # Get all the workflow run timestamps that we wound up with, which we can use
    # below to align the different groups.
    times: set = set()
    for df in overall.values():
        times.update(df.date.unique())

    print("Making chart...")
    altair.data_transformers.disable_max_rows()

    jobs = []
    with ProcessPoolExecutor() as executor:
        for name, df in overall.items():
            # Don't show this suite if it has passed all tests recently.
            if not len(df):
                continue
            jobs.append(executor.submit(make_chart, name, df, times))
        charts = [job.result() for job in jobs]

    # Concat the sub-charts and output to file
    chart = (
        altair.vconcat(*charts)
        .properties(
            title={
                "text": [f"{args.repo} {args.title}"],
                "subtitle": [" ".join(argv if argv is not None else sys.argv)],
            }
        )
        .configure_axis(labelLimit=1000)  # test names are long
        .configure_title(
            anchor="start",
            subtitleFont="monospace",
        )
        .resolve_scale(x="shared")  # enforce aligned x axes
    )

    chart.save(
        args.output,
        embed_options={
            "renderer": "svg",  # Makes the text searchable
            "loader": {"target": "_blank"},  # Open hrefs in a new window
        },
    )


if __name__ == "__main__":
    main()
