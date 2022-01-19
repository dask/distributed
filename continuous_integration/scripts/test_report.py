from __future__ import annotations

import io
import os
import re
import zipfile

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


def get_from_github(url, params={}):
    """
    Make an authenticated request to the GitHub REST API.
    """
    r = requests.get(url, params=params, headers={"Authorization": f"token {TOKEN}"})
    r.raise_for_status()
    return r


def maybe_get_next_page_path(response):
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


def get_workflow_listing(repo="dask/distributed", branch="main", event="push"):
    """
    Get a list of workflow runs from GitHub actions.
    """
    params = {"per_page": 100, "branch": branch, "event": event}
    r = get_from_github(
        f"https://api.github.com/repos/{repo}/actions/runs", params=params
    )
    workflows = r.json()["workflow_runs"]
    next_page = maybe_get_next_page_path(r)
    while next_page:
        r = get_from_github(next_page)
        workflows = workflows + r.json()["workflow_runs"]
        next_page = maybe_get_next_page_path(r)

    return workflows


def get_artifacts_for_workflow(run_id, repo="dask/distributed"):
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
        r = get_from_github(next_page)
        artifacts = workflows + r.json()["workflow_runs"]
        next_page = maybe_get_next_page_path(r)

    return artifacts


def suite_from_name(name: str):
    """
    Get a test suite name from an artifact name. The artifact
    can have matrix partitions, pytest marks, etc. Basically,
    just lop off the front of the name to get the suite.
    """
    return "-".join(name.split("-")[:3])


def download_and_parse_artifact(url):
    """
    Download the artifact at the url parse it.
    """
    r = get_from_github(url)
    f = zipfile.ZipFile(io.BytesIO(r.content))
    try:
        run = junitparser.JUnitXml.fromstring(f.read(f.filelist[0].filename))
        return run
    except Exception:
        print(f"Failed to parse {url}")
        return None


def dataframe_from_jxml(run):
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
            message.append(m)
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


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Failed to find a GitHub Token")
    print("Getting all recent workflows...")
    workflows = get_workflow_listing()

    # Filter the workflows listing to be in the last month,
    # and only be test runs (i.e., no linting) that completed.
    workflows = [
        w
        for w in workflows
        if (
            pandas.to_datetime(w["created_at"])
            > pandas.Timestamp.now(tz="UTC") - pandas.Timedelta(days=31)
            and w["conclusion"] != "cancelled"
            and w["name"].lower() == "tests"
        )
    ]

    print("Getting the artifact listing for each workflow...")
    for w in workflows:
        artifacts = get_artifacts_for_workflow(w["id"])
        # We also upload timeout reports as artifacts, but we don't want them here.
        w["artifacts"] = [a for a in artifacts if "timeouts" not in a["name"]]

    print("Downloading and parsing artifacts...")
    for w in workflows:
        w["dfs"] = []
        for a in w["artifacts"]:
            xml = download_and_parse_artifact(a["archive_download_url"])
            df = dataframe_from_jxml(xml)
            # Note: we assign a column with the workflow timestamp rather than the
            # artifact timestamp so that artifacts triggered under the same workflow
            # can be aligned according to the same trigger time.
            if df is not None:
                df = df.assign(
                    name=a["name"],
                    suite=suite_from_name(a["name"]),
                    date=w["created_at"],
                )
                w["dfs"].append(df)

    # Compute the set of test suites which form the top-level grouping for the chart
    # (e.g., ubuntu-latest-3.9, windows-latest-3.7)
    suites = set()
    for w in workflows:
        for a in w["artifacts"]:
            suites.add(suite_from_name(a["name"]))

    # Make a top-level dict of dataframes, mapping test suite name to a long-form
    # dataframe of all the tests run in that suite.
    overall: dict[str, pandas.DataFrame] = {}
    for s in sorted(suites):
        overall
        dfs = []
        for w in workflows:
            dfs.extend([df[df.suite == s] for df in w["dfs"]])
        overall[s] = pandas.concat(dfs, axis=0)

    # Get all of the workflow timestamps that we wound up with, which we can use
    # below to align the different suites.
    times = set()
    for df in overall.values():
        times.update(df.date.unique())

    print("Making chart...")
    altair.data_transformers.disable_max_rows()
    charts = []
    for name, df in overall.items():
        # Final reshaping for altair plotting.
        # Note: we drop **all** tests which did not have at least one failure.
        # This is because, as nice as a block of green tests can be, there are
        # far too many tests to visualize at once, so we only want to look at
        # flaky tests. If the test suite has been doing well, this chart should
        # dwindle to nothing!
        df = (
            df.groupby(df.index)
            .filter(lambda g: (g.status == "x").any())
            .reset_index()
            .assign(test=lambda df: df.file + "." + df.test)
        )
        # Don't show this suite if it has passed all tests recently.
        if not len(df):
            continue

        # Create an aggregated form of the suite with overall pass rate
        # over the time in question.
        df_agg = (
            df[df.status == "✓"]
            .groupby("test")
            .size()
            .truediv(df.groupby("test").size(), fill_value=0)
            .to_frame(name="Pass Rate")
            .reset_index()
        )

        # Create a grid with hover tooltip for error messages
        charts.append(
            altair.Chart(df)
            .mark_rect(stroke="gray")
            .encode(
                x=altair.X("date:O", scale=altair.Scale(domain=sorted(list(times)))),
                y=altair.Y("test:N", title=None),
                color=altair.Color(
                    "status:N",
                    scale=altair.Scale(
                        domain=list(COLORS.keys()),
                        range=list(COLORS.values()),
                    ),
                ),
                tooltip=["test:N", "date:O", "status:N", "message:N"],
            )
            .properties(title=name)
            | altair.Chart(df_agg.assign(_="_"))
            .mark_rect(stroke="gray")
            .encode(
                y=altair.Y("test:N", title=None, axis=altair.Axis(labels=False)),
                x=altair.X("_:N", title=None),
                color=altair.Color(
                    "Pass Rate:Q",
                    scale=altair.Scale(
                        range=[COLORS["x"], COLORS["✓"]], domain=[0.0, 1.0]
                    ),
                ),
                tooltip=["test:N", "Pass Rate:Q"],
            )
        )

    # Concat the sub-charts and output to file
    chart = (
        altair.vconcat(*charts)
        .configure_axis(labelLimit=1000)  # test names are long
        .resolve_scale(x="shared")  # enforce aligned x axes
    )
    altair_saver.save(chart, "test_report.html", embed_options={"renderer": "svg"})
