"""On Windows, pytest-timeout kills off the whole test suite, leaving no junit report
behind. Parse the stdout of pytest to generate one.
"""
from __future__ import annotations

import html
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import datetime

OUTCOMES = {
    "PASSED",
    "FAILED",
    # Test timeout. Marked as a variant of FAILED in the junit report
    None,
    # Setup failed or teardown failed.
    # In the latter case, if the test also failed, show both a FAILED and an ERROR line.
    "ERROR",
    # @pytest.mark.skip, @pytest.mark.skipif, or raise pytest.skip()
    "SKIPPED",
    # Reported as a variant of SKIPPED in the junit report
    "XFAIL",
    # These appear respectively before and after another status. Ignore.
    "RERUN",
    "LEAKED",
}


def parse_rows(rows: Iterable[str]) -> list[tuple[str, str, set[str | None]]]:
    match = re.compile(
        r"(distributed/.*test.*)::([^ ]*)"
        r"( (.*)(PASSED|FAILED|ERROR|SKIPPED|XFAIL|RERUN|LEAKED).*| )$"
    )

    out: defaultdict[tuple[str, str], set[str | None]] = defaultdict(set)

    for row in rows:
        m = match.match(row)
        if not m:
            continue

        fname = m.group(1)
        clsname = fname.replace("/", ".").replace(".py", "").replace("::", ".")

        tname = m.group(2).strip()
        if m.group(4) and "]" in m.group(4):
            tname += " " + m.group(4).split("]")[0] + "]"

        outcome = m.group(5)
        assert outcome in OUTCOMES
        if outcome not in {"RERUN", "LEAKED"}:
            out[clsname, tname].add(outcome)

    return [(clsname, tname, outcomes) for (clsname, tname), outcomes in out.items()]


def build_xml(rows: list[tuple[str, str, set[str | None]]]) -> None:
    cnt = Counter(outcome for _, _, outcomes in rows for outcome in outcomes)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

    # We could have used ElementTree but it feels like overkill here
    print('<?xml version="1.0" encoding="utf-8"?>')
    print("<testsuites>")
    print(
        '<testsuite name="distributed" '
        f'errors="{cnt["ERROR"]}" failures="{cnt["FAILED"] + cnt[None]}" '
        f'skipped="{cnt["SKIPPED"] + cnt["XFAIL"]}" tests="{sum(cnt.values())}" '
        f'time="0.0" timestamp="{timestamp}" hostname="">'
    )

    for clsname, tname, outcomes in rows:
        clsname = html.escape(clsname)
        tname = html.escape(tname)
        print(f'<testcase classname="{clsname}" name="{tname}" time="0.0"', end="")
        if outcomes == {"PASSED"}:
            print(" />")
        elif outcomes == {"FAILED"}:
            print('><failure message=""></failure></testcase>')
        elif outcomes == {None}:
            print('><failure message="pytest-timeout exceeded"></failure></testcase>')
        elif outcomes == {"ERROR"}:
            print('><error message="failed on setup"></error></testcase>')
        elif outcomes == {"PASSED", "ERROR"}:
            print('><error message="failed on teardown"></error></testcase>')
        elif outcomes == {"FAILED", "ERROR"}:
            print(
                '><failure message=""></failure></testcase>\n'
                f'<testcase classname="{clsname}" name="{tname}" time="0.0">'
                '<error message="failed on teardown"></error></testcase>'
            )
        elif outcomes == {"SKIPPED"}:
            print('><skipped type="pytest.skip" message="skip"></skipped></testcase>')
        elif outcomes == {"XFAIL"}:
            print('><skipped type="pytest.xfail" message="xfail"></skipped></testcase>')
        else:  # pragma: nocover
            # This should be unreachable. We would normally raise ValueError, except
            # that a crash in this script would be pretty much invisible.
            print(
                f' />\n<testcase classname="parse_stdout" name="build_xml" time="0.0">'
                f'><failure message="Unexpected {outcomes=}"></failure></testcase>'
            )

    print("</testsuite>")
    print("</testsuites>")


def main() -> None:  # pragma: nocover
    build_xml(parse_rows(sys.stdin))


if __name__ == "__main__":
    main()  # pragma: nocover
