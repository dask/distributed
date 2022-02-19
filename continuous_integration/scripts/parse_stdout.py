"""On Windows, pytest-timeout kills off the whole test suite, leaving no junit report
behind. Parse the stdout of pytest to generate one.
"""
from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import datetime

# If a test has multiple outcomes, report the leftmost one in this list
OUTCOMES_PRIORITY = (
    None,  # Test timeout. Marked as a variant of FAILED in the junit report
    "FAILED",
    "ERROR",  # teardown failed (it appears after another status) or setup failed
    "SKIPPED",  # @pytest.mark.skip, @pytest.mark.skipif, or raise pytest.skip()
    "XFAIL",  # Marked as a variant of SKIPPED in the junit report
    "PASSED",
    "LEAKED",  # this appears after another status
    "RERUN",  # this appears before another status
)


def parse_rows(rows: Iterable[str]) -> list[tuple[str, str, str | None]]:
    match = re.compile(
        r"(distributed/.*test.*)::([^ ]*)"
        r"( (.*)(PASSED|FAILED|ERROR|SKIPPED|XFAIL|RERUN|LEAKED).*| )$"
    )

    outcomes_idx = dict(zip(OUTCOMES_PRIORITY, range(len(OUTCOMES_PRIORITY))))
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
        assert outcome in outcomes_idx

        out[clsname, tname].add(m.group(5))

    return [
        (clsname, tname, sorted(outcomes, key=outcomes_idx.__getitem__)[0])
        for (clsname, tname), outcomes in out.items()
    ]


def build_xml(rows: list[tuple[str, str, str | None]]) -> None:
    cnt = Counter(outcome for _, _, outcome in rows)
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

    for clsname, tname, outcome in rows:
        print(f'<testcase classname="{clsname}" name="{tname}" time="0.0"', end="")
        if outcome == "PASSED":
            print(" />")
        elif outcome == "FAILED":
            print('><failure message=""></failure></testcase>')
        elif outcome is None:
            print('><failure message="pytest-timeout exceeded"></failure></testcase>')
        elif outcome == "ERROR":
            print('><error message=""></error></testcase>')
        elif outcome == "SKIPPED":
            print('><skipped type="pytest.skip" message="skip"></skipped></testcase>')
        elif outcome == "XFAIL":
            print('><skipped type="pytest.xfail" message="xfail"></skipped></testcase>')
        else:  # Unreachable
            # We should never get LEAKED or RERUN rows alone
            raise ValueError(outcome)  # pragma: nocover

    print("</testsuite>")
    print("</testsuites>")


def main() -> None:  # pragma: nocover
    build_xml(parse_rows(sys.stdin))


if __name__ == "__main__":
    main()  # pragma: nocover
