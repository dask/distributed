from __future__ import annotations

import os
import re
import sys

import pytest

script_dir = os.path.join(
    os.path.dirname(__file__), "..", "..", "continuous_integration", "scripts"
)
sys.path.append(script_dir)
# Will fail if test suite was pip-installed
parse_stdout = pytest.importorskip("parse_stdout")


# Note: test_timeout below ends with a whitespace!
# flake8: noqa: W291
stdout = """
Unrelated row, must ignore
distributed/tests/test1.py::test_fail [31mFAILED[0m[31m                             [ 10%][0m
distributed/tests/test1.py::test_error_in_setup [31mERROR[0m[31m                    [ 20%][0m
distributed/tests/test1.py::test_pass_and_then_error_in_teardown [32mPASSED[0m[31m  [ 30%][0m
distributed/tests/test1.py::test_pass_and_then_error_in_teardown [31mERROR[0m[31m   [ 30%][0m
distributed/tests/test1.py::test_fail_and_then_error_in_teardown [31mFAILED[0m[31m  [ 40%][0m
distributed/tests/test1.py::test_fail_and_then_error_in_teardown [31mERROR[0m[31m   [ 40%][0m
distributed/tests/test1.py::test_skip [33mSKIPPED[0m (unconditional skip)[31m       [ 50%][0m
distributed/tests/test1.py::test_xfail [33mXFAIL[0m[31m                             [ 60%][0m
distributed/tests/test1.py::test_flaky [33mRERUN[0m[31m                             [ 70%][0m
distributed/tests/test1.py::test_flaky [32mPASSED[0m[31m                            [ 70%][0m
distributed/tests/test1.py::test_leaking [32mPASSED[0m[31m                          [ 72%][0m
distributed/tests/test1.py::test_leaking [32mLEAKED[0m[31m                          [ 72%][0m
distributed/tests/test1.py::test_pass [32mPASSED[0m[31m                             [ 75%][0m
distributed/tests/test1.py::test_params[a a] [32mPASSED[0m[31m                      [ 78%][0m
distributed/tests/test1.py::test_escape_chars[<lambda>] [32mPASSED[0m[31m           [ 80%][0m
distributed/tests/test1.py::MyTest::test_unittest [32mPASSED[0m[31m                 [ 86%][0m
distributed/tests/test1.py::test_timeout 
"""


def test_parse_rows():
    actual = parse_stdout.parse_rows(stdout.splitlines())

    expect = [
        ("distributed.tests.test1", "test_fail", {"FAILED"}),
        ("distributed.tests.test1", "test_error_in_setup", {"ERROR"}),
        (
            "distributed.tests.test1",
            "test_pass_and_then_error_in_teardown",
            {"PASSED", "ERROR"},
        ),
        (
            "distributed.tests.test1",
            "test_fail_and_then_error_in_teardown",
            {"FAILED", "ERROR"},
        ),
        ("distributed.tests.test1", "test_skip", {"SKIPPED"}),
        ("distributed.tests.test1", "test_xfail", {"XFAIL"}),
        ("distributed.tests.test1", "test_flaky", {"PASSED"}),
        ("distributed.tests.test1", "test_leaking", {"PASSED"}),
        ("distributed.tests.test1", "test_pass", {"PASSED"}),
        ("distributed.tests.test1", "test_params[a a]", {"PASSED"}),
        ("distributed.tests.test1", "test_escape_chars[<lambda>]", {"PASSED"}),
        ("distributed.tests.test1.MyTest", "test_unittest", {"PASSED"}),
        ("distributed.tests.test1", "test_timeout", {None}),
    ]

    assert actual == expect


def test_build_xml(capsys):
    rows = parse_stdout.parse_rows(stdout.splitlines())
    parse_stdout.build_xml(rows)
    actual = capsys.readouterr().out.strip()
    # Remove timestamp
    actual = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}", r"snip", actual)

    expect = """
<?xml version="1.0" encoding="utf-8"?>
<testsuites>
<testsuite name="distributed" errors="3" failures="3" skipped="2" tests="15" time="0.0" timestamp="snip" hostname="">
<testcase classname="distributed.tests.test1" name="test_fail" time="0.0"><failure message=""></failure></testcase>
<testcase classname="distributed.tests.test1" name="test_error_in_setup" time="0.0"><error message="failed on setup"></error></testcase>
<testcase classname="distributed.tests.test1" name="test_pass_and_then_error_in_teardown" time="0.0"><error message="failed on teardown"></error></testcase>
<testcase classname="distributed.tests.test1" name="test_fail_and_then_error_in_teardown" time="0.0"><failure message=""></failure></testcase>
<testcase classname="distributed.tests.test1" name="test_fail_and_then_error_in_teardown" time="0.0"><error message="failed on teardown"></error></testcase>
<testcase classname="distributed.tests.test1" name="test_skip" time="0.0"><skipped type="pytest.skip" message="skip"></skipped></testcase>
<testcase classname="distributed.tests.test1" name="test_xfail" time="0.0"><skipped type="pytest.xfail" message="xfail"></skipped></testcase>
<testcase classname="distributed.tests.test1" name="test_flaky" time="0.0" />
<testcase classname="distributed.tests.test1" name="test_leaking" time="0.0" />
<testcase classname="distributed.tests.test1" name="test_pass" time="0.0" />
<testcase classname="distributed.tests.test1" name="test_params[a a]" time="0.0" />
<testcase classname="distributed.tests.test1" name="test_escape_chars[&lt;lambda&gt;]" time="0.0" />
<testcase classname="distributed.tests.test1.MyTest" name="test_unittest" time="0.0" />
<testcase classname="distributed.tests.test1" name="test_timeout" time="0.0"><failure message="pytest-timeout exceeded"></failure></testcase>
</testsuite>
</testsuites>
    """.strip()

    assert actual == expect
