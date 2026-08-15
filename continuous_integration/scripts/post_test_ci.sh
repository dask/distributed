#!/bin/bash

# Generate coverage.xml from .coverage
if [ -e .coverage ]
then
    coverage xml

    # https://community.codecov.com/t/files-missing-from-report/3902/7
    # The coverage file being created records filenames at the distributed/ directory
    # as opposed to root. This is causing filename mismatches in Codecov.
    # Prepend `distributed/` to all filenames.
    sed -i'' -e 's/filename="/filename="distributed\//g' coverage.xml
else
    echo "Could not find .coverage"
fi

# Generate pytest.xml in case of hard pytest-timeout
# This should only ever happen on Windows.
# On Linux and MacOS, pytest-timeout kills off the individual tests
# See (reconfigure pytest-timeout in tests.yaml)
if [ ! -e pytest.xml ]
then
    echo "Could not find pytest.xml; generating it from pytest-stdout.log"
    python continuous_integration/scripts/parse_pytest_stdout.py \
        < pytest-stdout.log > pytest.xml
fi
