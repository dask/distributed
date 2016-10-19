call activate %CONDA_ENV%

@echo on

set PYTHONFAULTHANDLER=1

set PYTEST=py.test --tb=native --timeout=60

%PYTEST% -v -m "not avoid_travis" --junit-xml="%JUNIT_OUT%" distributed
