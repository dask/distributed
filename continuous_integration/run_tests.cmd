call activate %CONDA_ENV%

@echo on

set PYTHONFAULTHANDLER=1

set PYTEST=py.test --tb=native --timeout=120 -r s

%PYTEST% -vx -m "not avoid_travis" --count=10 --junit-xml="%JUNIT_OUT%" distributed/tests/test_tls_functional.py
