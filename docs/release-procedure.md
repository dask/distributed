Distributed follows a similar procedure for releasing as the core Dask project.

See https://github.com/dask/dask/blob/main/docs/release-procedure.md for instructions.

Pushing a release tag to `dask/distributed` triggers
`.github/workflows/release-publish.yml`, which builds and smoke-tests the wheel
and source distribution, verifies that they depend on the matching Dask release,
publishes them to PyPI with Trusted Publishing, and publishes the GitHub Release.

To save time, the Dask and Distributed tags may be pushed together. The
Distributed workflow waits until the matching
`dask==YYYY.M.X` wheel and source distribution are available on PyPI before
smoke-testing and publishing. The Distributed smoke tests install dependencies
from PyPI and assert that the installed Dask version matches the release, so the
matching Dask release must be resolvable first.

GitHub Actions pauses at the `pypi` environment for manual approval. Open the
`Release Publisher` workflow at
https://github.com/dask/distributed/actions/workflows/release-publish.yml,
select the active release run, and use the `Review deployments` button to
approve the PyPI publishing job after the build, checks, PyPI wait, and smoke
tests are green. This approval gate is configured explicitly by the
`publish_pypi` job's `environment: pypi` setting.

During this brief interval, `dask[distributed]` for the new version may not
resolve from PyPI until the matching Distributed package has been published. The
Distributed workflow keeps this window short by waiting on PyPI before
publishing. If the PyPI wait times out or the Distributed publish fails, rerun
it after fixing the issue and before announcing the release or proceeding to
conda-forge.

PyPI publishing uses `skip-existing`, so rerunning the workflow after PyPI
succeeds can skip the already-uploaded wheel and source distribution and
continue to the GitHub Release step. Inspect the PyPI publish logs on reruns to
distinguish expected skipped files from unexpected duplicate uploads.
