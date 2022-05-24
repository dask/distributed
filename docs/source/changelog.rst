Changelog
=========

.. _v2022.05.1:

2022.05.1
---------

Released on May 24, 2022

New Features
^^^^^^^^^^^^
- Add HTTP API to scheduler (:pr:`6270`) `Matthew Murray`_
- Shuffle Service with Scheduler Logic (:pr:`6007`) `Matthew Rocklin`_

Enhancements
^^^^^^^^^^^^
- Follow-up on removing ``report`` and ``safe`` from ``Worker.close`` (:pr:`6423`) `Gabe Joseph`_
- Server close faster (:pr:`6415`) `Florian Jetter`_
- Disable HTTP API by default (:pr:`6420`) `Jacob Tomlinson`_
- Remove ``report`` and ``safe`` from ``Worker.close`` (:pr:`6363`) `Florian Jetter`_
- Allow deserialized plugins in ``register_scheduler_plugin`` (:pr:`6401`) `Matthew Rocklin`_
- ``WorkerState`` are different for different addresses (:pr:`6398`) `Florian Jetter`_
- Do not filter tasks before gathering data (:pr:`6371`) `crusaderky`_
- Remove worker reconnect (:pr:`6361`) `Gabe Joseph`_
- Add ``SchedulerPlugin.log_event handler`` (:pr:`6381`) `Matthew Rocklin`_
- Ensure occupancy tracking works as expected for long running tasks (:pr:`6351`) `Florian Jetter`_
- ``stimulus_id`` for all ``Instructions`` (:pr:`6347`) `crusaderky`_
- Refactor missing-data command (:pr:`6332`) `crusaderky`_
- Add ``idempotent`` to ``register_scheduler_plugin`` client (:pr:`6328`) `Alex Ford`_
- Add option to specify a scheduler address for workers to use (:pr:`5944`) `Enric Tejedor`_

Bug Fixes
^^^^^^^^^
- Remove stray ``breakpoint`` (:pr:`6417`) `Thomas Grainger`_
- Fix API JSON MIME type (:pr:`6397`) `Jacob Tomlinson`_
- Remove wrong ``assert`` in handle compute (:pr:`6370`) `Florian Jetter`_
- Ensure multiple clients can cancel their key without interference (:pr:`6016`) `Florian Jetter`_
- Fix ``Nanny`` shutdown assertion (:pr:`6357`) `Gabe Joseph`_
- Fix ``fail_hard`` for sync functions (:pr:`6269`) `Gabe Joseph`_
- Prevent infinite transition loops; more aggressive ``validate_state()`` (:pr:`6318`) `crusaderky`_
- Ensure cleanup of many GBs of spilled data on terminate (:pr:`6280`) `crusaderky`_
- Fix ``WORKER_ANY_RUNNING`` regression (:pr:`6297`) `Florian Jetter`_
- Race conditions from fetch to compute while AMM requests replica (:pr:`6248`) `Florian Jetter`_
- Ensure resumed tasks are not accidentally forgotten (:pr:`6217`) `Florian Jetter`_
- Do not allow closing workers to be awaited again (:pr:`5910`) `Florian Jetter`_

Deprecations
^^^^^^^^^^^^
- Move ``wait_for_signals`` to private module and deprecate ``distributed.cli.utils`` (:pr:`6367`) `Hendrik Makait`_

Documentation
^^^^^^^^^^^^^
- Fix typos and whitespace in ``worker.py`` (:pr:`6326`) `Hendrik Makait`_
- Fix link to memory trimming documentation (:pr:`6317`) `Marco Wolsza`_

Maintenance
^^^^^^^^^^^
- Make ``gen_test`` show up in VSCode test discovery (:pr:`6424`) `Gabe Joseph`_
- WSMR / ``deserialize_task`` (:pr:`6411`) `crusaderky`_
- Restore signal handlers after wait for signals is done (:pr:`6400`) `Thomas Grainger`_
- ``fail_hard`` should reraise (:pr:`6399`) `crusaderky`_
- Revisit tests mocking ``gather_dep`` (:pr:`6385`) `crusaderky`_
- Fix flaky ``test_in_flight_lost_after_resumed`` (:pr:`6372`) `Florian Jetter`_
- Restore install_signal_handlers due to downstream dependencies (:pr:`6366`) `Hendrik Makait`_
- Improve ``catch_unhandled_exceptions`` (:pr:`6358`) `Gabe Joseph`_
- Remove all invocations of ``IOLoop.run_sync`` from CLI (:pr:`6205`) `Hendrik Makait`_
- Remove ``transition-counter-max`` from config (:pr:`6349`) `crusaderky`_
- Use ``list`` comprehension in ``pickle_loads`` (:pr:`6343`) `jakirkham`_
- Improve ``ensure_memoryview`` test coverage & make minor fixes (:pr:`6333`) `jakirkham`_
- Remove leaking reference to ``workers`` from ``gen_cluster`` (:pr:`6337`) `Hendrik Makait`_
- Partial annotations for ``stealing.py`` (:pr:`6338`) `crusaderky`_
- Validate and debug state machine on ``handle_compute_task`` (:pr:`6327`) `crusaderky`_
- Bump pyupgrade and clean up ``# type: ignore`` (:pr:`6293`) `crusaderky`_
- ``gen_cluster`` to write to ``/tmp`` (:pr:`6335`) `crusaderky`_
- Transition table as a ``ClassVar`` (:pr:`6331`) `crusaderky`_
- Simplify ``ensure_memoryview`` test with ``array`` (:pr:`6322`) `jakirkham`_
- Refactor ``ensure_communicating`` (:pr:`6165`) `crusaderky`_
- Review scheduler annotations, part 2 (:pr:`6253`) `crusaderky`_
- Use ``w`` for ``writeable`` branch in ``pickle_loads`` (:pr:`6314`) `jakirkham`_
- Simplify frame handling in ``ws`` (:pr:`6294`) `jakirkham`_
- Use ``ensure_bytes`` from ``dask.utils`` (:pr:`6295`) `jakirkham`_
- Use ``ensure_memoryview`` in ``array`` deserialization (:pr:`6300`) `jakirkham`_
- Escape < > when generating Junit report (:pr:`6306`) `crusaderky`_
- Use ``codecs.decode`` to deserialize errors (:pr:`6274`) `jakirkham`_
- Minimize copying in ``maybe_compress`` & ``byte_sample`` (:pr:`6273`) `jakirkham`_
- Skip ``test_release_evloop_while_spilling`` on OSX (:pr:`6291`) `Florian Jetter`_
- Simplify logic in ``get_default_compression`` (:pr:`6260`) `jakirkham`_
- Cleanup old compression workarounds (:pr:`6259`) `jakirkham`_
- Re-enable NVML monitoring for WSL (:pr:`6119`) `Charles Blackmon-Luca`_


.. _v2022.05.0:

2022.05.0
---------

Released on May 2, 2022

Highlights
^^^^^^^^^^
This is a bugfix release for `this issue <https://github.com/dask/distributed/issues/6255>`_.

Enhancements
^^^^^^^^^^^^
- Handle ``writeable`` in ``buffer_callback`` (:pr:`6238`) `jakirkham`_
- Use ``.data`` with NumPy array allocation (:pr:`6242`) `jakirkham`_

Bug Fixes
^^^^^^^^^
- Close executor in event loop if interpreter is closing (:pr:`6256`) `Matthew Rocklin`_


.. _v2022.04.2:

2022.04.2
---------

Released on April 29, 2022

Enhancements
^^^^^^^^^^^^
- Unblock event loop while waiting for ``ThreadpoolExecutor`` to shut down (:pr:`6091`) `Florian Jetter`_
- ``RetireWorker`` policy is done if removed (:pr:`6234`) `Gabe Joseph`_
- Pause to disable dependency gathering (:pr:`6195`) `crusaderky`_
- Add ``EOFError`` to nanny ``multiprocessing.queue`` except list (:pr:`6213`) `Matthew Rocklin`_
- Re-interpret error in lost worker scenario (:pr:`6193`) `Matthew Rocklin`_
- Add Stimulus IDs to Scheduler (:pr:`6161`) `Florian Jetter`_
- Set a five minute TTL for Dask workers (:pr:`6200`) `Matthew Rocklin`_
- Add ``distributed.metrics.monotonic`` (:pr:`6181`) `crusaderky`_
- Send worker validation errors to scheduler and err on test completion (:pr:`6192`) `Matthew Rocklin`_
- Redesign worker exponential backoff on busy-gather (:pr:`6173`) `crusaderky`_
- Log all invalid worker transitions to scheduler (:pr:`6134`) `Matthew Rocklin`_
- Make Graph dashboard plot have invisible axes (:pr:`6149`) `Matthew Rocklin`_
- Remove ``Nanny`` ``auto_restart`` state (:pr:`6138`) `Matthew Rocklin`_

Bug Fixes
^^^^^^^^^
- Ensure scheduler events do not hold on to ``TaskState`` objects (:pr:`6226`) `Florian Jetter`_
- Allow pausing and choke event loop while spilling (:pr:`6189`) `crusaderky`_
- Do not use UUID in stealing (:pr:`6179`) `Florian Jetter`_
- Handle int worker names in info page (:pr:`6158`) `Brett Naul`_
- Fix ``psutil`` dio counters none (:pr:`6093`) `ungarj`_
- Join ``Nanny`` watch thread (:pr:`6146`) `Matthew Rocklin`_
- Improve logging when closing workers (:pr:`6129`) `Matthew Rocklin`_
- Avoid stack overflow in profiling (:pr:`6141`) `Matthew Rocklin`_
- Clean up ``SSHCluster`` if failure to start (:pr:`6130`) `Matthew Rocklin`_

Deprecations
^^^^^^^^^^^^
- Deprecate ``rpc`` synchronous context manager (:pr:`6171`) `Thomas Grainger`_

Documentation
^^^^^^^^^^^^^
- Update ``actors.rst`` (:pr:`6167`) `Scott Sievert`_

Maintenance
^^^^^^^^^^^
- Add ``fail_hard`` decorator for worker methods (:pr:`6210`) `Matthew Rocklin`_
- Do not require ``pytest_timeout`` (:pr:`6224`) `Florian Jetter`_
- Remove remaining ``run_sync`` calls from tests (:pr:`6196`) `Thomas Grainger`_
- Increase test timeout if debugger is running (:pr:`6218`) `Florian Jetter`_
- Do not list closes keyword in list of bullet points (:pr:`6219`) `Florian Jetter`_
- Harmonize (:pr:`6161`) and (:pr:`6173`) (:pr:`6207`) `crusaderky`_
- Xfail ``test_worker_death_timeout`` (:pr:`6186`) `Matthew Rocklin`_
- Use random port in ``test_dask_spec.py::test_text`` (:pr:`6187`) `Matthew Rocklin`_
- Mark all websocket tests as flaky (:pr:`6188`) `Matthew Rocklin`_
- Fix flaky ``test_dont_steal_long_running_tasks`` (:pr:`6197`) `crusaderky`_
- Cleanup names in stealing (:pr:`6185`) `Matthew Rocklin`_
- ``log_errors`` decorator (:pr:`6184`) `crusaderky`_
- Pass ``mypy`` validation on Windows (:pr:`6180`) `crusaderky`_
- Add ``locket`` as a dependency instead of vendoring (:pr:`6166`) `Michael Adkins`_
- Remove unittestmock for ``gather_dep`` and ``get_data_from_worker`` (:pr:`6172`) `Florian Jetter`_
- ``mypy`` tweaks (:pr:`6175`) `crusaderky`_
- Avoid easy deprecated calls to ``asyncio.get_event_loop()`` (:pr:`6170`) `Thomas Grainger`_
- Fix flaky ``test_cancel_fire_and_forget`` (:pr:`6099`) `crusaderky`_
- Remove deprecated code (:pr:`6144`) `Matthew Rocklin`_
- Update link of test badge (:pr:`6154`) `Florian Jetter`_
- Remove legacy state mappings (:pr:`6145`) `Matthew Rocklin`_
- Fix ``test_worker_waits_for_scheduler`` (:pr:`6155`) `Matthew Rocklin`_
- Disallow leaked threads on windows (:pr:`6152`) `Thomas Grainger`_
- Review annotations and docstrings in ``scheduler.py``, part 1 (:pr:`6132`) `crusaderky`_
- Relax ``test_asyncprocess.py::test_simple`` (:pr:`6150`) `Matthew Rocklin`_
- Drop ``cast`` ing which is effectively a no-op (:pr:`6101`) `jakirkham`_
- Mark tests that use a specific port as flaky (:pr:`6139`) `Matthew Rocklin`_
- AMM Suggestion namedtuples (:pr:`6108`) `crusaderky`_

.. _v2022.04.1:

2022.04.1
---------

Released on April 15, 2022

New Features
^^^^^^^^^^^^
- Add ``KillWorker`` Plugin (:pr:`6126`) `Matthew Rocklin`_

Enhancements
^^^^^^^^^^^^
- Sort worker list in info pages (:pr:`6135`) `Matthew Rocklin`_
- Add back ``Worker.transition_fetch_missing`` (:pr:`6112`) `Matthew Rocklin`_
- Log state machine events (:pr:`6092`) `crusaderky`_
- Migrate ``ensure_executing`` transitions to new ``WorkerState`` event mechanism - part 1 (:pr:`6003`) `crusaderky`_
- Migrate ``ensure_executing`` transitions to new ``WorkerState`` event mechanism - part 2 (:pr:`6062`) `crusaderky`_
- Annotate worker transitions to error (:pr:`6012`) `crusaderky`_

Bug Fixes
^^^^^^^^^
- Avoid transitioning from memory/released to missing in worker (:pr:`6123`) `Matthew Rocklin`_
- Don't try to reconnect client on interpreter shutdown (:pr:`6120`) `Matthew Rocklin`_
- Wrap UCX init warnings in importable functions (:pr:`6121`) `Charles Blackmon-Luca`_
- Cancel asyncio tasks on worker close (:pr:`6098`) `crusaderky`_
- Avoid port collisions when defining port ranges (:pr:`6054`) `crusaderky`_

Maintenance
^^^^^^^^^^^
- Avoid intermittent failure in ``test_cancel_fire_and_forget`` (:pr:`6131`) `Matthew Rocklin`_
- Ignore ``bokeh`` warning in pytest (:pr:`6127`) `Matthew Rocklin`_
- Start uncythonization (:pr:`6104`) `Martin Durant`_
- Avoid redundant cleanup fixture in ``gen_test`` tests (:pr:`6118`) `Thomas Grainger`_
- Move ``comm.close`` to finally in ``test_comms`` (:pr:`6109`) `Florian Jetter`_
- Use ``async`` with ``Server`` in ``test_core.py`` (:pr:`6100`) `Matthew Rocklin`_
- Elevate warnings to errors in the test suite (:pr:`6094`) `Thomas Grainger`_
- Add ``urllib3`` to nightly conda builds (:pr:`6102`) `James Bourbeau`_
- Drop Blosc (:pr:`6027`) `Matthew Rocklin`_
- Robust ``test_get_returns_early`` (:pr:`6090`) `Florian Jetter`_
- Overhaul ``test_priorities.py`` (:pr:`6077`) `crusaderky`_
- Remove ``pytest-asyncio`` (:pr:`6063`) `Thomas Grainger`_
- Clean up usage around plain ``rpc`` (:pr:`6082`) `Florian Jetter`_
- Drop OSX builds for Python 3.9 (:pr:`6073`) `Florian Jetter`_
- Bump periods in ``utils_test.wait_for`` (:pr:`6081`) `Florian Jetter`_
- Check for ucx-py nightlies when updating gpuCI (:pr:`6006`) `Charles Blackmon-Luca`_
- Type annotations for ``profile.py`` (:pr:`6067`) `crusaderky`_
- Fix flaky ``test_worker_time_to_live`` (:pr:`6061`) `crusaderky`_
- Fix flaky ``test_as_completed_async_for_cancel`` (:pr:`6072`) `crusaderky`_
- Fix regression in ``test_weakref_cache`` (:pr:`6033`) `crusaderky`_
- Trivial fix to ``test_nanny_worker_port_range`` (:pr:`6070`) `crusaderky`_
- Drop deprecated ``tornado.netutil.ExecutorResolver`` (:pr:`6031`) `Thomas Grainger`_
- Delete ``asyncio.py`` (:pr:`6066`) `Thomas Grainger`_
- Tweak conda environment files (:pr:`6037`) `crusaderky`_
- Harden ``test_abort_execution_to_fetch`` and more (:pr:`6026`) `crusaderky`_
- Fix ``test_as_completed_with_results_no_raise`` and name ``comm`` (:pr:`6042`) `Matthew Rocklin`_
- Use more robust limits in ``test_worker_memory`` (:pr:`6055`) `Florian Jetter`_

.. _v2022.04.0:

2022.04.0
---------

Released on April 1, 2022

New Features
^^^^^^^^^^^^
- Add Python 3.10 support (:pr:`5952`) `Thomas Grainger`_
- New cluster dump utilities (:pr:`5920`) `Simon Perkins`_
- New ``ClusterDump`` ``SchedulerPlugin`` for dumping cluster state on close (:pr:`5983`) `Simon Perkins`_
- Track Event Loop intervals in dashboard plot (:pr:`5964`) `Matthew Rocklin`_
- ``ToPickle`` - ``Unpickle`` on the Scheduler (:pr:`5728`) `Mads R. B. Kristensen`_

Enhancements
^^^^^^^^^^^^
- Retry on transient error codes in ``preload`` (:pr:`5982`) `Matthew Rocklin`_
- More idiomatic ``mypy`` configuration (:pr:`6022`) `crusaderky`_
- Name extensions and enable extension heartbeats (:pr:`5957`) `Matthew Rocklin`_
- Better error message on misspelled executor annotation (:pr:`6009`) `crusaderky`_
- Clarify that SchedulerPlugin must be subclassed (:pr:`6008`) `crusaderky`_
- Remove duplication from stealing (:pr:`5787`) `Duncan McGregor`_
- Remove cache in ``iscoroutinefunction`` to avoid holding on to refs (:pr:`5985`) `Florian Jetter`_
- Add title to individual plots (:pr:`5967`) `Matthew Rocklin`_
- Specify average in timeseries titles (:pr:`5974`) `Matthew Rocklin`_

Bug Fixes
^^^^^^^^^
- Do not catch ``CancelledError`` in ``CommPool`` (:pr:`6005`) `Florian Jetter`_

Deprecations
^^^^^^^^^^^^
- Remove ``distributed._ipython_utils`` and dependents (:pr:`6036`) `Thomas Grainger`_
- Remove support for PyPy (:pr:`6029`) `James Bourbeau`_
- Drop runtime dependency to setuptools (:pr:`6017`) `crusaderky`_
- Remove heartbeats from events (:pr:`5989`) `Matthew Rocklin`_

Documentation
^^^^^^^^^^^^^
- Mention default value of Client's ``timeout`` (:pr:`5933`) `Eric Engestrom`_
- Update celery and other outdated 3rd party URLs (:pr:`5988`) `Thomas Grainger`_

Maintenance
^^^^^^^^^^^
- Improve ``test_hardware`` test (:pr:`6039`) `Matthew Rocklin`_
- Short variant of test_report.html (:pr:`6034`) `crusaderky`_
- Make ``test_reconnect`` async (:pr:`6000`) `Matthew Rocklin`_
- Update gpuCI ``RAPIDS_VER`` to ``22.06`` (:pr:`5962`)
- Add tiny test for ``ToPickle`` (:pr:`6021`) `Matthew Rocklin`_
- Remove ``check_python_3`` (broken with ``click>=8.1.0``) (:pr:`6018`) `Thomas Grainger`_
- Fix black in CI (:pr:`6019`) `crusaderky`_
- Add a hardware benchmark to test memory, disk, and network bandwidths (:pr:`5966`) `Matthew Rocklin`_
- Relax variable ``test_race`` (:pr:`5993`) `Matthew Rocklin`_
- Skip ``dask-ssh`` tests without ``paramiko`` (:pr:`5907`) `Elliott Sales de Andrade`_
- Remove ``test_restart_sync_no_center`` (:pr:`5994`) `Matthew Rocklin`_
- Set lower tick frequency in tests (:pr:`5977`) `Matthew Rocklin`_
- Catch ``NotADirectoryError`` in ``SafeTemporaryDirectory`` (:pr:`5984`) `Florian Jetter`_
- Fix flaky ``test_weakref_cache`` (:pr:`5978`) `crusaderky`_
- Fixup ``test_worker_doesnt_await_task_completion`` (:pr:`5979`) `Matthew Rocklin`_
- Use broader range in ``test_nanny_worker_port_range`` (:pr:`5980`) `Matthew Rocklin`_
- Use ``tempfile`` directory in cluster ``fixture`` (:pr:`5825`) `Florian Jetter`_
- Drop ``setuptools`` from ``distributed`` recipe (:pr:`5963`) `jakirkham`_


.. _v2022.03.0:

2022.03.0
---------

Released on March 18, 2022

New Features
^^^^^^^^^^^^
- Support dumping cluster state to URL (:pr:`5863`) `Gabe Joseph`_

Enhancements
^^^^^^^^^^^^
- Prevent data duplication on unspill (:pr:`5936`) `crusaderky`_
- Encapsulate spill buffer and memory_monitor (:pr:`5904`) `crusaderky`_
- Drop ``pkg_resources`` in favour of ``importlib.metadata`` (:pr:`5923`) `Thomas Grainger`_
- Worker State Machine refactor: redesign ``TaskState`` and scheduler messages (:pr:`5922`) `crusaderky`_
- Tidying of OpenSSL 1.0.2/Python 3.9 (and earlier) handling (:pr:`5854`) `jakirkham`_
- ``zict`` type annotations (:pr:`5905`) `crusaderky`_
- Add key to compute failed message (:pr:`5928`) `Florian Jetter`_
- Change default log format to include timestamp (:pr:`5897`) `Florian Jetter`_
- Improve type annotations in worker.py (:pr:`5814`) `crusaderky`_

Bug Fixes
^^^^^^^^^
- Fix ``progress_stream`` teardown (:pr:`5823`) `Thomas Grainger`_
- Handle concurrent or failing handshakes in ``InProcListener`` (:pr:`5903`) `Thomas Grainger`_
- Make ``log_event`` threadsafe (:pr:`5946`) `Gabe Joseph`_

Documentation
^^^^^^^^^^^^^
- Fixes to documentation regarding plugins (:pr:`5940`) `crendoncoiled`_
- Some updates to scheduling policies docs (:pr:`5911`) `Gabe Joseph`_

Maintenance
^^^^^^^^^^^
- Fix ``test_nanny_worker_port_range`` hangs on Windows (:pr:`5956`) `crusaderky`_
- (REVERTED) Unblock event loop while waiting for ThreadpoolExecutor to shut down (:pr:`5883`) `Florian Jetter`_
- Revert :pr:`5883` (:pr:`5961`) `crusaderky`_
- Invert ``event_name`` check in ``test-report`` job (:pr:`5959`) `jakirkham`_
- Only run ``gh-pages`` workflow on ``dask/distributed`` (:pr:`5942`) `jakirkham`_
- ``absolufy-imports`` - No relative imports - PEP8 (:pr:`5924`) `Florian Jetter`_
- Fix ``track_features`` for distributed pre-releases (:pr:`5927`) `Charles Blackmon-Luca`_
- Xfail ``test_submit_different_names`` (:pr:`5916`) `Florian Jetter`_
- Fix ``distributed`` pre-release's ``distributed-impl`` constraint (:pr:`5867`) `Charles Blackmon-Luca`_
- Mock process memory readings in test_worker.py (v2) (:pr:`5878`) `crusaderky`_
- Drop unused ``_round_robin`` global variable (:pr:`5881`) `jakirkham`_
- Add GitHub URL for PyPi (:pr:`5886`) `Andrii Oriekhov`_
- Mark ``xfail`` COMPILED tests ``skipif`` instead (:pr:`5884`) `Florian Jetter`_


.. _v2022.02.1:

2022.02.1
---------

Released on February 25, 2022

New Features
^^^^^^^^^^^^
- Add the ability for ``Client`` to run ``preload`` code (:pr:`5773`) `Bryan W. Weber`_

Enhancements
^^^^^^^^^^^^
- Optionally use NumPy to allocate buffers (:pr:`5750`) `jakirkham`_
- Add git hash to ``distributed-impl`` version (:pr:`5865`) `Charles Blackmon-Luca`_
- Immediately raise exception when trying to connect to a closed cluster (:pr:`5855`) `Florian Jetter`_
- Lazily get ``dask`` version information (:pr:`5822`) `Thomas Grainger`_
- Remove the requirements to add ``comm`` to every handler (:pr:`5820`) `Florian Jetter`_
- Raise on unclosed comms in ``check_instances`` (:pr:`5836`) `Florian Jetter`_
- Python 3.8 f-strings (:pr:`5828`) `crusaderky`_
- Constrained spill (:pr:`5543`) `Naty Clementi`_
- Measure actual spilled bytes, not output of ``sizeof()`` (:pr:`5805`) `crusaderky`_
- Remove redundant ``str()`` conversions (:pr:`5810`) `crusaderky`_
- Cluster dump now excludes ``run_spec`` by default (:pr:`5803`) `Florian Jetter`_
- Dump more objects with ``dump_cluster_state``  (:pr:`5806`) `crusaderky`_
- Do not connect to any sockets on import (:pr:`5808`) `Florian Jetter`_

Bug Fixes
^^^^^^^^^
- Avoid deadlock when two tasks are concurrently waiting for an unresolved ``ActorFuture`` (:pr:`5709`) `Thomas Grainger`_

Deprecations
^^^^^^^^^^^^
- Drop Python 3.7 (:pr:`5683`) `James Bourbeau`_
- Remove support for UCX < 1.11.1 (:pr:`5859`) `Peter Andreas Entschev`_

Documentation
^^^^^^^^^^^^^
- Fix typo in memory types documentation relative links (:pr:`5845`) `James Bourbeau`_
- Document and test spill->target hysteresis cycle (:pr:`5813`) `crusaderky`_

Maintenance
^^^^^^^^^^^
- Fix flaky ``test_remove_replicas_while_computing`` (:pr:`5860`) `crusaderky`_
- Fix time based ``test_assert_worker_story_malformed_story`` parameterize (:pr:`5856`) `Thomas Grainger`_
- Remove ``xfail`` from ``test_no_unnecessary_imports_on_worker`` (:pr:`5862`) `crusaderky`_
- Start building pre-releases with cythonized scheduler (:pr:`5831`) `Charles Blackmon-Luca`_
- Do not mark tests ``xfail`` if they don't come up in time (:pr:`5824`) `Florian Jetter`_
- Use ``gen_cluster`` where possible in ``test_dask_worker.py`` (:pr:`5842`) `Florian Jetter`_
- Generate junit report when ``pytest-timeout`` kills ``pytest`` (:pr:`5832`) `crusaderky`_
- Decrease timeout-minutes for GHA jobs (:pr:`5837`) `Florian Jetter`_
- Fix some timeouts (:pr:`5647`) `Florian Jetter`_
- Bump pre-release version to be greater than stable releases (:pr:`5816`) `Charles Blackmon-Luca`_
- Do not run schedule jobs on forks (:pr:`5821`) `Florian Jetter`_
- Remove ``pillow<9`` pin in CI (:pr:`5775`) `Thomas Grainger`_
- Show scheduled test runs in report (:pr:`5812`) `Ian Rose`_
- Add obvious exclusions with pragma statement (:pr:`5801`) `Sarah Charlotte Johnson`_
- Add coverage exclusions for cli files (:pr:`5800`) `Sarah Charlotte Johnson`_
- Add pragma statements (:pr:`5749`) `Sarah Charlotte Johnson`_
- Remove pragma: no cover from ``distributed.cli.dask_ssh`` (:pr:`5809`) `Thomas Grainger`_
- Add pragma - worker.py, client.py, stealing.py (:pr:`5827`) `Sarah Charlotte Johnson`_
- Relax ``distributed`` / ``dask-core`` dependencies for pre-releases (:pr:`5802`) `Charles Blackmon-Luca`_
- Remove ``test_ucx_config_w_env_var`` flaky condition (:pr:`5765`) `Peter Andreas Entschev`_


.. _v2022.02.0:

2022.02.0
---------

Released on February 11, 2022

Enhancements
^^^^^^^^^^^^
- Update ``client.scheduler_info`` in ``wait_for_workers`` (:pr:`5785`) `Matthew Rocklin`_
- Increase robustness to ``TimeoutError`` during connect (:pr:`5096`) `Florian Jetter`_
- Respect ``KeyboardInterrupt`` in ``sync`` (:pr:`5758`) `Thomas Grainger`_
- Add workflow / recipe to generate Dask/distributed pre-releases (:pr:`5636`) `Charles Blackmon-Luca`_
- Review ``Scheduler`` / ``Worker`` display repr (:pr:`5746`) `crusaderky`_
- AMM: Graceful Worker Retirement (:pr:`5381`) `crusaderky`_
- AMM: tentatively stabilize flaky tests around worker pause (:pr:`5735`) `crusaderky`_
- AMM: speed up and stabilize test_memory (:pr:`5737`) `crusaderky`_
- Defer pandas import on worker in P2P shuffle (:pr:`5695`) `Gabe Joseph`_

Bug Fixes
^^^^^^^^^
- Fix for ``distributed.worker.memory.target=False`` and ``spill=0.7`` (:pr:`5788`) `crusaderky`_
- Transition ``flight`` to ``missing`` if no ``who_has`` (:pr:`5653`) `Florian Jetter`_

Deprecations
^^^^^^^^^^^^
- Remove deprecated ``ncores`` (:pr:`5780`) `crusaderky`_
- Deprecate registering plugins by class (:pr:`5699`) `Thomas Grainger`_
- Deprecate ``--nprocs`` option for ``dask-worker`` CLI (:pr:`5641`) `Bryan W. Weber`_


Documentation
^^^^^^^^^^^^^
- Fix imbalanced backticks (:pr:`5784`) `Matthias Bussonnier`_

Maintenance
^^^^^^^^^^^
- xfail ``test_worker_reconnects_mid_compute`` (:pr:`5797`) `crusaderky`_
- Fix linting CI build (:pr:`5794`) `James Bourbeau`_
- Update ``pre-commit`` versions (:pr:`5782`) `James Bourbeau`_
- Reactivate ``pytest_resourceleaks`` (:pr:`5771`) `crusaderky`_
- Set test assumption for ``test_client_timeout`` (:pr:`5790`) `Florian Jetter`_
- Remove client timeout from ``test_ucx_config_w_env_var`` (:pr:`5792`) `Florian Jetter`_
- Remove ``test_failed_worker_without_warning`` (:pr:`5789`) `Florian Jetter`_
- Fix longitudinal report (:pr:`5783`) `Ian Rose`_
- Fix flaky ``test_robust_to_bad_sizeof_estimates`` (:pr:`5753`) `crusaderky`_
- Revert "Pin coverage to 6.2 (:pr:`5716`)" (:pr:`5770`) `Thomas Grainger`_
- Trigger test runs periodically to increases failure statistics (:pr:`5769`) `Florian Jetter`_
- More fault tolerant test report (:pr:`5732`) `Ian Rose`_
- Pin ``pillow<9`` to work around ``torch`` incompatability (:pr:`5755`) `Thomas Grainger`_
- Overhaul ``check_process_leak`` (:pr:`5739`) `crusaderky`_
- Fix flaky ``test_exit_callback test`` (:pr:`5713`) `Jim Crist-Harif`_
- Generate tests summary (:pr:`5710`) `crusaderky`_
- Upload different architectured pre-releases separately (:pr:`5741`) `Charles Blackmon-Luca`_
- Ignore non-test directories (:pr:`5720`) `Gabe Joseph`_
- Bump gpuCI ``PYTHON_VER`` to 3.9 (:pr:`5738`) `Charles Blackmon-Luca`_
- Regression: threads noted down before they start (:pr:`5796`) `crusaderky`_


.. _v2022.01.1:

2022.01.1
---------

Released on January 28, 2022

New Features
^^^^^^^^^^^^
- P2P shuffle skeleton (:pr:`5520`) `Gabe Joseph`_

Enhancements
^^^^^^^^^^^^
- Fix ``<Task pending name='...' coro=<Client._handle_report()>`` (:pr:`5721`) `Thomas Grainger`_
- Add ``distributed.client.security-loader`` config (:pr:`5693`) `Jim Crist-Harif`_
- Avoid ``Client._handle_report`` cancelling itself on ``Client._close`` (:pr:`5672`) `Thomas Grainger`_
- Paused workers shouldn't steal tasks (:pr:`5665`) `crusaderky`_
- Add option for timestamps from output of ``Node.get_logs`` (:pr:`4932`) `Charles Blackmon-Luca`_
- Don't use ``time.time()`` or ``IOLoop.time()`` (:pr:`5661`) `crusaderky`_

Bug Fixes
^^^^^^^^^
- Raise plugin exceptions on ``Worker.start()`` (:pr:`4298`) `Peter Andreas Entschev`_

Documentation
^^^^^^^^^^^^^
- Fixing docstrings (:pr:`5696`) `Julia Signell`_
- Fix typo in ``Client.run`` docstring (:pr:`5687`) `Thomas Grainger`_
- Update ``client.py`` docstrings (:pr:`5670`) `Tim Harris`_

Maintenance
^^^^^^^^^^^
- Skip shuffle tests if ``pandas`` / ``dask.dataframe`` not installed (:pr:`5730`) `James Bourbeau`_
- Improve test coverage (:pr:`5655`) `Sarah Charlotte Johnson`_
- Test report improvements (:pr:`5714`) `Ian Rose`_
- P2P shuffle: ignore row order in tests (:pr:`5706`) `Gabe Joseph`_
- Fix flaky ``test_no_reconnect[--no-nanny]`` (:pr:`5686`) `Thomas Grainger`_
- Pin coverage to 6.2 (:pr:`5716`) `Thomas Grainger`_
- Check for new name of timeouts artifact and be more fault tolerant (:pr:`5707`) `Ian Rose`_
- Revisit rebalance unit tests (:pr:`5697`) `crusaderky`_
- Update comment in ``rearrange_by_column_p2p`` (:pr:`5701`) `James Bourbeau`_
- Update gpuCI ``RAPIDS_VER`` to ``22.04`` (:pr:`5676`)
- Fix groupby test after meta requirements got stricter in Dask PR#8563 (:pr:`5694`) `Julia Signell`_
- Fix flaky ``test_close_gracefully`` and ``test_lifetime`` (:pr:`5677`) `crusaderky`_
- Fix flaky ``test_workspace_concurrency`` (:pr:`5690`) `crusaderky`_
- Fix flaky ``test_shuffle_extension.py::test_get_partition`` (:pr:`5689`) `Gabe Joseph`_
- Fix flaky ``test_dump_cluster_unresponsive_remote_worker`` (:pr:`5679`) `crusaderky`_
- Dump cluster state on all test failures (:pr:`5674`) `crusaderky`_
- Update license format (:pr:`5652`) `James Bourbeau`_
- Fix flaky ``test_drop_with_paused_workers_with_running_tasks_3_4`` (:pr:`5673`) `crusaderky`_
- Do not raise an exception if the GitHub token cannot be found (:pr:`5668`) `Florian Jetter`_


.. _v2022.01.0:

2022.01.0
---------

Released on January 14, 2022

New Features
^^^^^^^^^^^^
- Task group stacked area chart (:pr:`5320`) `Ian Rose`_
- Support configuring TLS min/max version (:pr:`5594`) `Jim Crist-Harif`_
- Use asyncio for TCP/TLS comms (:pr:`5450`) `Jim Crist-Harif`_

Enhancements
^^^^^^^^^^^^
- Close comm on ``CancelledError`` (:pr:`5656`) `crusaderky`_
- Don't drop from the only running worker (:pr:`5626`) `crusaderky`_
- Transfer priority (:pr:`5625`) `crusaderky`_
- Add RPC call for getting task prefixes (:pr:`5617`) `Benjamin Zaitlen`_
- Long running occupancy (:pr:`5395`) `Florian Jetter`_
- Handle errors on individual workers in ``run``/``broadcast`` (:pr:`5590`) `crusaderky`_
- Allow work stealing in case there are heterogeneous resources for thief and victim (:pr:`5573`) `Florian Jetter`_
- Disable NVML monitoring on WSL (:pr:`5568`) `Charles Blackmon-Luca`_

Bug Fixes
^^^^^^^^^
- Ensure uniqueness of steal stimulus ID (:pr:`5620`) `Florian Jetter`_
- Fix ``KeyError: 'startstops'`` in performance report (:pr:`5608`) `Gabe Joseph`_
- Story timestamps can be slightly in the future (:pr:`5612`) `crusaderky`_
- Prevent ``RecursionError`` in ``Worker._to_dict`` (:pr:`5591`) `crusaderky`_
- Ensure distributed can be imported in thread (:pr:`5593`) `Jim Crist-Harif`_

Documentation
^^^^^^^^^^^^^
- Fix changelog section hyperlinks (:pr:`5638`) `Aneesh Nema`_
- Fix typo in ``unpublish_dataset`` example invocation (:pr:`5615`) `Deepyaman Datta`_
- Fix typo in test report badge in ``README`` (:pr:`5586`) `James Bourbeau`_

Maintenance
^^^^^^^^^^^
- Cosmetic changes to ``distributed.comm`` (:pr:`5657`) `crusaderky`_
- Consolidate broken comm testing utilities (:pr:`5654`) `James Bourbeau`_
- Fix concurrency assumptions for ``test_worker_reconnects_mid_compute`` (:pr:`5623`) `Florian Jetter`_
- Handle Bokeh 3.0 CDSView change (:pr:`5643`) `Bryan Van de Ven`_
- Use ``packaging`` rather than ``distutils`` to get version (:pr:`5624`) `Julia Signell`_
- XFAIL tls explicit comm close test on py3.7 (:pr:`5639`) `Jim Crist-Harif`_
- Mark some additional ucx-py tests for GPU (:pr:`5603`) `Charles Blackmon-Luca`_
- Rename ``ensure_default_get`` and add test (:pr:`5609`) `Naty Clementi`_
- Remove ``render_mode`` ``kwarg`` from ``boekh`` ``LabelSets`` (:pr:`5616`) `Garry O'Donnell`_
- Add lambda support to ``assert_worker_story`` (:pr:`5618`) `crusaderky`_
- Ignore file not found warning for timeout artifact (:pr:`5619`) `Florian Jetter`_
- Improved cluster state dump in ``@gen_cluster`` (:pr:`5592`) `crusaderky`_
- Work around SSL failures on MacOS CI (:pr:`5606`) `crusaderky`_
- Bump gpuCI ``CUDA_VER`` to 11.5 (:pr:`5604`) `Charles Blackmon-Luca`_
- ``assert_worker_story`` (:pr:`5598`) `crusaderky`_
- ``distributed.versions`` code refresh (:pr:`5600`) `crusaderky`_
- Updates to gpuCI and ``test_ucx_config_w_env_var`` (:pr:`5595`) `James Bourbeau`_
- Replace blacklist/whitelist with blocklist/allowlist (:pr:`5589`) `crusaderky`_
- Distributed test report (:pr:`5583`) `Ian Rose`_
- AMM: cosmetic tweaks (:pr:`5584`) `crusaderky`_


.. _v2021.12.0:

2021.12.0
---------

Released on December 10, 2021

Enhancements
^^^^^^^^^^^^
- Support pytest fixures and parametrize with ``gen_test`` (:pr:`5532`) `Fábio Rosado`_
- Allow idempotent scheduler plugins to be registered via the RPC (:pr:`5545`) `Jacob Tomlinson`_
- AMM logging (:pr:`5530`) `crusaderky`_
- Raise error if ``asyncssh`` isn't installed when using ``SSHCluster`` (:pr:`5535`) `Fábio Rosado`_
- Allow ``None`` in UCX configuration schema (:pr:`5534`) `Fábio Rosado`_
- Add ``distributed.comm.ucx.create-cuda-context`` config (:pr:`5526`) `Peter Andreas Entschev`_

Bug fixes
^^^^^^^^^
- Allow unknown tasks to be stolen (:pr:`5572`) `Florian Jetter`_
- Further ``RecursionError`` fixes in ``recursive_to_repr`` (:pr:`5579`) `crusaderky`_
- Revisit ``recursive_to_dict`` (:pr:`5557`) `crusaderky`_
- Handle ``UCXUnreachable`` exception (:pr:`5556`) `Peter Andreas Entschev`_

Documentation
^^^^^^^^^^^^^
- Separate ``Coordination`` section in API docs (:pr:`5412`) `Gabe Joseph`_
- Improved documentation for processing state and paused workers (:pr:`4985`) `Maximilian Roos`_
- Fix typo in ``TaskGroupGraph.update_layout`` comment (:pr:`5536`) `Hristo Georgiev`_
- Update documentation for ``register_worker_plugin`` (:pr:`5533`) `crusaderky`_

Maintenance
^^^^^^^^^^^
- Mark ``test_gpu_monitoring_recent`` as flaky (:pr:`5540`) `Peter Andreas Entschev`_
- Await worker arrival in SSH ``test_nprocs`` (:pr:`5575`) `James Bourbeau`_
- AMM: Test that acquire-replicas of a task already in flight is a no-op (:pr:`5566`) `crusaderky`_
- Make sure artifacts are tagged with CI partition so they don't race and overwrite each other (:pr:`5571`) `Ian Rose`_
- Minor refactoring and commentary in worker state machine (:pr:`5563`) `Florian Jetter`_
- Fix ``test_ucx_unreachable`` on UCX < 1.12 (:pr:`5562`) `Peter Andreas Entschev`_
- Bump Bokeh min version to 2.1.1 (:pr:`5548`) `Bryan Van de Ven`_
- Update ``gen_test`` tests to be more robust (:pr:`5551`) `James Bourbeau`_
- Skip ``test_ucx_unreachable`` if ``UCXUnreachable`` is unavailable (:pr:`5560`) `Peter Andreas Entschev`_
- Update gpuCI ``RAPIDS_VER`` to ``22.02`` (:pr:`5544`)
- Add workflow to automate gpuCI updates (:pr:`5541`) `Charles Blackmon-Luca`_
- Actually support ``uvloop`` in distributed (:pr:`5531`) `Jim Crist-Harif`_
- Standardize UCX config separator to ``-`` (:pr:`5539`) `Peter Andreas Entschev`_


.. _v2021.11.2:

2021.11.2
---------

Released on November 19, 2021

- Ensure cancelled error transition can properly release a key (:pr:`5528`) `Florian Jetter`_
- Refactor release key (:pr:`5507`) `Florian Jetter`_
- Fix deadlock caused by an erred task (executing->cancelled->error) (:pr:`5503`) `Florian Jetter`_
- Resolve ``KeyError``-related deadlock (:pr:`5525`) `Florian Jetter`_
- Remove extra quotation in worker failure docs (:pr:`5518`) `James Bourbeau`_
- Ensure ``safe_sizeof`` warning is accurate (:pr:`5519`) `James Bourbeau`_
- Visualize cluster-wide memory usage over time (:pr:`5477`) `crusaderky`_
- AMM: redesign start/stop methods (:pr:`5476`) `crusaderky`_
- Preserve ``contextvars`` during comm offload (:pr:`5486`) `Gabe Joseph`_
- Deserialization: zero-copy merge subframes when possible (:pr:`5208`) `Gabe Joseph`_
- Add support for multiple workers per SSH connection (:pr:`5506`) `Jacob Tomlinson`_
- Client method to dump cluster state (:pr:`5470`) `Florian Jetter`_


.. _v2021.11.1:

2021.11.1
---------

Released on November 8, 2021

- Revert "Avoid multiple blocking calls by gathering UCX frames" (:pr:`5505`) `Peter Andreas Entschev`_


.. _v2021.11.0:

2021.11.0
---------

Released on November 5, 2021

- Fix ``cluster_info`` sync handling (:pr:`5488`) `Jim Crist-Harif`_
- Serialization family to preserve headers of the underlying dumps functions (:pr:`5380`) `Mads R. B. Kristensen`_
- Point users to Discourse (:pr:`5489`) `James Bourbeau`_
- Avoid multiple blocking calls by gathering UCX frames (:pr:`5487`) `Peter Andreas Entschev`_
- Update all UCX tests to use ``asyncio`` marker (:pr:`5484`) `Peter Andreas Entschev`_
- Register UCX close callback (:pr:`5474`) `Peter Andreas Entschev`_
- Use older version of ``pynvml.nvmlDeviceGetComputeRunningProcesses`` (:pr:`5469`) `Jacob Tomlinson`_
- Check for Futures from the wrong ``Client`` in ``gather`` (:pr:`5468`) `Gabe Joseph`_
- Fix ``performance_report`` when used with ``%%time`` or ``%%timeit`` magic (:pr:`5463`) `Erik Welch`_
- Scatter and replicate to avoid paused workers (:pr:`5441`) `crusaderky`_
- AMM to avoid paused workers (:pr:`5440`) `crusaderky`_
- Update changelog with ``LocalCluster`` host security note (:pr:`5462`) `Jim Crist-Harif`_


.. _v2021.10.0:

2021.10.0
---------

Released on October 22, 2021

.. note::

    This release fixed a potential security vulnerability relating to
    single-machine Dask clusters. Clusters started with
    ``dask.distributed.LocalCluster`` or ``dask.distributed.Client()`` (which
    defaults to using ``LocalCluster``) would mistakenly configure their
    respective Dask workers to listen on external interfaces (typically with a
    randomly selected high port) rather than only on ``localhost``. A Dask
    cluster created using this method AND running on a machine that has these
    ports exposed could be used by a sophisticated attacker to enable remote
    code execution.  Users running on machines with standard firewalls in place
    should not be affected. This vulnerability is documented in `CVE-2021-42343
    <https://attackerkb.com/topics/oL1UXQct5f/cve-2021-42343>`__, and is fixed
    in this release (:pr:`5427`). Thanks to Jean-Pierre van Riel for
    discovering and reporting the issue.

- Ensure resumed flight tasks are still fetched (:pr:`5426`) `Florian Jetter`_
- AMM high level documentation (:pr:`5456`) `crusaderky`_
- Provide stack for suspended coro in test timeout (:pr:`5446`) `Florian Jetter`_
- Handle ``UCXNotConnected`` error (:pr:`5449`) `Peter Andreas Entschev`_
- Don't schedule tasks to paused workers (:pr:`5431`) `crusaderky`_
- Use ``pip install .`` instead of calling ``setup.py`` (:pr:`5442`) `Matthias Bussonnier`_
- Increase latency for stealing (:pr:`5390`) `Florian Jetter`_
- Type annotations for ``Worker`` and ``gen_cluster`` (:pr:`5438`) `crusaderky`_
- Ensure reconnecting workers do not loose required data (:pr:`5436`) `Florian Jetter`_
- Mark ``test_gather_dep*`` as ``xfail`` (:pr:`5432`) `crusaderky`_
- Remove ``zict``-related skips (:pr:`5429`) `James Bourbeau`_
- Pass ``host`` through ``LocalCluster`` to workers (:pr:`5427`) `Jim Crist-Harif`_
- Fixes ``async`` warnings in UCX tests (:pr:`5396`) `Peter Andreas Entschev`_
- Resolve work stealing deadlock caused by race in ``move_task_confirm`` (:pr:`5379`) `Florian Jetter`_
- Add scroll to dashboard dropdown (:pr:`5418`) `Jacob Tomlinson`_
- Fix regression where unknown tasks were allowed to be stolen (:pr:`5392`) `Florian Jetter`_
- Enable ``mypy`` in CI 2/2 (:pr:`5348`) `crusaderky`_
- Rewrite ``test_client_timeout`` (:pr:`5397`) `crusaderky`_
- Simple ``SSHCluster`` example (:pr:`5349`) `Ray Bell`_
- Do not attempt to fetch keys which are no longer in flight (:pr:`5160`) `Florian Jetter`_
- Revisit ``Scheduler.add_plugin`` / ``Scheduler.remove_plugin`` (:pr:`5394`) `crusaderky`_
- Fix flaky ``test_WorkerPlugin_overwrite`` (:pr:`5398`) `crusaderky`_
- Active Memory Manager to use bulk comms (:pr:`5357`) `crusaderky`_
- Add coverage badge to ``README`` (:pr:`5382`) `James Bourbeau`_
- Mark ``test_stress_creation_and_deletion`` as ``xfail`` (:pr:`5393`) `James Bourbeau`_
- Mark ``test_worker_reconnects_mid_compute*`` tests as flaky (:pr:`5378`) `James Bourbeau`_
- Use new Dask docs theme (:pr:`5391`) `Jacob Tomlinson`_
- Remove ``pytest.mark.repeat`` from ``test_prometheus_collect_task_states`` (:pr:`5376`) `James Bourbeau`_
- Log original exception upon compute failure (:pr:`5387`) `Florian Jetter`_
- Add code coverage (:pr:`4670`) `James Bourbeau`_
- Fix zombie worker tasks after missing transition (:pr:`5316`) `Florian Jetter`_
- Add support for partial functions to ``iscoroutinefunction`` util (:pr:`5344`) `Michael Adkins`_
- Mark ``distributed/tests/test_client.py::test_profile_server`` as flaky (:pr:`5375`) `James Bourbeau`_
- Enable ``mypy`` in CI 1/2 (:pr:`5328`) `crusaderky`_
- Ensure ``dask-worker`` and ``dask-scheduler`` pick up preload configuration values  (:pr:`5365`) `James Bourbeau`_
- Use ``dask-spec`` for ``SSHCluster`` (:pr:`5191`) `Charles Blackmon-Luca`_
- Update ``_cluster_info`` dict in ``__init__`` (:pr:`5305`) `Jacob Tomlinson`_
- Use Dask temporary file utility  (:pr:`5361`) `James Bourbeau`_
- Avoid deprecated random set sampling (:pr:`5360`) `James Bourbeau`_
- Add check for unsupported NVML metrics (:pr:`5343`) `Charles Blackmon-Luca`_
- Workers submit a reply to the scheduler if replica removal was rejected (:pr:`5356`) `Florian Jetter`_
- Pickle exception and traceback immediately (:pr:`5338`) `Mads R. B. Kristensen`_
- Reinstate: AMM ``ReduceReplicas`` to iterate only on replicated tasks (:pr:`5341`) `crusaderky`_
- Sync worker status to the scheduler; new 'paused' status (:pr:`5330`) `crusaderky`_
- Add pre-commit to environments (:pr:`5362`) `Ray Bell`_
- Worker State Machine Refactor: clean up dead handlers (:pr:`5359`) `crusaderky`_
- Bump ``RAPIDS_VER`` for gpuCI (:pr:`5358`) `Charles Blackmon-Luca`_
- Generate Cython HTML annotations (:pr:`5321`) `crusaderky`_
- Worker state machine refactor (:pr:`5046`) `Florian Jetter`_
- ``fsspec`` and ``s3fs`` git tips are incompatible (:pr:`5346`) `crusaderky`_
- Fix ``test_many_Progress`` and others (:pr:`5329`) `crusaderky`_
- Run multiple AMMs in parallel (:pr:`5339`) `crusaderky`_
- Enhance AMM docstrings (:pr:`5340`) `crusaderky`_
- Run ``pyupgrade`` in CI (:pr:`5327`) `crusaderky`_
- Fix typo in client side example ``foundations.rst`` (:pr:`5336`) `Genevieve Buckley`_


.. _v2021.09.1:

2021.09.1
---------

Released on September 21, 2021

- Revert AMM ``ReduceReplicas`` and parallel AMMs updates (:pr:`5335`) `James Bourbeau`_
- Run multiple AMMs in parallel (:pr:`5315`) `crusaderky`_
- AMM ``ReduceReplicas`` to iterate only on replicated tasks (:pr:`5297`) `crusaderky`_
- Add type annotations to various functions within ``distributed.worker`` (:pr:`5290`) `Tom Forbes`_
- Mark ``test_ucx_config_w_env_var`` flaky on UCX < 1.11 (:pr:`5262`) `Peter Andreas Entschev`_
- Warn if CUDA context is created on incorrect device in UCX (:pr:`5308`) `Peter Andreas Entschev`_
- Remove redundant timeouts from ``test_client`` (:pr:`5314`) `crusaderky`_
- Allow ``Client`` to subscribe to events // Remote printing and warning (:pr:`5217`) `Florian Jetter`_
- Test pickle protocols 4 & 5 (:pr:`5313`) `jakirkham`_
- Fix-up ``test_pickle_empty`` (:pr:`5303`) `jakirkham`_
- Increase timeout for ``test_worker_reconnects_mid_compute_multiple_states_on_scheduler`` (:pr:`5304`) `Florian Jetter`_
- Add synced dict between cluster and scheduler to store cluster info (:pr:`5033`) `Jacob Tomlinson`_
- Update ``test_sub_submit_priority`` (:pr:`5301`) `James Bourbeau`_
- Revert "Add test setup fixture (:pr:`5242`)" (:pr:`5300`) `James Bourbeau`_
- Fix flaky ``test_worker_reconnects_mid_compute`` (:pr:`5299`) `Florian Jetter`_
- Use ``gen_test`` in ``test_adaptive`` (:pr:`5298`) `crusaderky`_
- Increase ``worker.suspicious_counter`` threshold (:pr:`5228`) `Florian Jetter`_
- Active Memory Manager framework + discard excess replicas (:pr:`5111`) `crusaderky`_
- Add test setup fixture (:pr:`5242`) `James Bourbeau`_


.. _v2021.09.0:

2021.09.0
---------

Released on September 3, 2021

- Fix ``add_plugin`` warnings (:pr:`5267`) `Doug Davis`_
- Add ``list`` around iterator in ``handle_missing_dep`` (:pr:`5285`) `Matthew Rocklin`_
- Jupyter-client 7 compatiblity (:pr:`5286`) `Min RK`_
- Replace ``atop`` with ``blockwise`` (:pr:`5289`) `James Bourbeau`_
- Add pytest color to CI (:pr:`5276`) `James Bourbeau`_
- Fix ``test_map`` and others (:pr:`5278`) `crusaderky`_
- Use ``name`` argument with ``Scheduler.remove_plugin`` calls (:pr:`5260`) `Doug Davis`_
- Downgrade to ``jupyter_client`` 6 (:pr:`5273`) `crusaderky`_
- Migrate ``Security`` HTML repr to Jinja2 (:pr:`5264`) `Jacob Tomlinson`_
- Migrate ``ProcessInterface`` HTML repr to Jinja2 (:pr:`5263`) `Jacob Tomlinson`_
- Add support for diskless machines to system monitor (:pr:`5257`) `James Bourbeau`_
- Avoid during-iteration scheduler plugin changes (:pr:`5259`) `Doug Davis`_
- Remove ``GroupProgress`` scheduler plugin (:pr:`5256`) `James Bourbeau`_
- Properly check for ipv6 availability (:pr:`5255`) `crusaderky`_
- Improved IPv6 dask-worker support (:pr:`5197`) `Walt Woods`_
- Overwrite worker plugins (:pr:`5248`) `Matthew Rocklin`_
- Refactor scheduler plugins; store in a dictionary (:pr:`5120`) `Doug Davis`_
- Fix "then" -> "than" typo in docs (:pr:`5247`) `David Chudzicki`_
- Fix typo (remove extra verb "creates") in docs (:pr:`5244`) `David Chudzicki`_
- Fix "fractiom" -> "fraction" typo in docstring (:pr:`5245`) `David Chudzicki`_
- Fix "schedulers" -> "scheduler" typo in docs (:pr:`5246`) `David Chudzicki`_
- Use non-histogram plots up to 100 workers (:pr:`5249`) `Matthew Rocklin`_


.. _v2021.08.1:

2021.08.1
---------

Released on August 20, 2021

- Rename plots to fit in the labextension (:pr:`5239`) `Naty Clementi`_
- Log messages for ``CommClosedError`` now includes information about remote address (:pr:`5209`) `Florian Jetter`_
- Add ``target='_blank'`` for redirects of dashboard link (:pr:`5237`) `Naty Clementi`_
- Update computation code retrieval logic (:pr:`5236`) `James Bourbeau`_
- Minor polish on cfexecutor (:pr:`5233`) `crusaderky`_
- Use development version of ``dask`` in gpuCI build (:pr:`5232`) `James Bourbeau`_
- Use upstream ``dask.widgets`` (:pr:`5205`) `Jacob Tomlinson`_
- Fix flaky ``test_worker_reconnects_mid_compute`` (:pr:`5227`) `Florian Jetter`_
- Update ``WorkerPlugin`` docstring about usage of ``TaskState`` objects (:pr:`5226`) `Florian Jetter`_
- Worker Network Timeseries (:pr:`5129`) `Naty Clementi`_
- Add HTML Repr for ``ProcessInterface`` class and all its subclasses (:pr:`5181`) `Freyam Mehta`_
- Fix an issue where a reconnecting worker could cause an invalid transition (:pr:`5210`) `Florian Jetter`_
- Minor fixes for cfexecutor (:pr:`5177`) `Florian Jetter`_
- Add HTML Repr for ``Security`` class (:pr:`5178`) `Freyam Mehta`_
- Fix performance report sizing issue (:pr:`5213`) `Ian Rose`_
- Drop RMM compatibility code from RAPIDS < 0.11 (:pr:`5214`) `Peter Andreas Entschev`_


.. _v2021.08.0:

2021.08.0
---------

Released on August 13, 2021

- Include addresses in closed comm repr  (:pr:`5203`) `James Bourbeau`_
- Test ``nanny.environ`` precedence (:pr:`5204`) `Florian Jetter`_
- Migrating HTML reprs to jinja2 (:pr:`5188`) `Jacob Tomlinson`_
- Fix ``test_process_executor_kills_process`` flakyness (:pr:`5183`) `crusaderky`_
- Remove ``urllib3`` as a dependency downloading preloads (:pr:`5199`) `Marcos Moyano`_
- Download preload urls in the ``Preload`` constructor  (:pr:`5194`) `Marcos Moyano`_
- Avoid recursion error in ``profile.merge`` (:pr:`5195`) `Matthew Rocklin`_
- Add text exceptions to the ``Scheduler`` (:pr:`5148`) `Matthew Rocklin`_
- Use ``kwarg`` for ``Theme`` filename (:pr:`5190`) `Bryan Van de Ven`_
- Add a ``.git-ignore-revs`` file (:pr:`5187`) `Florian Jetter`_
- Replace ``not not`` with ``bool()`` (:pr:`5182`) `Jacob Tomlinson`_
- Resolve deadlock cause by transition error after fetching dependency (:pr:`5157`) `Florian Jetter`_
- Set z-index of data-table lower (:pr:`5175`) `Julia Signell`_
- Add ``no-worker`` - ``memory`` transition to scheduler (:pr:`5174`) `Florian Jetter`_
- Deprecate worker plugin overwrite policy (:pr:`5146`) `James Bourbeau`_
- Fix flaky tests in CI (:pr:`5168`) `crusaderky`_
- Instructions for jemalloc with brew on macOS (:pr:`4996`) `Gabe Joseph`_
- Bump ``RAPIDS_VER`` to 21.10 (:pr:`5165`) `Charles Blackmon-Luca`_
- Tweak verbiage around ``async`` functions (:pr:`5166`) `crusaderky`_
- Use Python 3 ``super()`` calls (:pr:`5167`) `crusaderky`_
- Support asynchronous tasks (:pr:`5151`) `Matthew Rocklin`_
- Rename total comm bytes and provide doc string (:pr:`5155`) `Florian Jetter`_
- Add GPU executor if GPU is present (:pr:`5123`) `Matthew Rocklin`_
- Fix RMM and UCX tests (:pr:`5158`) `Peter Andreas Entschev`_
- Remove excessive timeout of ``test_steal_during_task_deserialization`` (:pr:`5156`) `Florian Jetter`_
- Add gpuCI build script (:pr:`5147`) `Charles Blackmon-Luca`_
- Demote ``Worker.ensure_computing`` to function (:pr:`5153`) `Florian Jetter`_


.. _v2021.07.2:

2021.07.2
---------

Released on July 30, 2021

- Fix a deadlock connected to task stealing and task deserialization (:pr:`5128`) `Florian Jetter`_
- Include maximum shard size in second ``to_frames`` method (:pr:`5145`) `Matthew Rocklin`_
- Minor dashboard style updates (:pr:`5143`) `Bryan Van de Ven`_
- Cap maximum shard size at the size of an integer (:pr:`5141`) `Matthew Rocklin`_
- Document automatic ``MALLOC_TRIM_THRESHOLD_`` environment variable (:pr:`5139`) `James Bourbeau`_
- Mark ``ucx-py`` tests for GPU (:pr:`5133`) `Charles Blackmon-Luca`_
- Update individual profile plot sizing (:pr:`5131`) `James Bourbeau`_
- Handle ``NVMLError_Unknown`` in NVML diagnostics (:pr:`5121`) `Peter Andreas Entschev`_
- Unit tests to use a random port for the dashboard (:pr:`5060`) `crusaderky`_
- Ensure worker reconnect registers existing tasks properly (:pr:`5103`) `Florian Jetter`_
- Halve CI runtime! (:pr:`5074`) `crusaderky`_
- Add ``NannyPlugins`` (:pr:`5118`) `Matthew Rocklin`_
- Add ``WorkerNetworkBandwidth`` chart to dashboard (:pr:`5104`) `Naty Clementi`_
- Set nanny environment variables in config (:pr:`5098`) `Matthew Rocklin`_
- Read smaller frames to workaround OpenSSL bug (:pr:`5115`) `jakirkham`_
- Move UCX/RMM config variables to Distributed namespace  (:pr:`4916`) `Charles Blackmon-Luca`_
- Allow ws(s) messages greater than 10Mb (:pr:`5110`) `Marcos Moyano`_
- Short-circuit root-ish check for many deps (:pr:`5113`) `Gabe Joseph`_


.. _v2021.07.1:

2021.07.1
---------

Released on July 23, 2021

- Remove experimental feature warning from actors docs (:pr:`5108`) `James Bourbeau`_
- Keep dependents in worker dependency if TS is still known (:pr:`5034`) `Florian Jetter`_
- Add ``Scheduler.set_restrictions`` (:pr:`5101`) `Matthew Rocklin`_
- Make ``Actor`` futures awaitable and work with ``as_completed`` (:pr:`5092`) `Martin Durant`_
- Simplify ``test_secede_balances`` (:pr:`5071`) `Florian Jetter`_
- ``Computation`` class (:pr:`5001`) `Florian Jetter`_
- Some light dashboard cleanup (:pr:`5102`) `Bryan Van de Ven`_
- Don't package tests (:pr:`5054`) `James Bourbeau`_
- Add pytest marker for GPU tests (:pr:`5023`) `Charles Blackmon-Luca`_
- Actor: don't hold key references on workers (:pr:`4937`) `Gabe Joseph`_
- Collapse nav to hamburger sooner (:pr:`5094`) `Julia Signell`_
- Verify that actors survive pickling (:pr:`5086`) `Matthew Rocklin`_
- Reenable UCX-Py tests that used to segfault (:pr:`5076`) `Peter Andreas Entschev`_
- Better support ``ProcessPoolExecutors`` (:pr:`5063`) `Matthew Rocklin`_
- Simplify ``test_worker_heartbeat_after_cancel`` (:pr:`5067`) `Florian Jetter`_
- Avoid property validation in Bokeh (:pr:`5065`) `Matthew Rocklin`_
- Reduce default websocket frame size and make configurable (:pr:`5070`) `Ian Rose`_
- Disable pytest-timeout ``SIGALARM`` on MacOS (:pr:`5057`) `crusaderky`_
- ``rebalance()`` resilience to computations (:pr:`4968`) `crusaderky`_
- Improve CI stability (:pr:`5022`) `crusaderky`_
- Ensure heartbeats after cancelation do not raise ``KeyError`` s (:pr:`5053`) `Florian Jetter`_
- Add more useful exception message on TLS cert mismatch (:pr:`5040`) `Jacob Tomlinson`_
- Add bokeh ``mode`` parameter to performance reports (:pr:`5025`) `James Bourbeau`_


.. _v2021.07.0:

2021.07.0
---------

Released on July 9, 2021

- Fix Nbytes jitter - less expensive (:pr:`5043`) `Naty Clementi`_
- Use native GH actions cancel feature (:pr:`5037`) `Florian Jetter`_
- Don't require workers to report to scheduler if scheduler shuts down (:pr:`5032`) `Florian Jetter`_
- Add pandas to the list of checked packages for ``client.get_versions()`` (:pr:`5029`) `Ian Rose`_
- Move worker preload before scheduler address is set (:pr:`5024`) `Matthew Rocklin`_
- Fix flaky ``test_oversubscribing_leases`` (:pr:`5030`) `Florian Jetter`_
- Update scheduling policy docs for #4967 (:pr:`5018`) `Gabe Joseph`_
- Add echo handler to ``Server`` class (:pr:`5020`) `Matthew Rocklin`_
- Also include pngs when bundling package (:pr:`5016`) `Ian Rose`_
- Remove duplicated dashboard panes (:pr:`5017`) `Ian Rose`_
- Fix worker memory dashboard flickering (:pr:`4997`) `Naty Clementi`_
- Tabs on bottom left corner on dashboard (:pr:`5006`) `Naty Clementi`_
- Rename nbytes widgets (:pr:`4878`) `crusaderky`_
- Co-assign root-ish tasks (:pr:`4967`) `Gabe Joseph`_
- ``OSError`` tweaks (:pr:`5003`) `crusaderky`_
- Update imports to ``cudf.testing._utils`` (:pr:`5005`) `Peter Andreas Entschev`_
- Ensure shuffle split default durations uses proper prefix (:pr:`4991`) `Florian Jetter`_
- Follow up ``pyupgrade`` formatting (:pr:`4993`) `Florian Jetter`_
- Rename plot dropdown (:pr:`4992`) `James Bourbeau`_
- Pyupgrade (:pr:`4741`) `Florian Jetter`_
- Misc Sphinx tweaks (:pr:`4988`) `crusaderky`_
- No longer hold dependencies of erred tasks in memory #4918 `Florian Jetter`_
- Add maximum shard size to config (:pr:`4986`) `Matthew Rocklin`_
- Ensure shuffle split operations are blacklisted from work stealing (:pr:`4964`) `Florian Jetter`_
- Add dropdown menu to access individual plots (:pr:`4984`) `Jacob Tomlinson`_
- Edited the path to ``scheduler.py`` (:pr:`4983`) `Freyam Mehta`_
- Task Group Graph Visualization (:pr:`4886`) `Naty Clementi`_
- Remove more internal references to deprecated utilities (:pr:`4971`) `James Bourbeau`_
- Restructure nbytes hover (:pr:`4952`) `Naty Clementi`_
- Except more errors in ``pynvml.nvmlInit()`` (:pr:`4970`) `gerrymanoim`_
- Add occupancy as individual plot (:pr:`4963`) `Naty Clementi`_
- Deprecate utilities which have moved to dask (:pr:`4966`) `James Bourbeau`_
- Ensure connectionpool does not leave comms if closed mid connect (:pr:`4951`) `Florian Jetter`_
- Add support for registering scheduler plugins from `Client` (:pr:`4808`) `Doug Davis`_
- Stealing dashboard fixes (:pr:`4948`) `Florian Jetter`_
- Allow requirements verification to be ignored when loading backends from entrypoints (:pr:`4961`) `Florian Jetter`_
- Add ``Log`` and ``Logs`` to API docs (:pr:`4946`) `James Bourbeau`_
- Support fixtures and ``pytest.mark.parametrize`` with ``gen_cluster`` (:pr:`4958`) `Gabe Joseph`_


.. _v2021.06.2:

2021.06.2
---------

Released on June 22, 2021

- Revert refactor to ``utils.Log[s]`` and ``Cluster.get_logs`` (:pr:`4941`) `Charles Blackmon-Luca`_
- Use deprecation utility from Dask (:pr:`4924`) `James Bourbeau`_
- Add transition counter to ``Scheduler`` (:pr:`4934`) `Matthew Rocklin`_
- Remove ``nbytes_in_memory`` (:pr:`4930`) `Matthew Rocklin`_


.. _v2021.06.1:

2021.06.1
---------

Released on June 18, 2021

- Fix deadlock in ``handle_missing_dep`` if additional replicas are available (:pr:`4929`) `Florian Jetter`_
- Add configuration to enable/disable NVML diagnostics (:pr:`4893`) `Peter Andreas Entschev`_
- Add scheduler log tab to performance reports (:pr:`4909`) `Charles Blackmon-Luca`_
- Add HTML repr to ``scheduler_info`` and incorporate into client and cluster reprs (:pr:`4857`) `Jacob Tomlinson`_
- Fix error state typo (:pr:`4898`) `James Bourbeau`_
- Allow actor exceptions to propagate (:pr:`4232`) `Martin Durant`_
- Remove importing ``apply`` from ``dask.compatibility`` (:pr:`4913`) `Elliott Sales de Andrade`_
- Use more informative default name for ``WorkerPlugin`` s (:pr:`4908`) `James Bourbeau`_
- Removed unused utility functions (:pr:`4911`) `James Bourbeau`_
- Locally rerun successfully completed futures (:pr:`4813`) `ArtinSarraf`_
- Forget erred tasks and fix deadlocks on worker (:pr:`4784`) `Florian Jetter`_
- Handle ``HTTPClientError`` in websocket connector (:pr:`4900`) `Marcos Moyano`_
- Update ``dask_cuda`` usage in ``SSHCluster`` docstring (:pr:`4894`) `James Bourbeau`_
- Remove tests for ``process_time`` and ``thread_time`` (:pr:`4895`) `James Bourbeau`_
- Flake8 config cleanup (:pr:`4888`) `Florian Jetter`_
- Don't strip scheduler protocol when determining host (:pr:`4883`) `James Bourbeau`_
- Add more documentation on memory management (:pr:`4874`) `crusaderky`_
- Add ``range_query`` tests to NVML test suite (:pr:`4879`) `Charles Blackmon-Luca`_
- No longer cancel result future in async process when using timeouts (:pr:`4882`) `Florian Jetter`_


.. _v2021.06.0:

2021.06.0
---------

Released on June 4, 2021

- Multiple worker executors (:pr:`4869`) `Mads R. B. Kristensen`_
- Ensure PyNVML works correctly when installed with no GPUs (:pr:`4873`) `Peter Andreas Entschev`_
- Show more in test summary (:pr:`4875`) `James Bourbeau`_
- Move ``SystemMonitor`` s GPU initialization back to constructor (:pr:`4866`) `Peter Andreas Entschev`_
- Mark ``test_server_comms_mark_active_handlers`` with ``pytest.mark.asyncio`` (:pr:`4876`) `James Bourbeau`_
- Who has has what html reprs v2 (:pr:`4865`) `Jacob Tomlinson`_
- O(1) rebalance (:pr:`4774`) `crusaderky`_
- Ensure repr and eq for cluster always works (:pr:`4799`) `Florian Jetter`_


.. _v2021.05.1:

2021.05.1
---------

Released on May 28, 2021

- Drop usage of ``WhoHas`` & ``WhatHas`` from ``Client`` (:pr:`4863`) `jakirkham`_
- Ensure adaptive scaling is properly awaited and closed (:pr:`4720`) `Florian Jetter`_
- Fix ``WhoHas``/ ``HasWhat`` ``async`` usage (:pr:`4860`) `Benjamin Zaitlen`_
- Add HTML reprs for ``Client.who_has`` and ``Client.has_what`` (:pr:`4853`) `Jacob Tomlinson`_
- Prevent accidentally starting multiple ``Worker`` s in the same process (:pr:`4852`) `crusaderky`_
- Add system tab to performance reports (:pr:`4561`) `Charles Blackmon-Luca`_
- Let servers close faster if there are no active handlers (:pr:`4805`) `Florian Jetter`_
- Fix UCX scrub config logging (:pr:`4850`) `Peter Andreas Entschev`_
- Ensure worker clients are closed (:pr:`3921`) `Florian Jetter`_
- Fix warning for attribute error when deleting a client (:pr:`4807`) `Florian Jetter`_
- Ensure exceptions are raised if workers are incorrectly started (:pr:`4733`) `Florian Jetter`_
- Update handling of UCX exceptions on endpoint closing (:pr:`4836`) `Peter Andreas Entschev`_
- Ensure busy workloads properly look up ``who_has`` (:pr:`4793`) `Florian Jetter`_
- Check ``distributed.scheduler.pickle`` in ``Scheduler.run_function`` (:pr:`4838`) `James Bourbeau`_
- Add performance_report to API docs (:pr:`4840`) `James Bourbeau`_
- Use ``dict`` ``_workers_dv`` in unordered use cases (:pr:`4826`) `jakirkham`_
- Bump ``pre-commit`` hook versions (:pr:`4835`) `James Bourbeau`_
- Do not mindlessly spawn workers when no memory limit is set (:pr:`4397`) `Torsten Wörtwein`_
- ``test_memory`` to use ``gen_cluster`` (:pr:`4811`) `crusaderky`_
- Increase timeout of ``gen_test`` to 30s (:pr:`4821`) `Florian Jetter`_


.. _v2021.05.0:

2021.05.0
---------

Released on May 14, 2021

- Merge global annotations on the client (:pr:`4691`) `Mads R. B. Kristensen`_
- Add support for ``click`` 8 (:pr:`4810`) `James Bourbeau`_
- Add HTML reprs to some scheduler classes (:pr:`4795`) `James Bourbeau`_
- Use JupyterLab theme variables (:pr:`4796`) `Ian Rose`_
- Allow the dashboard to run on multiple ports (:pr:`4786`) `Jacob Tomlinson`_
- Remove ``release_dep`` from ``WorkerPlugin`` API (:pr:`4791`) `James Bourbeau`_
- Support for UCX 1.10+ (:pr:`4787`) `Peter Andreas Entschev`_
- Reduce complexity of ``test_gather_allow_worker_reconnect`` (:pr:`4739`) `Florian Jetter`_
- Fix doctests in ``utils.py`` (:pr:`4785`) `Jacob Tomlinson`_
- Ensure deps are actually logged in worker (:pr:`4753`) `Florian Jetter`_
- Add ``stacklevel`` keyword into ``performance_report()`` to allow for selecting calling code to be displayed (:pr:`4777`) `Nathan Danielsen`_
- Unregister worker plugin (:pr:`4748`) `Naty Clementi`_
- Fixes some pickling issues in the Cythonized ``Scheduler`` (:pr:`4768`) `jakirkham`_
- Improve graceful shutdown if nanny is involved (:pr:`4725`) `Florian Jetter`_
- Update cythonization in CI (:pr:`4764`) `James Bourbeau`_
- Use ``contextlib.nullcontext`` (:pr:`4763`) `James Bourbeau`_
- Cython fixes for ``MemoryState`` (:pr:`4761`) `jakirkham`_
- Fix errors in ``check_thread_leak`` (:pr:`4747`) `James Bourbeau`_
- Handle missing ``key`` case in ``report_on_key`` (:pr:`4755`) `jakirkham`_
- Drop temporary ``set`` variables ``s`` (:pr:`4758`) `jakirkham`_


.. _v2021.04.1:

2021.04.1
---------

Released on April 23, 2021

- Avoid ``active_threads`` changing size during iteration (:pr:`4729`) `James Bourbeau`_
- Fix ``UnboundLocalError`` in ``AdaptiveCore.adapt()`` (:pr:`4731`) `Anderson Banihirwe`_
- Minor formatting updates for HTTP endpoints doc (:pr:`4736`) `James Bourbeau`_
- Unit test for ``metrics["memory"]=None`` (:pr:`4727`) `crusaderky`_
- Enable configuration of prometheus metrics namespace (:pr:`4722`) `Jacob Tomlinson`_
- Reintroduce ``weight`` function (:pr:`4723`) `James Bourbeau`_
- Add ``ready->memory`` to transitions in worker (:pr:`4728`) `Gil Forsyth`_
- Fix regressions in :pr:`4651` (:pr:`4719`) `crusaderky`_
- Add descriptions for UCX config options (:pr:`4683`) `Charles Blackmon-Luca`_
- Split RAM measure into dask keys/other old/other new (:pr:`4651`) `crusaderky`_
- Fix ``DeprecationWarning`` on Python 3.9 (:pr:`4717`) `George Sakkis`_
- ipython causes ``test_profile_nested_sizeof`` crash on windows (:pr:`4713`) `crusaderky`_
- Add ``iterate_collection`` argument to ``serialize`` (:pr:`4641`) `Richard J Zamora`_
- When closing ``Server``, close all listeners (:pr:`4704`) `Florian Jetter`_
- Fix timeout in ``client.restart`` (:pr:`4690`) `Matteo De Wint`_
- Avoid repeatedly using the same worker on first task with quiet cluster (:pr:`4638`) `Doug Davis`_
- Grab ``func`` for ``finish`` case only if used (:pr:`4702`) `jakirkham`_
- Remove hostname check in ``test_dashboard`` (:pr:`4706`) `James Bourbeau`_
- Faster ``tests_semaphore::test_worker_dies`` (:pr:`4703`) `Florian Jetter`_
- Clean up ``test_dashboard`` (:pr:`4700`) `crusaderky`_
- Add timing information to ``TaskGroup`` (:pr:`4671`) `Matthew Rocklin`_
- Remove ``WSSConnector`` TLS presence check (:pr:`4695`) `Marcos Moyano`_
- Fix typo and remove unused ``time.time`` import (:pr:`4689`) `Hristo Georgiev`_
- Don't initialize CUDA context in monitor (:pr:`4688`) `Charles Blackmon-Luca`_
- Add support for extra conn args for HTTP protocols (:pr:`4682`) `Marcos Moyano`_
- Adjust timings in ``test_threadpoolworkers`` (:pr:`4681`) `Florian Jetter`_
- Add GPU metrics to ``SystemMonitor`` (:pr:`4661`) `Charles Blackmon-Luca`_
- Removing ``dumps_msgpack()`` and ``loads_msgpack()`` (:pr:`4677`) `Mads R. B. Kristensen`_
- Expose worker ``SystemMonitor`` s to scheduler via RPC (:pr:`4657`) `Charles Blackmon-Luca`_


.. _v2021.04.0:

2021.04.0
---------

Released on April 2, 2021

- Fix un-merged frames (:pr:`4666`) `Matthew Rocklin`_
- Add informative error message to install uvloop (:pr:`4664`) `Matthew Rocklin`_
- Remove incorrect comment regarding default ``LocalCluster`` creation (:pr:`4660`) `cameron16`_
- Treat empty/missing ``writeable`` as a no-op (:pr:`4659`) `jakirkham`_
- Avoid list mutation in ``pickle_loads`` (:pr:`4653`) `Matthew Rocklin`_
- Ignore ``OSError`` exception when scaling down (:pr:`4633`) `Gerald`_
- Add ``isort`` to pre-commit hooks, package resorting (:pr:`4647`) `Charles Blackmon-Luca`_
- Use powers-of-two when displaying RAM (:pr:`4649`) `crusaderky`_
- Support Websocket communication protocols (:pr:`4396`) `Marcos Moyano`_
- ``scheduler.py`` / ``worker.py`` code cleanup (:pr:`4626`) `crusaderky`_
- Update out-of-date references to ``config.yaml`` (:pr:`4643`) `Hristo Georgiev`_
- Suppress ``OSError`` on ``SpecCluster`` shutdown (:pr:`4567`) `Jacob Tomlinson`_
- Replace conda with mamba (:pr:`4585`) `crusaderky`_
- Expand documentation on pure functions (:pr:`4644`) `James Lamb`_


.. _v2021.03.1:

2021.03.1
---------

Released on March 26, 2021

- Add standalone dashboard page for GPU usage (:pr:`4556`) `Jacob Tomlinson`_
- Handle ``stream is None`` case in TCP comm finalizer (:pr:`4631`) `James Bourbeau`_
- Include ``LIST_PICKLE`` in NumPy array serialization (:pr:`4632`) `James Bourbeau`_
- Rename annotation plugin in ``test_highlevelgraph.py`` (:pr:`4618`) `James Bourbeau`_
- UCX use ``nbytes`` instead of ``len`` (:pr:`4621`) `Mads R. B. Kristensen`_
- Skip NumPy and pandas tests if not importable (:pr:`4563`) `Ben Greiner`_
- Remove ``utils.shutting_down`` in favor of ``sys.is_finalizing`` (:pr:`4624`) `James Bourbeau`_
- Handle ``async`` clients when closing (:pr:`4623`) `Matthew Rocklin`_
- Drop ``log`` from ``remove_key_from_stealable`` (:pr:`4609`) `jakirkham`_
- Introduce events log length config option (:pr:`4615`) `Fabian Gebhart`_
- Upstream config serialization and inheritance (:pr:`4372`) `Jacob Tomlinson`_
- Add check to scheduler creation in ``SpecCluster`` (:pr:`4605`) `Jacob Tomlinson`_
- Make length of events ``deque`` configurable (:pr:`4604`) `Fabian Gebhart`_
- Add explicit ``fetch`` state to worker ``TaskState`` (:pr:`4470`) `Gil Forsyth`_
- Update ``develop.rst`` (:pr:`4603`) `Florian Jetter`_
- ``pickle_loads()``: Handle empty ``memoryview`` (:pr:`4595`) `Mads R. B. Kristensen`_
- Switch documentation builds for PRs to readthedocs (:pr:`4599`) `James Bourbeau`_
- Track frame sizes along with frames (:pr:`4593`) `jakirkham`_
- Add support for a list of keys when using ``batch_size`` in ``client.map`` (:pr:`4592`) `Sultan Orazbayev`_
- If ``SpecCluster`` fails to start attempt to gracefully close out again (:pr:`4590`) `Jacob Tomlinson`_
- Multi-lock extension (:pr:`4503`) `Mads R. B. Kristensen`_
- Update ``PipInstall`` plugin command (:pr:`4584`) `James Bourbeau`_
- IPython magics: remove deprecated ``ioloop`` workarounds (:pr:`4530`) `Min RK`_
- Add GitHub actions workflow to cancel duplicate builds (:pr:`4581`) `James Bourbeau`_
- Remove outdated macOS build badge from ``README`` (:pr:`4576`) `James Bourbeau`_
- Dask master -> main (:pr:`4569`) `Julia Signell`_
- Drop support for Python 3.6 (:pr:`4390`) `James Bourbeau`_
- Add docstring for ``dashboard_link`` property (:pr:`4572`) `Doug Davis`_
- Change default branch from master to main (:pr:`4495`) `Julia Signell`_
- Msgpack handles extract serialize (:pr:`4531`) `Mads R. B. Kristensen`_


.. _v2021.03.0:

2021.03.0
---------

Released on March 5, 2021

.. note::

    This is the first release with support for Python 3.9 and the
    last release with support for Python 3.6

- ``tcp.write()``: cast ``memoryview`` to byte itemsize (:pr:`4555`) `Mads R. B. Kristensen`_
- Refcount the ``thread_state.asynchronous`` flag (:pr:`4557`) `Mads R. B. Kristensen`_
- Python 3.9 (:pr:`4460`) `crusaderky`_
- Better bokeh defaults for dashboard (:pr:`4554`) `Benjamin Zaitlen`_
- Expose system monitor dashboard as individual plot for lab extension (:pr:`4540`) `Jacob Tomlinson`_
- Pass on original temp dir from nanny to worker (:pr:`4549`) `Martin Durant`_
- Serialize and split (:pr:`4541`) `Mads R. B. Kristensen`_
- Use the new HLG pack/unpack API in Dask (:pr:`4489`) `Mads R. B. Kristensen`_
- Handle annotations for culled tasks (:pr:`4544`) `Tom Augspurger`_
- Make sphinx autosummary and autoclass consistent (:pr:`4367`) `Casey Clements`_
- Move ``_transition*`` to ``SchedulerState`` (:pr:`4545`) `jakirkham`_
- Migrate from travis to GitHub actions (:pr:`4504`) `crusaderky`_
- Move ``new_task`` to ``SchedulerState`` (:pr:`4527`) `jakirkham`_
- Batch more Scheduler sends (:pr:`4526`) `jakirkham`_
- ``transition_memory_released`` and ``get_nbytes()`` optimizations (:pr:`4516`) `jakirkham`_
- Pin ``black`` pre-commit (:pr:`4533`) `James Bourbeau`_
- Read & write all frames in one pass (:pr:`4506`) `jakirkham`_
- Skip ``stream.write`` call for empty frames (:pr:`4507`) `jakirkham`_
- Prepend frame metadata header (:pr:`4505`) `jakirkham`_
- ``transition_processing_memory`` optimizations, etc. (:pr:`4487`) `jakirkham`_
- Attempt to get client from worker in ``Queue`` and ``Variable`` (:pr:`4490`) `James Bourbeau`_
- Use ``main`` branch for ``zict`` (:pr:`4499`) `jakirkham`_
- Use a callback to close TCP Comms, rather than check every time (:pr:`4453`) `Matthew Rocklin`_


.. _v2021.02.0:

2021.02.0
---------

Released on February 5, 2021

- Bump minimum Dask to 2021.02.0 (:pr:`4486`) `James Bourbeau`_
- Update ``TaskState`` documentation about dependents attribute (:pr:`4440`) `Florian Jetter`_
- DOC: Autoreformat all functions docstrings (:pr:`4475`) `Matthias Bussonnier`_
- Use cached version of ``is_coroutine_function`` in stream handling to (:pr:`4481`) `Ian Rose`_
- Optimize ``transitions`` (:pr:`4451`) `jakirkham`_
- Create ``PULL_REQUEST_TEMPLATE.md`` (:pr:`4476`) `Ray Bell`_
- DOC: typo, directives ends with 2 colons ``::`` (:pr:`4472`) `Matthias Bussonnier`_
- DOC: Proper numpydoc syntax for ``distributed/protocol/*.py`` (:pr:`4473`) `Matthias Bussonnier`_
- Update ``pytest.skip`` usage in ``test_server_listen`` (:pr:`4467`) `James Bourbeau`_
- Unify annotations (:pr:`4406`) `Ian Rose`_
- Added worker resources from config (:pr:`4456`) `Tom Augspurger`_
- Fix var name in worker validation func (:pr:`4457`) `Gil Forsyth`_
- Refactor ``task_groups`` & ``task_prefixes`` (:pr:`4452`) `jakirkham`_
- Use ``parent._tasks`` in ``heartbeat`` (:pr:`4450`) `jakirkham`_
- Refactor ``SchedulerState`` from ``Scheduler`` (:pr:`4365`) `jakirkham`_


.. _v2021.01.1:

2021.01.1
---------

Released on January 22, 2021

- Make system monitor interval configurable (:pr:`4447`) `Matthew Rocklin`_
- Add ``uvloop`` config value (:pr:`4448`) `Matthew Rocklin`_
- Additional optimizations to stealing (:pr:`4445`) `jakirkham`_
- Give clusters names (:pr:`4426`) `Jacob Tomlinson`_
- Use worker comm pool in ``Semaphore`` (:pr:`4195`) `Florian Jetter`_
- Set ``runspec`` on all new tasks to avoid deadlocks (:pr:`4432`) `Florian Jetter`_
- Support ``TaskState`` objects in story methods (:pr:`4434`) `Matthew Rocklin`_
- Support missing event loop in ``Client.asynchronous`` (:pr:`4436`) `Matthew Rocklin`_
- Don't require network to inspect tests (:pr:`4433`) `Matthew Rocklin`_


.. _v2021.01.0:

2021.01.0
---------

Released on January 15, 2021

- Add time started to scheduler info (:pr:`4425`) `Jacob Tomlinson`_
- Log adaptive error (:pr:`4422`) `Jacob Tomlinson`_
- Xfail normalization tests (:pr:`4411`) `Jacob Tomlinson`_
- Use ``dumps_msgpack`` and ``loads_msgpack`` when packing high level graphs (:pr:`4409`) `Mads R. B. Kristensen`_
- Add ``nprocs`` auto option to ``dask-worker`` CLI (:pr:`4377`) `Jacob Tomlinson`_
- Type annotation of ``_reevaluate_occupancy_worker`` (:pr:`4398`) `jakirkham`_
- Type ``TaskGroup`` in ``active_states`` (:pr:`4408`) `jakirkham`_
- Fix ``test_as_current_is_thread_local`` (:pr:`4402`) `jakirkham`_
- Use ``list`` comprehensions to bind ``TaskGroup`` type (:pr:`4401`) `jakirkham`_
- Make tests pass after 2028 (:pr:`4403`) `Bernhard M. Wiedemann`_
- Fix compilation warnings, ``decide_worker`` now a C func, stealing improvements (:pr:`4375`) `jakirkham`_
- Drop custom ``__eq__`` from ``Status`` (:pr:`4270`) `jakirkham`_
- ``test_performance_report``: skip without bokeh (:pr:`4388`) `Bruno Pagani`_
- ``Nanny`` now respects dask settings from ctx mgr (:pr:`4378`) `Florian Jetter`_
- Better task duration estimates for outliers (:pr:`4213`) `selshowk`_
- Dask internal inherit config (:pr:`4364`) `Jacob Tomlinson`_
- Provide ``setup.py`` option to profile Cython code (:pr:`4362`) `jakirkham`_
- Optimizations of ``*State`` and ``Task*`` objects and stealing (:pr:`4358`) `jakirkham`_
- Cast ``SortedDict`` s to ``dict`` s in a few key places & other minor changes (:pr:`4355`) `jakirkham`_
- Use task annotation priorities for user-level priorities (:pr:`4354`) `James Bourbeau`_
- Added docs to highlevelgraph pack/unpack (:pr:`4352`) `Mads R. B. Kristensen`_
- Optimizations in notable functions used by transitions (:pr:`4351`) `jakirkham`_
- Silence exception when releasing futures on process shutdown (:pr:`4309`) `Benjamin Zaitlen`_


.. _v2020.12.0:

2020.12.0
---------

Released on December 10, 2020

Highlights
^^^^^^^^^^

- Switched to `CalVer <https://calver.org/>`_ for versioning scheme.
- The scheduler can now receives Dask ``HighLevelGraph`` s instead of raw dictionary task graphs.
  This allows for a much more efficient communication of task graphs from the client to the scheduler.
- Added support for using custom ``Layer``-level annotations like ``priority``, ``retries``,
  etc. with the ``dask.annotations`` context manager.
- Updated minimum supported version of Dask to 2020.12.0.
- Added many type annotations and updates to allow for gradually Cythonizing the scheduler.

All changes
^^^^^^^^^^^

- Some common optimizations across transitions (:pr:`4348`) `jakirkham`_
- Drop serialize extension (:pr:`4344`) `jakirkham`_
- Log duplciate workers in scheduler (:pr:`4338`) `Matthew Rocklin`_
- Annotation of some comm related methods in the ``Scheduler`` (:pr:`4341`) `jakirkham`_
- Optimize ``assert`` in ``validate_waiting`` (:pr:`4342`) `jakirkham`_
- Optimize ``decide_worker`` (:pr:`4332`) `jakirkham`_
- Store occupancy in ``_reevaluate_occupancy_worker`` (:pr:`4337`) `jakirkham`_
- Handle ``WorkerState`` ``memory_limit`` of ``None`` (:pr:`4335`) `jakirkham`_
- Use ``bint`` to annotate boolean attributes (:pr:`4334`) `jakirkham`_
- Optionally use offload executor in worker (:pr:`4307`) `Matthew Rocklin`_
- Optimize ``send_task_to_worker`` (:pr:`4331`) `jakirkham`_
- Optimize ``valid_workers`` (:pr:`4329`) `jakirkham`_
- Store occupancy in ``transition_waiting_processing`` (:pr:`4330`) `jakirkham`_
- Optimize ``get_comm_cost`` (:pr:`4328`) `jakirkham`_
- Use ``.pop(...)`` to remove ``key`` (:pr:`4327`) `jakirkham`_
- Use ``operator.attrgetter`` on ``WorkerState.address`` (:pr:`4324`) `jakirkham`_
- Annotate ``Task*`` objects for Cythonization (:pr:`4302`) `jakirkham`_
- Ensure ``retire_workers`` always ``return`` a ``dict`` (:pr:`4323`) `jakirkham`_
- Some Cython fixes for ``WorkerState`` (:pr:`4321`) `jakirkham`_
- Optimize ``WorkerState.__eq__`` (:pr:`4320`) `jakirkham`_
- Swap order of ``TaskGroup`` and ``TaskPrefix`` (:pr:`4319`) `jakirkham`_
- Check traceback object can be unpickled (:pr:`4299`) `jakirkham`_
- Move ``TaskGroup`` & ``TaskPrefix`` before `TaskState` (:pr:`4318`) `jakirkham`_
- Remove empty ``test_highgraph.py`` file (:pr:`4313`) `James Bourbeau`_
- Ensure that ``retire_workers`` returns a ``dict`` (:pr:`4315`) `Matthew Rocklin`_
- Annotate ``WorkerState`` for Cythonization (:pr:`4294`) `jakirkham`_
- Close ``comm`` on low-level errors (:pr:`4239`) `jochen-ott-by`_
- Coerce new ``TaskState.nbytes`` value to ``int`` (:pr:`4311`) `jakirkham`_
- Remove offload ``try``/``except`` for ``thread_name_prefix`` keyword (:pr:`4308`) `James Bourbeau`_
- Fix ``pip`` install issue on CI (:pr:`4310`) `jakirkham`_
- Transmit ``Layer`` annotations to scheduler (:pr:`4279`) `Simon Perkins`_
- Ignores any compiled files generated by Cython (:pr:`4301`) `jakirkham`_
- Protect against missing key in ``get_metrics`` (:pr:`4300`) `Matthew Rocklin`_
- Provide option to build Distributed with Cython (:pr:`4292`) `jakirkham`_
- Set ``WorkerState.processing`` w/``dict`` in ``clean`` (:pr:`4295`) `jakirkham`_
- Annotate ``ClientState`` for Cythonization (:pr:`4290`) `jakirkham`_
- Annotate ``check_idle_saturated`` for Cythonization (:pr:`4289`) `jakirkham`_
- Avoid flicker in ``TaskStream`` with "Scheduler is empty" message (:pr:`4284`) `Matthew Rocklin`_
- Make ``gather_dep`` robust to missing tasks (:pr:`4285`) `Matthew Rocklin`_
- Annotate ``extract_serialize`` (for Cythonization) (:pr:`4283`) `jakirkham`_
- Move ``nbytes`` from Worker's state to ``TaskState`` (:pr:`4274`) `Gil Forsyth`_
- Drop extra type check in ``_extract_serialize`` (:pr:`4281`) `jakirkham`_
- Move Status to top-level import (:pr:`4280`) `Matthew Rocklin`_
- Add ``__hash__`` and ``__eq__`` for ``TaskState`` (:pr:`4278`) `jakirkham`_
- Add ``__hash__`` and ``__eq__`` for ``ClientState`` (:pr:`4276`) `jakirkham`_
- Collect ``report``'s ``client_key``s in a ``list`` (:pr:`4275`) `jakirkham`_
- Precompute ``hash`` for ``WorkerState`` (:pr:`4271`) `jakirkham`_
- Use ``Status`` ``Enum`` in ``remove_worker`` (:pr:`4269`) `jakirkham`_
- Add aggregated topic logs and ``log_event`` method (:pr:`4230`) `James Bourbeau`_
- Find the set of workers instead of their frequency (:pr:`4267`) `jakirkham`_
- Use ``set.update`` to include other ``comms`` (:pr:`4268`) `jakirkham`_
- Support string timeouts in ``sync`` (:pr:`4266`) `James Bourbeau`_
- Use ``dask.utils.stringify()`` instead of ``distributed.utils.tokey()`` (:pr:`4255`) `Mads R. B. Kristensen`_
- Use ``.items()`` to walk through keys and values (:pr:`4261`) `jakirkham`_
- Simplify frame length packing in TCP write (:pr:`4257`) `jakirkham`_
- Comm/tcp listener: do not pass comm with failed handshake to ``comm_handler`` (:pr:`4240`) `jochen-ott-by`_
- Fuse steps in ``extract_serialize`` (:pr:`4254`) `jakirkham`_
- Drop ``test_sklearn`` (:pr:`4253`) `jakirkham`_
- Document task priority tie breaking (:pr:`4252`) `James Bourbeau`_
- ``__dask_distributed_pack__()``: client argument (:pr:`4248`) `Mads R. B. Kristensen`_
- Configurable timeouts for ``worker_client`` and ``get_client`` (:pr:`4146`) `GeethanjaliEswaran`_
- Add dask/distributed versions to ``performance_report`` (:pr:`4249`) `Matthew Rocklin`_
- Update miniconda GitHub action (:pr:`4250`) `James Bourbeau`_
- UCX closing ignore error (:pr:`4236`) `Mads R. B. Kristensen`_
- Redirect to ``dask-worker`` cli documentation (:pr:`4247`) `Timost`_
- Upload file worker plugin (:pr:`4238`) `Ian Rose`_
- Create dependency ``TaskState`` as needed in ``gather_dep`` (:pr:`4241`) `Gil Forsyth`_
- Instantiate plugin if needed in ``register_worker_plugin`` (:pr:`4198`) `Julia Signell`_
- Allow actors to call actors on the same worker (:pr:`4225`) `Martin Durant`_
- Special case profile thread in leaked thread check (:pr:`4229`) `James Bourbeau`_
- Use ``intersection()`` on a set instead of ``dict_keys`` in ``update_graph`` (:pr:`4227`) `Mads R. B. Kristensen`_
- Communicate ``HighLevelGraphs`` directly to the ``Scheduler`` (:pr:`4140`) `Mads R. B. Kristensen`_
- Add ``get_task_metadata`` context manager (:pr:`4216`) `James Bourbeau`_
- Task state logs and data fix (:pr:`4206`) `Gil Forsyth`_
- Send active task durations from worker to scheduler (:pr:`4192`) `James Bourbeau`_
- Fix state check in ``test_close_gracefully`` (:pr:`4203`) `Gil Forsyth`_
- Avoid materializing layers in ``Client.compute()`` (:pr:`4196`) `Mads R. B. Kristensen`_
- Add ``TaskState`` metadata (:pr:`4191`) `James Bourbeau`_
- Fix regression in task stealing for already released keys (:pr:`4182`) `Florian Jetter`_
- Fix ``_graph_to_futures`` bug for futures-based dependencies (:pr:`4178`) `Richard J Zamora`_
- High level graph ``dumps``/``loads`` support (:pr:`4174`) `Mads R. B. Kristensen`_
- Implement pass HighLevelGraphs through ``_graph_to_futures`` (:pr:`4139`) `Mads R. B. Kristensen`_
- Support ``async`` preload click commands (:pr:`4170`) `James Bourbeau`_
- ``dask-worker`` cli memory limit option doc fix (:pr:`4172`) `marwan116`_
- Add ``TaskState`` to ``worker.py`` (:pr:`4107`) `Gil Forsyth`_
- Increase robustness of ``Semaphore.release`` (:pr:`4151`) `Lucas Rademaker`_
- Skip batched comm test win / tornado5 (:pr:`4166`) `Tom Augspurger`_
- Set Zict buffer target to maxsize when ``memory_target_fraction`` is ``False`` (:pr:`4156`) `Krishan Bhasin`_
- Add ``PipInstall`` ``WorkerPlugin`` (:pr:`3216`) `Matthew Rocklin`_
- Log ``KilledWorker`` events in the scheduler (:pr:`4157`) `Matthew Rocklin`_
- Fix ``test_gpu_metrics`` failure (:pr:`4154`) `jakirkham`_


.. _v2.30.1 - 2020-11-03:

2.30.1 - 2020-11-03
-------------------

- Pin ``pytest-asyncio`` version (:pr:`4212`) `James Bourbeau`_
- Replace ``AsyncProcess`` exit handler by ``weakref.finalize`` (:pr:`4184`) `Peter Andreas Entschev`_
- Remove hard coded connect handshake timeouts (:pr:`4176`) `Florian Jetter`_


.. _v2.30.0 - 2020-10-06:

2.30.0 - 2020-10-06
-------------------

- Support ``SubgraphCallable`` in ``str_graph()`` (:pr:`4148`) `Mads R. B. Kristensen`_
- Handle exceptions in ``BatchedSend`` (:pr:`4135`) `Tom Augspurger`_
- Fix for missing ``:`` in autosummary docs (:pr:`4143`) `Gil Forsyth`_
- Limit GPU metrics to visible devices only (:pr:`3810`) `Jacob Tomlinson`_


.. _v2.29.0 - 2020-10-02:

2.29.0 - 2020-10-02
-------------------

- Use ``pandas.testing`` (:pr:`4138`) `jakirkham`_
- Fix a few typos (:pr:`4131`) `Pav A`_
- Return right away in ``Cluster.close`` if cluster is already closed (:pr:`4116`) `Tom Rochette`_
- Update async doc with example on ``.compute()`` vs ``client.compute()`` (:pr:`4137`) `Benjamin Zaitlen`_
- Correctly tear down ``LoopRunner`` in ``Client`` (:pr:`4112`) `Sergey Kozlov`_
- Simplify ``Client._graph_to_futures()`` (:pr:`4127`) `Mads R. B. Kristensen`_
- Cleanup new exception traceback (:pr:`4125`) `Krishan Bhasin`_
- Stop writing config files by default (:pr:`4123`) `Matthew Rocklin`_


.. _v2.28.0 - 2020-09-25:

2.28.0 - 2020-09-25
-------------------

- Fix SSL ``connection_args`` for ``progressbar`` connect (:pr:`4122`) `jennalc`_


.. _v2.27.0 - 2020-09-18:

2.27.0 - 2020-09-18
-------------------

- Fix registering a worker plugin with ``name`` arg (:pr:`4105`) `Nick Evans`_
- Support different ``remote_python`` paths on cluster nodes (:pr:`4085`) `Abdulelah Bin Mahfoodh`_
- Allow ``RuntimeError`` s when closing global clients (:pr:`4115`) `Matthew Rocklin`_
- Match ``pre-commit`` in dask (:pr:`4049`) `Julia Signell`_
- Update ``super`` usage (:pr:`4110`) `Poruri Sai Rahul`_


.. _v2.26.0 - 2020-09-11:

2.26.0 - 2020-09-11
-------------------

- Add logging for adaptive start and stop (:pr:`4101`) `Matthew Rocklin`_
- Don't close a nannied worker if it hasn't yet started (:pr:`4093`) `Matthew Rocklin`_
- Respect timeouts when closing clients synchronously (:pr:`4096`) `Matthew Rocklin`_
- Log when downloading a preload script (:pr:`4094`) `Matthew Rocklin`_
- ``dask-worker --nprocs`` accepts negative values (:pr:`4089`) `Dror Speiser`_
- Support zero-worker clients (:pr:`4090`) `Matthew Rocklin`_
- Exclude ``fire-and-forget`` client from metrics (:pr:`4078`) `Tom Augspurger`_
- Drop ``Serialized.deserialize()`` method (:pr:`4073`) `jakirkham`_
- Add ``timeout=`` keyword to ``Client.wait_for_workers`` method (:pr:`4087`) `Matthew Rocklin`_


.. _v2.25.0 - 2020-08-28:

2.25.0 - 2020-08-28
-------------------

- Update for black (:pr:`4081`) `Tom Augspurger`_
- Provide informative error when connecting an older version of Dask (:pr:`4076`) `Matthew Rocklin`_
- Simplify ``pack_frames`` (:pr:`4068`) `jakirkham`_
- Simplify ``frame_split_size`` (:pr:`4067`) `jakirkham`_
- Use ``list.insert`` to add prelude up front (:pr:`4066`) `jakirkham`_
- Graph helper text (:pr:`4064`) `Julia Signell`_
- Graph dashboard: Reset container data if task number is too large (:pr:`4056`) `Florian Jetter`_
- Ensure semaphore picks correct ``IOLoop`` for threadpool workers (:pr:`4060`) `Florian Jetter`_
- Add cluster log method (:pr:`4051`) `Jacob Tomlinson`_
- Cleanup more exception tracebacks (:pr:`4054`) `Krishan Bhasin`_
- Improve documentation of ``scheduler.locks`` options (:pr:`4062`) `Florian Jetter`_


.. _v2.24.0 - 2020-08-22:

2.24.0 - 2020-08-22
-------------------

-   Move toolbar to above and fix y axis (#4043) `Julia Signell`_
-   Make behavior clearer for how to get worker dashboard (#4047) `Julia Signell`_
-   Worker dashboard clean up (#4046) `Julia Signell`_
-   Add a default argument to the datasets and a possibility to override datasets (#4052) `Nils Braun`_
-   Discover HTTP endpoints (#3744) `Martin Durant`_


.. _v2.23.0 - 2020-08-14:

2.23.0 - 2020-08-14
-------------------

- Tidy up exception traceback in TCP Comms (:pr:`4042`) `Krishan Bhasin`_
- Angle on the x-axis labels (:pr:`4030`) `Mathieu Dugré`_
- Always set RMM's strides in the ``header`` (:pr:`4039`) `jakirkham`_
- Fix documentation ``upload_file`` (:pr:`4038`) `Roberto Panai`_
- Update UCX tests for new handshake step (:pr:`4036`) `jakirkham`_
- Add test for informative errors in serialization cases (:pr:`4029`) `Matthew Rocklin`_
- Add compression, pickle protocol to comm contexts (:pr:`4019`) `Matthew Rocklin`_
- Make GPU plots robust to not having GPUs (:pr:`4008`) `Matthew Rocklin`_
- Update ``PendingDeprecationWarning`` with correct version number (:pr:`4025`) `Matthias Bussonnier`_
- Install PyTorch on CI (:pr:`4017`) `jakirkham`_
- Try getting cluster ``dashboard_link`` before asking scheduler (:pr:`4018`) `Matthew Rocklin`_
- Ignore writeable frames with builtin ``array`` (:pr:`4016`) `jakirkham`_
- Just extend ``frames2`` by ``frames`` (:pr:`4015`) `jakirkham`_
- Serialize builtin array (:pr:`4013`) `jakirkham`_
- Use cuDF's ``assert_eq`` (:pr:`4014`) `jakirkham`_
- Clear function cache whenever we upload a new file (:pr:`3993`) `Jack Xiaosong Xu`_
- Emmit warning when assign/comparing string with ``Status`` ``Enum`` (:pr:`3875`) `Matthias Bussonnier`_
- Track mutable frames (:pr:`4004`) `jakirkham`_
- Improve ``bytes`` and ``bytearray`` serialization (:pr:`4009`) `jakirkham`_
- Fix memory histogram values in dashboard (:pr:`4006`) `Willi Rath`_


.. _v2.22.0 - 2020-07-31:

2.22.0 - 2020-07-31
-------------------

- Only call ``frame_split_size`` when there are frames (:pr:`3996`) `jakirkham`_
- Fix failing ``test_bandwidth`` (:pr:`3999`) `jakirkham`_
- Handle sum of memory percentage when ``memory_limit`` is 0 (:pr:`3984`) `Julia Signell`_
- Drop msgpack pre-0.5.2 compat code (:pr:`3977`) `jakirkham`_
- Revert to localhost for local IP if no network available (:pr:`3991`) `Matthew Rocklin`_
- Add missing backtick in inline directive. (:pr:`3988`) `Matthias Bussonnier`_
- Warn when ``threads_per_worker`` is set to zero (:pr:`3986`) `Julia Signell`_
- Use ``memoryview`` in ``unpack_frames`` (:pr:`3980`) `jakirkham`_
- Iterate over list of comms (:pr:`3959`) `Matthew Rocklin`_
- Streamline ``pack_frames``/``unpack_frames`` frames (:pr:`3973`) `jakirkham`_
- Always attempt to create ``dask-worker-space`` folder and continue if it exists (:pr:`3972`) `Jendrik Jördening`_
- Use ``merge_frames`` with host memory only (:pr:`3971`) `jakirkham`_
- Simplify ``pack_frames_prelude`` (:pr:`3961`) `jakirkham`_
- Use continuation prompt for proper example parsing (:pr:`3966`) `Matthias Bussonnier`_
- Ensure writable frames (:pr:`3967`) `jakirkham`_


.. _v2.21.0 - 2020-07-17:

2.21.0 - 2020-07-17
-------------------

- Fix data replication error (:pr:`3963`) `Andrew Fulton`_
- Treat falsey local directory as ``None`` (:pr:`3964`) `Tom Augspurger`_
- Unpin ``numpydoc`` now that 1.1 is released (:pr:`3957`) `Gil Forsyth`_
- Error hard when Dask has mismatched versions or lz4 installed (:pr:`3936`) `Matthew Rocklin`_
- Skip coercing to ``bytes`` in ``merge_frames`` (:pr:`3960`) `jakirkham`_
- UCX: reuse endpoints in order to fix NVLINK issue (:pr:`3953`) `Mads R. B. Kristensen`_
- Optionally use ``pickle5`` (:pr:`3849`) `jakirkham`_
- Update time per task chart with filtering and pie (:pr:`3933`) `Benjamin Zaitlen`_
- UCX: explicit shutdown message (:pr:`3950`) `Mads R. B. Kristensen`_
- Avoid too aggressive retry of connections (:pr:`3944`) `Matthias Bussonnier`_
- Parse timeouts in ``Client.sync`` (:pr:`3952`) `Matthew Rocklin`_
- Synchronize on non-trivial CUDA frame transmission (:pr:`3949`) `jakirkham`_
- Serialize ``memoryview`` with ``shape`` and ``format`` (:pr:`3947`) `jakirkham`_
- Move ``scheduler_comm`` into ``Cluster.__init__`` (:pr:`3945`) `Matthew Rocklin`_


.. _v2.20.0 - 2020-07-02:

2.20.0 - 2020-07-02
-------------------

- Link issue on using ``async`` with ``executor_submit`` (:pr:`3939`) `jakirkham`_
- Make dashboard server listens on all IPs by default even when interface is set explicitly (:pr:`3941`) `Loïc Estève`_
- Update logic for worker removal in check ttl (:pr:`3927`) `Benjamin Zaitlen`_
- Close a created cluster quietly (:pr:`3935`) `Matthew Rocklin`_
- Ensure ``Worker.run*`` handles ``kwargs`` correctly (:pr:`3937`) `jakirkham`_
- Restore ``Scheduler.time_started`` for Dask Gateway (:pr:`3934`) `Tom Augspurger`_
- Fix exception handling in ``_wait_until_connected`` (:pr:`3912`) `Alexander Clausen`_
- Make local directory if it does not exist (:pr:`3928`) `Matthew Rocklin`_
- Install vanilla status route if bokeh dependency is not satisfied (:pr:`3844`) `joshreback`_
- Make ``Worker.delete_data`` sync (:pr:`3922`) `Peter Andreas Entschev`_
- Fix ``ensure_bytes`` import location (:pr:`3919`) `jakirkham`_
- Fix race condition in repeated calls to ``cluster.adapt()`` (:pr:`3915`) `Jacob Tomlinson`_


.. _v2.19.0 - 2020-06-19:

2.19.0 - 2020-06-19
-------------------

- Notify worker plugins when a task is released (:pr:`3817`) `Nick Evans`_
- Update heartbeat checks in scheduler (:pr:`3896`) `Benjamin Zaitlen`_
- Make encryption default if ``Security`` is given arguments (:pr:`3887`) `Matthew Rocklin`_
- Show ``cpu_fraction`` on hover for dashboard workers circle plot. (:pr:`3906`) `Loïc Estève`_
- Prune virtual client on variable deletion (:pr:`3910`) `Marco Neumann`_
- Fix total aggregated metrics in dashboard (:pr:`3897`) `Loïc Estève`_
- Support Bokeh 2.1 (:pr:`3904`) `Matthew Rocklin`_
- Update ``related-work.rst`` (:pr:`3889`) `DomHudson`_
- Skip ``test_pid_file`` in older versions of Python (:pr:`3888`) `Matthew Rocklin`_
- Replace ``stream=`` with ``comm=`` in handlers (:pr:`3860`) `Julien Jerphanion`_
- Check hosts for ``None`` value in SSH cluster. (:pr:`3883`) `Matthias Bussonnier`_
- Allow dictionaries in ``security=`` keywords (:pr:`3874`) `Matthew Rocklin`_
- Use pickle protocol 5 with NumPy object arrays (:pr:`3871`) `jakirkham`_
- Cast any ``frame`` to ``uint8`` (same type as ``bytes``) (:pr:`3870`) `jakirkham`_
- Use ``Enum`` for worker, scheduler and nanny status. (:pr:`3853`) `Matthias Bussonnier`_
- Drop legacy ``buffer_interface`` assignment (:pr:`3869`) `jakirkham`_
- Drop old frame splitting in NumPy serialization (:pr:`3868`) `jakirkham`_
- Drop no longer needed local ``import pickle`` (:pr:`3865`) `jakirkham`_
- Fix typo in ``feed``'s log message (:pr:`3867`) `jakirkham`_
- Tidy pickle (:pr:`3866`) `jakirkham`_
- Handle empty times in task stream (:pr:`3862`) `Benjamin Zaitlen`_
- Change ``asyncssh`` objects to sphinx references (:pr:`3861`) `Jacob Tomlinson`_
- Improve ``SSHCluster`` docstring for ``connect_options`` (:pr:`3859`) `Jacob Tomlinson`_
- Validate address parameter in client constructor (:pr:`3842`) `joshreback`_
- Use ``SpecCluster`` name in worker names (:pr:`3855`) `Loïc Estève`_
- Allow async ``add_worker`` and ``remove_worker`` plugin methods (:pr:`3847`) `James Bourbeau`_


.. _v2.18.0 - 2020-06-05:

2.18.0 - 2020-06-05
-------------------

- Merge frames in ``deserialize_bytes`` (:pr:`3639`) `John Kirkham`_
- Allow ``SSHCluster`` to take a list of ``connect_options`` (:pr:`3854`) `Jacob Tomlinson`_
- Add favicon to performance report (:pr:`3852`) `Jacob Tomlinson`_
- Add dashboard plots for the amount of time spent per key and for transfer/serialization (:pr:`3792`) `Benjamin Zaitlen`_
- Fix variable name in journey of a task documentation (:pr:`3840`) `Matthias Bussonnier`_
- Fix typo in journey of a task doc (:pr:`3838`) `James Bourbeau`_
- Register ``dask_cudf`` serializers (:pr:`3832`) `John Kirkham`_
- Fix key check in ``rebalance`` missing keys (:pr:`3834`) `Jacob Tomlinson`_
- Allow collection of partial profile information in case of exceptions (:pr:`3773`) `Florian Jetter`_


.. _v2.17.0 - 2020-05-26:

2.17.0 - 2020-05-26
-------------------

- Record the time since the last run task on the scheduler (:pr:`3830`) `Matthew Rocklin`_
- Set colour of ``nbytes`` pane based on thresholds (:pr:`3805`) `Krishan Bhasin`_
- Include total number of tasks in the performance report (:pr:`3822`) `Abdulelah Bin Mahfoodh`_
- Allow to pass in task key strings in the worker restrictions (:pr:`3826`) `Nils Braun`_
- Control de/ser offload (:pr:`3793`) `Martin Durant`_
- Parse timeout parameters in ``Variable``/``Event``/``Lock`` to support text timeouts (:pr:`3825`) `Nils Braun`_
- Don't send empty dependencies (:pr:`3423`) `Jakub Beránek`_
- Add distributed Dask ``Event`` that mimics ``threading.Event`` (:pr:`3821`) `Nils Braun`_
- Enhance ``VersionMismatchWarning`` messages (:pr:`3786`) `Abdulelah Bin Mahfoodh`_
- Support Pickle's protocol 5 (:pr:`3784`) `jakirkham`_
- Replace ``utils.ignoring`` with ``contextlib.suppress`` (:pr:`3819`) `Nils Braun`_
- Make re-creating conda environments from the CI output easier (:pr:`3816`) `Lucas Rademaker`_
- Add prometheus metrics for semaphore (:pr:`3757`) `Lucas Rademaker`_
- Fix worker plugin called with superseded transition (:pr:`3812`) `Nick Evans`_
- Add retries to server listen (:pr:`3801`) `Jacob Tomlinson`_
- Remove commented out lines from ``scheduler.py`` (:pr:`3803`) `James Bourbeau`_
- Fix ``RuntimeWarning`` for never awaited coroutine when using ``distributed.Semaphore`` (:pr:`3713`) `Florian Jetter`_
- Fix profile thread leakage during test teardown on some platforms (:pr:`3795`) `Florian Jetter`_
- Await self before handling comms (:pr:`3788`) `Matthew Rocklin`_
- Fix typo in ``Cluster`` docstring (:pr:`3787`) `Scott Sanderson`_


.. _v2.16.0 - 2020-05-08:

2.16.0 - 2020-05-08
-------------------

- ``Client.get_dataset`` to always create ``Futures`` attached to itself (:pr:`3729`) `crusaderky`_
- Remove dev-requirements since it is unused (:pr:`3782`) `Julia Signell`_
- Use bokeh column for ``/system`` instead of custom css (:pr:`3781`) `Julia Signell`_
- Attempt to fix ``test_preload_remote_module`` on windows (:pr:`3775`) `James Bourbeau`_
- Fix broadcast for TLS comms (:pr:`3766`) `Florian Jetter`_
- Don't validate http preloads locally (:pr:`3768`) `Rami Chowdhury`_
- Allow range of ports to be specified for ``Workers`` (:pr:`3704`) `James Bourbeau`_
- Add UCX support for RDMACM (:pr:`3759`) `Peter Andreas Entschev`_
- Support web addresses in preload (:pr:`3755`) `Matthew Rocklin`_


.. _v2.15.2 - 2020-05-01:

2.15.2 - 2020-05-01
-------------------

- Connect to dashboard when address provided (:pr:`3758`) `Tom Augspurger`_
- Move ``test_gpu_metrics test`` (:pr:`3721`) `Tom Augspurger`_
- Nanny closing worker on ``KeyboardInterrupt`` (:pr:`3747`) `Mads R. B. Kristensen`_
- Replace ``OrderedDict`` with ``dict`` in scheduler (:pr:`3740`) `Matthew Rocklin`_
- Fix exception handling typo (:pr:`3751`) `Jonas Haag`_


.. _v2.15.1 - 2020-04-28:

2.15.1 - 2020-04-28
-------------------

- Ensure ``BokehTornado`` uses prefix (:pr:`3746`) `James Bourbeau`_
- Warn if cluster closes before starting (:pr:`3735`) `Matthew Rocklin`_
- Memoryview serialisation (:pr:`3743`) `Martin Durant`_
- Allows logging config under distributed key (:pr:`2952`) `Dillon Niederhut`_


.. _v2.15.0 - 2020-04-24:

2.15.0 - 2020-04-24
-------------------

- Reinstate support for legacy ``@gen_cluster`` functions (:pr:`3738`) `crusaderky`_
- Relax NumPy requirement in UCX (:pr:`3731`) `jakirkham`_
- Add Configuration Schema (:pr:`3696`) `Matthew Rocklin`_
- Reuse CI scripts for local installation process (:pr:`3698`) `crusaderky`_
- Use ``PeriodicCallback`` class from tornado (:pr:`3725`) `James Bourbeau`_
- Add ``remote_python`` option in ssh cmd (:pr:`3709`) `Abdulelah Bin Mahfoodh`_
- Configurable polling interval for cluster widget (:pr:`3723`) `Julia Signell`_
- Fix copy-paste in docs (:pr:`3728`) `Julia Signell`_
- Replace ``gen.coroutine`` with async-await in tests (:pr:`3706`) `crusaderky`_
- Fix flaky ``test_oversubscribing_leases`` (:pr:`3726`) `Florian Jetter`_
- Add ``batch_size`` to ``Client.map`` (:pr:`3650`) `Tom Augspurger`_
- Adjust semaphore test timeouts (:pr:`3720`) `Florian Jetter`_
- Dask-serialize dicts longer than five elements (:pr:`3689`) `Richard J Zamora`_
- Force ``threads_per_worker`` (:pr:`3715`) `crusaderky`_
- Idempotent semaphore acquire with retries (:pr:`3690`) `Florian Jetter`_
- Always use ``readinto`` in TCP (:pr:`3711`) `jakirkham`_
- Avoid ``DeprecationWarning`` from pandas (:pr:`3712`) `Tom Augspurger`_
- Allow modification of ``distributed.comm.retry`` at runtime (:pr:`3705`) `Florian Jetter`_
- Do not log an error on unset variable delete (:pr:`3652`) `Jonathan J. Helmus`_
- Add ``remote_python`` keyword to the new ``SSHCluster`` (:pr:`3701`) `Abdulelah Bin Mahfoodh`_
- Replace Example with Examples in docstrings (:pr:`3697`) `Matthew Rocklin`_
- Add ``Cluster`` ``__enter__`` and ``__exit__`` methods (:pr:`3699`) `Matthew Rocklin`_
- Fix propagating inherit config in ``SSHCluster`` for non-bash shells (:pr:`3688`) `Abdulelah Bin Mahfoodh`_
- Add ``Client.wait_to_workers`` to ``Client`` autosummary table (:pr:`3692`) `James Bourbeau`_
- Replace Bokeh Server with Tornado HTTPServer (:pr:`3658`) `Matthew Rocklin`_
- Fix ``dask-ssh`` after removing ``local-directory`` from ``dask_scheduler`` cli (:pr:`3684`) `Abdulelah Bin Mahfoodh`_
- Support preload modules in ``Nanny`` (:pr:`3678`) `Matthew Rocklin`_
- Refactor semaphore internals: make ``_get_lease`` synchronous (:pr:`3679`) `Lucas Rademaker`_
- Don't make task graphs too big (:pr:`3671`) `Martin Durant`_
- Pass through ``connection``/``listen_args`` as splatted keywords (:pr:`3674`) `Matthew Rocklin`_
- Run preload at import, start, and teardown (:pr:`3673`) `Matthew Rocklin`_
- Use relative URL in scheduler dashboard (:pr:`3676`) `Nicholas Smith`_
- Expose ``Security`` object as public API (:pr:`3675`) `Matthew Rocklin`_
- Add zoom tools to profile plots (:pr:`3672`) `James Bourbeau`_
- Update ``Scheduler.rebalance`` return value when data is missing (:pr:`3670`) `James Bourbeau`_


.. _v2.14.0 - 2020-04-03:

2.14.0 - 2020-04-03
-------------------

- Enable more UCX tests (:pr:`3667`) `jakirkham`_
- Remove openssl 1.1.1d pin for Travis (:pr:`3668`) `Jonathan J. Helmus`_
- More documentation for ``Semaphore`` (:pr:`3664`) `Florian Jetter`_
- Get CUDA context to finalize Numba ``DeviceNDArray`` (:pr:`3666`) `jakirkham`_
- Add Resouces option to ``get_task_stream`` and call ``output_file`` (:pr:`3653`) `Prasun Anand`_
- Add ``Semaphore`` extension (:pr:`3573`) `Lucas Rademaker`_
- Replace ``ncores`` with ``nthreads`` in work stealing tests (:pr:`3615`) `James Bourbeau`_
- Clean up some test warnings (:pr:`3662`) `Matthew Rocklin`_
- Write "why killed" docs (:pr:`3596`) `Martin Durant`_
- Update Python version checking (:pr:`3660`) `James Bourbeau`_
- Add newlines to ensure code formatting for ``retire_workers`` (:pr:`3661`) `Rami Chowdhury`_
- Clean up performance report test (:pr:`3655`) `Matthew Rocklin`_
- Avoid diagnostics time in performance report (:pr:`3654`) `Matthew Rocklin`_
- Introduce config for default task duration (:pr:`3642`) `Gabriel Sailer`_
- UCX simplify receiving frames in ``comm`` (:pr:`3651`) `jakirkham`_
- Bump checkout GitHub action to v2 (:pr:`3649`) `James Bourbeau`_
- Handle exception in ``faulthandler`` (:pr:`3646`) `Jacob Tomlinson`_
- Add prometheus metric for suspicious tasks (:pr:`3550`) `Gabriel Sailer`_
- Remove ``local-directory`` keyword (:pr:`3620`) `Prasun Anand`_
- Don't create output Futures in Client when there are mixed Client Futures (:pr:`3643`) `James Bourbeau`_
- Add link to ``contributing.md`` (:pr:`3621`) `Prasun Anand`_
- Update bokeh dependency in CI builds (:pr:`3637`) `James Bourbeau`_


.. _v2.13.0 - 2020-03-25:

2.13.0 - 2020-03-25
-------------------

- UCX synchronize default stream only on CUDA frames (:pr:`3638`) `Peter Andreas Entschev`_
- Add ``as_completed.clear`` method (:pr:`3617`) `Matthew Rocklin`_
- Drop unused line from ``pack_frames_prelude`` (:pr:`3634`) `John Kirkham`_
- Add logging message when closing idle dask scheduler (:pr:`3632`) `Matthew Rocklin`_
- Include frame lengths of CUDA objects in ``header["lengths"]`` (:pr:`3631`) `John Kirkham`_
- Ensure ``Client`` connection pool semaphore attaches to the ``Client`` event loop (:pr:`3546`) `James Bourbeau`_
- Remove dead stealing code (:pr:`3619`) `Florian Jetter`_
- Check ``nbytes`` and ``types`` before reading ``data`` (:pr:`3628`) `John Kirkham`_
- Ensure that we don't steal blacklisted fast tasks (:pr:`3591`) `Florian Jetter`_
- Support async ``Listener.stop`` functions (:pr:`3613`) `Matthew Rocklin`_
- Add str/repr methods to ``as_completed`` (:pr:`3618`) `Matthew Rocklin`_
- Add backoff to comm connect attempts. (:pr:`3496`) `Matthias Urlichs`_
- Make ``Listeners`` awaitable (:pr:`3611`) `Matthew Rocklin`_
- Increase number of visible mantissas in dashboard plots (:pr:`3585`) `Scott Sievert`_
- Pin openssl to 1.1.1d for Travis (:pr:`3602`) `Jacob Tomlinson`_
- Replace ``tornado.queues`` with ``asyncio.queues`` (:pr:`3607`) `James Bourbeau`_
- Remove ``dill`` from CI environments (:pr:`3608`) `Loïc Estève`_
- Fix linting errors (:pr:`3604`) `James Bourbeau`_
- Synchronize default CUDA stream before UCX send/recv (:pr:`3598`) `Peter Andreas Entschev`_
- Add configuration for ``Adaptive`` arguments (:pr:`3509`) `Gabriel Sailer`_
- Change ``Adaptive`` docs to reference ``adaptive_target`` (:pr:`3597`) `Julia Signell`_
- Optionally compress on a frame-by-frame basis (:pr:`3586`) `Matthew Rocklin`_
- Add Python version to version check (:pr:`3567`) `James Bourbeau`_
- Import ``tlz`` (:pr:`3579`) `John Kirkham`_
- Pin ``numpydoc`` to avoid double escaped ``*`` (:pr:`3530`) `Gil Forsyth`_
- Avoid ``performance_report`` crashing when a worker dies mid-compute (:pr:`3575`) `Krishan Bhasin`_
- Pin ``bokeh`` in CI builds (:pr:`3570`) `James Bourbeau`_
- Disable fast fail on GitHub Actions Windows CI (:pr:`3569`) `James Bourbeau`_
- Fix typo in ``Client.shutdown`` docstring (:pr:`3562`) `John Kirkham`_
- Add ``local_directory`` option to ``dask-ssh`` (:pr:`3554`) `Abdulelah Bin Mahfoodh`_


.. _v2.12.0 - 2020-03-06:

2.12.0 - 2020-03-06
-------------------

- Update ``TaskGroup`` remove logic (:pr:`3557`) `James Bourbeau`_
- Fix-up CuPy sparse serialization (:pr:`3556`) `John Kirkham`_
- API docs for ``LocalCluster`` and ``SpecCluster`` (:pr:`3548`) `Tom Augspurger`_
- Serialize sparse arrays (:pr:`3545`) `John Kirkham`_
- Allow tasks with restrictions to be stolen (:pr:`3069`) `Stan Seibert`_
- Use UCX default configuration instead of raising (:pr:`3544`) `Peter Andreas Entschev`_
- Support using other serializers with ``register_generic`` (:pr:`3536`) `John Kirkham`_
- DOC: update to async await (:pr:`3543`) `Tom Augspurger`_
- Use ``pytest.raises`` in ``test_ucx_config.py`` (:pr:`3541`) `John Kirkham`_
- Fix/more ucx config options (:pr:`3539`) `Benjamin Zaitlen`_
- Update heartbeat ``CommClosedError`` error handling (:pr:`3529`) `James Bourbeau`_
- Use ``makedirs`` when constructing ``local_directory`` (:pr:`3538`) `John Kirkham`_
- Mark ``None`` as MessagePack serializable (:pr:`3537`) `John Kirkham`_
- Mark ``bool`` as MessagePack serializable (:pr:`3535`) `John Kirkham`_
- Use 'temporary-directory' from ``dask.config`` for Nanny's directory (:pr:`3531`) `John Kirkham`_
- Add try-except around getting source code in performance report (:pr:`3505`) `Matthew Rocklin`_
- Fix typo in docstring (:pr:`3528`) `Davis Bennett`_
- Make work stealing callback time configurable (:pr:`3523`) `Lucas Rademaker`_
- RMM/UCX Config Flags (:pr:`3515`) `Benjamin Zaitlen`_
- Revise develop-docs: conda env example (:pr:`3406`) `Darren Weber`_
- Remove ``import ucp`` from the top of ``ucx.py`` (:pr:`3510`) `Peter Andreas Entschev`_
- Rename ``logs`` to ``get_logs`` (:pr:`3473`) `Jacob Tomlinson`_
- Stop keep alives when worker reconnecting to the scheduler (:pr:`3493`) `Jacob Tomlinson`_


.. _v2.11.0 - 2020-02-19:

2.11.0 - 2020-02-19
-------------------

- Add dask serialization of CUDA objects (:pr:`3482`) `John Kirkham`_
- Suppress cuML ``ImportError`` (:pr:`3499`) `John Kirkham`_
- Msgpack 1.0 compatibility (:pr:`3494`) `James Bourbeau`_
- Register cuML serializers (:pr:`3485`) `John Kirkham`_
- Check exact equality for worker state (:pr:`3483`) `Brett Naul`_
- Serialize 1-D, contiguous, ``uint8`` CUDA frames (:pr:`3475`) `John Kirkham`_
- Update NumPy array serialization to handle non-contiguous slices (:pr:`3474`) `James Bourbeau`_
- Propose fix for collection based resources docs (:pr:`3480`) `Chris Roat`_
- Remove ``--verbose`` flag from CI runs (:pr:`3484`) `Matthew Rocklin`_
- Do not duplicate messages in scheduler report (:pr:`3477`) `Jakub Beránek`_
- Register Dask cuDF serializers (:pr:`3478`) `John Kirkham`_
- Add support for Python 3.8 (:pr:`3249`) `James Bourbeau`_
- Add last seen column to worker table and highlight errant workers (:pr:`3468`) `kaelgreco`_
- Change default value of ``local_directory`` from empty string to ``None`` (:pr:`3441`) `condoratberlin`_
- Clear old docs (:pr:`3458`) `Matthew Rocklin`_
- Change default multiprocessing behavior to spawn (:pr:`3461`) `Matthew Rocklin`_
- Split dashboard host on additional slashes to handle inproc (:pr:`3466`) `Jacob Tomlinson`_
- Update ``locality.rst`` (:pr:`3470`) `Dustin Tindall`_
- Minor ``gen.Return`` cleanup (:pr:`3469`) `James Bourbeau`_
- Update comparison logic for worker state (:pr:`3321`) `rockwellw`_
- Update minimum ``tblib`` version to 1.6.0 (:pr:`3451`) `James Bourbeau`_
- Add total row to workers plot in dashboard (:pr:`3464`) `Julia Signell`_
- Workaround ``RecursionError`` on profile data (:pr:`3455`) `Tom Augspurger`_
- Include code and summary in performance report (:pr:`3462`) `Matthew Rocklin`_
- Skip ``test_open_close_many_workers`` on Python 3.6 (:pr:`3459`) `Matthew Rocklin`_
- Support serializing/deserializing ``rmm.DeviceBuffer`` s (:pr:`3442`) `John Kirkham`_
- Always add new ``TaskGroup`` to ``TaskPrefix`` (:pr:`3322`) `James Bourbeau`_
- Rerun ``black`` on the code base (:pr:`3444`) `John Kirkham`_
- Ensure ``__causes__`` s of exceptions raised on workers are serialized (:pr:`3430`) `Alex Adamson`_
- Adjust ``numba.cuda`` import and add check (:pr:`3446`) `John Kirkham`_
- Fix name of Numba serialization test (:pr:`3447`) `John Kirkham`_
- Checks for command parameters in ``ssh2`` (:pr:`3078`) `Peter Andreas Entschev`_
- Update ``worker_kwargs`` description in ``LocalCluster`` constructor (:pr:`3438`) `James Bourbeau`_
- Ensure scheduler updates task and worker states after successful worker data deletion (:pr:`3401`) `James Bourbeau`_
- Avoid ``loop=`` keyword in asyncio coordination primitives (:pr:`3437`) `Matthew Rocklin`_
- Call pip as a module to avoid warnings (:pr:`3436`) `Cyril Shcherbin`_
- Add documentation of parameters in coordination primitives (:pr:`3434`) `Søren Fuglede Jørgensen`_
- Replace ``tornado.locks`` with asyncio for Events/Locks/Conditions/Semaphore (:pr:`3397`) `Matthew Rocklin`_
- Remove object from class hierarchy (:pr:`3432`) `Anderson Banihirwe`_
- Add ``dashboard_link`` property to ``Client`` (:pr:`3429`) `Jacob Tomlinson`_
- Allow memory monitor to evict data more aggressively (:pr:`3424`) `fjetter`_
- Make ``_get_ip`` return an IP address when defaulting (:pr:`3418`) `Pierre Glaser`_
- Support version checking with older versions of Dask (:pr:`3390`) `Igor Gotlibovych`_
- Add Mac OS build to CI (:pr:`3358`) `James Bourbeau`_


.. _v2.10.0 - 2020-01-28:

2.10.0 - 2020-01-28
-------------------

- Fixed ``ZeroDivisionError`` in dashboard when no workers were present (:pr:`3407`) `James Bourbeau`_
- Respect the ``dashboard-prefix`` when redirecting from the root (:pr:`3387`) `Chrysostomos Nanakos`_
- Allow enabling / disabling work-stealing after the cluster has started (:pr:`3410`) `John Kirkham`_
- Support ``*args`` and ``**kwargs`` in offload (:pr:`3392`) `Matthew Rocklin`_
- Add lifecycle hooks to SchedulerPlugin (:pr:`3391`) `Matthew Rocklin`_


.. _v2.9.3 - 2020-01-17:

2.9.3 - 2020-01-17
------------------

- Raise ``RuntimeError`` if no running loop (:pr:`3385`) `James Bourbeau`_
- Fix ``get_running_loop`` import (:pr:`3383`) `James Bourbeau`_
- Get JavaScript document location instead of window and handle proxied url (:pr:`3382`) `Jacob Tomlinson`_


.. _v2.9.2 - 2020-01-16:

2.9.2 - 2020-01-16
------------------

- Move Windows CI to GitHub Actions (:pr:`3373`) `Jacob Tomlinson`_
- Add client join and leave hooks (:pr:`3371`) `Jacob Tomlinson`_
- Add cluster map dashboard (:pr:`3361`) `Jacob Tomlinson`_
- Close connection comm on retry (:pr:`3365`) `James Bourbeau`_
- Fix scheduler state in case of worker name collision (:pr:`3366`) `byjott`_
- Add ``--worker-class`` option to ``dask-worker`` CLI (:pr:`3364`) `James Bourbeau`_
- Remove ``locale`` check that fails on OS X (:pr:`3360`) `Jacob Tomlinson`_
- Rework version checking (:pr:`2627`) `Matthew Rocklin`_
- Add websocket scheduler plugin (:pr:`3335`) `Jacob Tomlinson`_
- Return task in ``dask-worker`` ``on_signal`` function (:pr:`3354`) `James Bourbeau`_
- Fix failures on mixed integer/string worker names (:pr:`3352`) `Benedikt Reinartz`_
- Avoid calling ``nbytes`` multiple times when sending data (:pr:`3349`) `Markus Mohrhard`_
- Avoid setting event loop policy if within IPython kernel and no running event loop (:pr:`3336`) `Mana Borwornpadungkitti`_
- Relax intermittent failing ``test_profile_server`` (:pr:`3346`) `Matthew Rocklin`_


.. _v2.9.1 - 2019-12-27:

2.9.1 - 2019-12-27
------------------

-  Add lock around dumps_function cache (:pr:`3337`) `Matthew Rocklin`_
-  Add setuptools to dependencies (:pr:`3320`) `James Bourbeau`_
-  Use TaskPrefix.name in Graph layout (:pr:`3328`) `Matthew Rocklin`_
-  Add missing `"` in performance report example (:pr:`3329`) `John Kirkham`_
-  Add performance report docs and color definitions to docs (:pr:`3325`) `Benjamin Zaitlen`_
-  Switch startstops to dicts and add worker name to transfer (:pr:`3319`) `Jacob Tomlinson`_
-  Add plugin entry point for out-of-tree comms library (:pr:`3305`) `Patrick Sodré`_
-  All scheduler task states in prometheus (:pr:`3307`) `fjetter`_
-  Use worker name in logs (:pr:`3309`) `Stephan Erb`_
-  Add TaskGroup and TaskPrefix scheduler state (:pr:`3262`)  `Matthew Rocklin`_
-  Update latencies with heartbeats (:pr:`3310`) `fjetter`_
-  Update inlining Futures in task graph in Client._graph_to_futures (:pr:`3303`) `James Bourbeau`_
-  Use hostname as default IP address rather than localhost (:pr:`3308`) `Matthew Rocklin`_
-  Clean up flaky test_nanny_throttle (:pr:`3295`) `Tom Augspurger`_
-  Add lock to scheduler for sensitive operations (:pr:`3259`) `Matthew Rocklin`_
-  Log address for each of the Scheduler listerners (:pr:`3306`) `Matthew Rocklin`_
-  Make ConnectionPool.close asynchronous (:pr:`3304`) `Matthew Rocklin`_


.. _v2.9.0 - 2019-12-06:

2.9.0 - 2019-12-06
------------------

- Add ``dask-spec`` CLI tool (:pr:`3090`) `Matthew Rocklin`_
- Connectionpool: don't hand out closed connections (:pr:`3301`) `byjott`_
- Retry operations on network issues (:pr:`3294`) `byjott`_
- Skip ``Security.temporary()`` tests if cryptography not installed (:pr:`3302`) `James Bourbeau`_
- Support multiple listeners in the scheduler (:pr:`3288`) `Matthew Rocklin`_
- Updates RMM comment to the correct release (:pr:`3299`) `John Kirkham`_
- Add title to ``performance_report`` (:pr:`3298`) `Matthew Rocklin`_
- Forgot to fix slow test (:pr:`3297`) `Benjamin Zaitlen`_
- Update ``SSHCluster`` docstring parameters (:pr:`3296`) `James Bourbeau`_
- ``worker.close()`` awaits ``batched_stream.close()`` (:pr:`3291`) `Mads R. B. Kristensen`_
- Fix asynchronous listener in UCX (:pr:`3292`) `Benjamin Zaitlen`_
- Avoid repeatedly adding deps to already in memory stack (:pr:`3293`) `James Bourbeau`_
- xfail ucx empty object typed dataframe (:pr:`3279`) `Benjamin Zaitlen`_
- Fix ``distributed.wait`` documentation (:pr:`3289`) `Tom Rochette`_
- Move Python 3 syntax tests into main tests (:pr:`3281`) `Matthew Rocklin`_
- xfail ``test_workspace_concurrency`` for Python 3.6 (:pr:`3283`) `Matthew Rocklin`_
- Add ``performance_report`` context manager for static report generation (:pr:`3282`) `Matthew Rocklin`_
- Update function serialization caches with custom LRU class (:pr:`3260`) `James Bourbeau`_
- Make ``Listener.start`` asynchronous (:pr:`3278`) `Matthew Rocklin`_
- Remove ``dask-submit`` and ``dask-remote`` (:pr:`3280`) `Matthew Rocklin`_
- Worker profile server (:pr:`3274`) `Matthew Rocklin`_
- Improve bandwidth workers plot (:pr:`3273`) `Matthew Rocklin`_
- Make profile coroutines consistent between ``Scheduler`` and ``Worker`` (:pr:`3277`) `Matthew Rocklin`_
- Enable saving profile information from server threads (:pr:`3271`) `Matthew Rocklin`_
- Remove memory use plot (:pr:`3269`) `Matthew Rocklin`_
- Add offload size to configuration (:pr:`3270`) `Matthew Rocklin`_
- Fix layout scaling on profile plots (:pr:`3268`) `Jacob Tomlinson`_
- Set ``x_range`` in CPU plot based on the number of threads (:pr:`3266`) `Matthew Rocklin`_
- Use base-2 values for byte-valued axes in dashboard (:pr:`3267`) `Matthew Rocklin`_
- Robust gather in case of connection failures (:pr:`3246`) `fjetter`_
- Use ``DeviceBuffer`` from newer RMM releases (:pr:`3261`) `John Kirkham`_
- Fix dev requirements for pytest (:pr:`3264`) `Elliott Sales de Andrade`_
- Add validate options to configuration (:pr:`3258`) `Matthew Rocklin`_


.. _v2.8.1 - 2019-11-22:

2.8.1 - 2019-11-22
------------------

- Fix hanging worker when the scheduler leaves (:pr:`3250`) `Tom Augspurger`_
- Fix NumPy writeable serialization bug (:pr:`3253`) `James Bourbeau`_
- Skip ``numba.cuda`` tests if CUDA is not available (:pr:`3255`) `Peter Andreas Entschev`_
- Add new dashboard plot for memory use by key (:pr:`3243`) `Matthew Rocklin`_
- Fix ``array.shape()`` -> ``array.shape`` (:pr:`3247`) `Jed Brown`_
- Fixed typos in ``pubsub.py`` (:pr:`3244`) `He Jia`_
- Fixed cupy array going out of scope (:pr:`3240`) `Mads R. B. Kristensen`_
- Remove ``gen.coroutine`` usage in scheduler (:pr:`3242`) `Jim Crist-Harif`_
- Use ``inspect.isawaitable`` where relevant (:pr:`3241`) `Jim Crist-Harif`_


.. _v2.8.0 - 2019-11-14:

2.8.0 - 2019-11-14
------------------

-  Add UCX config values (:pr:`3135`) `Matthew Rocklin`_
-  Relax test_MultiWorker (:pr:`3210`) `Matthew Rocklin`_
-  Avoid ucp.init at import time (:pr:`3211`) `Matthew Rocklin`_
-  Clean up rpc to avoid intermittent test failure (:pr:`3215`) `Matthew Rocklin`_
-  Respect protocol if given to Scheduler (:pr:`3212`) `Matthew Rocklin`_
-  Use legend_field= keyword in bokeh plots (:pr:`3218`) `Matthew Rocklin`_
-  Cache psutil.Process object in Nanny (:pr:`3207`) `Matthew Rocklin`_
-  Replace gen.sleep with asyncio.sleep (:pr:`3208`) `Matthew Rocklin`_
-  Avoid offloading serialization for small messages (:pr:`3224`) `Matthew Rocklin`_
-  Add desired_workers metric (:pr:`3221`) `Gabriel Sailer`_
-  Fail fast when importing distributed.comm.ucx (:pr:`3228`) `Matthew Rocklin`_
-  Add module name to Future repr (:pr:`3231`) `Matthew Rocklin`_
-  Add name to Pub/Sub repr (:pr:`3235`) `Matthew Rocklin`_
-  Import CPU_COUNT from dask.system (:pr:`3199`) `James Bourbeau`_
-  Efficiently serialize zero strided NumPy arrays (:pr:`3180`) `James Bourbeau`_
-  Cache function deserialization in workers (:pr:`3234`) `Matthew Rocklin`_
-  Respect ordering of futures in futures_of (:pr:`3236`) `Matthew Rocklin`_
-  Bump dask dependency to 2.7.0 (:pr:`3237`) `James Bourbeau`_
-  Avoid setting inf x_range (:pr:`3229`) `rockwellw`_
-  Clear task stream based on recent behavior (:pr:`3200`) `Matthew Rocklin`_
-  Use the percentage field for profile plots (:pr:`3238`) `Matthew Rocklin`_

.. _v2.7.0 - 2019-11-08:

2.7.0 - 2019-11-08
------------------

This release drops support for Python 3.5

-  Adds badges to README.rst [skip ci] (:pr:`3152`) `James Bourbeau`_
-  Don't overwrite `self.address` if it is present (:pr:`3153`) `Gil Forsyth`_
-  Remove outdated references to debug scheduler and worker bokeh pages. (:pr:`3160`) `darindf`_
-  Update CONTRIBUTING.md (:pr:`3159`) `Jacob Tomlinson`_
-  Add Prometheus metric for a worker's executing tasks count (:pr:`3163`) `darindf`_
-  Update Prometheus documentation (:pr:`3165`) `darindf`_
-  Fix Numba serialization when strides is None (:pr:`3166`) `Peter Andreas Entschev`_
-  Await cluster in Adaptive.recommendations (:pr:`3168`) `Simon Boothroyd`_
-  Support automatic TLS (:pr:`3164`) `Jim Crist`_
-  Avoid swamping high-memory workers with data requests (:pr:`3071`) `Tom Augspurger`_
-  Update UCX variables to use sockcm by default (:pr:`3177`) `Peter Andreas Entschev`_
-  Get protocol in Nanny/Worker from scheduler address (:pr:`3175`) `Peter Andreas Entschev`_
-  Add worker and tasks state for Prometheus data collection (:pr:`3174`) `darindf`_
-  Use async def functions for offload to/from_frames (:pr:`3171`) `Mads R. B. Kristensen`_
-  Subprocesses inherit the global dask config (:pr:`3192`) `Mads R. B. Kristensen`_
-  XFail test_open_close_many_workers (:pr:`3194`) `Matthew Rocklin`_
-  Drop Python 3.5 (:pr:`3179`) `James Bourbeau`_
-  UCX: avoid double init after fork (:pr:`3178`) `Mads R. B. Kristensen`_
-  Silence warning when importing while offline (:pr:`3203`) `James A. Bednar`_
-  Adds docs to Client methods for resources, actors, and traverse (:pr:`2851`) `IPetrik`_
-  Add test for concurrent scatter operations (:pr:`2244`) `Matthew Rocklin`_
-  Expand async docs (:pr:`2293`) `Dave Hirschfeld`_
-  Add PatchedDeviceArray to drop stride attribute for cupy<7.0 (:pr:`3198`) `Richard J Zamora`_

.. _v2.6.0 - 2019-10-15:

2.6.0 - 2019-10-15
------------------

- Refactor dashboard module (:pr:`3138`) `Jacob Tomlinson`_
- Use ``setuptools.find_packages`` in ``setup.py`` (:pr:`3150`) `Matthew Rocklin`_
- Move death timeout logic up to ``Node.start`` (:pr:`3115`) `Matthew Rocklin`_
- Only include metric in ``WorkerTable`` if it is a scalar (:pr:`3140`) `Matthew Rocklin`_
- Add ``Nanny(config={...})`` keyword (:pr:`3134`) `Matthew Rocklin`_
- Xfail ``test_worksapce_concurrency`` on Python 3.6 (:pr:`3132`) `Matthew Rocklin`_
- Extend Worker plugin API with transition method (:pr:`2994`) `matthieubulte`_
- Raise exception if the user passes in unused keywords to ``Client`` (:pr:`3117`) `Jonathan De Troye`_
- Move new ``SSHCluster`` to top level (:pr:`3128`) `Matthew Rocklin`_
- Bump dask dependency (:pr:`3124`) `Jim Crist`_


.. _v2.5.2 - 2019-10-04:

2.5.2 - 2019-10-04
------------------

-  Make dask-worker close quietly when given sigint signal (:pr:`3116`) `Matthew Rocklin`_
-  Replace use of tornado.gen with asyncio in dask-worker (:pr:`3114`) `Matthew Rocklin`_
-  UCX: allocate CUDA arrays using RMM and Numba (:pr:`3109`) `Mads R. B. Kristensen`_
-  Support calling `cluster.scale` as async method (:pr:`3110`) `Jim Crist`_
-  Identify lost workers in SpecCluster based on address not name (:pr:`3088`) `James Bourbeau`_
-  Add Client.shutdown method (:pr:`3106`) `Matthew Rocklin`_
-  Collect worker-worker and type bandwidth information (:pr:`3094`) `Matthew Rocklin`_
-  Send noise over the wire to keep dask-ssh connection alive (:pr:`3105`) `Gil Forsyth`_
-  Retry scheduler connect multiple times (:pr:`3104`) `Jacob Tomlinson`_
-  Add favicon of logo to the dashboard (:pr:`3095`) `James Bourbeau`_
-  Remove utils.py functions for their dask/utils.py equivalents (:pr:`3042`) `Matthew Rocklin`_
-  Lower default bokeh log level (:pr:`3087`) `Philipp Rudiger`_
-  Check if self.cluster.scheduler is a local scheduler (:pr:`3099`) `Jacob Tomlinson`_


.. _v2.5.1 - 2019-09-27:

2.5.1 - 2019-09-27
------------------

-   Support clusters that don't have .security or ._close methods (:pr:`3100`) `Matthew Rocklin`_


.. _v2.5.0 - 2019-09-27:

2.5.0 - 2019-09-27
------------------

-  Use the new UCX Python bindings (:pr:`3059`) `Mads R. B. Kristensen`_
-  Fix worker preload config (:pr:`3027`) `byjott`_
-  Fix widget with spec that generates multiple workers (:pr:`3067`) `Loïc Estève`_
-  Make Client.get_versions async friendly (:pr:`3064`) `Jacob Tomlinson`_
-  Add configuation option for longer error tracebacks (:pr:`3086`) `Daniel Farrell`_
-  Have Client get Security from passed Cluster (:pr:`3079`) `Matthew Rocklin`_
-  Respect Cluster.dashboard_link in Client._repr_html_ if it exists (:pr:`3077`) `Matthew Rocklin`_
-  Add monitoring with dask cluster docs (:pr:`3072`) `Arpit Solanki`_
-  Protocol of cupy and numba handles serialization exclusively  (:pr:`3047`) `Mads R. B. Kristensen`_
-  Allow specification of worker type in SSHCLuster (:pr:`3061`) `Jacob Tomlinson`_
-  Use Cluster.scheduler_info for workers= value in repr (:pr:`3058`) `Matthew Rocklin`_
-  Allow SpecCluster to scale by memory and cores (:pr:`3057`) `Matthew Rocklin`_
-  Allow full script in preload inputs (:pr:`3052`) `Matthew Rocklin`_
-  Check multiple cgroups dirs, ceil fractional cpus (:pr:`3056`) `Jim Crist`_
-  Add blurb about disabling work stealing (:pr:`3055`) `Chris White`_


.. _v2.4.0 - 2019-09-13:

2.4.0 - 2019-09-13
------------------

- Remove six (:pr:`3045`) `Matthew Rocklin`_
- Add missing test data to sdist tarball (:pr:`3050`) `Elliott Sales de Andrade`_
- Use mock from unittest standard library (:pr:`3049`) `Elliott Sales de Andrade`_
- Use cgroups resource limits to determine default threads and memory (:pr:`3039`) `Jim Crist`_
- Move task deserialization to immediately before task execution (:pr:`3015`) `James Bourbeau`_
- Drop joblib shim module in distributed (:pr:`3040`) `John Kirkham`_
- Redirect configuration doc page (:pr:`3038`) `Matthew Rocklin`_
- Support ``--name 0`` and ``--nprocs`` keywords in dask-worker cli (:pr:`3037`) `Matthew Rocklin`_
- Remove lost workers from ``SpecCluster.workers`` (:pr:`2990`) `Guillaume Eynard-Bontemps`_
- Clean up ``test_local.py::test_defaults`` (:pr:`3017`) `Matthew Rocklin`_
- Replace print statement in ``Queue.__init__`` with debug message (:pr:`3035`) `Mikhail Akimov`_
- Set the ``x_range`` limit of the Meory utilization plot to memory-limit (:pr:`3034`) `Matthew Rocklin`_
- Rely on cudf codebase for cudf serialization (:pr:`2998`) `Benjamin Zaitlen`_
- Add fallback html repr for Cluster (:pr:`3023`) `Jim Crist`_
- Add support for zstandard compression to comms (:pr:`2970`) `Abael He`_
- Avoid collision when using ``os.environ`` in ``dashboard_link`` (:pr:`3021`) `Matthew Rocklin`_
- Fix ``ConnectionPool`` limit handling (:pr:`3005`) `byjott`_
- Support Spec jobs that generate multiple workers (:pr:`3013`) `Matthew Rocklin`_
- Tweak ``Logs`` styling (:pr:`3012`) `Jim Crist`_
- Better name for cudf deserialization function name (:pr:`3008`) `Benjamin Zaitlen`_
- Make ``spec.ProcessInterface`` a valid no-op worker (:pr:`3004`) `Matthew Rocklin`_
- Return dictionaries from ``new_worker_spec`` rather than name/worker pairs (:pr:`3000`) `Matthew Rocklin`_
- Fix minor typo in documentation (:pr:`3002`) `Mohammad Noor`_
- Permit more keyword options when scaling with cores and memory (:pr:`2997`) `Matthew Rocklin`_
- Add ``cuda_ipc`` to UCX environment for NVLink (:pr:`2996`) `Benjamin Zaitlen`_
- Add ``threads=`` and ``memory=`` to Cluster and Client reprs (:pr:`2995`) `Matthew Rocklin`_
- Fix PyNVML initialization (:pr:`2993`) `Richard J Zamora`_


.. _v2.3.2 - 2019-08-23:

2.3.2 - 2019-08-23
------------------

-  Skip exceptions in startup information (:pr:`2991`) `Jacob Tomlinson`_


.. _v2.3.1 - 2019-08-22:

2.3.1 - 2019-08-22
------------------

-  Add support for separate external address for SpecCluster scheduler (:pr:`2963`) `Jacob Tomlinson`_
-  Defer cudf serialization/deserialization to that library (:pr:`2881`) `Benjamin Zaitlen`_
-  Workaround for hanging test now calls ucp.fin() (:pr:`2967`) `Mads R. B. Kristensen`_
-  Remove unnecessary bullet point (:pr:`2972`) `Pav A`_
-  Directly import progress from diagnostics.progressbar (:pr:`2975`) `Matthew Rocklin`_
-  Handle buffer protocol objects in ensure_bytes (:pr:`2969`) `Tom Augspurger`_
-  Fix documentatation syntax and tree (:pr:`2981`) `Pav A`_
-  Improve get_ip_interface error message when interface does not exist (:pr:`2964`) `Loïc Estève`_
-  Add cores= and memory= keywords to scale (:pr:`2974`) `Matthew Rocklin`_
-  Make workers robust to bad custom metrics (:pr:`2984`) `Matthew Rocklin`_


.. _v2.3.0 - 2019-08-16:

2.3.0 - 2019-08-16
------------------

- Except all exceptions when checking ``pynvml`` (:pr:`2961`) `Matthew Rocklin`_
- Pass serialization down through small base collections (:pr:`2948`) `Peter Andreas Entschev`_
- Use ``pytest.warning(Warning)`` rather than ``Exception`` (:pr:`2958`) `Matthew Rocklin`_
- Allow ``server_kwargs`` to override defaults in dashboard (:pr:`2955`) `Bruce Merry`_
- Update ``utils_perf.py`` (:pr:`2954`) `Shayan Amani`_
- Normalize names with ``str`` in ``retire_workers`` (:pr:`2949`) `Matthew Rocklin`_
- Update ``client.py`` (:pr:`2951`) `Shayan Amani`_
- Add ``GPUCurrentLoad`` dashboard plots (:pr:`2944`) `Matthew Rocklin`_
- Pass GPU diagnostics from worker to scheduler (:pr:`2932`) `Matthew Rocklin`_
- Import from ``collections.abc`` (:pr:`2938`) `Jim Crist`_
- Fixes Worker docstring formatting (:pr:`2939`) `James Bourbeau`_
- Redirect setup docs to docs.dask.org (:pr:`2936`) `Matthew Rocklin`_
- Wrap offload in ``gen.coroutine`` (:pr:`2934`) `Matthew Rocklin`_
- Change ``TCP.close`` to a coroutine to avoid task pending warning (:pr:`2930`) `Matthew Rocklin`_
- Fixup black string normalization (:pr:`2929`) `Jim Crist`_
- Move core functionality from ``SpecCluster`` to ``Cluster`` (:pr:`2913`) `Matthew Rocklin`_
- Add aenter/aexit protocols to ``ProcessInterface`` (:pr:`2927`) `Matthew Rocklin`_
- Add real-time CPU utilization plot to dashboard (:pr:`2922`) `Matthew Rocklin`_
- Always kill processes in clean tests, even if we don't check (:pr:`2924`) `Matthew Rocklin`_
- Add timeouts to processes in SSH tests (:pr:`2925`) `Matthew Rocklin`_
- Add documentation around ``spec.ProcessInterface`` (:pr:`2923`) `Matthew Rocklin`_
- Cleanup async warnings in tests (:pr:`2920`) `Matthew Rocklin`_
- Give 404 when requesting nonexistent tasks or workers (:pr:`2921`) `Martin Durant`_
- Raise informative warning when rescheduling an unknown task (:pr:`2916`) `James Bourbeau`_
- Fix docstring (:pr:`2917`) `Martin Durant`_
- Add keep-alive message between worker and scheduler (:pr:`2907`) `Matthew Rocklin`_
- Rewrite ``Adaptive``/``SpecCluster`` to support slowly arriving workers (:pr:`2904`) `Matthew Rocklin`_
- Call heartbeat rather than reconnect on disconnection (:pr:`2906`) `Matthew Rocklin`_


.. _v2.2.0 - 2019-07-31:

2.2.0 - 2019-07-31
------------------

-  Respect security configuration in LocalCluster (:pr:`2822`) `Russ Bubley`_
-  Add Nanny to worker docs (:pr:`2826`) `Christian Hudon`_
-  Don't make False add-keys report to scheduler (:pr:`2421`) `tjb900`_
-  Include type name in SpecCluster repr (:pr:`2834`) `Jacob Tomlinson`_
-  Extend prometheus metrics endpoint (:pr:`2833`) `Gabriel Sailer`_
-  Add alternative SSHCluster implementation (:pr:`2827`) `Matthew Rocklin`_
-  Dont reuse closed worker in get_worker (:pr:`2841`) `Pierre Glaser`_
-  SpecCluster: move init logic into start (:pr:`2850`) `Jacob Tomlinson`_
-  Document distributed.Reschedule in API docs (:pr:`2860`) `James Bourbeau`_
-  Add fsspec to installation of test builds (:pr:`2859`) `Martin Durant`_
-  Make await/start more consistent across Scheduler/Worker/Nanny (:pr:`2831`) `Matthew Rocklin`_
-  Add cleanup fixture for asyncio tests (:pr:`2866`) `Matthew Rocklin`_
-  Use only remote connection to scheduler in Adaptive (:pr:`2865`) `Matthew Rocklin`_
-  Add Server.finished async function  (:pr:`2864`) `Matthew Rocklin`_
-  Align text and remove bullets in Client HTML repr (:pr:`2867`) `Matthew Rocklin`_
-  Test dask-scheduler --idle-timeout flag (:pr:`2862`) `Matthew Rocklin`_
-  Remove ``Client.upload_environment`` (:pr:`2877`) `Jim Crist`_
-  Replace gen.coroutine with async/await in core (:pr:`2871`) `Matthew Rocklin`_
-  Forcefully kill all processes before each test (:pr:`2882`) `Matthew Rocklin`_
-  Cleanup Security class and configuration (:pr:`2873`) `Jim Crist`_
-  Remove unused variable in SpecCluster scale down (:pr:`2870`) `Jacob Tomlinson`_
-  Add SpecCluster ProcessInterface (:pr:`2874`) `Jacob Tomlinson`_
-  Add Log(str) and Logs(dict) classes for nice HTML reprs (:pr:`2875`) `Jacob Tomlinson`_
-  Pass Client._asynchronous to Cluster._asynchronous (:pr:`2890`) `Matthew Rocklin`_
-  Add default logs method to Spec Cluster (:pr:`2889`) `Matthew Rocklin`_
-  Add processes keyword back into clean (:pr:`2891`) `Matthew Rocklin`_
-  Update black (:pr:`2901`) `Matthew Rocklin`_
-  Move Worker.local_dir attribute to Worker.local_directory (:pr:`2900`) `Matthew Rocklin`_
-  Link from TapTools to worker info pages in dashboard (:pr:`2894`) `Matthew Rocklin`_
-  Avoid exception in Client._ensure_connected if closed (:pr:`2893`) `Matthew Rocklin`_
-  Convert Pythonic kwargs to CLI Keywords for SSHCluster (:pr:`2898`) `Matthew Rocklin`_
-  Use kwargs in CLI (:pr:`2899`) `Matthew Rocklin`_
-  Name SSHClusters by providing name= keyword to SpecCluster (:pr:`2903`) `Matthew Rocklin`_
-  Request feed of worker information from Scheduler to SpecCluster (:pr:`2902`) `Matthew Rocklin`_
-  Clear out compatibillity file (:pr:`2896`) `Matthew Rocklin`_
-  Remove future imports (:pr:`2897`) `Matthew Rocklin`_
-  Use click's show_default=True in relevant places (:pr:`2838`) `Christian Hudon`_
-  Close workers more gracefully (:pr:`2905`) `Matthew Rocklin`_
-  Close workers gracefully with --lifetime keywords (:pr:`2892`) `Matthew Rocklin`_
-  Add closing <li> tags to Client._repr_html_ (:pr:`2911`) `Matthew Rocklin`_
-  Add endline spacing in Logs._repr_html_ (:pr:`2912`) `Matthew Rocklin`_

.. _v2.1.0 - 2019-07-08:

2.1.0 - 2019-07-08
------------------

- Fix typo that prevented error message (:pr:`2825`) `Russ Bubley`_
- Remove ``dask-mpi`` (:pr:`2824`) `Matthew Rocklin`_
- Updates to use ``update_graph`` in task journey docs (:pr:`2821`) `James Bourbeau`_
- Fix Client repr with ``memory_info=None`` (:pr:`2816`) `Matthew Rocklin`_
- Fix case where key, rather than ``TaskState``, could end up in ``ts.waiting_on`` (:pr:`2819`) `tjb900`_
- Use Keyword-only arguments (:pr:`2814`) `Matthew Rocklin`_
- Relax check for worker references in cluster context manager (:pr:`2813`) `Matthew Rocklin`_
- Add HTTPS support for the dashboard (:pr:`2812`) `Jim Crist`_
- Use ``dask.utils.format_bytes`` (:pr:`2810`) `Tom Augspurger`_


.. _v2.0.1 - 2019-06-26:

2.0.1 - 2019-06-26
------------------

We neglected to include ``python_requires=`` in our setup.py file, resulting in
confusion for Python 2 users who erroneously get packages for 2.0.0.
This is fixed in 2.0.1 and we have removed the 2.0.0 files from PyPI.

-  Add python_requires entry to setup.py (:pr:`2807`) `Matthew Rocklin`_
-  Correctly manage tasks beyond deque limit in TaskStream plot (:pr:`2797`) `Matthew Rocklin`_
-  Fix diagnostics page for memory_limit=None (:pr:`2770`) `Brett Naul`_


.. _v2.0.0 - 2019-06-25:

2.0.0 - 2019-06-25
------------------

-  **Drop support for Python 2**
-  Relax warnings before release (:pr:`2796`) `Matthew Rocklin`_
-  Deprecate --bokeh/--no-bokeh CLI (:pr:`2800`) `Tom Augspurger`_
-  Typo in bokeh service_kwargs for dask-worker (:pr:`2783`) `Tom Augspurger`_
-  Update command line cli options docs (:pr:`2794`) `James Bourbeau`_
-  Remove "experimental" from TLS docs (:pr:`2793`) `James Bourbeau`_
-  Add warnings around ncores= keywords (:pr:`2791`) `Matthew Rocklin`_
-  Add --version option to scheduler and worker CLI (:pr:`2782`) `Tom Augspurger`_
-  Raise when workers initialization times out (:pr:`2784`) `Tom Augspurger`_
-  Replace ncores with nthreads throughout codebase (:pr:`2758`) `Matthew Rocklin`_
-  Add unknown pytest markers (:pr:`2764`) `Tom Augspurger`_
-  Delay lookup of allowed failures. (:pr:`2761`) `Tom Augspurger`_
-  Change address -> worker in ColumnDataSource for nbytes plot (:pr:`2755`) `Matthew Rocklin`_
-  Remove module state in Prometheus Handlers (:pr:`2760`) `Matthew Rocklin`_
-  Add stress test for UCX (:pr:`2759`) `Matthew Rocklin`_
-  Add nanny logs (:pr:`2744`) `Tom Augspurger`_
-  Move some of the adaptive logic into the scheduler (:pr:`2735`) `Matthew Rocklin`_
-  Add SpecCluster.new_worker_spec method (:pr:`2751`) `Matthew Rocklin`_
-  Worker dashboard fixes (:pr:`2747`) `Matthew Rocklin`_
-  Add async context managers to scheduler/worker classes (:pr:`2745`) `Matthew Rocklin`_
-  Fix the resource key representation before sending graphs (:pr:`2733`) `Michael Spiegel`_
-  Allow user to configure whether workers are daemon. (:pr:`2739`) `Caleb`_
-  Pin pytest >=4 with pip in appveyor and python 3.5 (:pr:`2737`) `Matthew Rocklin`_
-  Add Experimental UCX Comm (:pr:`2591`) `Ben Zaitlen`_ `Tom Augspurger`_ `Matthew Rocklin`_
-  Close nannies gracefully (:pr:`2731`) `Matthew Rocklin`_
-  add kwargs to progressbars (:pr:`2638`) `Manuel Garrido`_
-  Add back LocalCluster.__repr__. (:pr:`2732`) `Loïc Estève`_
-  Move bokeh module to dashboard (:pr:`2724`) `Matthew Rocklin`_
-  Close clusters at exit (:pr:`2730`) `Matthew Rocklin`_
-  Add SchedulerPlugin TaskState example (:pr:`2622`) `Matt Nicolls`_
-  Add SpecificationCluster (:pr:`2675`) `Matthew Rocklin`_
-  Replace register_worker_callbacks with worker plugins (:pr:`2453`) `Matthew Rocklin`_
-  Proxy worker dashboards from scheduler dashboard (:pr:`2715`) `Ben Zaitlen`_
-  Add docstring to Scheduler.check_idle_saturated (:pr:`2721`) `Matthew Rocklin`_
-  Refer to LocalCluster in Client docstring (:pr:`2719`) `Matthew Rocklin`_
-  Remove special casing of Scikit-Learn BaseEstimator serialization (:pr:`2713`) `Matthew Rocklin`_
-  Fix two typos in Pub class docstring (:pr:`2714`) `Magnus Nord`_
-  Support uploading files with multiple modules (:pr:`2587`) `Sam Grayson`_
-  Change the main workers bokeh page to /status (:pr:`2689`) `Ben Zaitlen`_
-  Cleanly stop periodic callbacks in Client (:pr:`2705`) `Matthew Rocklin`_
-  Disable pan tool for the Progress, Byte Stored and Tasks Processing plot (:pr:`2703`) `Mathieu Dugré`_
-  Except errors in Nanny's memory monitor if process no longer exists (:pr:`2701`) `Matthew Rocklin`_
-  Handle heartbeat when worker has just left (:pr:`2702`) `Matthew Rocklin`_
-  Modify styling of histograms for many-worker dashboard plots (:pr:`2695`) `Mathieu Dugré`_
-  Add method to wait for n workers before continuing (:pr:`2688`) `Daniel Farrell`_
-  Support computation on delayed(None) (:pr:`2697`)  `Matthew Rocklin`_
-  Cleanup localcluster (:pr:`2693`)  `Matthew Rocklin`_
-  Use 'temporary-directory' from dask.config for Worker's directory (:pr:`2654`) `Matthew Rocklin`_
-  Remove support for Iterators and Queues (:pr:`2671`) `Matthew Rocklin`_


.. _v1.28.1 - 2019-05-13:

1.28.1 - 2019-05-13
-------------------

This is a small bugfix release due to a config change upstream.

-  Use config accessor method for "scheduler-address" (:pr:`2676`) `James Bourbeau`_


.. _v1.28.0 - 2019-05-08:

1.28.0 - 2019-05-08
-------------------

- Add Type Attribute to TaskState (:pr:`2657`) `Matthew Rocklin`_
- Add waiting task count to progress title bar (:pr:`2663`) `James Bourbeau`_
- DOC: Clean up reference to cluster object (:pr:`2664`) `K.-Michael Aye`_
- Allow scheduler to politely close workers as part of shutdown (:pr:`2651`) `Matthew Rocklin`_
- Check direct_to_workers before using get_worker in Client (:pr:`2656`) `Matthew Rocklin`_
- Fixed comment regarding keeping existing level if less verbose (:pr:`2655`) `Brett Randall`_
- Add idle timeout to scheduler (:pr:`2652`) `Matthew Rocklin`_
- Avoid deprecation warnings (:pr:`2653`) `Matthew Rocklin`_
- Use an LRU cache for deserialized functions (:pr:`2623`) `Matthew Rocklin`_
- Rename Worker._close to Worker.close (:pr:`2650`) `Matthew Rocklin`_
- Add Comm closed bookkeeping (:pr:`2648`) `Matthew Rocklin`_
- Explain LocalCluster behavior in Client docstring (:pr:`2647`) `Matthew Rocklin`_
- Add last worker into KilledWorker exception to help debug (:pr:`2610`) `@plbertrand`_
- Set working worker class for dask-ssh (:pr:`2646`) `Martin Durant`_
- Add as_completed methods to docs (:pr:`2642`) `Jim Crist`_
- Add timeout to Client._reconnect (:pr:`2639`) `Jim Crist`_
- Limit test_spill_by_default memory, reenable it (:pr:`2633`) `Peter Andreas Entschev`_
- Use proper address in worker -> nanny comms (:pr:`2640`) `Jim Crist`_
- Fix deserialization of bytes chunks larger than 64MB (:pr:`2637`) `Peter Andreas Entschev`_


.. _v1.27.1 - 2019-04-29:

1.27.1 - 2019-04-29
-------------------

-  Adaptive: recommend close workers when any are idle (:pr:`2330`) `Michael Delgado`_
-  Increase GC thresholds (:pr:`2624`) `Matthew Rocklin`_
-  Add interface= keyword to LocalCluster (:pr:`2629`) `Matthew Rocklin`_
-  Add worker_class argument to LocalCluster (:pr:`2625`) `Matthew Rocklin`_
-  Remove Python 2.7 from testing matrix (:pr:`2631`) `Matthew Rocklin`_
-  Add number of trials to diskutils test (:pr:`2630`) `Matthew Rocklin`_
-  Fix parameter name in LocalCluster docstring (:pr:`2626`) `Loïc Estève`_
-  Integrate stacktrace for low-level profiling (:pr:`2575`) `Peter Andreas Entschev`_
-  Apply Black to standardize code styling (:pr:`2614`) `Matthew Rocklin`_
-  added missing whitespace to start_worker cmd (:pr:`2613`) `condoratberlin`_
-  Updated logging module doc links from docs.python.org/2 to docs.python.org/3. (:pr:`2635`) `Brett Randall`_


.. _v1.27.0 - 2019-04-12:

1.27.0 - 2019-04-12
-------------------

-  Add basic health endpoints to scheduler and worker bokeh. (:pr:`2607`) `amerkel2`_
-  Improved description accuracy of --memory-limit option. (:pr:`2601`) `Brett Randall`_
-  Check self.dependencies when looking at dependent tasks in memory (:pr:`2606`) `deepthirajagopalan7`_
-  Add RabbitMQ SchedulerPlugin example (:pr:`2604`) `Matt Nicolls`_
-  add resources to scheduler update_graph plugin (:pr:`2603`) `Matt Nicolls`_
-  Use ensure_bytes in serialize_error (:pr:`2588`) `Matthew Rocklin`_
-  Specify data storage explicitly from Worker constructor (:pr:`2600`) `Matthew Rocklin`_
-  Change bokeh port keywords to dashboard_address (:pr:`2589`) `Matthew Rocklin`_
-  .detach_() pytorch tensor to serialize data as numpy array. (:pr:`2586`) `Muammar El Khatib`_
-  Add warning if creating scratch directories takes a long time (:pr:`2561`) `Matthew Rocklin`_
-  Fix typo in pub-sub doc. (:pr:`2599`) `Loïc Estève`_
-  Allow return_when='FIRST_COMPLETED' in wait (:pr:`2598`) `Nikos Tsaousis`_
-  Forward kwargs through Nanny to Worker (:pr:`2596`) `Brian Chu`_
-  Use ensure_dict instead of dict (:pr:`2594`) `James Bourbeau`_
-  Specify protocol in LocalCluster (:pr:`2489`) `Matthew Rocklin`_

.. _v1.26.1 - 2019-03-29:

1.26.1 - 2019-03-29
-------------------

-  Fix LocalCluster to not overallocate memory when overcommitting threads per worker (:pr:`2541`) `George Sakkis`_
-  Make closing resilient to lacking an address (:pr:`2542`) `Matthew Rocklin`_
-  fix typo in comment (:pr:`2546`) `Brett Jurman`_
-  Fix double init of prometheus metrics (:pr:`2544`) `Marco Neumann`_
-  Skip test_duplicate_clients without bokeh. (:pr:`2553`) `Elliott Sales de Andrade`_
-  Add blocked_handlers to servers (:pr:`2556`) `Chris White`_
-  Always yield Server.handle_comm coroutine (:pr:`2559`) `Tom Augspurger`_
-  Use yaml.safe_load (:pr:`2566`) `Matthew Rocklin`_
-  Fetch executables from build root. (:pr:`2551`) `Elliott Sales de Andrade`_
-  Fix Torando 6 test failures (:pr:`2570`) `Matthew Rocklin`_
-  Fix test_sync_closed_loop (:pr:`2572`) `Matthew Rocklin`_

.. _v1.26.0 - 2019-02-25:

1.26.0 - 2019-02-25
-------------------

-  Update style to fix recent flake8 update (:pr:`2500`) (:pr:`2509`) `Matthew Rocklin`_
-  Fix typo in gen_cluster log message (:pr:`2503`) `Loïc Estève`_
-  Allow KeyError when closing event loop (:pr:`2498`) `Matthew Rocklin`_
-  Avoid thread testing for TCP ThreadPoolExecutor (:pr:`2510`) `Matthew Rocklin`_
-  Find Futures inside SubgraphCallable (:pr:`2505`) `Jim Crist`_
-  Avoid AttributeError when closing and sending a message (:pr:`2514`) `Matthew Rocklin`_
-  Add deprecation warning to dask_mpi.py (:pr:`2522`) `Julia Kent`_
-  Relax statistical profiling test (:pr:`2527`) `Matthew Rocklin`_
-  Support alternative --remote-dask-worker SSHCluster() and dask-ssh CLI (:pr:`2526`) `Adam Beberg`_
-  Iterate over full list of plugins in transition (:pr:`2518`) `Matthew Rocklin`_
-  Create Prometheus Endpoint (:pr:`2499`) `Adam Beberg`_
-  Use pytest.importorskip for prometheus test (:pr:`2533`) `Matthew Rocklin`_
-  MAINT skip prometheus test when no installed (:pr:`2534`) `Olivier Grisel`_
-  Fix intermittent testing failures (:pr:`2535`) `Matthew Rocklin`_
-  Avoid using nprocs keyword in dask-ssh if set to one (:pr:`2531`)  `Matthew Rocklin`_
-  Bump minimum Tornado version to 5.0


.. _v1.25.3 - 2019-01-31:

1.25.3 - 2019-01-31
-------------------

-  Fix excess threading on missing connections (:pr:`2403`) `Daniel Farrell`_
-  Fix typo in doc (:pr:`2457`) `Loïc Estève`_
-  Start fewer but larger workers with LocalCluster (:pr:`2452`) `Matthew Rocklin`_
-  Check for non-zero ``length`` first in ``read`` loop (:pr:`2465`) `John Kirkham`_
-  DOC: Use of local cluster in script (:pr:`2462`) `Peter Killick`_
-  DOC/API: Signature for base class write / read (:pr:`2472`) `Tom Augspurger`_
-  Support Pytest 4 in Tests (:pr:`2478`) `Adam Beberg`_
-  Ensure async behavior in event loop with LocalCluster (:pr:`2484`) `Matthew Rocklin`_
-  Fix spurious CancelledError (:pr:`2485`) `Loïc Estève`_
-  Properly reset dask.config scheduler and shuffle when closing the client (:pr:`2475`) `George Sakkis`_
-  Make it more explict that resources are per worker. (:pr:`2470`) `Loïc Estève`_
-  Remove references to center (:pr:`2488`)  `Matthew Rocklin`_
-  Expand client clearing timeout to 10s in testing (:pr:`2493`) `Matthew Rocklin`_
-  Propagate key keyword in progressbar (:pr:`2492`) `Matthew Rocklin`_
-  Use provided cluster's IOLoop if present in Client (:pr:`2494`) `Matthew Rocklin`_


.. _v1.25.2 - 2019-01-04:

1.25.2 - 2019-01-04
-------------------

-  Clean up LocalCluster logging better in async mode (:pr:`2448`) `Matthew Rocklin`_
-  Add short error message if bokeh cannot be imported (:pr:`2444`) `Dirk Petersen`_
-  Add optional environment variables to Nanny (:pr:`2431`) `Matthew Rocklin`_
-  Make the direct keyword docstring entries uniform (:pr:`2441`) `Matthew Rocklin`_
-  Make LocalCluster.close async friendly (:pr:`2437`) `Matthew Rocklin`_
-  gather_dep: don't request dependencies we already found out we don't want (:pr:`2428`) `tjb900`_
-  Add parameters to Client.run docstring (:pr:`2429`) `Matthew Rocklin`_
-  Support coroutines and async-def functions in run/run_scheduler (:pr:`2427`) `Matthew Rocklin`_
-  Name threads in ThreadPoolExecutors (:pr:`2408`) `Matthew Rocklin`_



.. _v1.25.1 - 2018-12-15:

1.25.1 - 2018-12-15
-------------------

-  Serialize numpy.ma.masked objects properly (:pr:`2384`) `Jim Crist`_
-  Turn off bokeh property validation in dashboard (:pr:`2387`) `Jim Crist`_
-  Fully initialize WorkerState objects (:pr:`2388`) `Jim Crist`_
-  Fix typo in scheduler docstring (:pr:`2393`) `Russ Bubley`_
-  DOC: fix typo in distributed.worker.Worker docstring (:pr:`2395`) `Loïc Estève`_
-  Remove clients and workers from event log after removal (:pr:`2394`) `tjb900`_
-  Support msgpack 0.6.0 by providing length keywords (:pr:`2399`) `tjb900`_
-  Use async-await on large messages test (:pr:`2404`) `Matthew Rocklin`_
-  Fix race condition in normalize_collection (:pr:`2386`) `Jim Crist`_
-  Fix redict collection after HighLevelGraph fix upstream (:pr:`2413`) `Matthew Rocklin`_
-  Add a blocking argument to Lock.acquire() (:pr:`2412`) `Stephan Hoyer`_
-  Fix long traceback test (:pr:`2417`) `Matthew Rocklin`_
-  Update x509 certificates to current OpenSSL standards. (:pr:`2418`) `Diane Trout`_


.. _v1.25.0 - 2018-11-28:

1.25.0 - 2018-11-28
-------------------

-  Fixed the 404 error on the Scheduler Dashboard homepage (:pr:`2361`) `Michael Wheeler`_
-  Consolidate two Worker classes into one (:pr:`2363`) `Matthew Rocklin`_
-  Avoid warnings in pyarrow and msgpack (:pr:`2364`) `Matthew Rocklin`_
-  Avoid race condition in Actor's Future (:pr:`2374`) `Matthew Rocklin`_
-  Support missing packages keyword in Client.get_versions (:pr:`2379`) `Matthew Rocklin`_
-  Fixup serializing masked arrays (:pr:`2373`) `Jim Crist`_


.. _v1.24.2 - 2018-11-15:

1.24.2 - 2018-11-15
-------------------

-  Add support for Bokeh 1.0 (:pr:`2348`) (:pr:`2356`) `Matthew Rocklin`_
-  Fix regression that dropped support for Tornado 4 (:pr:`2353`) `Roy Wedge`_
-  Avoid deprecation warnings (:pr:`2355`) (:pr:`2357`) `Matthew Rocklin`_
-  Fix typo in worker documentation (:pr:`2349`) `Tom Rochette`_


.. _v1.24.1 - 2018-11-09:

1.24.1 - 2018-11-09
-------------------

-  Use tornado's builtin AnyThreadLoopEventPolicy (:pr:`2326`) `Matthew Rocklin`_
-  Adjust TLS tests for openssl 1.1 (:pr:`2331`) `Marius van Niekerk`_
-  Avoid setting event loop policy if within Jupyter notebook server (:pr:`2343`) `Matthew Rocklin`_
-  Add preload script to conf (:pr:`2325`) `Guillaume Eynard-Bontemps`_
-  Add serializer for Numpy masked arrays (:pr:`2335`) `Peter Killick`_
-  Use psutil.Process.oneshot (:pr:`2339`) `NotSqrt`_
-  Use worker SSL context when getting client from worker. (:pr:`2301`) Anonymous


.. _v1.24.0 - 2018-10-26:

1.24.0 - 2018-10-26
-------------------

-  Remove Joblib Dask Backend from codebase (:pr:`2298`) `Matthew Rocklin`_
-  Include worker tls protocol in Scheduler.restart (:pr:`2295`) `Matthew Rocklin`_
-  Adapt to new Bokeh selection for 1.0 (:pr:`2292`) `Matthew Rocklin`_
-  Add explicit retry method to Future and Client (:pr:`2299`) `Matthew Rocklin`_
-  Point to main worker page in bokeh links (:pr:`2300`) `Matthew Rocklin`_
-  Limit concurrency when gathering many times (:pr:`2303`) `Matthew Rocklin`_
-  Add tls_cluster pytest fixture (:pr:`2302`) `Matthew Rocklin`_
-  Convert ConnectionPool.open and active to properties (:pr:`2304`) `Matthew Rocklin`_
-  change export_tb to format_tb (:pr:`2306`) `Eric Ma`_
-  Redirect joblib page to dask-ml (:pr:`2307`) `Matthew Rocklin`_
-  Include unserializable object in error message (:pr:`2310`) `Matthew Rocklin`_
-  Import Mapping, Iterator, Set from collections.abc in Python 3 (:pr:`2315`) `Gaurav Sheni`_
-  Extend Client.scatter docstring (:pr:`2320`) `Eric Ma`_
-  Update for new flake8 (:pr:`2321`)  `Matthew Rocklin`_


.. _v1.23.3 - 2018-10-05:

1.23.3 - 2018-10-05
-------------------

-  Err in dask serialization if not a NotImplementedError (:pr:`2251`) `Matthew Rocklin`_
-  Protect against key missing from priority in GraphLayout (:pr:`2259`) `Matthew Rocklin`_
-  Do not pull data twice in Client.gather (:pr:`2263`) `Adam Klein`_
-  Add pytest fixture for cluster tests (:pr:`2262`) `Matthew Rocklin`_
-  Cleanup bokeh callbacks  (:pr:`2261`) (:pr:`2278`) `Matthew Rocklin`_
-  Fix bokeh error for `memory_limit=None` (:pr:`2255`) `Brett Naul`_
-  Place large keywords into task graph in Client.map (:pr:`2281`) `Matthew Rocklin`_
-  Remove redundant blosc threading code from protocol.numpy (:pr:`2284`) `Mike Gevaert`_
-  Add ncores to workertable (:pr:`2289`) `Matthew Rocklin`_
-  Support upload_file on files with no extension (:pr:`2290`) `Matthew Rocklin`_


.. _v1.23.2 - 2018-09-17:

1.23.2 - 2018-09-17
-------------------

-  Discard dependent rather than remove (:pr:`2250`) `Matthew Rocklin`_
-  Use dask_sphinx_theme `Matthew Rocklin`_
-  Drop the Bokeh index page (:pr:`2241`) `John Kirkham`_
-  Revert change to keep link relative (:pr:`2242`) `Matthew Rocklin`_
-  docs: Fix broken AWS link in setup.rst file (:pr:`2240`) `Vladyslav Moisieienkov`_
-  Return cancelled futures in as_completed (:pr:`2233`) `Chris White`_


.. _v1.23.1 - 2018-09-06:

1.23.1 - 2018-09-06
-------------------

-  Raise informative error when mixing futures between clients (:pr:`2227`) `Matthew Rocklin`_
-  add byte_keys to unpack_remotedata call (:pr:`2232`) `Matthew Rocklin`_
-  Add documentation for gist/rawgit for get_task_stream (:pr:`2236`) `Matthew Rocklin`_
-  Quiet Client.close by waiting for scheduler stop signal (:pr:`2237`) `Matthew Rocklin`_
-  Display system graphs nicely on different screen sizes (:pr:`2239`) `Derek Ludwig`_
-  Mutate passed in workers dict in TaskStreamPlugin.rectangles (:pr:`2238`) `Matthew Rocklin`_


.. _v1.23.0 - 2018-08-30:

1.23.0 - 2018-08-30
-------------------

-  Add direct_to_workers to Client `Matthew Rocklin`_
-  Add Scheduler.proxy to workers `Matthew Rocklin`_
-  Implement Actors `Matthew Rocklin`_
-  Fix tooltip (:pr:`2168`) `Loïc Estève`_
-  Fix scale /  avoid returning coroutines (:pr:`2171`) `Joe Hamman`_
-  Clarify dask-worker --nprocs (:pr:`2173`) `Yu Feng`_
-  Concatenate all bytes of small messages in TCP comms (:pr:`2172`) `Matthew Rocklin`_
-  Add dashboard_link property (:pr:`2176`) `Jacob Tomlinson`_
-  Always offload to_frames (:pr:`2170`) `Matthew Rocklin`_
-  Warn if desired port is already in use (:pr:`2191`) (:pr:`2199`) `Matthew Rocklin`_
-  Add profile page for event loop thread (:pr:`2144`) `Matthew Rocklin`_
-  Use dispatch for dask serialization, also add sklearn, pytorch (:pr:`2175`) `Matthew Rocklin`_
-  Handle corner cases with busy signal (:pr:`2182`) `Matthew Rocklin`_
-  Check self.dependencies when looking at tasks in memory (:pr:`2196`) `Matthew Rocklin`_
-  Add ability to log additional custom metrics from each worker (:pr:`2169`) `Loïc Estève`_
-  Fix formatting when port is a tuple (:pr:`2204`) `Loïc Estève`_
-  Describe what ZeroMQ is (:pr:`2211`) `Mike DePalatis`_
-  Tiny typo fix (:pr:`2214`) `Anderson Banihirwe`_
-  Add Python 3.7 to travis.yml (:pr:`2203`) `Matthew Rocklin`_
-  Add plot= keyword to get_task_stream (:pr:`2198`) `Matthew Rocklin`_
-  Add support for optional versions in Client.get_versions (:pr:`2216`) `Matthew Rocklin`_
-  Add routes for solo bokeh figures in dashboard (:pr:`2185`) `Matthew Rocklin`_
-  Be resilient to missing dep after busy signal (:pr:`2217`) `Matthew Rocklin`_
-  Use CSS Grid to layout status page on the dashboard (:pr:`2213`) `Derek Ludwig`_ and `Luke Canavan`_
-  Fix deserialization of queues on main ioloop thread (:pr:`2221`) `Matthew Rocklin`_
-  Add a worker initialization function (:pr:`2201`) `Guillaume Eynard-Bontemps`_
-  Collapse navbar in dashboard (:pr:`2223`) `Luke Canavan`_


.. _v1.22.1 - 2018-08-03:

1.22.1 - 2018-08-03
-------------------

-  Add worker_class= keyword to Nanny to support different worker types (:pr:`2147`) `Martin Durant`_
-  Cleanup intermittent worker failures (:pr:`2152`) (:pr:`2146`) `Matthew Rocklin`_
-  Fix msgpack PendingDeprecationWarning for encoding='utf-8' (:pr:`2153`) `Olivier Grisel`_
-  Make bokeh coloring deterministic using hash function (:pr:`2143`) `Matthew Rocklin`_
-  Allow client to query the task stream plot (:pr:`2122`) `Matthew Rocklin`_
-  Use PID and counter in thread names (:pr:`2084`) (:pr:`2128`) `Dror Birkman`_
-  Test that worker restrictions are cleared after cancellation (:pr:`2107`) `Matthew Rocklin`_
-  Expand resources in graph_to_futures (:pr:`2131`) `Matthew Rocklin`_
-  Add custom serialization support for pyarrow  (:pr:`2115`) `Dave Hirschfeld`_
-  Update dask-scheduler cli help text for preload (:pr:`2120`) `Matt Nicolls`_
-  Added another nested parallelism test (:pr:`1710`) `Tom Augspurger`_
-  insert newline by default after TextProgressBar (:pr:`1976`) `Phil Tooley`_
-  Retire workers from scale (:pr:`2104`) `Matthew Rocklin`_
-  Allow worker to refuse data requests with busy signal (:pr:`2092`) `Matthew Rocklin`_
-  Don't forget released keys (:pr:`2098`) `Matthew Rocklin`_
-  Update example for stopping a worker (:pr:`2088`) `John Kirkham`_
-  removed hardcoded value of memory terminate fraction from a log message (:pr:`2096`) `Bartosz Marcinkowski`_
-  Adjust worker doc after change in config file location and treatment (:pr:`2094`) `Aurélien Ponte`_
-  Prefer gathering data from same host (:pr:`2090`) `Matthew Rocklin`_
-  Handle exceptions on deserialized comm with text error (:pr:`2093`) `Matthew Rocklin`_
-  Fix typo in docstring (:pr:`2087`) `Loïc Estève`_
-  Provide communication context to serialization functions (:pr:`2054`) `Matthew Rocklin`_
-  Allow `name` to be explicitly passed in publish_dataset (:pr:`1995`) `Marius van Niekerk`_
-  Avoid accessing Worker.scheduler_delay around yield point (:pr:`2074`) `Matthew Rocklin`_
-  Support TB and PB in format bytes (:pr:`2072`) `Matthew Rocklin`_
-  Add test for as_completed for loops in Python 2 (:pr:`2071`) `Matthew Rocklin`_
-  Allow adaptive to exist without a cluster (:pr:`2064`) `Matthew Rocklin`_
-  Have worker data transfer wait until recipient acknowledges (:pr:`2052`) `Matthew Rocklin`_
-  Support async def functions in Client.sync (:pr:`2070`) `Matthew Rocklin`_
-  Add asynchronous parameter to docstring of LocalCluster `Matthew Rocklin`_
-  Normalize address before comparison (:pr:`2066`) `Tom Augspurger`_
-  Use ConnectionPool for Worker.scheduler `Matthew Rocklin`_
-  Avoid reference cycle in str_graph `Matthew Rocklin`_
-  Pull data outside of while loop in gather (:pr:`2059`) `Matthew Rocklin`_


.. _v1.22.0 - 2018-06-14:

1.22.0 - 2018-06-14
-------------------

-  Overhaul configuration (:pr:`1948`) `Matthew Rocklin`_
-  Replace get= keyword with scheduler= (:pr:`1959`) `Matthew Rocklin`_
-  Use tuples in msgpack (:pr:`2000`) `Matthew Rocklin`_ and `Marius van Niekerk`_
-  Unify handling of high-volume connections (:pr:`1970`) `Matthew Rocklin`_
-  Automatically scatter large arguments in joblib connector (:pr:`2020`) (:pr:`2030`) `Olivier Grisel`_
-  Turn click Python 3 locales failure into a warning (:pr:`2001`) `Matthew Rocklin`_
-  Rely on dask implementation of sizeof (:pr:`2042`) `Matthew Rocklin`_
-  Replace deprecated workers.iloc with workers.values() (:pr:`2013`) `Grant Jenks`_
-  Introduce serialization families (:pr:`1912`) `Matthew Rocklin`_

-  Add PubSub (:pr:`1999`) `Matthew Rocklin`_
-  Add Dask stylesheet to documentation `Matthew Rocklin`_
-  Avoid recomputation on partially-complete results (:pr:`1840`) `Matthew Rocklin`_
-  Use sys.prefix in popen for testing (:pr:`1954`) `Matthew Rocklin`_
-  Include yaml files in manifest `Matthew Rocklin`_
-  Use self.sync so Client.processing works in asynchronous context (:pr:`1962`) `Henry Doupe`_
-  Fix bug with bad repr on closed client (:pr:`1965`) `Matthew Rocklin`_
-  Parse --death-timeout keyword in dask-worker (:pr:`1967`) `Matthew Rocklin`_
-  Support serializers in BatchedSend (:pr:`1964`) `Matthew Rocklin`_
-  Use normal serialization mechanisms to serialize published datasets (:pr:`1972`) `Matthew Rocklin`_
-  Add security support to LocalCluster. (:pr:`1855`) `Marius van Niekerk`_
-  add ConnectionPool.remove method (:pr:`1977`) `Tony Lorenzo`_
-  Cleanly close workers when scheduler closes (:pr:`1981`) `Matthew Rocklin`_
-  Add .pyz support in upload_file  (:pr:`1781`) `@bmaisson`_
-  add comm to packages (:pr:`1980`) `Matthew Rocklin`_
-  Replace dask.set_options with dask.config.set `Matthew Rocklin`_
-  Exclude versions of sortedcontainers which do not have .iloc. (:pr:`1993`) `Russ Bubley`_
-  Exclude gc statistics under PyPy (:pr:`1997`) `Marius van Niekerk`_
-  Manage recent config and dataframe changes in dask (:pr:`2009`) `Matthew Rocklin`_
-  Cleanup lingering clients in tests (:pr:`2012`) `Matthew Rocklin`_
-  Use timeouts during `Client._ensure_connected` (:pr:`2011`) `Martin Durant`_
-  Avoid reference cycle in joblib backend (:pr:`2014`) `Matthew Rocklin`_, also `Olivier Grisel`_
-  DOC: fixed test example (:pr:`2017`) `Tom Augspurger`_
-  Add worker_key parameter to Adaptive (:pr:`1992`) `Matthew Rocklin`_
-  Prioritize tasks with their true keys, before stringifying (:pr:`2006`) `Matthew Rocklin`_
-  Serialize worker exceptions through normal channels (:pr:`2016`) `Matthew Rocklin`_
-  Include exception in progress bar (:pr:`2028`) `Matthew Rocklin`_
-  Avoid logging orphaned futures in All (:pr:`2008`) `Matthew Rocklin`_
-  Don't use spill-to-disk dictionary if we're not spilling to disk `Matthew Rocklin`_
-  Only avoid recomputation if key exists (:pr:`2036`) `Matthew Rocklin`_
-  Use client connection and serialization arguments in progress (:pr:`2035`) `Matthew Rocklin`_
-  Rejoin worker client on closing context manager (:pr:`2041`) `Matthew Rocklin`_
-  Avoid forgetting erred tasks when losing dependencies (:pr:`2047`) `Matthew Rocklin`_
-  Avoid collisions in graph_layout (:pr:`2050`) `Matthew Rocklin`_
-  Avoid recursively calling bokeh callback in profile plot (:pr:`2048`) `Matthew Rocklin`_


.. _v1.21.8 - 2018-05-03:

1.21.8 - 2018-05-03
-------------------

-  Remove errant print statement (:pr:`1957`) `Matthew Rocklin`_
-  Only add reevaluate_occupancy callback once (:pr:`1953`) `Tony Lorenzo`_


.. _v1.21.7 - 2018-05-02:

1.21.7 - 2018-05-02
-------------------

-  Newline needed for doctest rendering (:pr:`1917`) `Loïc Estève`_
-  Support Client._repr_html_ when in async mode (:pr:`1909`) `Matthew Rocklin`_
-  Add parameters to dask-ssh command (:pr:`1910`) `Irene Rodriguez`_
-  Santize get_dataset trace (:pr:`1888`) `John Kirkham`_
-  Fix bug where queues would not clean up cleanly (:pr:`1922`) `Matthew Rocklin`_
-  Delete cached file safely in upload file (:pr:`1921`) `Matthew Rocklin`_
-  Accept KeyError when closing tornado IOLoop in tests (:pr:`1937`) `Matthew Rocklin`_
-  Quiet the client and scheduler when gather(..., errors='skip') (:pr:`1936`) `Matthew Rocklin`_
-  Clarify couldn't gather keys warning (:pr:`1942`) `Kenneth Koski`_
-  Support submit keywords in joblib (:pr:`1947`) `Matthew Rocklin`_
-  Avoid use of external resources in bokeh server (:pr:`1934`) `Matthew Rocklin`_
-  Drop `__contains__` from `Datasets` (:pr:`1889`) `John Kirkham`_
-  Fix bug with queue timeouts (:pr:`1950`) `Matthew Rocklin`_
-  Replace msgpack-python by msgpack (:pr:`1927`) `Loïc Estève`_


.. _v1.21.6 - 2018-04-06:

1.21.6 - 2018-04-06
-------------------

-  Fix numeric environment variable configuration (:pr:`1885`) `Joseph Atkins-Kurkish`_
-  support bytearrays in older lz4 library (:pr:`1886`) `Matthew Rocklin`_
-  Remove started timeout in nanny (:pr:`1852`) `Matthew Rocklin`_
-  Don't log errors in sync (:pr:`1894`) `Matthew Rocklin`_
-  downgrade stale lock warning to info logging level (:pr:`1890`) `Matthew Rocklin`_
-  Fix ``UnboundLocalError`` for ``key`` (:pr:`1900`) `John Kirkham`_
-  Resolve deployment issues in Python 2 (:pr:`1905`) `Matthew Rocklin`_
-  Support retries and priority in Client.get method (:pr:`1902`) `Matthew Rocklin`_
-  Add additional attributes to task page if applicable (:pr:`1901`) `Matthew Rocklin`_
-  Add count method to as_completed (:pr:`1897`) `Matthew Rocklin`_
-  Extend default timeout to 10s (:pr:`1904`) `Matthew Rocklin`_




.. _v1.21.5 - 2018-03-31:

1.21.5 - 2018-03-31
-------------------

-  Increase default allowable tick time to 3s (:pr:`1854`) `Matthew Rocklin`_
-  Handle errant workers when another worker has data (:pr:`1853`) `Matthew Rocklin`_
-  Close multiprocessing queue in Nanny to reduce open file descriptors (:pr:`1862`) `Matthew Rocklin`_
-  Extend nanny started timeout to 30s, make configurable (:pr:`1865`) `Matthew Rocklin`_
-  Comment out the default config file (:pr:`1871`) `Matthew Rocklin`_
-  Update to fix bokeh 0.12.15 update errors (:pr:`1872`) `Matthew Rocklin`_
-  Downgrade Event Loop unresponsive warning to INFO level (:pr:`1870`) `Matthew Rocklin`_
-  Add fifo timeout to control priority generation (:pr:`1828`) `Matthew Rocklin`_
-  Add retire_workers API to Client (:pr:`1876`) `Matthew Rocklin`_
-  Catch NoSuchProcess error in Nanny.memory_monitor (:pr:`1877`) `Matthew Rocklin`_
-  Add uid to nanny queue communitcations (:pr:`1880`) `Matthew Rocklin`_


.. _v1.21.4 - 2018-03-21:

1.21.4 - 2018-03-21
-------------------

-  Avoid passing bytearrays to snappy decompression (:pr:`1831`) `Matthew Rocklin`_
-  Specify IOLoop in Adaptive (:pr:`1841`) `Matthew Rocklin`_
-  Use connect-timeout config value throughout client (:pr:`1839`) `Matthew Rocklin`_
-  Support direct= keyword argument in Client.get (:pr:`1845`) `Matthew Rocklin`_


.. _v1.21.3 - 2018-03-08:

1.21.3 - 2018-03-08
-------------------

-  Add cluster superclass and improve adaptivity (:pr:`1813`) `Matthew Rocklin`_
-  Fixup tests and support Python 2 for Tornado 5.0 (:pr:`1818`) `Matthew Rocklin`_
-  Fix bug in recreate_error when dependencies are dropped (:pr:`1815`) `Matthew Rocklin`_
-  Add worker time to live in Scheduler (:pr:`1811`) `Matthew Rocklin`_
-  Scale adaptive based on total_occupancy (:pr:`1807`) `Matthew Rocklin`_
-  Support calling compute within worker_client (:pr:`1814`) `Matthew Rocklin`_
-  Add percentage to profile plot (:pr:`1817`) `Brett Naul`_
-  Overwrite option for remote python in dask-ssh (:pr:`1812`) `Sven Kreiss`_


.. _v1.21.2 - 2018-03-05:

1.21.2 - 2018-03-05
-------------------

-  Fix bug where we didn't check idle/saturated when stealing (:pr:`1801`) `Matthew Rocklin`_
-  Fix bug where client was noisy when scheduler closed unexpectedly (:pr:`1806`) `Matthew Rocklin`_
-  Use string-based timedeltas (like ``'500 ms'``) everywhere (:pr:`1804`) `Matthew Rocklin`_
-  Keep logs in scheduler and worker even if silenced (:pr:`1803`) `Matthew Rocklin`_
-  Support minimum, maximum, wait_count keywords in Adaptive (:pr:`1797`) `Jacob Tomlinson`_ and `Matthew Rocklin`_
-  Support async protocols for LocalCluster, replace start= with asynchronous= (:pr:`1798`) `Matthew Rocklin`_
-  Avoid restarting workers when nanny waits on scheduler (:pr:`1793`) `Matthew Rocklin`_
-  Use ``IOStream.read_into()`` when available (:pr:`1477`) `Antoine Pitrou`_
-  Reduce LocalCluster logging threshold from CRITICAL to WARN (:pr:`1785`) `Andy Jones`_
-  Add `futures_of` to API docs (:pr:`1783`) `John Kirkham`_
-  Make diagnostics link in client configurable (:pr:`1810`) `Matthew Rocklin`_


.. _v1.21.1 - 2018-02-22:

1.21.1 - 2018-02-22
-------------------

-  Fixed an uncaught exception in ``distributed.joblib`` with a ``LocalCluster`` using only threads (:issue:`1775`) `Tom Augspurger`_
-  Format bytes in info worker page (:pr:`1752`) `Matthew Rocklin`_
-  Add pass-through arguments for scheduler/worker `--preload` modules. (:pr:`1634`) `Alex Ford`_
-  Use new LZ4 API (:pr:`1757`) `Thrasibule`_
-  Replace dask.optimize with dask.optimization (:pr:`1754`) `Matthew Rocklin`_
-  Add graph layout engine and bokeh plot (:pr:`1756`) `Matthew Rocklin`_
-  Only expand name with --nprocs if name exists (:pr:`1776`) `Matthew Rocklin`_
-  specify IOLoop for stealing PeriodicCallback (:pr:`1777`) `Matthew Rocklin`_
-  Fixed distributed.joblib with no processes `Tom Augspurger`_
-  Use set.discard to avoid KeyErrors in stealing (:pr:`1766`) `Matthew Rocklin`_
-  Avoid KeyError when task has been released during steal (:pr:`1765`) `Matthew Rocklin`_
-  Add versions routes to avoid the use of run in Client.get_versions (:pr:`1773`) `Matthew Rocklin`_
-  Add write_scheduler_file to Client (:pr:`1778`) `Joe Hamman`_
-  Default host to tls:// if tls information provided (:pr:`1780`) `Matthew Rocklin`_


.. _v1.21.0 - 2018-02-09:

1.21.0 - 2018-02-09
-------------------

-  Refactor scheduler to use TaskState objects rather than dictionaries (:pr:`1594`) `Antoine Pitrou`_
-  Plot CPU fraction of total in workers page (:pr:`1624`) `Matthew Rocklin`_
-  Use thread CPU time in Throttled GC (:pr:`1625`) `Antoine Pitrou`_
-  Fix bug with ``memory_limit=None`` (:pr:`1639`) `Matthew Rocklin`_
-  Add futures_of to top level api (:pr:`1646`) `Matthew Rocklin`_
-  Warn on serializing large data in Client (:pr:`1636`) `Matthew Rocklin`_
-  Fix intermittent windows failure when removing lock file (:pr:`1652`) `Antoine Pitrou`_
-  Add diagnosis and logging of poor GC Behavior (:pr:`1635`) `Antoine Pitrou`_
-  Add client-scheduler heartbeats (:pr:`1657`) `Matthew Rocklin`_
-  Return dictionary of worker info in ``retire_workers`` (:pr:`1659`) `Matthew Rocklin`_
-  Ensure dumps_function works with unhashable functions (:pr:`1662`) `Matthew Rocklin`_
-  Collect client name ids rom client-name config variable (:pr:`1664`) `Matthew Rocklin`_
-  Allow simultaneous use of --name and --nprocs in dask-worker (:pr:`1665`) `Matthew Rocklin`_
-  Add support for grouped adaptive scaling and adaptive behavior overrides (:pr:`1632`) `Alex Ford`_
-  Share scheduler RPC between worker and client (:pr:`1673`) `Matthew Rocklin`_
-  Allow ``retries=`` in ClientExecutor (:pr:`1672`) `@rqx`_
-  Improve documentation for get_client and dask.compute examples (:pr:`1638`) `Scott Sievert`_
-  Support DASK_SCHEDULER_ADDRESS environment variable in worker (:pr:`1680`) `Matthew Rocklin`_
-  Support tuple-keys in retries (:pr:`1681`) `Matthew Rocklin`_
-  Use relative links in bokeh dashboard (:pr:`1682`) `Matthew Rocklin`_
-  Make message log length configurable, default to zero (:pr:`1691`) `Matthew Rocklin`_
-  Deprecate ``Client.shutdown`` (:pr:`1699`) `Matthew Rocklin`_
-  Add warning in configuration docs to install pyyaml (:pr:`1701`) `Cornelius Riemenschneider`_
-  Handle nested parallelism in distributed.joblib (:pr:`1705`) `Tom Augspurger`_
-  Don't wait for Worker.executor to shutdown cleanly when restarting process (:pr:`1708`) `Matthew Rocklin`_
-  Add support for user defined priorities (:pr:`1651`) `Matthew Rocklin`_
-  Catch and log OSErrors around worker lock files (:pr:`1714`) `Matthew Rocklin`_
-  Remove worker prioritization.  Coincides with changes to dask.order (:pr:`1730`) `Matthew Rocklin`_
-  Use process-measured memory rather than nbytes in Bokeh dashboard (:pr:`1737`) `Matthew Rocklin`_
-  Enable serialization of Locks  (:pr:`1738`) `Matthew Rocklin`_
-  Support Tornado 5 beta (:pr:`1735`) `Matthew Rocklin`_
-  Cleanup remote_magic client cache after tests (:pr:`1743`) `Min RK`_
-  Allow service ports to be specified as (host, port) (:pr:`1744`) `Bruce Merry`_


.. _v1.20.2 - 2017-12-07:

1.20.2 - 2017-12-07
-------------------

-  Clear deque handlers after each test (:pr:`1586`) `Antoine Pitrou`_
-  Handle deserialization in FutureState.set_error (:pr:`1592`) `Matthew Rocklin`_
-  Add process leak checker to tests (:pr:`1596`) `Antoine Pitrou`_
-  Customize process title for subprocess (:pr:`1590`) `Antoine Pitrou`_
-  Make linting a separate CI job (:pr:`1599`) `Antoine Pitrou`_
-  Fix error from get_client() with no global client (:pr:`1595`) `Daniel Li`_
-  Remove Worker.host_health, correct WorkerTable metrics (:pr:`1600`) `Matthew Rocklin`_
-  Don't mark tasks as suspicious when retire_workers called. Addresses (:pr:`1607`) `Russ Bubley`_
-  Do not include processing workers in workers_to_close (:pr:`1609`) `Russ Bubley`_
-  Disallow simultaneous scale up and down in Adaptive (:pr:`1608`) `Russ Bubley`_
-  Parse bytestrings in --memory-limit (:pr:`1615`) `Matthew Rocklin`_
-  Use environment variable for scheduler address if present (:pr:`1610`) `Matthew Rocklin`_
-  Fix deprecation warning from logger.warn (:pr:`1616`) `Brett Naul`_




.. _v1.20.1 - 2017-11-26:

1.20.1 - 2017-11-26
-------------------

- Wrap ``import ssl`` statements with try-except block for ssl-crippled environments, (:pr:`1570`) `Xander Johnson`_
- Support zero memory-limit in Nanny (:pr:`1571`) `Matthew Rocklin`_
- Avoid PeriodicCallback double starts (:pr:`1573`) `Matthew Rocklin`_
- Add disposable workspace facility (:pr:`1543`) `Antoine Pitrou`_
- Use format_time in task_stream plots (:pr:`1575`) `Matthew Rocklin`_
- Avoid delayed finalize calls in compute (:pr:`1577`) `Matthew Rocklin`_
- Doc fix about secede (:pr:`1583`) `Scott Sievert`_
- Add tracemalloc option when tracking test leaks (:pr:`1585`) `Antoine Pitrou`_
- Add JSON routes to Bokeh server (:pr:`1584`) `Matthew Rocklin`_
- Handle exceptions cleanly in Variables and Queues (:pr:`1580`) `Matthew Rocklin`_


.. _v1.20.0 - 2017-11-17:

1.20.0 - 2017-11-17
-------------------

-  Drop use of pandas.msgpack (:pr:`1473`) `Matthew Rocklin`_
-  Add methods to get/set scheduler metadata `Matthew Rocklin`_
-  Add distributed lock `Matthew Rocklin`_
-  Add reschedule exception for worker tasks `Matthew Rocklin`_
-  Fix ``nbytes()`` for ``bytearrays`` `Matthew Rocklin`_
-  Capture scheduler and worker logs `Matthew Rocklin`_
-  Garbage collect after data eviction on high worker memory usage (:pr:`1488`) `Olivier Grisel`_
-  Add scheduler HTML routes to bokeh server (:pr:`1478`) (:pr:`1514`) `Matthew Rocklin`_
-  Add pytest plugin to test for resource leaks (:pr:`1499`) `Antoine Pitrou`_
-  Improve documentation for scheduler states (:pr:`1498`) `Antoine Pitrou`_
-  Correct warn_if_longer timeout in ThrottledGC (:pr:`1496`) `Fabian Keller`_
-  Catch race condition in as_completed on cancelled futures (:pr:`1507`) `Matthew Rocklin`_
-  Transactional work stealing (:pr:`1489`) (:pr:`1528`) `Matthew Rocklin`_
-  Avoid forkserver in PyPy (:pr:`1509`) `Matthew Rocklin`_
-  Add dict access to get/set datasets (:pr:`1508`) `Mike DePalatis`_
-  Support Tornado 5 (:pr:`1509`) (:pr:`1512`) (:pr:`1518`) (:pr:`1534`) `Antoine Pitrou`_
-  Move thread_state in Dask (:pr:`1523`) `Jim Crist`_
-  Use new Dask collections interface (:pr:`1513`) `Matthew Rocklin`_
-  Add nanny flag to dask-mpi `Matthew Rocklin`_
-  Remove JSON-based HTTP servers `Matthew Rocklin`_
-  Avoid doing I/O in repr/str (:pr:`1536`) `Matthew Rocklin`_
-  Fix URL for MPI4Py project (:pr:`1546`) `Ian Hopkinson`_
-  Allow automatic retries of a failed task (:pr:`1524`) `Antoine Pitrou`_
-  Clean and accelerate tests (:pr:`1548`) (:pr:`1549`) (:pr:`1552`)
   (:pr:`1553`) (:pr:`1560`) (:pr:`1564`) `Antoine Pitrou`_
- Move HDFS functionality to the hdfs3 library (:pr:`1561`) `Jim Crist`_
-  Fix bug when using events page with no events (:pr:`1562`) `@rbubley`_
-  Improve diagnostic naming of tasks within tuples (:pr:`1566`) `Kelvyn Yang`_

.. _v1.19.3 - 2017-10-16:

1.19.3 - 2017-10-16
-------------------

-  Handle None case in profile.identity (:pr:`1456`)
-  Asyncio rewrite (:pr:`1458`)
-  Add rejoin function partner to secede (:pr:`1462`)
-  Nested compute (:pr:`1465`)
-  Use LooseVersion when comparing Bokeh versions (:pr:`1470`)


.. _v1.19.2 - 2017-10-06:

1.19.2 - 2017-10-06
-------------------

-  as_completed doesn't block on cancelled futures (:pr:`1436`)
-  Notify waiting threads/coroutines on cancellation (:pr:`1438`)
-  Set Future(inform=True) as default (:pr:`1437`)
-  Rename Scheduler.transition_story to story (:pr:`1445`)
-  Future uses default client by default (:pr:`1449`)
-  Add keys= keyword to Client.call_stack (:pr:`1446`)
-  Add get_current_task to worker (:pr:`1444`)
-  Ensure that Client remains asynchornous before ioloop starts (:pr:`1452`)
-  Remove "click for worker page" in bokeh plot (:pr:`1453`)
-  Add Client.current() (:pr:`1450`)
-  Clean handling of restart timeouts (:pr:`1442`)

.. _v1.19.1 - 2017-09-25:

1.19.1 - 2017-09-25
-----------------------------

-  Fix tool issues with TaskStream plot (:pr:`1425`)
-  Move profile module to top level (:pr:`1423`)

.. _v1.19.0 - 2017-09-24:

1.19.0 - 2017-09-24
-----------------------------

-  Avoid storing messages in message log (:pr:`1361`)
-  fileConfig does not disable existing loggers (:pr:`1380`)
-  Offload upload_file disk I/O to separate thread (:pr:`1383`)
-  Add missing SSLContext (:pr:`1385`)
-  Collect worker thread information from sys._curent_frames (:pr:`1387`)
-  Add nanny timeout (:pr:`1395`)
-  Restart worker if memory use goes above 95% (:pr:`1397`)
-  Track workers memory use with psutil (:pr:`1398`)
-  Track scheduler delay times in workers (:pr:`1400`)
-  Add time slider to profile plot (:pr:`1403`)
-  Change memory-limit keyword to refer to maximum number of bytes (:pr:`1405`)
-  Add ``cancel(force=)`` keyword (:pr:`1408`)

.. _v1.18.2 - 2017-09-02:

1.18.2 - 2017-09-02
----------------------------
-  Silently pass on cancelled futures in as_completed (:pr:`1366`)
-  Fix unicode keys error in Python 2 (:pr:`1370`)
-  Support numeric worker names
-  Add dask-mpi executable (:pr:`1367`)

.. _v1.18.1 - 2017-08-25:

1.18.1 - 2017-08-25
--------------------------
-  Clean up forgotten keys in fire-and-forget workloads (:pr:`1250`)
-  Handle missing extensions (:pr:`1263`)
-  Allow recreate_exception on persisted collections (:pr:`1253`)
-  Add asynchronous= keyword to blocking client methods (:pr:`1272`)
-  Restrict to horizontal panning in bokeh plots (:pr:`1274`)
-  Rename client.shutdown to client.close (:pr:`1275`)
-  Avoid blocking on event loop (:pr:`1270`)
-  Avoid cloudpickle errors for Client.get_versions (:pr:`1279`)
-  Yield on Tornado IOStream.write futures (:pr:`1289`)
-  Assume async behavior if inside a sync statement (:pr:`1284`)
-  Avoid error messages on closing (:pr:`1297`), (:pr:`1296`) (:pr:`1318`) (:pr:`1319`)
-  Add timeout= keyword to get_client (:pr:`1290`)
-  Respect timeouts when restarting (:pr:`1304`)
-  Clean file descriptor and memory leaks in tests (:pr:`1317`)
-  Deprecate Executor (:pr:`1302`)
-  Add timeout to ThreadPoolExecutor.shutdown (:pr:`1330`)
-  Clean up AsyncProcess handling (:pr:`1324`)
-  Allow unicode keys in Python 2 scheduler (:pr:`1328`)
-  Avoid leaking stolen data (:pr:`1326`)
-  Improve error handling on failed nanny starts (:pr:`1337`), (:pr:`1331`)
-  Make Adaptive more flexible
-  Support ``--contact-address`` and ``--listen-address`` in worker (:pr:`1278`)
-  Remove old dworker, dscheduler executables (:pr:`1355`)
-  Exit workers if nanny process fails (:pr:`1345`)
-  Auto pep8 and flake (:pr:`1353`)

.. _v1.18.0 - 2017-07-08:

1.18.0 - 2017-07-08
-----------------------
-  Multi-threading safety (:pr:`1191`), (:pr:`1228`), (:pr:`1229`)
-  Improve handling of byte counting (:pr:`1198`) (:pr:`1224`)
-  Add get_client, secede functions, refactor worker-client relationship (:pr:`1201`)
-  Allow logging configuraiton using logging.dictConfig() (:pr:`1206`) (:pr:`1211`)
-  Offload serialization and deserialization to separate thread (:pr:`1218`)
-  Support fire-and-forget tasks (:pr:`1221`)
-  Support bytestrings as keys (for Julia) (:pr:`1234`)
-  Resolve testing corner-cases (:pr:`1236`), (:pr:`1237`), (:pr:`1240`), (:pr:`1241`), (:pr:`1242`), (:pr:`1244`)
-  Automatic use of scatter/gather(direct=True) in more cases (:pr:`1239`)

.. _v1.17.1 - 2017-06-14:

1.17.1 - 2017-06-14
------------------------

-  Remove Python 3.4 testing from travis-ci (:pr:`1157`)
-  Remove ZMQ Support (:pr:`1160`)
-  Fix memoryview nbytes issue in Python 2.7 (:pr:`1165`)
-  Re-enable counters (:pr:`1168`)
-  Improve scheduler.restart (:pr:`1175`)


.. _v1.17.0 - 2017-06-09:

1.17.0 - 2017-06-09
-----------------------

-  Reevaluate worker occupancy periodically during scheduler downtime
   (:pr:`1038`) (:pr:`1101`)
-  Add ``AioClient`` asyncio-compatible client API (:pr:`1029`) (:pr:`1092`)
   (:pr:`1099`)
-  Update Keras serializer (:pr:`1067`)
-  Support TLS/SSL connections for security (:pr:`866`) (:pr:`1034`)
-  Always create new worker directory when passed ``--local-directory``
   (:pr:`1079`)
-  Support pre-scattering data when using joblib frontent (:pr:`1022`)
-  Make workers more robust to failure of ``sizeof`` function (:pr:`1108`) and
   writing to disk (:pr:`1096`)
-  Add ``is_empty`` and ``update`` methods to ``as_completed`` (:pr:`1113`)
-  Remove ``_get`` coroutine and replace with ``get(..., sync=False)``
   (:pr:`1109`)
-  Improve API compatibility with async/await syntax (:pr:`1115`) (:pr:`1124`)
-  Add distributed Queues (:pr:`1117`) and shared Variables (:pr:`1128`) to
   enable inter-client coordination
-  Support direct client-to-worker scattering and gathering (:pr:`1130`) as
   well as performance enhancements when scattering data
-  Style improvements for bokeh web dashboards (:pr:`1126`) (:pr:`1141`) as
   well as a removal of the external bokeh process
-  HTML reprs for Future and Client objects (:pr:`1136`)
-  Support nested collections in client.compute (:pr:`1144`)
-  Use normal client API in asynchronous mode (:pr:`1152`)
-  Remove old distributed.collections submodule (:pr:`1153`)


.. _v1.16.3 - 2017-05-05:

1.16.3 - 2017-05-05
----------------------

-  Add bokeh template files to MANIFEST (:pr:`1063`)
-  Don't set worker_client.get as default get (:pr:`1061`)
-  Clean up logging on Client().shutdown() (:pr:`1055`)

.. _v1.16.2 - 2017-05-03:

1.16.2 - 2017-05-03
----------------------

-  Support ``async with Client`` syntax (:pr:`1053`)
-  Use internal bokeh server for default diagnostics server (:pr:`1047`)
-  Improve styling of bokeh plots when empty (:pr:`1046`) (:pr:`1037`)
-  Support efficient serialization for sparse arrays (:pr:`1040`)
-  Prioritize newly arrived work in worker (:pr:`1035`)
-  Prescatter data with joblib backend (:pr:`1022`)
-  Make client.restart more robust to worker failure (:pr:`1018`)
-  Support preloading a module or script in dask-worker or dask-scheduler processes (:pr:`1016`)
-  Specify network interface in command line interface (:pr:`1007`)
-  Client.scatter supports a single element (:pr:`1003`)
-  Use blosc compression on all memoryviews passing through comms (:pr:`998`)
-  Add concurrent.futures-compatible Executor (:pr:`997`)
-  Add as_completed.batches method and return results (:pr:`994`) (:pr:`971`)
-  Allow worker_clients to optionally stay within the thread pool (:pr:`993`)
-  Add bytes-stored and tasks-processing diagnostic histograms (:pr:`990`)
-  Run supports non-msgpack-serializable results (:pr:`965`)


.. _v1.16.1 - 2017-03-22:

1.16.1 - 2017-03-22
-------------------------

-  Use inproc transport in LocalCluster (:pr:`919`)
-  Add structured and queryable cluster event logs (:pr:`922`)
-  Use connection pool for inter-worker communication (:pr:`935`)
-  Robustly shut down spawned worker processes at shutdown (:pr:`928`)
-  Worker death timeout (:pr:`940`)
-  More visual reporting of exceptions in progressbar (:pr:`941`)
-  Render disk and serialization events to task stream visual (:pr:`943`)
-  Support async for / await protocol (:pr:`952`)
-  Ensure random generators are re-seeded in worker processes (:pr:`953`)
-  Upload sourcecode as zip module (:pr:`886`)
-  Replay remote exceptions in local process (:pr:`894`)

.. _v1.16.0 - 2017-02-24:

1.16.0 - 2017-02-24
----------------------------

- First come first served priorities on client submissions (:pr:`840`)
- Can specify Bokeh internal ports (:pr:`850`)
- Allow stolen tasks to return from either worker (:pr:`853`), (:pr:`875`)
- Add worker resource constraints during execution (:pr:`857`)
- Send small data through Channels (:pr:`858`)
- Better estimates for SciPy sparse matrix memory costs (:pr:`863`)
- Avoid stealing long running tasks (:pr:`873`)
- Maintain fortran ordering of NumPy arrays (:pr:`876`)
- Add ``--scheduler-file`` keyword to dask-scheduler (:pr:`877`)
- Add serializer for Keras models (:pr:`878`)
- Support uploading modules from zip files (:pr:`886`)
- Improve titles of Bokeh dashboards (:pr:`895`)

.. _v1.15.2 - 2017-01-27:

1.15.2 - 2017-01-27
---------------------------

*  Fix a bug where arrays with large dtypes or shapes were being improperly compressed (:pr:`830` :pr:`832` :pr:`833`)
*  Extend ``as_completed`` to accept new futures during iteration (:pr:`829`)
*  Add ``--nohost`` keyword to ``dask-ssh`` startup utility (:pr:`827`)
*  Support scheduler shutdown of remote workers, useful for adaptive clusters (:pr: `811` :pr:`816` :pr:`821`)
*  Add ``Client.run_on_scheduler`` method for running debug functions on the scheduler (:pr:`808`)

.. _v1.15.1 - 2017-01-11:

1.15.1 - 2017-01-11
---------------------------

*  Make compatibile with Bokeh 0.12.4 (:pr:`803`)
*  Avoid compressing arrays if not helpful  (:pr:`777`)
*  Optimize inter-worker data transfer (:pr:`770`) (:pr:`790`)
*  Add --local-directory keyword to worker (:pr:`788`)
*  Enable workers to arrive to the cluster with their own data.
   Useful if a worker leaves and comes back (:pr:`785`)
*  Resolve thread safety bug when using local_client (:pr:`802`)
*  Resolve scheduling issues in worker (:pr:`804`)


.. _v1.15.0 - 2017-01-02:

1.15.0 - 2017-01-02
--------------------------

*  Major Worker refactor (:pr:`704`)
*  Major Scheduler refactor (:pr:`717`) (:pr:`722`) (:pr:`724`) (:pr:`742`) (:pr:`743`
*  Add ``check`` (default is ``False``) option to ``Client.get_versions``
   to raise if the versions don't match on client, scheduler & workers (:pr:`664`)
*  ``Future.add_done_callback`` executes in separate thread (:pr:`656`)
*  Clean up numpy serialization (:pr:`670`)
*  Support serialization of Tornado v4.5 coroutines (:pr:`673`)
*  Use CPickle instead of Pickle in Python 2 (:pr:`684`)
*  Use Forkserver rather than Fork on Unix in Python 3 (:pr:`687`)
*  Support abstract resources for per-task constraints (:pr:`694`) (:pr:`720`) (:pr:`737`)
*  Add TCP timeouts (:pr:`697`)
*  Add embedded Bokeh server to workers (:pr:`709`) (:pr:`713`) (:pr:`738`)
*  Add embedded Bokeh server to scheduler (:pr:`724`) (:pr:`736`) (:pr:`738`)
*  Add more precise timers for Windows (:pr:`713`)
*  Add Versioneer (:pr:`715`)
*  Support inter-client channels  (:pr:`729`) (:pr:`749`)
*  Scheduler Performance improvements (:pr:`740`) (:pr:`760`)
*  Improve load balancing and work stealing (:pr:`747`) (:pr:`754`) (:pr:`757`)
*  Run Tornado coroutines on workers
*  Avoid slow sizeof call on Pandas dataframes (:pr:`758`)


.. _v1.14.3 - 2016-11-13:

1.14.3 - 2016-11-13
----------------------------

*  Remove custom Bokeh export tool that implicitly relied on nodejs (:pr:`655`)
*  Clean up scheduler logging (:pr:`657`)


.. _v1.14.2 - 2016-11-11:

1.14.2 - 2016-11-11
----------------------------

*  Support more numpy dtypes in custom serialization, (:pr:`627`), (:pr:`630`), (:pr:`636`)
*  Update Bokeh plots (:pr:`628`)
*  Improve spill to disk heuristics (:pr:`633`)
*  Add Export tool to Task Stream plot
*  Reverse frame order in loads for very many frames (:pr:`651`)
*  Add timeout when waiting on write (:pr:`653`)


.. _v1.14.0 - 2016-11-03:

1.14.0 - 2016-11-03
---------------------------

*   Add ``Client.get_versions()`` function to return software and package
    information from the scheduler, workers, and client (:pr:`595`)
*   Improved windows support (:pr:`577`) (:pr:`590`) (:pr:`583`) (:pr:`597`)
*   Clean up rpc objects explicitly (:pr:`584`)
*   Normalize collections against known futures (:pr:`587`)
*   Add key= keyword to map to specify keynames (:pr:`589`)
*   Custom data serialization (:pr:`606`)
*   Refactor the web interface (:pr:`608`) (:pr:`615`) (:pr:`621`)
*   Allow user-supplied Executor in Worker (:pr:`609`)
*   Pass Worker kwargs through LocalCluster


.. _v1.13.3 - 2016-10-15:

1.13.3 - 2016-10-15
---------------------------

*   Schedulers can retire workers cleanly
*   Add ``Future.add_done_callback`` for ``concurrent.futures`` compatibility
*   Update web interface to be consistent with Bokeh 0.12.3
*   Close streams explicitly, avoiding race conditions and supporting
    more robust restarts on Windows.
*   Improved shuffled performance for dask.dataframe
*   Add adaptive allocation cluster manager
*   Reduce administrative overhead when dealing with many workers
*   ``dask-ssh --log-directory .`` no longer errors
*   Microperformance tuning for the scheduler

.. _v1.13.2:

1.13.2
------

*   Revert dask_worker to use fork rather than subprocess by default
*   Scatter retains type information
*   Bokeh always uses subprocess rather than spawn

.. _v1.13.1:

1.13.1
------

*   Fix critical Windows error with dask_worker executable

.. _v1.13.0:

1.13.0
------

*   Rename Executor to Client (:pr:`492`)
*   Add ``--memory-limit`` option to ``dask-worker``, enabling spill-to-disk
    behavior when running out of memory (:pr:`485`)
*   Add ``--pid-file`` option to dask-worker and ``--dask-scheduler`` (:pr:`496`)
*   Add ``upload_environment`` function to distribute conda environments.
    This is experimental, undocumented, and may change without notice.  (:pr:`494`)
*   Add ``workers=`` keyword argument to ``Client.compute`` and ``Client.persist``,
    supporting location-restricted workloads with Dask collections (:pr:`484`)
*   Add ``upload_environment`` function to distribute conda environments.
    This is experimental, undocumented, and may change without notice.  (:pr:`494`)

    *   Add optional ``dask_worker=`` keyword to ``client.run`` functions that gets
        provided the worker or nanny object
    *   Add ``nanny=False`` keyword to ``Client.run``, allowing for the execution
        of arbitrary functions on the nannies as well as normal workers


.. _v1.12.2:

1.12.2
------

This release adds some new features and removes dead code

*   Publish and share datasets on the scheduler between many clients (:pr:`453`).
    See :doc:`publish`.
*   Launch tasks from other tasks (experimental) (:pr:`471`). See :doc:`task-launch`.
*   Remove unused code, notably the ``Center`` object and older client functions (:pr:`478`)
*   ``Executor()`` and ``LocalCluster()`` is now robust to Bokeh's absence (:pr:`481`)
*   Removed s3fs and boto3 from requirements.  These have moved to Dask.

.. _v1.12.1:

1.12.1
------

This release is largely a bugfix release, recovering from the previous large
refactor.

*  Fixes from previous refactor
    *  Ensure idempotence across clients
    *  Stress test losing scattered data permanently
*  IPython fixes
    *  Add ``start_ipython_scheduler`` method to Executor
    *  Add ``%remote`` magic for workers
    *  Clean up code and tests
*  Pool connects to maintain reuse and reduce number of open file handles
*  Re-implement work stealing algorithm
*  Support cancellation of tuple keys, such as occur in dask.arrays
*  Start synchronizing against worker data that may be superfluous
*  Improve bokeh plots styling
    *  Add memory plot tracking number of bytes
    *  Make the progress bars more compact and align colors
    *  Add workers/ page with workers table, stacks/processing plot, and memory
*  Add this release notes document


.. _v1.12.0:

1.12.0
------

This release was largely a refactoring release.  Internals were changed
significantly without many new features.

*  Major refactor of the scheduler to use transitions system
*  Tweak protocol to traverse down complex messages in search of large
   bytestrings
*  Add dask-submit and dask-remote
*  Refactor HDFS writing to align with changes in the dask library
*  Executor reconnects to scheduler on broken connection or failed scheduler
*  Support sklearn.external.joblib as well as normal joblib

.. _`Antoine Pitrou`: https://github.com/pitrou
.. _`Olivier Grisel`: https://github.com/ogrisel
.. _`Fabian Keller`: https://github.com/bluenote10
.. _`Mike DePalatis`: https://github.com/mivade
.. _`Matthew Rocklin`: https://github.com/mrocklin
.. _`Jim Crist`: https://github.com/jcrist
.. _`Ian Hopkinson`: https://github.com/IanHopkinson
.. _`@rbubley`: https://github.com/rbubley
.. _`Kelvyn Yang`: https://github.com/kelvynyang
.. _`Scott Sievert`: https://github.com/stsievert
.. _`Xander Johnson`: https://github.com/metasyn
.. _`Daniel Li`: https://github.com/li-dan
.. _`Brett Naul`: https://github.com/bnaul
.. _`Cornelius Riemenschneider`: https://github.com/corni
.. _`Alex Ford`: https://github.com/asford
.. _`@rqx`: https://github.com/rqx
.. _`Min RK`: https://github.comminrk/
.. _`Bruce Merry`: https://github.com/bmerry
.. _`Tom Augspurger`: https://github.com/TomAugspurger
.. _`Joe Hamman`: https://github.com/jhamman
.. _`Thrasibule`: https://github.com/thrasibule
.. _`Jacob Tomlinson`: https://github.com/jacobtomlinson
.. _`Andy Jones`: https://github.com/andyljones
.. _`John Kirkham`: https://github.com/jakirkham
.. _`Sven Kreiss`:  https://github.com/svenkreiss
.. _`Russ Bubley`: https://github.com/rbubley
.. _`Joseph Atkins-Kurkish`: https://github.com/spacerat
.. _`Irene Rodriguez`: https://github.com/irenerodriguez
.. _`Loïc Estève`: https://github.com/lesteve
.. _`Kenneth Koski`: https://github.com/knkski
.. _`Tony Lorenzo`: https://github.com/alorenzo175
.. _`Henry Doupe`: https://github.com/hdoupe
.. _`Marius van Niekerk`: https://github.com/mariusvniekerk
.. _`@bmaisson`: https://github.com/bmaisson
.. _`Martin Durant`: https://github.com/martindurant
.. _`Grant Jenks`: https://github.com/grantjenks
.. _`Dror Birkman`: https://github.com/Dror-LightCyber
.. _`Dave Hirschfeld`: https://github.com/dhirschfeld
.. _`Matt Nicolls`: https://github.com/nicolls1
.. _`Phil Tooley`: https://github.com/ptooley
.. _`Bartosz Marcinkowski`: https://github.com/bm371613
.. _`Aurélien Ponte`: https://github.com/apatlpo
.. _`Luke Canavan`: https://github.com/canavandl
.. _`Derek Ludwig`: https://github.com/dsludwig
.. _`Anderson Banihirwe`: https://github.com/andersy005
.. _`Yu Feng`: https://github.com/rainwoodman
.. _`Guillaume Eynard-Bontemps`: https://github.com/guillaumeeb
.. _`Vladyslav Moisieienkov`: https://github.com/VMois
.. _`Chris White`: https://github.com/cicdw
.. _`Adam Klein`: https://github.com/adamklein
.. _`Mike Gevaert`: https://github.com/mgeplf
.. _`Gaurav Sheni`: https://github.com/gsheni
.. _`Eric Ma`: https://github.com/ericmjl
.. _`Peter Killick`: https://github.com/dkillick
.. _`NotSqrt`: https://github.com/NotSqrt
.. _`Tom Rochette`: https://github.com/tomzx
.. _`Roy Wedge`: https://github.com/rwedge
.. _`Michael Wheeler`: https://github.com/mikewheel
.. _`Diane Trout`: https://github.com/detrout
.. _`tjb900`: https://github.com/tjb900
.. _`Stephan Hoyer`: https://github.com/shoyer
.. _`Dirk Petersen`: https://github.com/dirkpetersen
.. _`Daniel Farrell`: https://github.com/danpf
.. _`George Sakkis`: https://github.com/gsakkis
.. _`Adam Beberg`: https://github.com/beberg
.. _`Marco Neumann`: https://github.com/crepererum
.. _`Elliott Sales de Andrade`: https://github.com/QuLogic
.. _`Brett Jurman`: https://github.com/ibebrett
.. _`Julia Kent`: https://github.com/jukent
.. _`Brett Randall`: https://github.com/javabrett
.. _`deepthirajagopalan7`: https://github.com/deepthirajagopalan7
.. _`Muammar El Khatib`: https://github.com/muammar
.. _`Nikos Tsaousis`: https://github.com/tsanikgr
.. _`Brian Chu`: https://github.com/bchu
.. _`James Bourbeau`: https://github.com/jrbourbeau
.. _`amerkel2`: https://github.com/amerkel2
.. _`Michael Delgado`: https://github.com/delgadom
.. _`Peter Andreas Entschev`: https://github.com/pentschev
.. _`condoratberlin`: https://github.com/condoratberlin
.. _`K.-Michael Aye`: https://github.com/michaelaye
.. _`@plbertrand`: https://github.com/plbertrand
.. _`Michael Spiegel`: https://github.com/Spiegel0
.. _`Caleb`: https://github.com/calebho
.. _`Ben Zaitlen`: https://github.com/quasiben
.. _`Benjamin Zaitlen`: https://github.com/quasiben
.. _`Manuel Garrido`: https://github.com/manugarri
.. _`Magnus Nord`: https://github.com/magnunor
.. _`Sam Grayson`: https://github.com/charmoniumQ
.. _`Mathieu Dugré`: https://github.com/mathdugre
.. _`Christian Hudon`: https://github.com/chrish42
.. _`Gabriel Sailer`: https://github.com/sublinus
.. _`Pierre Glaser`: https://github.com/pierreglase
.. _`Shayan Amani`: https://github.com/SHi-ON
.. _`Pav A`: https://github.com/rs2
.. _`Mads R. B. Kristensen`: https://github.com/madsbk
.. _`Mikhail Akimov`: https://github.com/roveo
.. _`Abael He`: https://github.com/abaelhe
.. _`byjott`: https://github.com/byjott
.. _`Mohammad Noor`: https://github.com/MdSalih
.. _`Richard J Zamora`: https://github.com/rjzamora
.. _`Arpit Solanki`: https://github.com/arpit1997
.. _`Gil Forsyth`: https://github.com/gforsyth
.. _`Philipp Rudiger`: https://github.com/philippjfr
.. _`Jonathan De Troye`: https://github.com/detroyejr
.. _`matthieubulte`: https://github.com/matthieubulte
.. _`darindf`: https://github.com/darindf
.. _`James A. Bednar`: https://github.com/jbednar
.. _`IPetrik`: https://github.com/IPetrik
.. _`Simon Boothroyd`: https://github.com/SimonBoothroyd
.. _`rockwellw`: https://github.com/rockwellw
.. _`Jed Brown`: https://github.com/jedbrown
.. _`He Jia`: https://github.com/HerculesJack
.. _`Jim Crist-Harif`: https://github.com/jcrist
.. _`fjetter`: https://github.com/fjetter
.. _`Florian Jetter`: https://github.com/fjetter
.. _`Patrick Sodré`: https://github.com/sodre
.. _`Stephan Erb`: https://github.com/StephanErb
.. _`Benedikt Reinartz`: https://github.com/filmor
.. _`Markus Mohrhard`: https://github.com/mmohrhard
.. _`Mana Borwornpadungkitti`: https://github.com/potpath
.. _`Chrysostomos Nanakos`: https://github.com/cnanakos
.. _`Chris Roat`: https://github.com/chrisroat
.. _`Jakub Beránek`: https://github.com/Kobzol
.. _`kaelgreco`: https://github.com/kaelgreco
.. _`Dustin Tindall`: https://github.com/dustindall
.. _`Julia Signell`: https://github.com/jsignell
.. _`Alex Adamson`: https://github.com/aadamson
.. _`Cyril Shcherbin`: https://github.com/shcherbin
.. _`Søren Fuglede Jørgensen`: https://github.com/fuglede
.. _`Igor Gotlibovych`: https://github.com/ig248
.. _`Stan Seibert`: https://github.com/seibert
.. _`Davis Bennett`: https://github.com/d-v-b
.. _`Lucas Rademaker`: https://github.com/lr4d
.. _`Darren Weber`: https://github.com/dazza-codes
.. _`Matthias Urlichs`: https://github.com/smurfix
.. _`Krishan Bhasin`: https://github.com/KrishanBhasin
.. _`Abdulelah Bin Mahfoodh`: https://github.com/abduhbm
.. _`jakirkham`: https://github.com/jakirkham
.. _`Prasun Anand`: https://github.com/prasunanand
.. _`Jonathan J. Helmus`: https://github.com/jjhelmus
.. _`Rami Chowdhury`: https://github.com/necaris
.. _`crusaderky`: https://github.com/crusaderky
.. _`Nicholas Smith`: https://github.com/nsmith-
.. _`Dillon Niederhut`: https://github.com/deniederhut
.. _`Jonas Haag`: https://github.com/jonashaag
.. _`Nils Braun`: https://github.com/nils-braun
.. _`Nick Evans`: https://github.com/nre
.. _`Scott Sanderson`: https://github.com/ssanderson
.. _`Matthias Bussonnier`: https://github.com/Carreau
.. _`DomHudson`: https://github.com/DomHudson
.. _`Julien Jerphanion`: https://github.com/jjerphan
.. _`joshreback`: https://github.com/joshreback
.. _`Alexander Clausen`: https://github.com/sk1p
.. _`Andrew Fulton`: https://github.com/andrewfulton9
.. _`Jendrik Jördening`: https://github.com/jendrikjoe
.. _`Jack Xiaosong Xu`: https://github.com/jackxxu
.. _`Willi Rath`: https://github.com/willirath
.. _`Roberto Panai`: https://github.com/rpanai
.. _`Dror Speiser`: https://github.com/drorspei
.. _`Poruri Sai Rahul`: https://github.com/rahulporuri
.. _`jennalc`: https://github.com/jennalc
.. _`Sergey Kozlov`: https://github.com/skozlovf
.. _`jochen-ott-by`: https://github.com/jochen-ott-by
.. _`Simon Perkins`: https://github.com/sjperkins
.. _`GeethanjaliEswaran`: https://github.com/geethanjalieswaran
.. _`Timost`: https://github.com/Timost
.. _`Ian Rose`: https://github.com/ian-r-rose
.. _`marwan116`: https://github.com/marwan116
.. _`Bernhard M. Wiedemann`: https://github.com/bmwiedemann
.. _`Bruno Pagani`: https://github.com/ArchangeGabriel
.. _`selshowk`: https://github.com/selshowk
.. _`Ray Bell`: https://github.com/raybellwaves
.. _`Casey Clements`: https://github.com/caseyclements
.. _`Ben Greiner`: https://github.com/bnavigator
.. _`Fabian Gebhart`: https://github.com/fgebhart
.. _`Sultan Orazbayev`: https://github.com/SultanOrazbayev
.. _`Doug Davis`: https://github.com/douglasdavis
.. _`cameron16`: https://github.com/cameron16
.. _`Gerald`: https://github.com/gerald732
.. _`Charles Blackmon-Luca`: https://github.com/charlesbluca
.. _`Marcos Moyano`: https://github.com/marcosmoyano
.. _`James Lamb`: https://github.com/jameslamb
.. _`Hristo Georgiev`: https://github.com/hristog
.. _`Matteo De Wint`: https://github.com/mdwint
.. _`Naty Clementi`: https://github.com/ncclementi
.. _`Nathan Danielsen`: https://github.com/ndanielsen
.. _`Torsten Wörtwein`: https://github.com/twoertwein
.. _`ArtinSarraf`: https://github.com/ArtinSarraf
.. _`Gabe Joseph`: https://github.com/gjoseph92
.. _`Freyam Mehta`: https://github.com/freyam
.. _`gerrymanoim`: https://github.com/gerrymanoim
.. _`Bryan Van de Ven`: https://github.com/bryevdv
.. _`David Chudzicki`: https://github.com/dchudz
.. _`Walt Woods`: https://github.com/wwoods
.. _`Tom Forbes`: https://github.com/orf
.. _`Michael Adkins`: https://github.com/madkinsz
.. _`Genevieve Buckley`: https://github.com/GenevieveBuckley
.. _`Erik Welch`: https://github.com/eriknw
.. _`Fábio Rosado`: https://github.com/FabioRosado
.. _`Maximilian Roos`: https://github.com/max-sixty
.. _`Aneesh Nema`: https://github.com/aneeshnema
.. _`Deepyaman Datta`: https://github.com/deepyaman
.. _`Garry O'Donnell`: https://github.com/garryod
.. _`Thomas Grainger`: https://github.com/graingert
.. _`Sarah Charlotte Johnson`: https://github.com/scharlottej13
.. _`Tim Harris`: https://github.com/tharris72
.. _`Bryan W. Weber`: https://github.com/bryanwweber
.. _`crendoncoiled`: https://github.com/crendoncoiled
.. _`Andrii Oriekhov`: https://github.com/andriyor
.. _`Duncan McGregor`: https://github.com/dmcg
.. _`Eric Engestrom`: https://github.com/lace
.. _`ungarj`: https://github.com/ungarj
.. _`Matthew Murray`: https://github.com/Matt711
.. _`Enric Tejedor`: https://github.com/etejedor
.. _`Hendrik Makait`: https://github.com/hendrikmakait
.. _`Marco Wolsza`: https://github.com/maawoo
