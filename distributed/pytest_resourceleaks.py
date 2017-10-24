# -*- coding: utf-8 -*-
"""
A pytest plugin to trace resource leaks.
"""
from __future__ import print_function, division

import collections
import gc
import time
import os
import threading

import pytest


def pytest_addoption(parser):
    group = parser.getgroup('resource leaks')
    group.addoption(
        '-L', '--leaks',
        action='store',
        dest='leaks',
        help='''\
List of resources to monitor for leaks before and after each test.
Can be 'all' or a comma-separated list of resource names
(possible values: {known_checkers}).
'''.format(known_checkers=', '.join(sorted("'%s'" % s for s in all_checkers)))
    )
    group.addoption(
        '--leaks-timeout',
        action='store',
        type='float',
        dest='leaks_timeout',
        default=0.5,
        help='''\
Wait at most this number of seconds to mark a test leaking
(default: %(default)s).
'''
    )
    group.addoption(
        '--leaks-fail',
        action='store_true',
        dest='leaks_mark_failed',
        default=False,
        help='''Mark leaked tests failed.'''
    )
    #group.addoption(
        #'--leak-retries',
        #action='store',
        #type=int,
        #dest='leak_retries',
        #default=1,
        #help='''\
#Max number of times to retry a test when it leaks, to ignore
#warmup-related issues (default: 1).
#'''
    #)


def pytest_configure(config):
    leaks = config.getvalue('leaks')
    if leaks:
        if leaks == 'all':
            leaks = sorted(all_checkers)
        else:
            leaks = leaks.split(',')
        unknown = sorted(set(leaks) - set(all_checkers))
        if unknown:
            raise ValueError("unknown resources: %r" % (unknown,))

        checkers = [all_checkers[leak]() for leak in leaks]
        checker = LeakChecker(checkers=checkers,
                              grace_delay=config.getvalue('leaks_timeout'),
                              mark_failed=config.getvalue('leaks_mark_failed'))
        config.pluginmanager.register(checker, 'leaks_checker')


all_checkers = {}


def register_checker(name):
    def decorate(cls):
        assert issubclass(cls, ResourceChecker), cls
        assert name not in all_checkers
        all_checkers[name] = cls
        return cls

    return decorate


class ResourceChecker(object):

    def measure(self):
        raise NotImplementedError

    def has_leak(self, before, after):
        raise NotImplementedError

    def on_retry(self):
        raise NotImplementedError

    def format(self, before, after):
        raise NotImplementedError


@register_checker('fds')
class FDChecker(ResourceChecker):

    def measure(self):
        if os.name == 'posix':
            import psutil
            return psutil.Process().num_fds()
        else:
            return 0

    def has_leak(self, before, after):
        return after > before

    def on_retry(self):
        pass

    def format(self, before, after):
        return "leaked %d file descriptor(s)" % (after - before)


@register_checker('memory')
class RSSMemoryChecker(ResourceChecker):

    def measure(self):
        import psutil
        return psutil.Process().memory_info().rss

    def has_leak(self, before, after):
        return after > before + 1e7

    def on_retry(self):
        pass

    def format(self, before, after):
        return "leaked %d MB of RSS memory" % ((after - before) / 1e6)


@register_checker('threads')
class ActiveThreadsChecker(ResourceChecker):

    def measure(self):
        return set(threading.enumerate())

    def has_leak(self, before, after):
        return not after <= before

    def on_retry(self):
        pass

    def format(self, before, after):
        leaked = after - before
        assert leaked
        return ("leaked %d Python threads: %s"
                % (len(leaked), sorted(leaked, key=str)))


class LeakChecker(object):
    def __init__(self, checkers, grace_delay, mark_failed):
        self.checkers = checkers
        self.grace_delay = grace_delay
        self.mark_failed = mark_failed

        # {nodeid: {checkers}}
        self.skip_checkers = {}
        # {nodeid: {checker: [(before, after)]}}
        self.counters = {}
        # {nodeid: [(checker, before, after)]}
        self.leaks = {}
        # {nodeid: {outcomes}}
        self.outcomes = collections.defaultdict(set)

    def cleanup(self):
        gc.collect()

    def checks_for_item(self, nodeid):
        return [c for c in self.checkers
                if c not in self.skip_checkers.get(nodeid, ())]

    def measure(self, nodeid):
        # Return items in order
        return [(c, c.measure()) for c in self.checks_for_item(nodeid)]

    def measure_before_test(self, nodeid):
        for checker, before in self.measure(nodeid):
            assert before is not None
            self.counters[nodeid][checker].append((before, None))

    def measure_after_test(self, nodeid):
        outcomes = self.outcomes[nodeid]
        assert outcomes
        if outcomes != {'passed'}:
            # Test failed or skipped
            return

        def run_measurements():
            leaks = []
            for checker, after in self.measure(nodeid):
                assert after is not None
                c = self.counters[nodeid][checker]
                before, _ = c[-1]
                c[-1] = (before, after)
                if checker.has_leak(before, after):
                    leaks.append((checker, before, after))
            return leaks

        t1 = time.time()
        deadline = t1 + self.grace_delay
        leaks = run_measurements()
        if leaks:
            self.cleanup()
            for c, _, _ in leaks:
                c.on_retry()
            leaks = run_measurements()

        while leaks and time.time() < deadline:
            time.sleep(0.1)
            self.cleanup()
            for c, _, _ in leaks:
                c.on_retry()
            leaks = run_measurements()

        if leaks:
            self.leaks[nodeid] = leaks
            # ...
            pass

    # Note on hook execution order:
    #   pytest_runtest_protocol
    #       pytest_runtest_setup
    #       pytest_report_teststatus
    #       pytest_runtest_call
    #       pytest_report_teststatus
    #       pytest_runtest_teardown
    #       pytest_report_teststatus

    # See also https://github.com/abalkin/pytest-leaks/blob/master/pytest_leaks.py

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item, nextitem):
        def run_test():
            # Unused
            hook = item.ihook
            hook.pytest_runtest_setup(item=item)
            hook.pytest_runtest_call(item=item)
            hook.pytest_runtest_teardown(item=item, nextitem=nextitem)

        nodeid = item.nodeid
        assert nodeid not in self.counters
        self.counters[nodeid] = {c: [] for c in self.checkers}

        leaking = item.get_marker('leaking')
        if leaking is not None:
            unknown = sorted(set(leaking.args) - set(all_checkers))
            if unknown:
                raise ValueError("pytest.mark.leaking: unknown resources %r"
                                 % (unknown,))
            classes = tuple(all_checkers[a] for a in leaking.args)
            self.skip_checkers[nodeid] = {c for c in self.checkers
                                          if isinstance(c, classes)}
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item):
        self.measure_before_test(item.nodeid)
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item):
        outcome = yield
        del outcome
        self.measure_after_test(item.nodeid)
        if self.mark_failed and self.leaks.get(item.nodeid):
            # Trigger fail here to allow stopping with `-x`
            pytest.fail()

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_report_teststatus(self, report):
        nodeid = report.nodeid
        outcomes = self.outcomes[nodeid]
        outcomes.add(report.outcome)
        outcome = yield
        if report.when == 'teardown':
            leaks = self.leaks.get(report.nodeid)
            if leaks:
                if self.mark_failed:
                    outcome.force_result(('failed', 'L', 'LEAKED'))
                    report.outcome = 'failed'
                    report.longrepr = "\n".join(
                        ["%s %s" % (nodeid, checker.format(before, after))
                         for checker, before, after in leaks])
                else:
                    outcome.force_result(('leaked', 'L', 'LEAKED'))

    @pytest.hookimpl
    def pytest_terminal_summary(self, terminalreporter, exitstatus):
        tr = terminalreporter
        leaked = tr.getreports('leaked')
        if leaked:
            # If mark_failed is False, leaks are output as a separate
            # results section
            tr.write_sep("=", 'RESOURCE LEAKS')
            for rep in leaked:
                nodeid = rep.nodeid
                for checker, before, after in self.leaks[nodeid]:
                    tr.line("%s %s"
                            % (rep.nodeid, checker.format(before, after)))
