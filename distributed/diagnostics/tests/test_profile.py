import sys
import time
from toolz import first
from threading import Thread

from distributed.diagnostics.profile import process, merge, create, call_stack
from distributed.compatibility import get_thread_identity


def test_basic():
    def test_g():
        time.sleep(0.01)

    def test_h():
        time.sleep(0.02)

    def test_f():
        for i in range(100):
            test_g()
            test_h()

    thread = Thread(target=test_f)
    thread.daemon = True
    thread.start()

    state = create()

    for i in range(100):
        time.sleep(0.02)
        frame = sys._current_frames()[thread.ident]
        process(frame, None, state)

    assert state['description'] == 'root'
    assert state['count'] == 100
    d = state
    while len(d['children']) == 1:
        d = first(d['children'].values())

    assert d['count'] == 100
    assert 'test_f' in d['description']
    g = [c for c in d['children'].values() if 'test_g' in c['description']][0]
    h = [c for c in d['children'].values() if 'test_h' in c['description']][0]

    assert g['count'] < h['count']
    assert g['count'] + h['count'] == 100


def test_merge():
    a1 = {
         'count': 5,
         'description': 'a',
         'children': {
             'b': {'count': 3,
                   'description': 'b-func',
                   'children': {}},
             'c': {'count': 2,
                   'description': 'c-func',
                   'children': {}}}}

    a2 = {
         'count': 4,
         'description': 'a',
         'children': {
             'd': {'count': 2,
                   'description': 'd-func',
                   'children': {}},
             'c': {'count': 2,
                   'description': 'c-func',
                   'children': {}}}}

    expected = {
         'count': 9,
         'description': 'a',
         'children': {
             'b': {'count': 3,
                   'description': 'b-func',
                   'children': {}},
             'd': {'count': 2,
                   'description': 'd-func',
                   'children': {}},
             'c': {'count': 4,
                   'description': 'c-func',
                   'children': {}}}}

    assert merge(a1, a2) == expected
