""" This module contains utility functions to construct and manipulate counting
data structures for frames.

When performing statistical profiling we obtain many call stacks.  We aggregate
these call stacks into data structures that maintain counts of how many times
each function in that call stack has been called.  Because these stacks will
overlap this aggregation counting structure forms a tree, such as is commonly
visualized by profiling tools.

We represent this tree as a nested dictionary with the following form:

    {
     'description': 'A long description of the line of code being run.',
     'count': 10  # the number of times we have seen this line
     'children': {  # callers of this line. Recursive dicts
         'ident': {'description': ...
                   'count': ...
                   'children': {...}},
         'ident': {'description': ...
                   'count': ...
                   'children': {...}}}
    }
"""


from collections import defaultdict
import linecache


def identifier(frame):
    return ';'.join((frame.f_code.co_name,
                     frame.f_code.co_filename,
                     str(frame.f_code.co_firstlineno)))


def repr_frame(frame):
    co = frame.f_code
    text = '  File "%s", line %s, in %s' % (co.co_filename,
                                            frame.f_lineno,
                                            co.co_name)
    line = linecache.getline(co.co_filename, frame.f_lineno, frame.f_globals).lstrip()
    return text + '\n\t' + line


def process(frame, child, state):
    """ Add counts from a frame stack onto existing state

    This recursively adds counts to the existing state dictionary and creates
    new entries for new functions.

    Example
    -------
    >>> import sys, threading
    >>> ident = threading.get_ident()  # replace with your thread of interest
    >>> frame = sys._current_frames()[ident]
    >>> state = {'children': {}, 'count': 0, 'description': 'root'}
    >>> process(frame, None, state)
    >>> state
    {'count': 1,
     'description': 'root',
     'children': {'...'}}
    """
    ident = identifier(frame)
    if frame.f_back is not None:
        state = process(frame.f_back, frame, state)

    try:
        d = state['children'][ident]
    except KeyError:
        d = {'count': 0,
             'description': repr_frame(frame),
             'children': {}}
        state['children'][ident] = d

    state['count'] += 1

    if child is not None:
        return d
    else:
        d['count'] += 1


def merge(*args):
    """ Merge multiple frame states together """
    assert len({arg['description'] for arg in args}) == 1
    children = defaultdict(list)
    for arg in args:
        for child in arg['children']:
            children[child].append(arg['children'][child])

    children = {k: merge(*v) for k, v in children.items()}
    count = sum(arg['count'] for arg in args)
    return {'description': args[0]['description'],
            'children': dict(children),
            'count': count}


def create():
    return {'description': 'root', 'count': 0, 'children': {}}


def call_stack(frame):
    L = []
    while frame:
        L.append(repr_frame(frame))
        frame = frame.f_back
    return L[::-1]
