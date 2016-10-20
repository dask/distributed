from __future__ import print_function, division, absolute_import

from toolz import partition

def parse_args(args):
    options = dict(partition(2, args))
    for k, v in options.items():
        if v.isnumeric():
            options[k] = int(v)

    return options
