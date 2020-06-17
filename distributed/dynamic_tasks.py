import math
from operator import getitem

from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.dataframe.shuffle import (
    _concat,
    shuffle_group,
    shuffle_group_2,
    shuffle_group_get,
)
from dask.highlevelgraph import HighLevelGraph
from dask.utils import digit, insert
from distributed import get_client, get_worker
from distributed.worker import dumps_task


def _rearguard():
    pass


def dynshuffle_kernel(
    df, name, token, inp, stage, rank, n, k, nfinal, col, ignore_index,
):
    worker = get_worker()
    client = get_client()
    myself = worker.get_current_task()
    assert name in myself

    shuffle_getitem_name = name + "-getitem-" + token
    shuffle_concat_name = name + "-concat-" + token

    groups = shuffle_group(df, col, stage, k, n, ignore_index, nfinal)

    new_tasks = []
    for i in range(k):
        getitem_key = insert(inp, stage, i)
        new_tasks.append(
            {
                "key": str((shuffle_getitem_name, stage, rank, i)),
                "dependencies": [str((f"{name}-{token}", stage, getitem_key))],
                "task": dumps_task(
                    (getitem, str((f"{name}-{token}", stage, getitem_key)), inp[stage])
                ),
                "priority": 0,
                "releases": [(str((f"{name}-{token}", stage, getitem_key)), k)],
            }
        )
    getitem_keys = [t["key"] for t in new_tasks]

    new_tasks.append(
        {
            "key": str((shuffle_concat_name, stage, rank)),
            "dependencies": getitem_keys,
            "task": dumps_task((_concat, getitem_keys)),
        }
    )

    client.sync(
        worker.scheduler.insert_tasks,
        cur_key=myself,
        new_tasks=new_tasks,
        rearguard_key=str((f"rearguard_{name}_{stage}-{token}", rank)),
        rearguard_input=str((shuffle_concat_name, stage, rank)),
    )

    return groups


def rearrange_by_column_dynamic_tasks(
    df, column, max_branch=None, npartitions=None, ignore_index=False
):
    token = tokenize(df, column, max_branch, npartitions, ignore_index)
    max_branch = max_branch if max_branch else 32
    n = df.npartitions
    nfinal = npartitions if npartitions else n

    stages = int(math.ceil(math.log(n) / math.log(max_branch)))
    if stages > 1:
        k = int(math.ceil(n ** (1 / stages)))
    else:
        k = n

    inputs = [tuple(digit(i, j, k) for j in range(stages)) for i in range(k ** stages)]
    name = "dynshuffle"

    dsk = {}
    for stage in range(stages):
        for rank, inp in enumerate(inputs):
            if stage == 0:
                if rank < df.npartitions:
                    start = (df._name, rank)
                else:
                    start = df._meta
            else:
                start = (f"rearguard_{name}_{stage-1}-{token}", rank)
            dsk[(f"{name}-{token}", stage, inp)] = (
                dynshuffle_kernel,
                start,
                name,
                token,
                inp,
                stage,
                rank,
                n,
                k,
                nfinal,
                column,
                ignore_index,
            )
            if stage == stages - 1 and rank == df.npartitions - 1:
                dsk[(f"rearguard_{name}_{stage}-{token}", rank)] = (
                    _rearguard,
                    [(f"{name}-{token}", stage, inn) for inn in inputs[rank:]]
                    + [
                        (f"rearguard_{name}_{stage}-{token}", r)
                        for r in range(rank + 1, len(inputs))
                    ],
                )
            else:
                dsk[(f"rearguard_{name}_{stage}-{token}", rank)] = (
                    _rearguard,
                    (f"{name}-{token}", stage, inp),
                )

    df2 = new_dd_object(
        HighLevelGraph.from_collections(
            f"rearguard_{name}_{stages-1}-{token}", dsk, dependencies=[df]
        ),
        f"rearguard_{name}_{stages-1}-{token}",
        df._meta,
        df.divisions,
    )

    # If the npartitions doesn't match, we use the old shuffle code for now.
    if npartitions is not None and npartitions != df.npartitions:
        token = tokenize(df2, npartitions)
        repartition_group_token = "repartition-group-" + token

        dsk = {
            (repartition_group_token, i): (
                shuffle_group_2,
                k,
                column,
                ignore_index,
                npartitions,
            )
            for i, k in enumerate(df2.__dask_keys__())
        }

        repartition_get_name = "repartition-get-" + token

        for p in range(npartitions):
            dsk[(repartition_get_name, p)] = (
                shuffle_group_get,
                (repartition_group_token, p % df.npartitions),
                p,
            )

        graph2 = HighLevelGraph.from_collections(
            repartition_get_name, dsk, dependencies=[df2]
        )
        df3 = new_dd_object(
            graph2, repartition_get_name, df2._meta, [None] * (npartitions + 1)
        )
    else:
        df3 = df2
        df3.divisions = (None,) * (df.npartitions + 1)

    # print(f"rearrange_by_column_dynamic_tasks() - len(dsk): {len(dsk)}")
    return df3
