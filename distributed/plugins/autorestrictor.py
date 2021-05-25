from collections import defaultdict

from distributed import SchedulerPlugin
from dask.core import reverse_dict
from dask.base import tokenize
from dask.order import graph_metrics, ndependencies


def install_plugin(dask_scheduler=None, **kwargs):
    dask_scheduler.add_plugin(AutoRestrictor(**kwargs), idempotent=True)


def unravel_deps(hlg_deps, name, unravelled_deps=None):
    """Recursively construct a set of all dependencies for a specific task."""

    if unravelled_deps is None:
        unravelled_deps = set()

    for dep in hlg_deps[name]:
        unravelled_deps |= {dep}
        unravel_deps(hlg_deps, dep, unravelled_deps)

    return unravelled_deps


def get_node_depths(dependencies, root_nodes, metrics):

    node_depths = {}

    for k in dependencies.keys():
        # Get dependencies per node.
        deps = unravel_deps(dependencies, k)
        # Associate nodes with root nodes.
        roots = root_nodes & deps
        offset = metrics[k][-1]
        node_depths[k] = \
            max(metrics[r][-1] - offset for r in roots) if roots else 0

    return node_depths


class AutoRestrictor(SchedulerPlugin):

    def update_graph(self, scheduler, dsk=None, keys=None, restrictions=None,
                     **kw):
        """Processes dependencies to assign tasks to specific workers."""

        workers = list(scheduler.workers.keys())
        n_worker = len(workers)

        tasks = scheduler.tasks
        dependencies = kw["dependencies"]
        dependents = reverse_dict(dependencies)

        _, total_dependencies = ndependencies(dependencies, dependents)
        metrics = graph_metrics(dependencies, dependents, total_dependencies)

        # Terminal nodes have no dependents, root nodes have no dependencies.
        terminal_nodes = {k for (k, v) in dependents.items() if not v}
        root_nodes = {k for (k, v) in dependencies.items() if not v}

        # Figure out the depth of every task. Depth is defined as maximum
        # distance from a root node.
        node_depths = get_node_depths(dependencies, root_nodes, metrics)
        max_depth = max(node_depths.values())

        # If we have fewer terminal nodes than workers, we cannot utilise all
        # the workers and are likely dealing with a reduction. We work our way
        # back through the graph, starting at the deepest terminal nodes, and
        # try to find a depth at which there was enough work to utilise all
        # workers.
        while len(terminal_nodes) < n_worker:
            _terminal_nodes = terminal_nodes.copy()
            for tn in _terminal_nodes:
                if node_depths[tn] == max_depth:
                    terminal_nodes ^= set((tn,))
                    terminal_nodes |= dependencies[tn]
            max_depth -= 1
            if max_depth == -1:
                raise ValueError("AutoRestrictor cannot determine a sensible "
                                 "work assignment pattern. Falling back to "
                                 "default behaviour.")

        roots_per_terminal = {}
        terminal_dependencies = {}
        terminal_dependents = {}

        for tn in terminal_nodes:
            # Get dependencies per terminal node.
            terminal_dependencies[tn] = unravel_deps(dependencies, tn)
            # Get dependents per terminal node. TODO: This terminology is
            # confusing - the terminal nodes are not necessarily the last.
            terminal_dependents[tn] = unravel_deps(dependents, tn)
            # Associate terminal nodes with root nodes.
            roots_per_terminal[tn] = root_nodes & terminal_dependencies[tn]

        # Create a unique token for each set of terminal roots. TODO: This is
        # very strict. What about nodes with very similar roots? Tokenization
        # may be overkill too.
        root_tokens = \
            {tokenize(*sorted(v)): v for v in roots_per_terminal.values()}

        hash_map = defaultdict(set)
        group_offset = 0

        # Associate terminal roots with a specific group if they are not a
        # subset of another larger root set. TODO: This can likely be improved.
        for k, v in root_tokens.items():
            if any([v < vv for vv in root_tokens.values()]):  # Strict subset.
                continue
            else:
                hash_map[k] |= set([group_offset])
                group_offset += 1

        # If roots were a subset, they should share the annotation of their
        # superset/s.
        for k, v in root_tokens.items():
            shared_roots = \
                {kk: None for kk, vv in root_tokens.items() if v < vv}
            if shared_roots:
                hash_map[k] = \
                    set().union(*[hash_map[kk] for kk in shared_roots.keys()])

        for k in terminal_dependencies.keys():

            tdp = terminal_dependencies[k]
            tdn = terminal_dependents[k] if terminal_dependents[k] else set()

            # TODO: This can likely be improved.
            group = hash_map[tokenize(*sorted(roots_per_terminal[k]))]

            # Set restrictions on a terminal node and its dependencies.
            for tn in [k, *tdp, *tdn]:
                try:
                    task = tasks[tn]
                except KeyError:  # Keys may not have an assosciated task.
                    continue
                if task._worker_restrictions is None:
                    task._worker_restrictions = set()
                task._worker_restrictions |= \
                    {workers[g % n_worker] for g in group}
                task._loose_restrictions = False
