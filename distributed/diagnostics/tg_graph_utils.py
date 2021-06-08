# Get stack order
def toposort_layers(dependencies):
    """Sorts the layers in a graph topologically
    Parameters
    ----------
    dependencies : dict
        task groups dependencies
    Returns
    -------
    ret: list
        List of task groups names sorted topologically
    """
    degree = {k: len(v) for k, v in dependencies.items()}

    reverse_deps = {
        k: [] for k in dependencies
    }  ### maybe we want to use this to return also dependents

    ready = []
    for k, v in dependencies.items():
        for dep in v:
            reverse_deps[dep].append(k)  ## this are the dependents
        if not v:
            ready.append(k)
    ret = []

    while len(ready) > 0:
        layer = ready.pop()
        ret.append(layer)
        for rdep in reverse_deps[layer]:
            degree[rdep] -= 1
            if degree[rdep] == 0:
                ready.append(rdep)
    return ret
