from .plugin import SchedulerPlugin


class GraphLayout(SchedulerPlugin):
    def __init__(self, scheduler):
        self.x = {}
        self.y = {}
        self.scheduler = scheduler
        self.index = {}
        self.next_y = 0
        self.next_index = 0
        self.new = []

        scheduler.add_plugin(self)

        if self.scheduler.tasks:
            dependencies = {k: [ds.key for ds in ts.dependencies]
                            for k, ts in scheduler.tasks.items()}
            priority = {k: ts.priority for k, ts in scheduler.tasks.items()}
            self.update_graph(self.scheduler, dependencies=dependencies,
                    priority=priority)

    def update_graph(self, scheduler, dependencies=None, priority=None,
                     **kwargs):
        for key in sorted(dependencies, key=priority.get):
            deps = dependencies[key]
            if key in self.x or key not in scheduler.tasks:
                continue
            if deps:
                total_deps = sum(len(scheduler.tasks[dep].dependents)
                                 for dep in deps)
                y = sum(self.y[dep] * len(scheduler.tasks[dep].dependents)
                                      / total_deps
                        for dep in deps)
                x = max(self.x[dep] for dep in deps) + 1
            else:
                x = 0
                y = self.next_y
                self.next_y += 1

            self.x[key] = x
            self.y[key] = y
            self.index[key] = self.next_index
            self.next_index += 1
            self.new.append(key)
            if key not in scheduler.tasks:
                import pdb; pdb.set_trace()
                1 + 1
