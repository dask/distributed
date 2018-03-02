class Cluster(object):
    scheduler : Scheduler

    @property
    def scheduler_address(self):
        """ This what Client(...) currently looks for to find an address """
        return self.scheduler.address

    def scale_up(self, n: int) -> List[T]:
        """ Scale up the cluster to at least n workers

        This is mostly intended to be used with Adaptive.  See scale for a more
        general user-facing function.

        Parameters
        ----------
        n: int
            Total number of workers to have after calling this function

        Returns
        -------
        A list of the newly created workers, represented in whatever way makes
        sense for the cluster

        See Also
        --------
        scale: more user facing
        """
        pass

    def scale_down(self, workers: Union[List[T], List[str]]):
        """ Scale cluster down by removing particular workers

        This is sometimes used by ``Adaptive`` *after* it has already asked the
        scheduler to gracefully retire the workers.  This method is used if a
        more forceful approach should be taken.

        Parameters
        ----------
        workers: list
            List of particular workers to remove, either addresses (strings) of
            the workers or else a list of the things returned by scale_up.
        """
        pass

    def scale(self, n: int):
        """ Scale the cluster to n workers """
        pass

    def adapt(self, **kwargs) -> Adaptive:
        """ Turn on adaptive scaling """
        from distributed.deploy import Adaptive
        if not hasattr(self, '_adaptive'):
            self._adaptive = Adaptive(self.scheduler, self, **kwargs)
        return self._adaptive

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def close(self):
        self.scale(0)
