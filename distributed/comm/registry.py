from __future__ import annotations

from abc import ABC, abstractmethod


class Backend(ABC):
    """
    A communication backend, selected by a given URI scheme (e.g. 'tcp').
    """

    # I/O

    @abstractmethod
    def get_connector(self):
        """
        Get a connector object usable for connecting to addresses.
        """

    @abstractmethod
    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        """
        Get a listener object for the scheme-less address *loc*.
        """

    # Address handling

    @abstractmethod
    def get_address_host(self, loc):
        """
        Get a host name (normally an IP address) identifying the host the
        address is located on.
        *loc* is a scheme-less address.
        """

    @abstractmethod
    def resolve_address(self, loc):
        """
        Resolve the address into a canonical form.
        *loc* is a scheme-less address.

        Simple implementations may return *loc* unchanged.
        """

    def get_address_host_port(self, loc):
        """
        Get the (host, port) tuple of the scheme-less address *loc*.
        This should only be implemented by IP-based transports.
        """
        raise NotImplementedError

    @abstractmethod
    def get_local_address_for(self, loc):
        """
        Get the local listening address suitable for reaching *loc*.
        """


# The {scheme: Backend} mapping
backends: dict[str, Backend] = {}


def get_backend(scheme: str, require: bool = True) -> Backend:
    """
    Get the Backend instance for the given *scheme*.
    It looks for matching scheme in dask's internal cache, and falls-back to
    package metadata for the group name ``distributed.comm.backends``

    Parameters
    ----------

    require : bool
        Verify that the backends requirements are properly installed. See
        https://setuptools.readthedocs.io/en/latest/pkg_resources.html for more
        information.
    """

    backend = backends.get(scheme)
    if backend is None:
        import pkg_resources

        backend = None
        for backend_class_ep in pkg_resources.iter_entry_points(
            "distributed.comm.backends", scheme
        ):
            # resolve and require are equivalent to load
            backend_factory = backend_class_ep.resolve()
            if require:
                backend_class_ep.require()
            backend = backend_factory()

        if backend is None:
            raise ValueError(
                "unknown address scheme %r (known schemes: %s)"
                % (scheme, sorted(backends))
            )
        else:
            backends[scheme] = backend
    return backend
