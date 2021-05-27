"""This file contains custom objects.
These are mostly regular objects with more useful _repr_ and _repr_html_ methods."""
import datetime

from dask.utils import format_bytes, format_time_ago

from .utils import clean_dashboard_address


class HasWhat(dict):
    """A dictionary of all workers and which keys that worker has."""

    def _repr_html_(self):
        rows = ""

        for worker, keys in sorted(self.items()):
            summary = ""
            for key in keys:
                summary += f"""<tr><td>{key}</td></tr>"""

            rows += f"""<tr>
            <td>{worker}</td>
            <td>{len(keys)}</td>
            <td>
                <details>
                <summary style='display:list-item'>Expand</summary>
                <table>
                {summary}
                </table>
                </details>
            </td>
        </tr>"""

        output = f"""
        <table>
        <tr>
            <th>Worker</th>
            <th>Key count</th>
            <th>Key list</th>
        </tr>
        {rows}
        </table>
        """

        return output


class WhoHas(dict):
    """A dictionary of all keys and which workers have that key."""

    def _repr_html_(self):
        rows = ""

        for title, keys in sorted(self.items()):
            rows += f"""<tr>
            <td>{title}</td>
            <td>{len(keys)}</td>
            <td>{", ".join(keys)}</td>
        </tr>"""

        output = f"""
        <table>
        <tr>
            <th>Key</th>
            <th>Copies</th>
            <th>Workers</th>
        </tr>
        {rows}
        </table>
        """

        return output


class SchedulerInfo(dict):
    """A dictionary of information about the scheduler and workers."""

    def _repr_html_(self):
        dashboard_address = None
        if "dashboard" in self["services"]:
            dashboard_address = self["address"].split(":")
            dashboard_address[0] = "http"
            dashboard_address[-1] = str(self["services"]["dashboard"]) + "/status"
            dashboard_address = ":".join(dashboard_address)

        scheduler = f"""
            <div>
                <div style="width: 24px; height: 24px; background-color: #FFF7E5; border: 3px solid #FF6132; border-radius: 5px; position: absolute;">&nbsp;</div>
                <div style="margin-left: 48px;">
                    <h3 style="margin-bottom: 0px;">{self["type"]}</h3>
                    <p style="color: #9D9D9D; margin-bottom: 0px;">{self["id"]}</p>
                    <table style="width: 100%; text-align: left;">
                        <tr>
                            <td style="text-align: left;"><strong>Comm:</strong> {self["address"]}</td>
                            <td style="text-align: left;"><strong>Workers:</strong> {len(self["workers"])}</td>
                        </tr>
                        <tr>
                            <td style="text-align: left;"><strong>Dashboard:</strong> <a href="{dashboard_address}">{dashboard_address}</a></td>
                            <td style="text-align: left;"><strong>Total threads:</strong> {sum([w["nthreads"] for w in self["workers"].values()])}</td>
                        </tr>
                        <tr>
                            <td style="text-align: left;"><strong>Started:</strong> {format_time_ago(datetime.datetime.fromtimestamp(self["started"]))}</td>
                            <td style="text-align: left;"><strong>Total memory:</strong> {format_bytes(sum([w["memory_limit"] for w in self["workers"].values()]))}</td>
                        </tr>
                    </table>
                </div>
            </div>
        """

        workers = ""
        for worker_name in self["workers"]:
            self["workers"][worker_name]["comm"] = worker_name
        for worker in sorted(self["workers"].values(), key=lambda k: k["name"]):
            dashboard_address = None
            if "dashboard" in worker["services"]:
                dashboard_address = worker["comm"].split(":")
                dashboard_address[0] = "http"
                dashboard_address[-1] = str(worker["services"]["dashboard"])
                dashboard_address = ":".join(dashboard_address)

            metrics = ""

            if "metrics" in worker:
                metrics = f"""
                <tr>
                    <td style="text-align: left;"><strong>Tasks executing:</strong> {worker["metrics"]["executing"]}</td>
                    <td style="text-align: left;"><strong>Tasks in memory:</strong> {worker["metrics"]["in_memory"]}</td>
                </tr>
                <tr>
                    <td style="text-align: left;"><strong>Tasks ready:</strong> {worker["metrics"]["ready"]}</td>
                    <td style="text-align: left;"><strong>Tasks in flight:</strong> {worker["metrics"]["in_flight"]}</td>
                </tr>
                <tr>
                    <td style="text-align: left;"><strong>CPU usage:</strong> {worker["metrics"]["cpu"]}%</td>
                    <td style="text-align: left;"><strong>Last seen:</strong> {format_time_ago(datetime.datetime.fromtimestamp(worker["last_seen"]))}</td>
                </tr>
                <tr>
                    <td style="text-align: left;"><strong>Memory usage:</strong> {((worker["metrics"]["memory"] / worker["memory_limit"]) * 100):.1f}%</td>
                    <td style="text-align: left;"><strong>Spilled bytes:</strong> {format_bytes(worker["metrics"]["spilled_nbytes"])}</td>
                </tr>
                <tr>
                    <td style="text-align: left;"><strong>Read bytes:</strong> {format_bytes(worker["metrics"]["read_bytes"])}</td>
                    <td style="text-align: left;"><strong>Write bytes:</strong> {format_bytes(worker["metrics"]["write_bytes"])}</td>
                </tr>
                """

            workers += f"""
            <div style="margin-bottom: 20px;">
                <div style="width: 24px; height: 24px; background-color: #DBF5FF; border: 3px solid #4CC9FF; border-radius: 5px; position: absolute;">&nbsp;</div>
                <div style="margin-left: 48px;">
                <details>
                    <summary><h3 style="margin-bottom: 0px; display: inline;">{worker["type"]}: {worker["name"]}</h3></summary>
                    <table style="width: 100%; text-align: left;">
                        <tr>
                            <td style="text-align: left;"><strong>Comm:</strong> {worker["comm"]}</td>
                            <td style="text-align: left;"><strong>Total threads:</strong> {worker["nthreads"]}</td>
                        </tr>
                        <tr>
                            <td style="text-align: left;"><strong>Dashboard:</strong> <a href="{dashboard_address}">{dashboard_address}</a></td>
                            <td style="text-align: left;"><strong>Memory:</strong> {format_bytes(worker["memory_limit"])}</td>
                        </tr>
                        <tr>
                            <td style="text-align: left;"><strong>Nanny:</strong> {worker["nanny"]}</td>
                            <td style="text-align: left;"></td>
                        </tr>
                        <tr>
                            <td colspan="2" style="text-align: left;"><strong>Local directory:</strong> {worker["local_directory"]}</td>
                        </tr>
                        {metrics}
                    </table>
                </details>
                </div>
            </div>
            """

        return f"""
        <div style="">
            {scheduler}
            <details style="margin-left: 48px;">
            <summary style="margin-bottom: 20px;"><h3 style="display: inline;">Workers</h3></summary>
            {workers}
            </details>
        </div>
        """
