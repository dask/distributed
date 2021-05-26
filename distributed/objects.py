"""This file contains custom objects.
These are mostly regular objects with more useful _repr_ and _repr_html_ methods."""


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
