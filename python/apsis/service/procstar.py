import logging
import sanic

from apsis.lib.api import error, response_json
from apsis.procstar import get_agent_server, NoServerError

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------

API = sanic.Blueprint("procstar")


@API.route("/connections")
def on_connections(request):
    """
    Returns a mapping from conn ID to connection info.
    """
    try:
        server = get_agent_server()
    except NoServerError as err:
        return error(str(err))
    return response_json({i: c.to_jso() for i, c in server.connections.items()})


@API.route("/groups")
def on_groups(request):
    """
    Returns a mapping from group ID to list of connection info.
    """
    try:
        server = get_agent_server()
    except NoServerError as err:
        return error(str(err))
    return response_json(
        {
            i: [server.connections[c].to_jso() for c in g]
            for i, g in server.connections.groups.items()
        }
    )
