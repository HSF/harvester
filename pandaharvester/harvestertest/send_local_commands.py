import json
import sys

from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy


def make_command_spec(command, params, command_id):
    """
    Build a CommandSpec to be inserted into the DB from only command and params.

    command_id must be provided explicitly: it is the primary key and (unlike on
    sqlite) is NOT auto-assigned by the backend. Locally-submitted commands use
    negative ids so they never collide with the positive ids assigned by the
    panda server. receiver is resolved from the command->receiver map and
    processed/ack_requested are initialized so the command can actually be
    retrieved by get_commands_for_receiver().
    """
    command_spec = CommandSpec()
    command_spec.command_id = command_id
    command_spec.command = command
    command_spec.params = params
    command_spec.ack_requested = 0
    command_spec.processed = 0
    # resolve receiver the same way command_manager does, otherwise the command
    # is never picked up (retrieval filters on receiver and processed=0)
    for com_str, receiver in CommandSpec.receiver_map.items():
        if command.startswith(com_str):
            command_spec.receiver = receiver
            break
    return command_spec


if len(sys.argv) < 2:
    print('ERROR: missing argument; usage: python send_local_commands.py \'{"<command>": "<params>", ...}\'')
    sys.exit(1)

command_json_str = str(sys.argv[1])

try:
    command_dict = json.loads(command_json_str)
except json.JSONDecodeError as e:
    print(f"ERROR: failed to parse argument as JSON: {e}")
    sys.exit(1)

if not command_dict:
    print("WARNING: no commands found in input; nothing to store")
    sys.exit(0)

db_proxy = DBProxy()

# pick negative command_ids so local commands never collide with the positive
# ids assigned by the panda server; keep going below any existing local id
min_command_id = db_proxy.get_min_command_id()
if min_command_id is None or min_command_id >= 0:
    next_command_id = -1
else:
    next_command_id = min_command_id - 1

print(f"Preparing {len(command_dict)} command(s) to store")
command_specs = []
for command, params in command_dict.items():
    command_spec = make_command_spec(command, params, next_command_id)
    if command_spec.receiver is None:
        print(f"  WARNING: command '{command}' has no matching receiver and will not be picked up by any agent")
    print(f"  prepared: command_id={command_spec.command_id} command='{command}' receiver='{command_spec.receiver}' params={params}")
    command_specs.append(command_spec)
    next_command_id -= 1

print(f"Storing {len(command_specs)} command(s) into the DB ...")
ret = db_proxy.store_commands(command_specs)
if ret:
    print(f"SUCCESS: stored {len(command_specs)} command(s)")
    sys.exit(0)
else:
    print("FAILURE: store_commands returned False; check harvester logs for details")
    sys.exit(1)
