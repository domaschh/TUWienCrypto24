import threading
from sre_constants import error
from typing import Dict, Any

import jcs

from Peer import Peer
import src.constants as const
from src.message.msgexceptions import *
from jcs import canonicalize

import mempool
import objects
import peer_db

import asyncio
import ipaddress
import json
import random
import re
import sqlite3
import sys

from src.Peer import is_valid_peer
from src.exceptions import InvalidHandshakeException, InvalidFormatException

PEERS = set()
CONNECTIONS = dict()
BACKGROUND_TASKS = set()
BLOCK_VERIFY_TASKS = dict()
BLOCK_WAIT_LOCK = None
TX_WAIT_LOCK = None
MEMPOOL = mempool.Mempool(const.GENESIS_BLOCK_ID, {})
PEERS_LOCK = threading.Lock()
CONNECTIONS_LOCK = threading.Lock()

LISTEN_CFG = {
        "address": const.ADDRESS,
        "port": const.PORT
}

# Add peer to your list of peers
def add_peer(peer):
    with PEERS_LOCK:
        PEERS.add(peer)

def get_bootstrap_peers():
    print("Loading bootsrap peers...")
    bootsrap_peers =  peer_db.get_bootstrap_peers()
    with PEERS_LOCK:
        PEERS.update(bootsrap_peers)
    return bootsrap_peers

# Add connection if not already open
def add_connection(peer, queue):
    with CONNECTIONS_LOCK:
        CONNECTIONS[peer] = queue


# Delete connection
def del_connection(peer):
    with CONNECTIONS_LOCK:
        if peer in CONNECTIONS:
            del CONNECTIONS[peer]
            print(f"Removed connection to {peer}")
        else:
            print(f"No active connection found for {peer}")


# Make msg objects
def mk_error_msg(error_str, error_name):
    return {
        "type": "error",
        "payload": {
            "error": error_str,
            "error_name": error_name
        }
    }

def mk_hello_msg():
    return {
        "type": "hello",
        "version": const.VERSION,
        "agent": const.AGENT
    }

def mk_getpeers_msg():
    return {
        "type": "getpeers",
    }

# you find that a node is faulty, disconnect from it and remove it from your set of known
#peers (i.e., forget them). Likewise, if you discover that a node is offline, you should forget
#it. You must not, however, block further communication requests from this node or refuse to
#add this node again to your known nodes if another node reports this as known. Note that
#there may are (edge) cases where forgetting a node is not possible - we will not check this
#behaviour.
def mk_peers_msg():

    myself: Peer = (Peer(const.ADDRESS, const.PORT))  # TODO how do we find out or ip address or DNS??
    with PEERS_LOCK:
        PEERS.update(peer_db.load_peers())
        selected_peers: [Peer] = random.sample(list(PEERS), min(len(PEERS), const.MAX_PEERS_IN_MSG-1))
        selected_peers.insert(0, myself)
    return {
        "type": "peers",
        "peers": [peer.host_formated for peer in selected_peers]
    }

def mk_getobject_msg(objid):
    return {
        "type": "getobject",
        "objectid": objid,
    }

def mk_object_msg(obj_dict):
    pass # TODO

def mk_ihaveobject_msg(objid):
    pass # TODO

def mk_chaintip_msg(blockid):
    pass # TODO

def mk_mempool_msg(txids):
    pass # TODO

def mk_getchaintip_msg():
    pass # TODO

def mk_getmempool_msg():
    pass # TODO

# parses a message as json. returns decoded message
def parse_msg(msg_str):
    try:
        json_msg = json.loads(msg_str.decode())
        canon = json.loads(canonicalize(json_msg))
        return canon
    except error as e:
        print(e.msg)
        pass #TODO Handling

# Send data over the network as a message
async def write_msg(writer: asyncio.StreamWriter, msg: Dict[str, Any]):
    try:
        writer.write(canonicalize(msg) + b'\n')
        await writer.drain()
    except Exception as e:
        print(f"Error: {e} with message {msg}")


async def read_msg(reader: asyncio.StreamReader) -> Dict[str, Any]:
    data = await reader.readline()
    return parse_msg(data)


# Check if message contains no invalid keys,
# raises a MalformedMsgException
def validate_allowed_keys(msg_dict, allowed_keys, msg_type):
    unwanted_keys = set(msg_dict.keys()) - allowed_keys
    if unwanted_keys:
        raise MalformedMsgException("Additional invalid keys")


# Validate the hello message
# raises an exception
def validate_hello_msg(msg_dict: Dict[str, Any]):
    allowed_keys = {'type', 'version', 'agent'}
    try:
        validate_allowed_keys(msg_dict, allowed_keys, msg_dict.get('type'))
    except MalformedMsgException:
        raise InvalidFormatException("Malformed keys")

    if msg_dict.get("type") != "hello":
        raise InvalidHandshakeException("First message must be a hello message")
    if "version" not in msg_dict or "agent" not in msg_dict:
        raise InvalidFormatException("Missing required keys in hello message")
    if not re.fullmatch(r'0\.10\.\d', msg_dict["version"]):
        raise InvalidFormatException("Invalid version format")
    if len(msg_dict["agent"]) > 128 or not msg_dict["agent"].isascii():
        raise InvalidFormatException("Invalid agent format")

async def perform_handshake(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    # Send our hello message
    print(f"Performing handshake...")
    await write_msg(writer, mk_hello_msg())
    # Wait for their hello message
    try:
        msg = await asyncio.wait_for(read_msg(reader), timeout=const.HELLO_MSG_TIMEOUT)
        validate_hello_msg(msg)
        print(f"... Handshake complete and valid")
    except InvalidFormatException as e:
         raise InvalidFormatException(f"Invalid format: {str(e)}")
    except asyncio.TimeoutError:
        raise InvalidHandshakeException("Handshake timeout")
    except json.JSONDecodeError:
        raise InvalidHandshakeException("Invalid JSON in hello message")


# returns true iff host_str is a valid hostname
def validate_hostname(host_str):
    pass # TODO

# returns true iff host_str is a valid ipv4 address
def validate_ipv4addr(host_str):
    pass # TODO

# returns true iff peer_str is a valid peer address
def validate_peer_str(peer_str):
    pass # TODO

# raise an exception if not valid
def validate_peers_msg(msg_dict):
    allowed_keys = {'type', 'peers'}
    try:
        validate_allowed_keys(msg_dict, allowed_keys, None)
        if msg_dict.get("type") != "peers":
            raise InvalidFormatException("Invalid peers format")
        if "peers" not in msg_dict:
            raise InvalidFormatException("Missing peers key in peers message")
        peers =  msg_dict.get("peers")
        if len(peers) > 30:
            raise InvalidFormatException("Invalid peers format: Too many peers sent")
        for peer in peers:
            host, port = peer.split(":")
            p: Peer = Peer(host, int(port))
            if not is_valid_peer(p):
                raise InvalidFormatException(f"Invalid peers format {peer}")
    except MalformedMsgException:
        raise InvalidFormatException("Malformed keys")
    except error as e:
        print(f"Error: {e}")

# raise an exception if not valid
def validate_getpeers_msg(msg_dict):
    allowed_keys = {'type'}
    try:
        validate_allowed_keys(msg_dict, allowed_keys, None)
    except MalformedMsgException:
        raise InvalidFormatException("Malformed keys")

# raise an exception if not valid
def validate_getchaintip_msg(msg_dict):
    pass # TODO

# raise an exception if not valid
def validate_getmempool_msg(msg_dict):
    pass # TODO

# raise an exception if not valid
def validate_error_msg(msg_dict):
    pass # TODO

# raise an exception if not valid
def validate_ihaveobject_msg(msg_dict):
    pass # TODO

# raise an exception if not valid
def validate_getobject_msg(msg_dict):
    pass # TODO

# raise an exception if not valid
def validate_object_msg(msg_dict):
    pass # TODO

# raise an exception if not valid
def validate_chaintip_msg(msg_dict):
    pass # todo
    
# raise an exception if not valid
def validate_mempool_msg(msg_dict):
    pass # todo
        
def validate_msg(msg_dict):
    msg_type = msg_dict['type']
    if msg_type == 'hello':
        validate_hello_msg(msg_dict)
    elif msg_type == 'getpeers':
        validate_getpeers_msg(msg_dict)
    elif msg_type == 'peers':
        validate_peers_msg(msg_dict)
    elif msg_type == 'getchaintip':
        validate_getchaintip_msg(msg_dict)
    elif msg_type == 'getmempool':
        validate_getmempool_msg(msg_dict)
    elif msg_type == 'error':
        validate_error_msg(msg_dict)
    elif msg_type == 'ihaveobject':
        validate_ihaveobject_msg(msg_dict)
    elif msg_type == 'getobject':
        validate_getobject_msg(msg_dict)
    elif msg_type == 'object':
        validate_object_msg(msg_dict)
    elif msg_type == 'chaintip':
        validate_chaintip_msg(msg_dict)
    elif msg_type == 'mempool':
        validate_mempool_msg(msg_dict)
    else:
        pass # TODO


def handle_peers_msg(msg_dict):
    try:
        validate_msg(msg_dict)
        peers = msg_dict.get('peers')
        for peer in peers:
            host, port = peer.split(":")
            p = Peer(host, int(port))
        with PEERS_LOCK:
            peer_db.store_peer(p)
            PEERS.update(peer_db.load_peers())
    except MalformedMsgException:
        raise InvalidFormatException("Malformed keys")
    except InvalidFormatException as e:
        raise InvalidFormatException(f"Invalid format: {str(e)}")


def handle_getpeers_msg(msg_dict):
    try:
        validate_msg(msg_dict)
        message = mk_peers_msg()
        return message
    except InvalidFormatException as e:
        raise InvalidHandshakeException(f"Invalid format: {str(e)}")



def handle_error_msg(msg_dict, peer_self):
    pass # TODO


async def handle_ihaveobject_msg(msg_dict, writer):
    pass # TODO


async def handle_getobject_msg(msg_dict, writer):
    pass # TODO

# return a list of transactions that tx_dict references
def gather_previous_txs(db_cur, tx_dict):
    # coinbase transaction
    if 'height' in tx_dict:
        return {}

    pass # TODO

# get the block, the current utxo and block height
def get_block_utxo_height(blockid):
    # TODO
    block = ''
    utxo = ''
    height = ''
    return (block, utxo, height)

# get all transactions as a dict txid -> tx from a list of ids
def get_block_txs(txids):
    pass # TODO


# Stores for a block its utxoset and height
def store_block_utxo_height(block, utxo, height: int):
    pass # TODO

# runs a task to verify a block
# raises blockverifyexception
async def verify_block_task(block_dict):
    pass # TODO

# adds a block verify task to queue and starting it
def add_verify_block_task(objid, block, queue):
    pass # TODO

# abort a block verify task
async def del_verify_block_task(task, objid):
    pass # TODO

# what to do when an object message arrives
async def handle_object_msg(msg_dict, peer_self, writer):
    pass # TODO


# returns the chaintip blockid
def get_chaintip_blockid():
    pass # TODO


async def handle_getchaintip_msg(msg_dict, writer):
    pass # TODO


async def handle_getmempool_msg(msg_dict, writer):
    pass # TODO


async def handle_chaintip_msg(msg_dict):
    pass # TODO


async def handle_mempool_msg(msg_dict):
    pass # TODO


def handle_msg(msg_dict):
    msg_type = msg_dict['type']
    if msg_type == 'hello':
        pass #TODO
    elif msg_type == 'getpeers':
        return handle_getpeers_msg(msg_dict)
    elif msg_type == 'peers':
        return handle_peers_msg(msg_dict)
    elif msg_type == 'getchaintip':
        pass #TODO
    elif msg_type == 'getmempool':
        pass #TODO
    elif msg_type == 'error':
        pass #TODO
    elif msg_type == 'ihaveobject':
        pass #TODO
    elif msg_type == 'getobject':
        pass #TODO
    elif msg_type == 'object':
        pass #TODO
    elif msg_type == 'chaintip':
        pass #TODO
    elif msg_type == 'mempool':
        pass #TODO
    else:
        pass # TODO


# Helper function
async def handle_queue_msg(msg: Dict[str, Any], writer: asyncio.StreamWriter):
    """
    Handle messages from the outgoing queue and send them to the peer.
    """
    try:
        if msg is not None:
            await write_msg(writer, msg)
            print(f"Sent message to {writer.get_extra_info('peername')}: {msg}")

    except Exception as e:
        print(f"Error sending message to {writer.get_extra_info('peername')}: {str(e)}")
        # If there's an error sending the message, we might want to close the connection
        writer.close()
        await writer.wait_closed()
        del_connection(writer.get_extra_info('peername'))

# how to handle a connection
async def handle_connection(reader, writer):
    read_task = None
    queue_task = None

    peer = None
    queue = asyncio.Queue()
    try:
        peer = writer.get_extra_info('peername')
        if not peer:
            raise Exception("Failed to get peername!")
        add_connection(peer, queue)

        print("New connection with {}".format(peer))
    except Exception as e:
        print(str(e))
        del_connection(peer)
        try:
            writer.close()
        except:
            pass
        return

    try:
        # Send initial messages
        # Complete handshake
        await perform_handshake(reader, writer)
        await write_msg(writer, mk_getpeers_msg())

        msg_str = None
        while True:
            if read_task is None:
                read_task = asyncio.create_task(reader.readline())
            if queue_task is None:
                queue_task = asyncio.create_task(queue.get())

            # wait for network or queue messages
            done, pending = await asyncio.wait([read_task, queue_task],
                    return_when = asyncio.FIRST_COMPLETED)
            if read_task in done:
                msg_str = read_task.result()
                read_task = None
            # handle queue messages
            if queue_task in done:
                queue_msg = queue_task.result()
                queue_task = None
                await handle_queue_msg(queue_msg, writer)
                queue.task_done()

            # if no message was received over the network continue
            if read_task is not None:
                continue

            #handle received message
            print(f" Handling msg From: {peer}: {msg_str}")
            await queue.put(handle_msg(parse_msg(msg_str)))

            # for now, close connection
            #raise MessageException("closing connection")
    except InvalidFormatException as e:
        print(f"{peer}: Invalid format error: {str(e)}")
        await write_msg(writer, mk_error_msg(str(e), "INVALID_FORMAT"))
        #remove node from known_peers
        peer_db.del_peer(peer)
    except InvalidHandshakeException as e:
        print(f"{peer}: Handshake error: {str(e)}")
        #remove node from known_peers
        peer_db.del_peer(peer)
        await write_msg(writer, mk_error_msg(str(e), "INVALID_HANDSHAKE"))
    except asyncio.exceptions.TimeoutError:
        print("{}: Timeout".format(peer))
        try:
            await write_msg(writer, mk_error_msg("Timeout"))
        except:
            pass
    except MessageException as e:
        print("{}: {}".format(peer, str(e)))
        #try:
            #await write_msg(writer, mk_error_msg(e.NETWORK_ERROR_MESSAGE,))
        #except:
            #pass
    except Exception as e:
        print("{}: {}".format(peer, str(e)))
    finally:
        print("Closing connection with {}".format(peer))
        writer.close()
        del_connection(peer)
        if read_task is not None and not read_task.done():
            read_task.cancel()
        if queue_task is not None and not queue_task.done():
            queue_task.cancel()


async def connect_to_node(peer: Peer):
    try:
        reader, writer = await asyncio.open_connection(peer.host, peer.port,
                limit=const.RECV_BUFFER_LIMIT)
    except Exception as e:
        print(str(e))
        return

    await handle_connection(reader, writer)



async def listen():
    server = await asyncio.start_server(handle_connection, LISTEN_CFG['address'],
            LISTEN_CFG['port'], limit=const.RECV_BUFFER_LIMIT)

    print("Listening on {}:{}".format(LISTEN_CFG['address'], LISTEN_CFG['port']))

    async with server:
        await server.serve_forever()

# bootstrap peers. connect to hardcoded peers
async def bootstrap():
    print("Connecting to bootstrap peers...")

    #load bootsrap peers
    bootstrap_peers = get_bootstrap_peers()
    for peer in bootstrap_peers:
        await connect_to_node(peer)

# connect to some peers
async def resupply_connections():
    print("Resupplying peers...")
    peer_resupply = []
    with PEERS_LOCK:
        with CONNECTIONS_LOCK:
            diff = const.PEERS_RESUPPLY_LIMIT - len(CONNECTIONS)
            if diff > 0:
                peers = peer_db.load_peers()
                new_peers = peers.difference(CONNECTIONS)
                peer_resupply.extend(random.sample(new_peers, min(len(PEERS), diff)))
                PEERS.update(peer_resupply)

    print(f"Attempting to connect to {len(peer_resupply)} re_supply peers")
    for peer in peer_resupply:
        connection_task = asyncio.create_task(connect_to_node(peer))


async def init():
    global BLOCK_WAIT_LOCK
    BLOCK_WAIT_LOCK = asyncio.Condition()
    global TX_WAIT_LOCK
    TX_WAIT_LOCK = asyncio.Condition()


    #print("Add ourselves as peer???")
    #PEERS.add(Peer(const.ADDRESS, const.PORT))#TODO how do we find out or ip address or DNS??

    bootstrap_task = asyncio.create_task(bootstrap())
    listen_task = asyncio.create_task(listen())

    # Service loop
    while True:
        print("Service loop reporting in.")
        with CONNECTIONS_LOCK:
            print("Open connections: {}".format(len(CONNECTIONS)))

        # Open more connections if necessary
        await resupply_connections()

        await asyncio.sleep(const.SERVICE_LOOP_DELAY)

        #todo why is this here? ij loop or outside loop?

    await bootstrap_task
    await listen_task


def main():
    asyncio.run(init())


if __name__ == "__main__":
    if len(sys.argv) == 3:
        LISTEN_CFG['address'] = sys.argv[1]
        LISTEN_CFG['port'] = sys.argv[2]

    main()
