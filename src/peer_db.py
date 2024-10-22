import csv

from Peer import Peer, is_valid_peer
from typing import Iterable, Set

from exceptions import PeerValidationError

PEER_DB_FILE = "peers.csv"

def store_peer(peer: Peer, existing_peers: Iterable[Peer] = None):
    if not is_valid_peer(peer):
        raise PeerValidationError("Peer is wrong", "PeerValidationError")

    peers = set(existing_peers) if existing_peers else load_peers()
    peers.add(peer)

    with open(PEER_DB_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        for p in peers:
            writer.writerow([p.host, p.port])

def load_peers() -> Set[Peer]:
    peers = set()
    try:
        with open(PEER_DB_FILE, 'r', newline='') as f:
            reader = csv.reader(f)
            for i,row in enumerate(reader):
                if i == 0:
                    continue
                if len(row) == 2:
                    try:
                        peer = Peer(row[0], int(row[1]))
                        peers.add(peer)
                    except PeerValidationError:
                        #TODO decide on what todo if peer is false? I guess
                        pass
    except FileNotFoundError:
        #TODO decide todo
        pass
    return peers

def get_bootstrap_peers() -> Set[Peer]:
    peers = set()
    peers.add(Peer("128.130.122.101", 18018))
    return peers

def del_peer(peer: Peer):
    peers = load_peers()
    if peer in peers:
        peers.remove(peer)

    with open(PEER_DB_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        for p in peers:
            writer.writerow([p.host, p.port])