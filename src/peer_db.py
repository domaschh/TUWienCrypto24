import csv

from Peer import Peer, is_valid_peer
from typing import Iterable, Set

PEER_DB_FILE = "peers.csv"

def store_peer(peer: Peer, existing_peers: Iterable[Peer] = None):
    if not is_valid_peer(peer):
        return #TODO handle if not valid peer

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
            for row in reader:
                if len(row) == 2:
                    peer = Peer(row[0], int(row[1]))
                    if is_valid_peer(peer):
                        peers.add(peer)
    except FileNotFoundError:
        pass
    return peers
