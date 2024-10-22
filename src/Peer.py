import ipaddress
import re

from exceptions import PeerValidationError

"""
host
host_formated == host for hostname and ipv4
"""
class Peer:
    def __init__(self, host_str, port:int):
        self.port = port
        self.host_formated = f'{host_str}:{port}'
        self.host = host_str
        if not is_valid_peer(self):
            raise PeerValidationError("Peer creation is false", "PeerValidationError")

    def __str__(self) -> str:
        return f"{self.host_formated}"

    def __eq__(self, o: object) -> bool:
        return isinstance(o, Peer) and self.host == o.host \
            and self.port == o.port

    def __hash__(self) -> int:
        return (self.port, self.host).__hash__()

    def __repr__(self) -> str:
        return f"Peer: {self}"

def is_valid_peer(peer: Peer) -> bool:
    # Implement peer validation logic based on the requirements
    if not 1 <= peer.port <= 65535:
        return False

    if re.match(r'^(\d{1,3}\.){3}\d{1,3}$', peer.host):
        # IPv4 address
        return all(0 <= int(octet) <= 255 for octet in peer.host.split('.'))
    else:
        # DNS entry
        return (re.match(r'^[a-zA-Z\d\.\-\_]{3,50}$', peer.host) and
                '.' in peer.host[1:-1] and
                any(c.isalpha() for c in peer.host))