import socket
import struct

def encode(ip):
    return struct.unpack("!I", socket.inet_aton(ip))[0]

def decode(encoded):
    return socket.inet_ntoa(struct.pack("!I", encoded))
