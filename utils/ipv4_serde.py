import socket
import struct

def encode(ip):
    return struct.unpack("!I", socket.inet_aton(ip))[0]

def decode(encoded):
    return socket.inet_ntoa(struct.pack("!I", encoded))

if __name__ == '__main__':
    encoded = encode('0.1.0.1')
    print encoded
    print decode(encoded)
