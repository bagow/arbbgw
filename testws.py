import socket, ssl, json, time, struct, base64, os
host = 'fx-ws.gateio.ws'
port = 443
path = '/v4/ws/usdt'

context = ssl.create_default_context()
sock = socket.create_connection((host, port))
ssock = context.wrap_socket(sock, server_hostname=host)

key = base64.b64encode(os.urandom(16)).decode('utf-8')
req = f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
ssock.sendall(req.encode())
headers = ssock.recv(4096)
print("Handshake:", headers.split(b'\r\n')[0])

def send_msg(ssock, msg_bytes):
    header = bytearray([0x81])
    L = len(msg_bytes)
    if L < 126: header.append(0x80 | L)
    mask = os.urandom(4)
    masked = bytearray(b ^ mask[i%4] for i, b in enumerate(msg_bytes))
    ssock.sendall(header + mask + masked)

def recv_msg(ssock):
    header = ssock.recv(2)
    L = header[1] & 127
    if L == 126: L = struct.unpack(">H", ssock.recv(2))[0]
    return ssock.recv(L)

msg = json.dumps({"time": int(time.time()), "channel": "futures.tickers", "event": "subscribe", "payload": ["BTC_USDT", "ETH_USDT"]})
send_msg(ssock, msg.encode())

print("Subscribed. Waiting for response...")
for _ in range(3):
    r = recv_msg(ssock)
    print("RECV:", r.decode('utf-8', errors='ignore'))
ssock.close()
