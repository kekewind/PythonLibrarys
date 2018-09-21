import sys
import struct
from base64 import b64encode
from hashlib import sha1
import logging
from socket import error as SocketError
import errno
import uuid
from datetime import datetime

if sys.version_info[0] < 3:
    from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler
else:
    from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler

logger = logging.getLogger(__name__)
logging.basicConfig(filename="socket_server_" + str(datetime.now().strftime("%Y%m%d")) + ".log",
                    filemode='a',
                    format="%(asctime)s  %(filename)s : %(levelname)s  %(message)s")

'''
+-+-+-+-+-------+-+-------------+-------------------------------+
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
|N|V|V|V|       |S|             |   (if payload len==126/127)   |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
|     Extended payload length continued, if payload len == 127  |
+ - - - - - - - - - - - - - - - +-------------------------------+
|                     Payload Data continued ...                |
+---------------------------------------------------------------+
'''

FIN = 0x80
OP_CODE = 0x0f
MASKED = 0x80
PAYLOAD_LEN = 0x7f
PAYLOAD_LEN_EXT16 = 0x7e
PAYLOAD_LEN_EXT64 = 0x7f

OP_CODE_CONTINUATION = 0x0
OP_CODE_TEXT = 0x1
OP_CODE_BINARY = 0x2
OP_CODE_CLOSE_CONN = 0x8
OP_CODE_PING = 0x9
OP_CODE_PONG = 0xA


# ------------------------- Implementation -----------------------------

class WebSocketServer(ThreadingMixIn, TCPServer):
    """
    A web_socket server waiting for clients to connect.
    Args:
        port(int): Port to bind to
        host(str): Hostname or IP to listen for connections. By default 127.0.0.1
            is being used. To accept connections from any client, you should use
            0.0.0.0.
        logging level: Logging level from logging module to use for logging. By default
            warnings and errors are being logged.
    Properties:
        clients(list): A list of connected clients. A client is a dictionary
            like below.
                {
                 'id'      : id,
                 'handler' : handler,
                 'address' : (address, port)
                }
    """

    allow_reuse_address = True
    daemon_threads = True  # comment to keep threads alive until finished

    clients = []
    id_counter = 0

    def __init__(self, port, host='127.0.0.1', loglevel=logging.WARNING):
        logger.setLevel(loglevel)
        TCPServer.__init__(self, (host, port), WebSocketHandler)
        self.port = self.socket.getsockname()[1]
        self.handler_fn_list = {}

    def run_forever(self):
        try:
            logger.info("Listening on port %d for clients.." % self.port)
            self.serve_forever()
        except KeyboardInterrupt:
            self.server_close()
            logger.info("Server terminated.")
        except Exception as e:
            logger.error(str(e), exc_info=True)
            exit(1)

    def set_handler_fn(self, name, fn):
        self.handler_fn_list[name] = fn

    def get_handler_fn(self, name):
        return self.handler_fn_list.get(name)

    def remove_handler_fn(self, name):
        self.handler_fn_list.pop(name)

    def new_client(self, client, server):
        pass

    def client_left(self, client, server):
        pass

    def message_received(self, client, server, message):
        pass

    def set_fn_new_client(self, fn):
        self.new_client = fn

    def set_fn_client_left(self, fn):
        self.client_left = fn

    def set_fn_message_received(self, fn):
        self.message_received = fn

    def get_client_by_id(self, client_id):
        for client in self.clients:
            if client["id"] == client_id:
                return client

    def send_message(self, client, msg):
        self._unique_cast_(client, msg)

    def send_message_to_all(self, msg):
        self._multi_cast_(msg)

    def _message_received_(self, handler, msg):
        self.message_received(self.handler_to_client(handler), self, msg)

    @staticmethod
    def _ping_received_(handler, msg):
        handler.send_pong(msg)

    def _pong_received_(self, handler, msg):
        pass

    def _new_client_(self, handler):
        self.id_counter += 1
        client = {
            'id': str(uuid.uuid4()),
            'handler': handler,
            'address': handler.client_address
        }
        self.clients.append(client)
        self.new_client(client, self)
        self.send_message(client, str("{id:'" + client["id"] + "'}"))
        logger.info("New Client from %s:%d id:%s Connected to Server" % (
            handler.client_address[0], handler.client_address[1], client['id']))

    def _client_left_(self, handler):
        client = self.handler_to_client(handler)
        self.client_left(client, self)
        if client in self.clients:
            self.clients.remove(client)

    @staticmethod
    def _unique_cast_(to_client, msg):
        to_client['handler'].send_message(msg)

    def _multi_cast_(self, msg):
        for client in self.clients:
            self._unique_cast_(client, msg)

    def handler_to_client(self, handler):
        for client in self.clients:
            if client['handler'] == handler:
                return client


class WebSocketHandler(StreamRequestHandler):

    def __init__(self, socket, address, server):
        self.keep_alive = True
        self.handshake_done = False
        self.valid_client = False
        self.server = server
        StreamRequestHandler.__init__(self, socket, address, server)

    def setup(self):
        StreamRequestHandler.setup(self)

    def handle(self):
        while self.keep_alive:
            if not self.handshake_done:
                self.handshake()
            elif self.valid_client:
                self.read_next_message()

    def read_bytes(self, num):
        # python3 gives ordinal of byte directly
        head_bytes = self.rfile.read(num)
        if sys.version_info[0] < 3:
            return map(ord, head_bytes)
        else:
            return head_bytes

    def read_next_message(self):
        try:
            b1, b2 = self.read_bytes(2)
        except SocketError as e:  # to be replaced with ConnectionResetError for py3
            if e.errno == errno.ECONNRESET:
                logger.info("Client closed connection.")
                print("Error: {}".format(e))
                self.keep_alive = 0
                return
            b1, b2 = 0, 0
        except ValueError as e:
            b1, b2 = 0, 0

        op_code = b1 & OP_CODE
        masked = b2 & MASKED
        payload_length = b2 & PAYLOAD_LEN

        if op_code == OP_CODE_CLOSE_CONN:
            logger.info("Client asked to close connection.")
            self.keep_alive = 0
            return
        if not masked:
            logger.warning("Client must always be masked.")
            self.keep_alive = 0
            return
        if op_code == OP_CODE_CONTINUATION:
            logger.warning("Continuation frames are not supported.")
            return
        elif op_code == OP_CODE_BINARY:
            logger.warning("Binary frames are not supported.")
            return
        elif op_code == OP_CODE_TEXT:
            op_code_handler = self.server._message_received_
        elif op_code == OP_CODE_PING:
            op_code_handler = self.server._ping_received_
        elif op_code == OP_CODE_PONG:
            op_code_handler = self.server._pong_received_
        else:
            logger.warning("Unknown opcode %#x." % op_code)
            self.keep_alive = 0
            return

        if payload_length == 126:
            payload_length = struct.unpack(">H", self.rfile.read(2))[0]
        elif payload_length == 127:
            payload_length = struct.unpack(">Q", self.rfile.read(8))[0]

        masks = self.read_bytes(4)
        message_bytes = bytearray()
        for message_byte in self.read_bytes(payload_length):
            message_byte ^= masks[len(message_bytes) % 4]
            message_bytes.append(message_byte)
        op_code_handler(self, message_bytes.decode('utf8'))

    def send_message(self, message):
        self.send_text(message)

    def send_pong(self, message):
        self.send_text(message, OP_CODE_PONG)

    def send_text(self, message, opcode=OP_CODE_TEXT):
        """
        Important: Fragmented(=continuation) messages are not supported since
        their usage cases are limited - when we don't know the payload length.
        """

        # Validate message
        if isinstance(message, bytes):
            message = self.try_decode_utf8(message)  # this is slower but ensures we have UTF-8
            if not message:
                logger.warning("Can\'t send message, message is not valid UTF-8")
                return False
        elif sys.version_info < (3, 0) and (isinstance(message, str) or isinstance(message, bytes)):
            pass
        elif isinstance(message, str):
            pass
        else:
            logger.warning('Can\'t send message, message has to be a string or bytes. Given type is %s' % type(message))
            return False

        header = bytearray()
        payload = self.encode_to_utf8(message)
        payload_length = len(payload)

        # Normal payload
        if payload_length <= 125:
            header.append(FIN | opcode)
            header.append(payload_length)

        # Extended payload
        elif 126 <= payload_length <= 65535:
            header.append(FIN | opcode)
            header.append(PAYLOAD_LEN_EXT16)
            header.extend(struct.pack(">H", payload_length))

        # Huge extended payload
        elif payload_length < 18446744073709551616:
            header.append(FIN | opcode)
            header.append(PAYLOAD_LEN_EXT64)
            header.extend(struct.pack(">Q", payload_length))

        else:
            raise Exception("Message is too big. Consider breaking it into chunks.")

        self.request.send(header + payload)

    def read_http_headers(self):
        headers = {}
        # first line should be HTTP GET
        http_get = self.rfile.readline().decode().strip()
        assert http_get.upper().startswith('GET')
        # remaining should be headers
        while True:
            header = self.rfile.readline().decode().strip()
            if not header:
                break
            head, value = header.split(':', 1)
            headers[head.lower().strip()] = value.strip()
        return headers

    def handshake(self):
        headers = self.read_http_headers()

        try:
            assert headers['upgrade'].lower() == 'websocket'
        except AssertionError:
            self.keep_alive = False
            return

        try:
            key = headers['sec-websocket-key']
        except KeyError:
            logger.warning("Client tried to connect but was missing a key")
            self.keep_alive = False
            return

        response = self.make_handshake_response(key)
        self.handshake_done = self.request.send(response.encode())
        self.valid_client = True
        self.server._new_client_(self)

    @classmethod
    def make_handshake_response(cls, key):
        return \
            'HTTP/1.1 101 Switching Protocols\r\n' \
            'Upgrade: websocket\r\n' \
            'Connection: Upgrade\r\n' \
            'Sec-WebSocket-Accept: %s\r\n' \
            '\r\n' % cls.calculate_response_key(key)

    @classmethod
    def calculate_response_key(cls, key):
        GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
        hash = sha1(key.encode() + GUID.encode())
        response_key = b64encode(hash.digest()).strip()
        return response_key.decode('ASCII')

    def finish(self):
        self.server._client_left_(self)

    @staticmethod
    def encode_to_utf8(data):
        try:
            return data.encode('UTF-8')
        except UnicodeEncodeError as e:
            logger.error("Could not encode data to UTF-8 -- %s" % e)
            return False
        except Exception as e:
            raise e

    @staticmethod
    def try_decode_utf8(data):
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return False
        except Exception as e:
            raise e
