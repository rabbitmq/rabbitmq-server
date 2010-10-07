import os
import socket
import sys
import threading
import uuid

sys.path.append(os.getcwd())

from stomp import utils, backward

class StompServer(threading.Thread):
    def __init__(self, listen_host_and_port, connection_class):
        threading.Thread.__init__(self)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
        self.socket.bind(listen_host_and_port)
        self.socket.listen(1)
        print('Listening for STOMP connections on %s:%s' % listen_host_and_port)
        self.running = True
        self.connections = [ ]
        self.connection_class = connection_class
        
    def notify(self, queue, msg_id):
        for conn in self.connections:
            conn.notify(queue, msg_id)

    def add_connection(self, conn):
        self.connections.append(conn)
        
    def remove_connection(self, conn):
        pos = self.connections.index(conn)
        if pos >= 0:
            del self.connections[pos]
        
    def shutdown(self):
        pass
        
    def run(self):
        try:
            while self.running:
                conn, addr = self.socket.accept()
                conn = self.connection_class(self, conn, addr)
                self.add_connection(conn)
                conn.start()
        finally:
            for conn in self.connections:
                conn.shutdown()
        self.shutdown()
        
        
class StompConnection(threading.Thread):
    def __init__(self, server, conn, addr):
        threading.Thread.__init__(self)
        self.server = server
        self.conn = conn
        self.addr = addr
        self.running = True
        self.id = str(uuid.uuid4())
        
    def send_error(self, msg):
        self.send('ERROR\nmessage: %s\n\n' % msg)
        
    def send(self, msg):
        if not msg.endswith('\x00'):
            msg = msg + '\x00'
        backward.socksend(self.conn, msg)
        
    def run(self):
        try:
            data = []
            while self.running:
                c = self.conn.recv(1)
                if c == '' or len(c) == 0:
                    break
                data.append(c)
                if ord(c) == 0:
                    frame = backward.join(data)
                    print(frame)
                    (frame_type, headers, body) = utils.parse_frame(frame)
                    method = 'handle_%s' % frame_type
                    print('Method = %s' % method)
                    if hasattr(self, method):
                        getattr(self, method)(headers, body)
                    else:
                        self.send_error('invalid command %s' % frame_type)
                    data = []
        except Exception:
            _, e, tb = sys.exc_info()
            print(e)
            import traceback
            traceback.print_tb(tb)
            self.server.remove_connection(self)
        self.shutdown()

    def shutdown(self):
        self.conn.close()
        self.running = False
