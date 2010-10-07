from stomp import ConnectionListener

class TestListener(ConnectionListener):
    def __init__(self):
        self.errors = 0
        self.connections = 0
        self.messages = 0

    def on_error(self, headers, message):
        print('received an error %s' % message)
        self.errors = self.errors + 1

    def on_connecting(self, host_and_port):
        print('connecting %s %s' % host_and_port)
        self.connections = self.connections + 1

    def on_message(self, headers, message):
        print('received a message %s' % message)
        self.messages = self.messages + 1
