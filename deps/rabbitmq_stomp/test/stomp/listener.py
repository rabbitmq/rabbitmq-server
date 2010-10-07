class ConnectionListener(object):
    """
    This class should be used as a base class for objects registered
    using Connection.set_listener().
    """
    def on_connecting(self, host_and_port):
        """
        Called by the STOMP connection once a TCP/IP connection to the
        STOMP server has been established or re-established. Note that
        at this point, no connection has been established on the STOMP
        protocol level. For this, you need to invoke the "connect"
        method on the connection.

        \param host_and_port a tuple containing the host name and port
        number to which the connection has been established.
        """
        pass

    def on_connected(self, headers, body):
        """
        Called by the STOMP connection when a CONNECTED frame is
        received, that is after a connection has been established or
        re-established.

        \param headers a dictionary containing all headers sent by the
        server as key/value pairs.

        \param body the frame's payload. This is usually empty for
        CONNECTED frames.
        """
        pass

    def on_disconnected(self):
        """
        Called by the STOMP connection when a TCP/IP connection to the
        STOMP server has been lost.  No messages should be sent via
        the connection until it has been reestablished.
        """
        pass

    def on_message(self, headers, body):
        """
        Called by the STOMP connection when a MESSAGE frame is
        received.

        \param headers a dictionary containing all headers sent by the
        server as key/value pairs.

        \param body the frame's payload - the message body.
        """
        pass

    def on_receipt(self, headers, body):
        """
        Called by the STOMP connection when a RECEIPT frame is
        received, sent by the server if requested by the client using
        the 'receipt' header.

        \param headers a dictionary containing all headers sent by the
        server as key/value pairs.

        \param body the frame's payload. This is usually empty for
        RECEIPT frames.
        """
        pass

    def on_error(self, headers, body):
        """
        Called by the STOMP connection when an ERROR frame is
        received.

        \param headers a dictionary containing all headers sent by the
        server as key/value pairs.

        \param body the frame's payload - usually a detailed error
        description.
        """
        pass

    def on_send(self, headers, body):
        """
        Called by the STOMP connection when it is in the process of sending a message
        
        \param headers a dictionary containing the headers that will be sent with this message
        
        \param body the message payload
        """
        pass


class StatsListener(ConnectionListener):
    """
    A connection listener for recording statistics on messages sent and received.
    """
    def __init__(self):
        self.errors = 0
        self.connections = 0
        self.messages_recd = 0
        self.messages_sent = 0

    def on_error(self, headers, message):
        """
        \see ConnectionListener::on_error
        """
        self.errors += 1

    def on_connecting(self, host_and_port):
        """
        \see ConnectionListener::on_connecting
        """
        self.connections += 1

    def on_message(self, headers, message):
        """
        \see ConnectionListener::on_message
        """
        self.messages_recd += 1
        
    def on_send(self, headers, message):
        """
        \see ConnectionListener::on_send
        """
        self.messages_sent += 1
        
    def __str__(self):
        """
        Return a string containing the current statistics (messages sent and received,
        errors, etc)
        """
        return '''Connections: %s
Messages sent: %s
Messages received: %s
Errors: %s''' % (self.connections, self.messages_sent, self.messages_recd, self.errors)
