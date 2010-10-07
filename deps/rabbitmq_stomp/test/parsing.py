'''
Few tests for a rabbitmq-stomp adaptor. They intend to increase code coverage
of the erlang stomp code.
'''
import unittest
import re
import socket
import functools
import time
import sys

def connect(cnames):
    ''' Decorator that creates stomp connections and issues CONNECT '''
    cmd=('CONNECT\n'
        'login:guest\n'
        'passcode:guest\n'
        '\n'
        '\n\0')
    resp = ('CONNECTED\n'
            'session:(.*)\n'
            '\n\x00')
    def w(m):
        @functools.wraps(m)
        def wrapper(self, *args, **kwargs):
            for cname in cnames:
                sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sd.settimeout(3)
                sd.connect((self.host, self.port))
                sd.sendall(cmd)
                self.match(resp, sd.recv(4096))
                setattr(self, cname, sd)
            try:
                r = m(self, *args, **kwargs)
            finally:
                for cname in cnames:
                    try:
                        getattr(self, cname).close()
                    except IOError:
                        pass
            return r
        return wrapper
    return w


class TestParsing(unittest.TestCase):
    host='127.0.0.1'
    port=61613


    def match(self, pattern, data):
        ''' helper: try to match 'pattern' regexp with 'data' string.
            Fail testif they don't match.
        '''
        matched = re.match(pattern, data)
        if matched:
            return matched.groups()
        self.assertTrue(False, 'No match:\n%r\n%r' % (pattern, data) )

    @connect(['cd'])
    def test_newline_after_nul(self):
        self.cd.sendall('\n'
                        'SUBSCRIBE\n'
                        'destination:/exchange/amq.fanout\n'
                        '\n\x00\n'
                        'SEND\n'
                        'destination:/exchange/amq.fanout\n\n'
                        'hello\n\x00\n')
        resp = ('MESSAGE\n'
                'destination:/exchange/amq.fanout\n'
                'message-id:session-(.*)\n'
                'content-type:text/plain\n'
                'content-length:6\n'
                '\n'
                'hello\n\0')
        self.match(resp, self.cd.recv(4096))


    @connect(['cd'])
    def test_newline_after_nul_and_leading_nul(self):
        self.cd.sendall('\n'
                        '\x00SUBSCRIBE\n'
                        'destination:/exchange/amq.fanout\n'
                        '\n\x00\n'
                        '\x00SEND\n'
                        'destination:/exchange/amq.fanout\n'
                        '\nhello\n\x00\n')
        resp = ('MESSAGE\n'
                'destination:/exchange/amq.fanout\n'
                'message-id:session-(.*)\n'
                'content-type:text/plain\n'
                'content-length:6\n'
                '\n'
                'hello\n\0')
        self.match(resp, self.cd.recv(4096))

    @connect(['cd'])
    def test_bad_command(self):
        ''' Trigger an error message. '''
        self.cd.sendall('WRONGCOMMAND\n'
                        'destination:a\n'
                        'exchange:amq.fanout\n'
                        '\n\0')
        resp = ('ERROR\n'
                'message:Bad command\n'
                'content-type:text/plain\n'
                'content-length:41\n'
                '\n'
                'Could not interpret command WRONGCOMMAND\n'
                '\0')
        self.match(resp, self.cd.recv(4096))

    @connect(['sd', 'cd1', 'cd2'])
    def test_broadcast(self):
        ''' Single message should be delivered to two consumers:
            amq.topic --routing_key--> first_queue --> first_connection
                     \--routing_key--> second_queue--> second_connection
        '''
        subscribe=( 'SUBSCRIBE\n'
                    'id: XsKNhAf\n'
                    'destination:/exchange/amq.topic/da9d4779\n'
                    '\n\0')
        for cd in [self.cd1, self.cd2]:
            cd.sendall(subscribe)

        time.sleep(0.1)

        self.sd.sendall('SEND\n'
                        'destination:/exchange/amq.topic/da9d4779\n'
                        '\n'
                        'message'
                        '\n\0')

        resp=('MESSAGE\n'
            'destination:/exchange/amq.topic/da9d4779\n'
            'message-id:(.*)\n'
            'content-type:text/plain\n'
            'subscription:(.*)\n'
            'content-length:8\n'
            '\n'
            'message'
            '\n\x00')
        for cd in [self.cd1, self.cd2]:
            self.match(resp, cd.recv(4096))


    @connect(['cd'])
    def test_huge_message(self):
        ''' Test sending/receiving huge (16MB) message. '''
        subscribe=( 'SUBSCRIBE\n'
                    'id: xxx\n'
                    'destination:/exchange/amq.topic/test_huge_message\n'
                    '\n\0')
        self.cd.sendall(subscribe)

        message = 'x' * 1024*1024*16

        self.cd.sendall('SEND\n'
                        'destination:/exchange/amq.topic/test_huge_message\n'
                        '\n'
                        '%s'
                        '\0' % message)

        resp=('MESSAGE\n'
            'destination:/exchange/amq.topic/test_huge_message\n'
            'message-id:(.*)\n'
            'content-type:text/plain\n'
            'subscription:(.*)\n'
            'content-length:%i\n'
            '\n'
            '%s(.*)'
             % (len(message), message[:8000]) )

        recv = []
        s = 0
        while len(recv) < 1 or recv[-1][-1] != '\0':
            buf =  self.cd.recv(4096*16)
            s += len(buf)
            recv.append( buf )
        buf = ''.join(recv)

        # matching 100MB regexp is way too expensive.
        self.match(resp, buf[:8192])
        self.assertEqual(len(buf) > len(message), True)

