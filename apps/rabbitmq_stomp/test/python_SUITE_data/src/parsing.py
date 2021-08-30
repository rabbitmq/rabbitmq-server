## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import unittest
import re
import socket
import functools
import time
import sys
import os

def connect(cnames):
    ''' Decorator that creates stomp connections and issues CONNECT '''
    cmd=('CONNECT\n'
        'login:guest\n'
        'passcode:guest\n'
        '\n'
        '\n\0')
    resp = ('CONNECTED\n'
            'server:RabbitMQ/(.*)\n'
            'session:(.*)\n'
            'heart-beat:0,0\n'
            'version:1.0\n'
            '\n\x00')
    def w(m):
        @functools.wraps(m)
        def wrapper(self, *args, **kwargs):
            for cname in cnames:
                sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sd.settimeout(30000)
                sd.connect((self.host, self.port))
                sd.sendall(cmd.encode('utf-8'))
                self.match(resp, sd.recv(4096).decode('utf-8'))
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
    # The default port is 61613 but it's in the middle of the ephemeral
    # ports range on many operating systems. Therefore, there is a
    # chance this port is already in use. Let's use a port close to the
    # AMQP default port.
    port=int(os.environ["STOMP_PORT"])


    def match(self, pattern, data):
        ''' helper: try to match a regexp with a string.
            Fail test if they do not match.
        '''
        matched = re.match(pattern, data)
        if matched:
            return matched.groups()
        self.assertTrue(False, 'No match:\n{}\n\n{}'.format(pattern, data))

    def recv_atleast(self, bufsize):
        recvhead = []
        rl = bufsize
        while rl > 0:
            buf = self.cd.recv(rl).decode('utf-8')
            bl = len(buf)
            if bl==0: break
            recvhead.append( buf )
            rl -= bl
        return ''.join(recvhead)


    @connect(['cd'])
    def test_newline_after_nul(self):
        cmd = ('\n'
               'SUBSCRIBE\n'
               'destination:/exchange/amq.fanout\n'
               '\n\x00\n'
               'SEND\n'
               'content-type:text/plain\n'
               'destination:/exchange/amq.fanout\n\n'
               'hello\n\x00\n')
        self.cd.sendall(cmd.encode('utf-8'))
        resp = ('MESSAGE\n'
                'destination:/exchange/amq.fanout\n'
                'message-id:Q_/exchange/amq.fanout@@session-(.*)\n'
                'redelivered:false\n'
                'content-type:text/plain\n'
                'content-length:6\n'
                '\n'
                'hello\n\0')
        self.match(resp, self.cd.recv(4096).decode('utf-8'))

    @connect(['cd'])
    def test_send_without_content_type(self):
        cmd = ('\n'
               'SUBSCRIBE\n'
               'destination:/exchange/amq.fanout\n'
               '\n\x00\n'
               'SEND\n'
               'destination:/exchange/amq.fanout\n\n'
               'hello\n\x00')
        self.cd.sendall(cmd.encode('utf-8'))
        resp = ('MESSAGE\n'
                'destination:/exchange/amq.fanout\n'
                'message-id:Q_/exchange/amq.fanout@@session-(.*)\n'
                'redelivered:false\n'
                'content-length:6\n'
                '\n'
                'hello\n\0')
        self.match(resp, self.cd.recv(4096).decode('utf-8'))

    @connect(['cd'])
    def test_send_without_content_type_binary(self):
        msg = 'hello'
        cmd = ('\n'
               'SUBSCRIBE\n'
               'destination:/exchange/amq.fanout\n'
               '\n\x00\n'
               'SEND\n'
               'destination:/exchange/amq.fanout\n' +
               'content-length:{}\n\n'.format(len(msg)) +
               '{}\x00'.format(msg))
        self.cd.sendall(cmd.encode('utf-8'))
        resp = ('MESSAGE\n'
                'destination:/exchange/amq.fanout\n'
                'message-id:Q_/exchange/amq.fanout@@session-(.*)\n'
                'redelivered:false\n' +
                'content-length:{}\n'.format(len(msg)) +
                '\n{}\0'.format(msg))
        self.match(resp, self.cd.recv(4096).decode('utf-8'))

    @connect(['cd'])
    def test_newline_after_nul_and_leading_nul(self):
        cmd = ('\n'
               '\x00SUBSCRIBE\n'
               'destination:/exchange/amq.fanout\n'
               '\n\x00\n'
               '\x00SEND\n'
               'destination:/exchange/amq.fanout\n'
               'content-type:text/plain\n'
               '\nhello\n\x00\n')
        self.cd.sendall(cmd.encode('utf-8'))
        resp = ('MESSAGE\n'
                'destination:/exchange/amq.fanout\n'
                'message-id:Q_/exchange/amq.fanout@@session-(.*)\n'
                'redelivered:false\n'
                'content-type:text/plain\n'
                'content-length:6\n'
                '\n'
                'hello\n\0')
        self.match(resp, self.cd.recv(4096).decode('utf-8'))

    @connect(['cd'])
    def test_bad_command(self):
        ''' Trigger an error message. '''
        cmd = ('WRONGCOMMAND\n'
               'destination:a\n'
               'exchange:amq.fanout\n'
               '\n\0')
        self.cd.sendall(cmd.encode('utf-8'))
        resp = ('ERROR\n'
                'message:Bad command\n'
                'content-type:text/plain\n'
                'version:1.0,1.1,1.2\n'
                'content-length:43\n'
                '\n'
                'Could not interpret command "WRONGCOMMAND"\n'
                '\0')
        self.match(resp, self.cd.recv(4096).decode('utf-8'))

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
            cd.sendall(subscribe.encode('utf-8'))

        time.sleep(0.1)

        cmd = ('SEND\n'
               'content-type:text/plain\n'
               'destination:/exchange/amq.topic/da9d4779\n'
               '\n'
               'message'
               '\n\0')
        self.sd.sendall(cmd.encode('utf-8'))

        resp=('MESSAGE\n'
            'subscription:(.*)\n'
            'destination:/topic/da9d4779\n'
            'message-id:(.*)\n'
            'redelivered:false\n'
            'content-type:text/plain\n'
            'content-length:8\n'
            '\n'
            'message'
            '\n\x00')
        for cd in [self.cd1, self.cd2]:
            self.match(resp, cd.recv(4096).decode('utf-8'))

    @connect(['cd'])
    def test_message_with_embedded_nulls(self):
        ''' Test sending/receiving message with embedded nulls. '''
        dest='destination:/exchange/amq.topic/test_embed_nulls_message\n'
        resp_dest='destination:/topic/test_embed_nulls_message\n'
        subscribe=( 'SUBSCRIBE\n'
                    'id:xxx\n'
                    +dest+
                    '\n\0')
        self.cd.sendall(subscribe.encode('utf-8'))

        boilerplate = '0123456789'*1024 # large enough boilerplate
        message = '01'
        oldi = 2
        for i in [5, 90, 256-1, 384-1, 512, 1024, 1024+256+64+32]:
            message = message + '\0' + boilerplate[oldi+1:i]
            oldi = i
        msg_len = len(message)

        cmd = ('SEND\n'
               +dest+
               'content-type:text/plain\n'
               'content-length:%i\n'
               '\n'
               '%s'
               '\0' % (len(message), message))
        self.cd.sendall(cmd.encode('utf-8'))

        headresp=('MESSAGE\n'            # 8
            'subscription:(.*)\n'        # 14 + subscription
            +resp_dest+                  # 44
            'message-id:(.*)\n'          # 12 + message-id
            'redelivered:false\n'        # 18
            'content-type:text/plain\n'  # 24
            'content-length:%i\n'        # 16 + 4==len('1024')
            '\n'                         # 1
            '(.*)$'                      # prefix of body+null (potentially)
             % len(message) )
        headlen = 8 + 24 + 14 + (3) + 44 + 12 + 18 + (48) + 16 + (4) + 1 + (1)

        headbuf = self.recv_atleast(headlen)
        self.assertFalse(len(headbuf) == 0)

        (sub, msg_id, bodyprefix) = self.match(headresp, headbuf)
        bodyresp=( '%s\0' % message )
        bodylen = len(bodyresp);

        bodybuf = ''.join([bodyprefix,
                           self.recv_atleast(bodylen - len(bodyprefix))])

        self.assertEqual(len(bodybuf), msg_len+1,
            "body received not the same length as message sent")
        self.assertEqual(bodybuf, bodyresp,
            "   body (...'%s')\nincorrectly returned as (...'%s')"
            % (bodyresp[-10:], bodybuf[-10:]))

    @connect(['cd'])
    def test_message_in_packets(self):
        ''' Test sending/receiving message in packets. '''
        base_dest='topic/test_embed_nulls_message\n'
        dest='destination:/exchange/amq.' + base_dest
        resp_dest='destination:/'+ base_dest
        subscribe=( 'SUBSCRIBE\n'
                    'id:xxx\n'
                    +dest+
                    '\n\0')
        self.cd.sendall(subscribe.encode('utf-8'))

        boilerplate = '0123456789'*1024 # large enough boilerplate

        message = boilerplate[:1024 + 512 + 256 + 32]
        msg_len = len(message)

        msg_to_send = ('SEND\n'
                       +dest+
                       'content-type:text/plain\n'
                       '\n'
                       '%s'
                       '\0' % (message) )
        packet_size = 191
        part_index = 0
        msg_to_send_len = len(msg_to_send)
        while part_index < msg_to_send_len:
            part = msg_to_send[part_index:part_index+packet_size]
            time.sleep(0.1)
            self.cd.sendall(part.encode('utf-8'))
            part_index += packet_size

        headresp=('MESSAGE\n'           # 8
            'subscription:(.*)\n'       # 14 + subscription
            +resp_dest+                 # 44
            'message-id:(.*)\n'         # 12 + message-id
            'redelivered:false\n'       # 18
            'content-type:text/plain\n' # 24
            'content-length:%i\n'       # 16 + 4==len('1024')
            '\n'                        # 1
            '(.*)$'                     # prefix of body+null (potentially)
             % len(message) )
        headlen = 8 + 24 + 14 + (3) + 44 + 12 + 18 + (48) + 16 + (4) + 1 + (1)

        headbuf = self.recv_atleast(headlen)
        self.assertFalse(len(headbuf) == 0)

        (sub, msg_id, bodyprefix) = self.match(headresp, headbuf)
        bodyresp=( '%s\0' % message )
        bodylen = len(bodyresp);

        bodybuf = ''.join([bodyprefix,
                           self.recv_atleast(bodylen - len(bodyprefix))])

        self.assertEqual(len(bodybuf), msg_len+1,
            "body received not the same length as message sent")
        self.assertEqual(bodybuf, bodyresp,
            "   body ('%s')\nincorrectly returned as ('%s')"
            % (bodyresp, bodybuf))


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)