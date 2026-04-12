## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
##

import unittest
import re
import socket
import functools
import time
import sys
import os
import test_util

def connect(cnames):
    ''' Decorator that creates stomp connections and issues CONNECT '''
    cmd=('CONNECT\n'
        'login:guest\n'
        'passcode:guest\n'
        '\n'
        '\n\0')
    def w(m):
        @functools.wraps(m)
        def wrapper(self, *args, **kwargs):
            for cname in cnames:
                sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sd.settimeout(30000)
                sd.connect((self.host, self.port))
                sd.sendall(cmd.encode('utf-8'))
                data = sd.recv(4096).decode('utf-8')
                self.assert_frame(data, 'CONNECTED', {
                    'version': '1.0',
                    'heart-beat': '0,0',
                })
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

    def parse_frame(self, data):
        ''' Parse a STOMP frame into (command, headers_dict, body) '''
        # Strip trailing LF (server sends trailing_lf=true)
        if data.endswith('\n'):
            data = data[:-1]
        parts = data.split('\n\n', 1)
        header_section = parts[0]
        body = parts[1] if len(parts) > 1 else ''
        # Body ends with NUL
        if body.endswith('\x00'):
            body = body[:-1]
        lines = header_section.split('\n')
        command = lines[0]
        headers = {}
        for line in lines[1:]:
            if ':' in line:
                k, v = line.split(':', 1)
                headers[k] = v
        return command, headers, body

    def assert_frame(self, data, expected_command, expected_headers=None, expected_body=None):
        ''' Assert a STOMP frame matches expected values (header-order independent) '''
        command, headers, body = self.parse_frame(data)
        self.assertEqual(expected_command, command)
        if expected_headers:
            for k, v in expected_headers.items():
                self.assertIn(k, headers, f'Missing header: {k}')
                if v is not None:
                    if isinstance(v, re.Pattern):
                        self.assertRegex(headers[k], v)
                    else:
                        self.assertEqual(v, headers[k])
        if expected_body is not None:
            self.assertEqual(expected_body, body)
        return headers

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

    def recv_frame(self):
        ''' Receive one complete STOMP frame, honoring content-length for binary bodies. '''
        buf = b''
        while b'\n\n' not in buf:
            chunk = self.cd.recv(4096)
            if not chunk:
                return buf.decode('utf-8', errors='replace')
            buf += chunk
        hdr_end = buf.index(b'\n\n')
        header_text = buf[:hdr_end].decode('utf-8')
        content_length = None
        for line in header_text.split('\n')[1:]:
            if line.startswith('content-length:'):
                content_length = int(line.split(':', 1)[1])
                break
        body_start = hdr_end + 2
        if content_length is not None:
            needed = body_start + content_length + 1
            while len(buf) < needed:
                chunk = self.cd.recv(needed - len(buf))
                if not chunk:
                    break
                buf += chunk
        else:
            while b'\x00' not in buf[body_start:]:
                chunk = self.cd.recv(4096)
                if not chunk:
                    break
                buf += chunk
        return buf.decode('utf-8')


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
        data = self.cd.recv(4096).decode('utf-8')
        self.assert_frame(data, 'MESSAGE', {
            'destination': '/exchange/amq.fanout',
            'redelivered': 'false',
            'content-type': 'text/plain',
            'content-length': '6',
        }, 'hello\n')

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
        data = self.cd.recv(4096).decode('utf-8')
        self.assert_frame(data, 'MESSAGE', {
            'destination': '/exchange/amq.fanout',
            'redelivered': 'false',
            'content-length': '6',
        }, 'hello\n')

    @connect(['cd'])
    def test_unicode(self):
        cmd = ('\n'
               'SUBSCRIBE\n'
               'destination:/exchange/amq.fanout\n'
               '\n\x00\n'
               'SEND\n'
               'destination:/exchange/amq.fanout\n'
               'headꙕr1:valꙕe1\n\n'
               'hello\n\x00')
        self.cd.sendall(cmd.encode('utf-8'))
        data = self.cd.recv(4096).decode('utf-8')
        self.assert_frame(data, 'MESSAGE', {
            'destination': '/exchange/amq.fanout',
            'redelivered': 'false',
            'headꙕr1': 'valꙕe1',
            'content-length': '6',
        }, 'hello\n')

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
        data = self.cd.recv(4096).decode('utf-8')
        self.assert_frame(data, 'MESSAGE', {
            'destination': '/exchange/amq.fanout',
            'redelivered': 'false',
            'content-length': str(len(msg)),
        }, msg)

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
        data = self.cd.recv(4096).decode('utf-8')
        self.assert_frame(data, 'MESSAGE', {
            'destination': '/exchange/amq.fanout',
            'redelivered': 'false',
            'content-type': 'text/plain',
            'content-length': '6',
        }, 'hello\n')

    @connect(['cd'])
    def test_bad_command(self):
        ''' Trigger an error message. '''
        cmd = ('WRONGCOMMAND\n'
               'destination:a\n'
               'exchange:amq.fanout\n'
               '\n\0')
        self.cd.sendall(cmd.encode('utf-8'))
        data = self.cd.recv(4096).decode('utf-8')
        self.assert_frame(data, 'ERROR', {
            'message': 'Bad command',
            'content-type': 'text/plain',
            'version': '1.0,1.1,1.2',
        }, 'Could not interpret command "WRONGCOMMAND"\n')

    @connect(['sd', 'cd1', 'cd2'])
    def test_broadcast(self):
        ''' Single message should be delivered to two consumers:
            amq.topic --routing_key--> first_queue --> first_connection
                     \\--routing_key--> second_queue--> second_connection
        '''
        subscribe=( 'SUBSCRIBE\n'
                    'id: XsKNhAf\n'
                    'destination:/exchange/amq.topic/da9d4779\n'
                    '\n\0')
        for cd in [self.cd1, self.cd2]:
            cd.sendall(subscribe.encode('utf-8'))

        bindings_count = 0
        while bindings_count != 2:
            time.sleep(0.1)
            output = test_util.rabbitmqctl_output(['list_bindings'])
            bindings_count = output.split().count('da9d4779')

        cmd = ('SEND\n'
               'content-type:text/plain\n'
               'destination:/exchange/amq.topic/da9d4779\n'
               '\n'
               'message'
               '\n\0')
        self.sd.sendall(cmd.encode('utf-8'))

        for cd in [self.cd1, self.cd2]:
            data = cd.recv(4096).decode('utf-8')
            self.assert_frame(data, 'MESSAGE', {
                'destination': '/topic/da9d4779',
                'redelivered': 'false',
                'content-type': 'text/plain',
                'content-length': '8',
            }, 'message\n')

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

        fullbuf = self.recv_frame()
        self.assertFalse(len(fullbuf) == 0)

        command, headers, body = self.parse_frame(fullbuf)
        self.assertEqual('MESSAGE', command)
        self.assertEqual('/topic/test_embed_nulls_message', headers.get('destination'))
        self.assertEqual('false', headers.get('redelivered'))
        self.assertEqual(str(msg_len), headers.get('content-length'))
        self.assertEqual(message, body,
            "   body (...'%s')\nincorrectly returned as (...'%s')"
            % (message[-10:], body[-10:]))

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

        fullbuf = self.recv_frame()
        self.assertFalse(len(fullbuf) == 0)

        command, headers, body = self.parse_frame(fullbuf)
        self.assertEqual('MESSAGE', command)
        self.assertEqual('false', headers.get('redelivered'))
        self.assertEqual('text/plain', headers.get('content-type'))
        self.assertEqual(str(msg_len), headers.get('content-length'))
        self.assertEqual(message, body,
            "   body ('%s')\nincorrectly returned as ('%s')"
            % (message, body))


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)
