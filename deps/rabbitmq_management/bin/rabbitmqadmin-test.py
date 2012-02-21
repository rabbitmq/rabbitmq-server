#!/usr/bin/env python

import unittest
import os
import os.path
import socket
import subprocess
import sys

# TODO test: SSL, depth, config file, encodings(?), completion(???)

class TestRabbitMQAdmin(unittest.TestCase):
    def test_no_args(self):
        self.run_fail([])

    def test_help(self):
        self.run_success(['--help'])
        self.run_success(['help', 'subcommands'])
        self.run_success(['help', 'config'])
        self.run_fail(['help', 'astronomy'])

    def test_host(self):
        self.run_success(['show', 'overview'])
        self.run_success(['--host', 'localhost', 'show', 'overview'])
        self.run_success(['--host', socket.gethostname(), 'show', 'overview'])
        self.run_fail(['--host', 'some-host-that-does-not-exist', 'show', 'overview'])

    def test_port(self):
        self.run_success(['--port', '55672', 'show', 'overview'])
        self.run_fail(['--port', '55673', 'show', 'overview'])
        self.run_fail(['--port', '5672', 'show', 'overview'])

    def test_user(self):
        self.run_success(['--user', 'guest', '--password', 'guest', 'show', 'overview'])
        self.run_fail(['--user', 'no', '--password', 'guest', 'show', 'overview'])
        self.run_fail(['--user', 'guest', '--password', 'no', 'show', 'overview'])

    def test_fmt_long(self):
        self.assert_output("""
--------------------------------------------------------------------------------

   name: /
tracing: False

--------------------------------------------------------------------------------

""", ['--format', 'long', 'list', 'vhosts'])

    def test_fmt_kvp(self):
        self.assert_output("""name="/" tracing="False"
""", ['--format', 'kvp', 'list', 'vhosts'])

    def test_fmt_tsv(self):
        self.assert_output("""name	tracing
/	False
""", ['--format', 'tsv', 'list', 'vhosts'])

    def test_fmt_table(self):
        out = """+------+---------+
| name | tracing |
+------+---------+
| /    | False   |
+------+---------+
"""
        self.assert_output(out, ['list', 'vhosts'])
        self.assert_output(out, ['--format', 'table', 'list', 'vhosts'])

    def test_fmt_bash(self):
        self.assert_output("""/
""", ['--format', 'bash', 'list', 'vhosts'])

    def test_vhosts(self):
        self.assert_list(['/'], l('vhosts'))
        self.run_success(['declare', 'vhost', 'name=foo'])
        self.assert_list(['/', 'foo'], l('vhosts'))
        self.run_success(['delete', 'vhost', 'name=foo'])
        self.assert_list(['/'], l('vhosts'))

    def test_users(self):
        self.assert_list(['guest'], l('users'))
        self.run_fail(['declare', 'user', 'name=foo'])
        self.run_success(['declare', 'user', 'name=foo', 'password=foo', 'tags='])
        self.assert_list(['foo', 'guest'], l('users'))
        self.run_success(['delete', 'user', 'name=foo'])
        self.assert_list(['guest'], l('users'))

    def test_permissions(self):
        self.run_success(['declare', 'vhost', 'name=foo'])
        self.run_success(['declare', 'user', 'name=bar', 'password=foo', 'tags='])
        self.assert_table([['guest', '/']], ['list', 'permissions', 'user', 'vhost'])
        self.run_success(['declare', 'permission', 'user=bar', 'vhost=foo', 'configure=.*', 'write=.*', 'read=.*'])
        self.assert_table([['guest', '/'], ['bar', 'foo']], ['list', 'permissions', 'user', 'vhost'])
        self.run_success(['delete', 'user', 'name=bar'])
        self.run_success(['delete', 'vhost', 'name=foo'])

    def test_alt_vhost(self):
        self.run_success(['declare', 'vhost', 'name=foo'])
        self.run_success(['declare', 'permission', 'user=guest', 'vhost=foo', 'configure=.*', 'write=.*', 'read=.*'])
        self.run_success(['declare', 'queue', 'name=in_/'])
        self.run_success(['--vhost', 'foo', 'declare', 'queue', 'name=in_foo'])
        self.assert_table([['/', 'in_/'], ['foo', 'in_foo']], ['list', 'queues', 'vhost', 'name'])
        self.run_success(['--vhost', 'foo', 'delete', 'queue', 'name=in_foo'])
        self.run_success(['delete', 'queue', 'name=in_/'])
        self.run_success(['delete', 'vhost', 'name=foo'])

    def test_exchanges(self):
        self.run_success(['declare', 'exchange', 'name=foo', 'type=direct'])
        self.assert_list(['', 'amq.direct', 'amq.fanout', 'amq.headers', 'amq.match', 'amq.rabbitmq.log', 'amq.rabbitmq.trace', 'amq.topic', 'foo'], l('exchanges'))
        self.run_success(['delete', 'exchange', 'name=foo'])

    def test_queues(self):
        self.run_success(['declare', 'queue', 'name=foo'])
        self.assert_list(['foo'], l('queues'))
        self.run_success(['delete', 'queue', 'name=foo'])

    def test_bindings(self):
        self.run_success(['declare', 'queue', 'name=foo'])
        self.run_success(['declare', 'binding', 'source=amq.direct', 'destination=foo', 'destination_type=queue', 'routing_key=test'])
        self.assert_table([['', 'foo', 'queue', 'foo'], ['amq.direct', 'foo', 'queue', 'test']], ['list', 'bindings', 'source', 'destination', 'destination_type', 'routing_key'])
        self.run_success(['delete', 'queue', 'name=foo'])

    def test_publish(self):
        self.run_success(['declare', 'queue', 'name=test'])
        self.run_success(['publish', 'routing_key=test', 'payload=test_1'])
        self.run_success(['publish', 'routing_key=test', 'payload=test_2'])
        self.run_success(['publish', 'routing_key=test'], stdin='test_3')
        self.assert_table([exp_msg('test', 2, False, 'test_1')], ['get', 'queue=test', 'requeue=false'])
        self.assert_table([exp_msg('test', 1, False, 'test_2')], ['get', 'queue=test', 'requeue=true'])
        self.assert_table([exp_msg('test', 1, True,  'test_2')], ['get', 'queue=test', 'requeue=false'])
        self.assert_table([exp_msg('test', 0, False, 'test_3')], ['get', 'queue=test', 'requeue=false'])
        self.run_success(['publish', 'routing_key=test'], stdin='test_4')
        filename = '/tmp/rabbitmq-test/get.txt'
        self.run_success(['get', 'queue=test', 'requeue=false', 'payload_file=' + filename])
        with open(filename) as f:
            self.assertEqual('test_4', f.read())
        os.remove(filename)
        self.run_success(['delete', 'queue', 'name=test'])

    # ---------------------------------------------------------------------------

    def run_success(self, args, **kwargs):
        self.assertEqual(0, run(args, **kwargs)[1])

    def run_fail(self, args):
        self.assertNotEqual(0, run(args)[1])

    def assert_output(self, expected, args):
        self.assertEqual(expected, run(args)[0])

    def assert_list(self, expected, args0):
        args = ['-f', 'tsv', '-q']
        args.extend(args0)
        self.assertEqual(expected, run(args)[0].splitlines())

    def assert_table(self, expected, args0):
        args = ['-f', 'tsv', '-q']
        args.extend(args0)
        self.assertEqual(expected, [l.split('\t') for l in run(args)[0].splitlines()])

def run(args, stdin=None):
    path = os.path.normpath(os.path.join(os.getcwd(), sys.argv[0], '../rabbitmqadmin'))
    cmdline = [path]
    cmdline.extend(args)
    proc = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate(stdin)
    returncode = proc.returncode
    return (stdout, returncode)

def l(thing):
    return ['list', thing, 'name']

def exp_msg(key, count, redelivered, payload):
    # routing_key, exchange, message_count, payload, payload_bytes, payload_encoding, properties, redelivered
    return [key, '', str(count), payload, str(len(payload)), 'string', '', str(redelivered)]

if __name__ == '__main__':
    print "\nrabbitmqadmin tests\n===================\n"
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRabbitMQAdmin)
    unittest.TextTestRunner(verbosity=2).run(suite)
