#!/usr/bin/env python
#
# Tests for the head_msg_timestamp queue stat.

from datetime import datetime
import json
import os
import subprocess
import sys
from time import clock, mktime, sleep
import unittest

TIMESTAMP1 = mktime(datetime(2010,1,1,12,00,01).timetuple())
TIMESTAMP2 = mktime(datetime(2010,1,1,12,00,02).timetuple())

class RabbitTestCase(unittest.TestCase):
    def setUp(self):
        self.run_success(['declare', 'queue', 'name=test'])
        self.run_success(['declare', 'binding', 'source=amq.direct', 'destination=test', 'destination_type=queue', 'routing_key=test'])

    def tearDown(self):
        self.run_success(['delete', 'queue', 'name=test'])

class RabbitHeadMsgTimestampTestCase(RabbitTestCase):

    def test_no_timestamp_when_queue_is_empty(self):
        self.assert_timestamp('undefined')

    def test_has_timestamp_when_first_msg_is_added(self):
        self.run_success(['publish', 'routing_key=test', 'payload=test_1', 'properties={{"timestamp":{0:.0f}}}'.format(TIMESTAMP1)])
        self.assert_timestamp_changed('undefined', TIMESTAMP1)
        
    def test_no_timestamp_when_last_msg_is_removed(self):
        self.run_success(['publish', 'routing_key=test', 'payload=test_1', 'properties={{"timestamp":{0:.0f}}}'.format(TIMESTAMP1)])
        self.assert_timestamp_changed('undefined', TIMESTAMP1)
        self.run_success(['get', 'queue=test', 'requeue=false'])
        self.assert_timestamp_changed(TIMESTAMP1, 'undefined')

    def test_timestamp_updated_when_msg_is_removed(self):
        self.run_success(['publish', 'routing_key=test', 'payload=test_1', 'properties={{"timestamp":{0:.0f}}}'.format(TIMESTAMP1)])
        self.assert_timestamp_changed('undefined', TIMESTAMP1)
        self.run_success(['publish', 'routing_key=test', 'payload=test_2', 'properties={{"timestamp":{0:.0f}}}'.format(TIMESTAMP2)])
        self.run_success(['get', 'queue=test', 'requeue=false'])
        self.assert_timestamp_changed(TIMESTAMP1, TIMESTAMP2)

    # This is the best we can do to delay ACK via the mgmt interface
    def test_timestamp_not_updated_before_msg_is_acked(self):
        self.run_success(['publish', 'routing_key=test', 'payload=test_1', 'properties={{"timestamp":{0:.0f}}}'.format(TIMESTAMP1)])
        self.assert_timestamp_changed('undefined', TIMESTAMP1)
        self.run_success(['get', 'queue=test', 'requeue=true']) 
        self.assert_timestamp(TIMESTAMP1)

    # ---------------------------------------------------------------------------

    def run_success(self, args, **kwargs):
        (stdout, ret) = self.admin(args, **kwargs)
        if ret != 0:
            self.fail(stdout)

    def assert_timestamp_changed(self, from_expected, to_expected):
        stats_wait_start = clock()
        while ((clock() - stats_wait_start) < 10 and
               self.get_queue_timestamp('test') == from_expected):
            sleep(0.1)
        return self.assert_timestamp(to_expected)

    def assert_timestamp(self, expected):
        self.assertEqual(expected, self.get_queue_timestamp('test'))

    def get_queue_timestamp(self, queue_name):
        for queue_timestamp in self.get_timestamp_stats():
            if queue_timestamp['name'] == 'test':
               return queue_timestamp['head_msg_timestamp']
        return None

    def get_timestamp_stats(self):
        return self.get_stats(['list', 'queues', 'name,head_msg_timestamp'])

    def get_stats(self, args0):
        args = ['-f', 'pretty_json', '-q']
        args.extend(args0)
        return json.loads(self.admin(args)[0])

    def admin(self, args, stdin=None):
        return run('../../../rabbitmq-management/bin/rabbitmqadmin', args, stdin)


def run(cmd, args, stdin):
    path = os.path.normpath(os.path.join(os.getcwd(), cmd))
    cmdline = [path]
    cmdline.extend(args)
    proc = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate(stdin)
    returncode = proc.returncode
    return (stdout + stderr, returncode)

if __name__ == '__main__':
    print "\nrabbit head msg timestamp tests\n===============================\n"
    suite = unittest.TestLoader().loadTestsFromTestCase(RabbitHeadMsgTimestampTestCase)
    results = unittest.TextTestRunner(verbosity=2).run(suite)
    if not results.wasSuccessful():
        sys.exit(1)

