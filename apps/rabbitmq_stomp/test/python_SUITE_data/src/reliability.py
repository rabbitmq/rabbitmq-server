## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import base
import stomp
import unittest
import time

class TestReliability(base.BaseTest):

    def test_send_and_disconnect(self):
        ''' Test close socket after send does not lose messages '''
        destination = "/queue/reliability"
        pub_conn = self.create_connection()
        try:
            msg = "0" * (128)

            count = 10000

            listener = base.WaitableListener()
            listener.reset(count)
            self.conn.set_listener('', listener)
            self.subscribe_dest(self.conn, destination, None)

            for x in range(0, count):
                pub_conn.send(destination, msg + str(x))
            time.sleep(2.0)
            pub_conn.disconnect()

            if listener.wait(30):
                self.assertEqual(count, len(listener.messages))
            else:
                listener.print_state("Final state of listener:")
                self.fail("Did not receive %s messages in time" % count)
        finally:
            if pub_conn.is_connected():
                pub_conn.disconnect()


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)