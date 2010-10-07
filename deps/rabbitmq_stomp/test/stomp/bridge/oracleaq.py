#!  /usr/bin/env python

import cx_Oracle
from optparse import OptionParser
import re
import sys
try:
    from SocketServer import ThreadingMixIn, ThreadingTCPServer, BaseRequestHandler
except ImportError:
    from socketserver import ThreadingMixIn, ThreadingTCPServer, BaseRequestHandler
try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

from bridge import StompServer, StompConnection

from stomp import utils, backward

global QUEUE_TABLE
QUEUE_TABLE = 'STOMP_MSG_QUEUE'

SETUP_SQLS = ['''CREATE OR REPLACE PROCEDURE stomp_enq(queue_name in varchar2, msg in varchar2, props in varchar2) AS
  enqueue_options dbms_aq.enqueue_options_t;
  message_properties dbms_aq.message_properties_t;
  message_handle RAW(16);
BEGIN
  message_properties.user_property := sys.anyData.convertVarchar2(props);
  dbms_aq.enqueue(queue_name => queue_name, enqueue_options => enqueue_options, message_properties => message_properties, payload => utl_raw.cast_to_raw(msg), msgid => message_handle);
END;''',
'''CREATE OR REPLACE PROCEDURE stomp_sub(qn in varchar2, subscriber_name in varchar2, notification_address in varchar2) AS
BEGIN
dbms_aqadm.add_subscriber(queue_name => qn, subscriber => sys.aq$_agent(subscriber_name, null, null));
dbms_aq.register(sys.aq$_reg_info_list(sys.aq$_reg_info(qn || ':' || subscriber_name, DBMS_AQ.NAMESPACE_AQ, notification_address, HEXTORAW('FF')) ), 1);
END;''',
'''CREATE OR REPLACE PROCEDURE stomp_unsub(qn in varchar2, subscriber_name in varchar2, notification_address in varchar2) AS
subscriber_count int;
BEGIN
dbms_aq.unregister(sys.aq$_reg_info_list(sys.aq$_reg_info(qn || ':' || subscriber_name, dbms_aq.namespace_aq, notification_address, HEXTORAW('FF')) ), 1);
dbms_aqadm.remove_subscriber(queue_name => qn, subscriber => sys.aq$_agent(subscriber_name, null, null));
select count(*) into subscriber_count from user_queue_subscribers where queue_name = qn;
IF subscriber_count = 0 THEN
    dbms_aqadm.stop_queue(qn);
    dbms_aqadm.drop_queue(qn);
END IF;
END;''',
'''CREATE OR REPLACE FUNCTION getvarchar2(anydata_p in sys.anydata) return varchar2 is
    x number;
    thevarchar2 varchar2(4000);
BEGIN
    IF anydata_p IS NULL THEN
        return '';
    ELSE
        x := anydata_p.getvarchar2(thevarchar2);
        return thevarchar2;
    END IF;
END;''']

DEST_RE = re.compile(r'<destination>"[^"]*"."([^"]*)"</destination>')
MSGID_RE = re.compile(r'<message_id>([^<]*)</message_id>')

class NotificationHandler(BaseHTTPRequestHandler):
    '''
    Handler for message notifications from Oracle
    '''
    def do_POST(self):
        try:
            length = backward.getheader(self.headers, 'Content-Length')
            s = self.rfile.read(int(length))
            s = s.decode('UTF-8')
            queue = DEST_RE.search(s).group(1)
            msg_id = MSGID_RE.search(s).group(1).lstrip().rstrip()
            self.send_response(200)
            self.end_headers()
            self.wfile.write("OK".encode())
            if msg_id not in self.server.msg_ids:
                self.server.notify(queue, msg_id)
                self.server.msg_ids.append(msg_id)
                if len(self.server.msg_ids) > 100:
                    del self.server.msg_ids[0]
        except Exception:
            _, e, tb = sys.exc_info()
            import traceback
            traceback.print_tb(tb)

class NotificationListener(ThreadingMixIn, HTTPServer, threading.Thread):
    def __init__(self, notify, host_and_port):
        HTTPServer.__init__(self, host_and_port, NotificationHandler)
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.notify = notify
        self.msg_ids = [ ]
        print('Listening for Oracle Notifications on %s:%s' % host_and_port)
        
    def serve_forever(self):
        self.stop_serving = False
        while not self.stop_serving:
            self.handle_request()

    def run(self):
        self.serve_forever()
            
    def stop(self):
        self.stop_serving = True

class OracleStompConnection(StompConnection):
    def __init__(self, server, conn, addr):
        StompConnection.__init__(self, server, conn, addr)
        self.dbconn = cx_Oracle.connect('%s/%s@//%s:%s/%s' % (server.username, server.passwd, server.oracle_host_and_port[0], server.oracle_host_and_port[1], server.db))
        print("Connected to Oracle")
        self.client_id = self.__get_client_id()
        print("Client Id %s" % self.client_id)
        self.queues = {}
        self.transactions = {}
        self.semaphore = threading.BoundedSemaphore(1)
        
    def __get_client_id(self):
        cursor = self.dbconn.cursor()
        cursor.execute('SELECT stomp_client_id_seq.nextval FROM dual')
        row = cursor.fetchone()
        return 's%s' % row[0]
        
    def __is_created(self, cursor, destination):
        if destination in self.queues:
            return True
        else:
            cursor.execute('SELECT COUNT(*) FROM user_queues WHERE name = UPPER(:queue)', queue = destination)
            row = cursor.fetchone()
            return row[0] > 0
        
    def __create(self, cursor, destination):
        cursor.callproc('DBMS_AQADM.CREATE_QUEUE', [], { 'queue_name' : destination, 'queue_table' : QUEUE_TABLE })
        cursor.callproc('DBMS_AQADM.START_QUEUE', [], { 'queue_name' : destination })
        
    def __sanitise(self, headers):
        if 'destination' in headers:
            dest = headers['destination'].replace('/', '_')
            if dest.startswith('_'):
                dest = dest[1:]
            headers['destination'] = dest.upper()
            
    def __get_notification_address(self):
        return 'http://%s:%s' % (self.server.notification_host_and_port[0], self.server.notification_host_and_port[1])

    def __commit_or_rollback(self, headers, commit = True):
        self.__sanitise(headers)
        if 'transaction' not in headers:
            self.send_error('Transaction identifier is required')
            return
        transaction_id = headers['transaction']
        if transaction_id not in self.transactions:
            self.send_error('Transaction %s does not exist' % transaction_id)
            return
        else:
            if commit:
                for (method, headers, body) in self.transactions[transaction_id]:
                    getattr(self, method)(headers, body)
            del self.transactions[transaction_id]
            
    def __save(self, command, headers, body):
        transaction_id = headers['transaction']
        if transaction_id not in self.transactions:
            self.send_error('No such transaction %s' % transaction_id)
        else:
            del headers['transaction']
            self.transactions[transaction_id].append((command, headers, body))
        
    def notify(self, queue, msg_id):
        if queue in self.queues.keys():
            self.semaphore.acquire()
            cursor = self.dbconn.cursor()
            try:
                cursor.execute('SELECT user_data, getvarchar2(user_prop) AS user_props FROM %s WHERE msgid = :msgid' % QUEUE_TABLE, msgid = msg_id) 
                row = cursor.fetchone()
                headers = utils.parse_headers(row[1].split('\n'))
                headers['destination'] = self.queues[queue]
                headers['message-id'] = msg_id
                hdr = [ ]
                for key, val in headers.items():
                    hdr.append('%s:%s' % (key, val))
                msg = row[0].read().decode('UTF-8')
                self.send('MESSAGE\n%s\n\n%s' % ('\n'.join(hdr), msg))
            except Exception:
                _, e, tb = sys.exc_info()
                import traceback
                traceback.print_tb(tb)
                print(e)
            finally:
                cursor.close()
                self.semaphore.release()

    def handle_ACK(self, headers, body):
        self.__sanitise(headers)
        if 'transaction' in headers:
            self.__save('handle_ACK', headers, body)
        else:
            # FIXME
            self.send_error('Not currently supported')
            
        
    def handle_BEGIN(self, headers, body):
        if 'transaction' not in headers:
            self.send_error('Transaction identifier is required')
            return
        transaction_id = headers['transaction']
        if transaction_id in self.transactions:
            self.send_error('Transaction %s already started' % transaction_id)
            return
        else:
            self.transactions[transaction_id] = [ ]
            
    def handle_COMMIT(self, headers, body):
        self.__commit_or_rollback(headers)
            
    def handle_ABORT(self, headers, body):
        self.__commit_or_rollback(headers, False)
        
    def handle_CONNECT(self, headers, body):
        self.send('CONNECTED\nsession: %s\n\n' % self.id)
        
    def handle_DISCONNECT(self, headers, body):
        self.shutdown()
    
    def handle_SUBSCRIBE(self, headers, body):
        self.semaphore.acquire()
        cursor = self.dbconn.cursor()
        try:
            orig_qn = headers['destination']
            self.__sanitise(headers)
            if not self.__is_created(cursor, headers['destination']):
                self.__create(cursor, headers['destination'])
            try:
                cursor.callproc('stomp_sub', [headers['destination'], self.client_id, self.__get_notification_address()])
                self.queues[headers['destination']] = orig_qn
            except Exception:
                _, e, _ = sys.exc_info()
                print(e)
        finally:
            cursor.close()
            self.semaphore.release()
        
    def handle_UNSUBSCRIBE(self, headers, body):
        self.semaphore.acquire()
        cursor = self.dbconn.cursor()
        try:
            self.__sanitise(headers)
            try:
                cursor.callproc('stomp_unsub', [headers['destination'], self.client_id, self.__get_notification_address()])
            except:
                pass
            if headers['destination'] in self.queues.keys():
                del self.queues[headers['destination']]
        finally:
            cursor.close()
            self.semaphore.release()
            
    def handle_SEND(self, headers, body):
        self.__sanitise(headers)
        if 'transaction' in headers:
            self.__save('handle_SEND', headers, body)
        else:
            self.semaphore.acquire()
            cursor = self.dbconn.cursor()
            try:
                if not self.__is_created(cursor, headers['destination']):
                    self.__create(cursor, headers['destination'])
                hdr = [ ]
                for key, val in headers.items():
                    hdr.append('%s:%s\n' % (key, val))
                cursor.callproc('stomp_enq', [headers['destination'], body.rstrip(), ''.join(hdr)])
                self.dbconn.commit()
            except Exception:
                _, e, tb = sys.exc_info()
                import traceback
                traceback.print_tb(tb)
                print(e)
            finally:
                cursor.close()
                self.semaphore.release()

    def shutdown(self):
        self.running = False
        self.semaphore.acquire()
        for queue in list(self.queues.keys()):
            self.handle_UNSUBSCRIBE({'destination' : queue}, '')
        self.dbconn.close()
        self.semaphore.release()
        StompConnection.shutdown(self)
        

class OracleStompServer(StompServer):
    def __init__(self, listen_host_and_port, oracle_host_and_port, username, passwd, db, notification_host_and_port):
        StompServer.__init__(self, listen_host_and_port, OracleStompConnection)
        self.oracle_host_and_port = oracle_host_and_port
        self.username = username
        self.passwd = passwd
        self.db = db
        self.notification_host_and_port = notification_host_and_port
        
        #
        # setup
        #
        dbconn = cx_Oracle.connect('%s/%s@//%s:%s/%s' % (username, passwd, oracle_host_and_port[0], oracle_host_and_port[1], db))
        cursor = dbconn.cursor()
        for sql in SETUP_SQLS:
            cursor.execute(sql)
        cursor.execute('SELECT COUNT(*) FROM user_queue_tables WHERE queue_table = :queue', queue = QUEUE_TABLE)
        row = cursor.fetchone()
        if row[0] == 0:
            cursor.callproc('DBMS_AQADM.CREATE_QUEUE_TABLE', [], {'queue_table' : QUEUE_TABLE, 'queue_payload_type' : 'raw', 'multiple_consumers' : True})
        cursor.close()
        dbconn.close()
        
        #
        # Oracle notification listener
        #
        self.listener = NotificationListener(self.notify, self.notification_host_and_port)
        self.listener.start()
        
    def shutdown(self):
        print('OracleStompServer shutdown')

        
def main():
    parser = OptionParser()
    
    parser.add_option('-P', '--port', type = int, dest = 'port', default = 61613,
                      help = 'Port to listen for STOMP connections. Defaults to 61613, if not specified.')
    parser.add_option('-D', '--dbhost', type = 'string', dest = 'db_host', default = None,
                      help = 'Oracle hostname to connect to')
    parser.add_option('-B', '--dbport', type = 'int', dest = 'db_port', default = None,
                      help = 'Oracle port to connect to')
    parser.add_option('-I', '--dbinst', type = 'string', dest = 'db_inst', default = None,
                      help = 'Oracle database instance (for example "xe")')
    parser.add_option('-U', '--user', type = 'string', dest = 'db_user', default = None,
                      help = 'Username for the database connection')
    parser.add_option('-W', '--passwd', type = 'string', dest = 'db_passwd', default = None,
                      help = 'Password for the database connection')
    parser.add_option('-N', '--nhost', type = 'string', dest = 'notification_host',
                      help = 'IP address (i.e. this machine) which is listening for Oracle AQ notifications.')
    parser.add_option('-T', '--nport', type = 'int', dest = 'notification_port',
                      help = 'Port which is listening for Oracle AQ notifications.')
                      
    (options, args) = parser.parse_args()
    
    if not options.db_host:
        parser.error("Database hostname (-D) is required")
        
    if not options.db_port:
        parser.error("Database port (-B) is required")
        
    if not options.db_inst:
        parser.error("Database instance (-I) is required")

    if not options.db_user:
        parser.error("Database user (-U) is required")
        
    if not options.db_passwd:
        parser.error("Database password (-W) is required")
        
    if not options.notification_host:
        parser.error("Notification host or IP (-N) is required")
        
    if not options.notification_port:
        parser.error("Notification port (-T) is required")

    server = OracleStompServer(('', options.port), (options.db_host, options.db_port), options.db_user, options.db_passwd, options.db_inst, (options.notification_host, options.notification_port))
    server.start()


if __name__ == '__main__':
    main()
