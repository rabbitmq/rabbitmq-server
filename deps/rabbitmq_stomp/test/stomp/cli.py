import base64
import os
import sys
import time

from optparse import OptionParser

from connect import Connection
from listener import ConnectionListener, StatsListener
from exception import NotConnectedException
from backward import input_prompt

def sysout(msg, end='\n'):
    sys.stdout.write(str(msg) + end)

def get_commands():
    """
    Return a list of commands available on a \link StompCLI \endlink (the command line interface
    to stomp.py)
    """
    commands = [ ]
    for f in dir(StompCLI):
        if f.startswith('_') or f.startswith('on_') or f == 'c':
            continue
        else:
            commands.append(f)
    return commands


class StompCLI(ConnectionListener):
    """
    A command line interface to the stomp.py client.  See \link stomp::internal::connect::Connection \endlink
    for more information on establishing a connection to a stomp server.
    """
    def __init__(self, host='localhost', port=61613, user='', passcode=''):
        self.conn = Connection([(host, port)], user, passcode)
        self.conn.set_listener('', self)
        self.conn.start()
        self.__commands = get_commands()
        self.transaction_id = None

    def __print_async(self, frame_type, headers, body):
        """
        Utility function to print a message and setup the command prompt
        for the next input
        """
        sysout("\r  \r", end='')
        sysout(frame_type)
        for header_key in headers.keys():
            sysout('%s: %s' % (header_key, headers[header_key]))
        sysout('')
        sysout(body)
        sysout('> ', end='')
        sys.stdout.flush()

    def on_connecting(self, host_and_port):
        """
        \see ConnectionListener::on_connecting
        """
        self.conn.connect(wait=True)

    def on_disconnected(self):
        """
        \see ConnectionListener::on_disconnected
        """
        sysout("lost connection")

    def on_message(self, headers, body):
        """
        \see ConnectionListener::on_message
        
        Special case: if the header 'filename' is present, the content is written out
        as a file
        """
        if 'filename' in headers:
            content = base64.b64decode(body.encode())
            if os.path.exists(headers['filename']):
                fname = '%s.%s' % (headers['filename'], int(time.time()))
            else:
                fname = headers['filename']
            f = open(fname, 'wb')
            f.write(content)
            f.close()
            self.__print_async("MESSAGE", headers, "Saved file: %s" % fname)
        else:
            self.__print_async("MESSAGE", headers, body)

    def on_error(self, headers, body):
        """
        \see ConnectionListener::on_error
        """
        self.__print_async("ERROR", headers, body)

    def on_receipt(self, headers, body):
        """
        \see ConnectionListener::on_receipt
        """
        self.__print_async("RECEIPT", headers, body)

    def on_connected(self, headers, body):
        """
        \see ConnectionListener::on_connected
        """
        self.__print_async("CONNECTED", headers, body)

    def ack(self, args):
        """
        Usage:
            ack <message-id>

        Required Parameters:
            message-id - the id of the message being acknowledged

        Description:
            The command 'ack' is used to acknowledge consumption of a message from a subscription using client
            acknowledgment. When a client has issued a 'subscribe' with the ack flag set to client, any messages
            received from that destination will not be considered to have been consumed (by the server) until
            the message has been acknowledged.
        """
        if len(args) < 2:
            sysout("Expecting: ack <message-id>")
        elif not self.transaction_id:
            self.conn.ack(headers = { 'message-id' : args[1] })
        else:
            self.conn.ack(headers = { 'message-id' : args[1] }, transaction=self.transaction_id)

    def abort(self, args):
        """
        Usage:
            abort

        Description:
            Roll back a transaction in progress.
        """
        if not self.transaction_id:
            sysout("Not currently in a transaction")
        else:
            self.conn.abort(transaction = self.transaction_id)
            self.transaction_id = None

    def begin(self, args):
        """
        Usage:
            begin

        Description:
            Start a transaction. Transactions in this case apply to sending and acknowledging -
            any messages sent or acknowledged during a transaction will be handled atomically based on the
            transaction.
        """
        if self.transaction_id:
            sysout("Currently in a transaction (%s)" % self.transaction_id)
        else:
            self.transaction_id = self.conn.begin()
            sysout('Transaction id: %s' % self.transaction_id)

    def commit(self, args):
        """
        Usage:
            commit

        Description:
            Commit a transaction in progress.
        """
        if not self.transaction_id:
            sysout("Not currently in a transaction")
        else:
            sysout('Committing %s' % self.transaction_id)
            self.conn.commit(transaction=self.transaction_id)
            self.transaction_id = None

    def disconnect(self, args):
        """
        Usage:
            disconnect

        Description:
            Gracefully disconnect from the server.
        """
        try:
            self.conn.disconnect()
        except NotConnectedException:
            pass # ignore if no longer connected

    def send(self, args):
        """
        Usage:
            send <destination> <message>

        Required Parameters:
            destination - where to send the message
            message - the content to send

        Description:
            Sends a message to a destination in the messaging system.
        """
        if len(args) < 3:
            sysout('Expecting: send <destination> <message>')
        elif not self.transaction_id:
            self.conn.send(destination=args[1], message=' '.join(args[2:]))
        else:
            self.conn.send(destination=args[1], message=' '.join(args[2:]), transaction=self.transaction_id)

    def sendreply(self, args):
        """
        Usage:
            sendreply <destination> <correlation-id> <message>

        Required Parameters:
            destination - where to send the message
            correlation-id - the correlating identifier to send with the response
            message - the content to send

        Description:
            Sends a reply message to a destination in the messaging system.
        """
        if len(args) < 4:
            sysout('expecting: sendreply <destination> <correlation-id> <message>')
        else:
            self.conn.send(destination=args[1], message="%s\n" % ' '.join(args[3:]), headers={'correlation-id': args[2]})

    def sendfile(self, args):
        """
        Usage:
            sendfile <destination> <filename>

        Required Parameters:
            destination - where to send the message
            filename - the file to send

        Description:
            Sends a file to a destination in the messaging system.
        """
        if len(args) < 3:
            sysout('Expecting: sendfile <destination> <filename>')
        elif not os.path.exists(args[2]):
            sysout('File %s does not exist' % args[2])
        else:
            s = open(args[2], mode='rb').read()
            msg = base64.b64encode(s).decode()
            if not self.transaction_id:
                self.conn.send(destination=args[1], message=msg, filename=args[2])
            else:
                self.conn.send(destination=args[1], message=msg, filename=args[2], transaction=self.transaction_id)
            
    def subscribe(self, args):
        """
        Usage:
            subscribe <destination> [ack]

        Required Parameters:
            destination - the name to subscribe to

        Optional Parameters:
            ack - how to handle acknowledgements for a message; either automatically (auto) or manually (client)

        Description:
            Register to listen to a given destination. Like send, the subscribe command requires a destination
            header indicating which destination to subscribe to. The ack parameter is optional, and defaults to
            auto.
        """
        if len(args) < 2:
            sysout('Expecting: subscribe <destination> [ack]')
        elif len(args) > 2:
            sysout('Subscribing to "%s" with acknowledge set to "%s"' % (args[1], args[2]))
            self.conn.subscribe(destination=args[1], ack=args[2])
        else:
            sysout('Subscribing to "%s" with auto acknowledge' % args[1])
            self.conn.subscribe(destination=args[1], ack='auto')

    def unsubscribe(self, args):
        """
        Usage:
            unsubscribe <destination>

        Required Parameters:
            destination - the name to unsubscribe from

        Description:
            Remove an existing subscription - so that the client no longer receive messages from that destination.
        """
        if len(args) < 2:
            sysout('Expecting: unsubscribe <destination>')
        else:
            sysout('Unsubscribing from "%s"' % args[1])
            self.conn.unsubscribe(destination=args[1])

    def stats(self, args):
        """
        Usage:
            stats [on|off]
            
        Description:
            Record statistics on messages sent, received, errors, etc. If no argument (on|off) is specified,
            dump the current statistics.
        """
        if len(args) < 2:
            stats = self.conn.get_listener('stats')
            if stats:
                sysout(stats)
            else:
                sysout('No stats available')
        elif args[1] == 'on':
            self.conn.set_listener('stats', StatsListener())
        elif args[1] == 'off':
            self.conn.remove_listener('stats')
        else:
            sysout('Expecting: stats [on|off]')
            
    def run(self, args):
        """
        Usage:
            run <filename>
            
        Description:
            Execute commands in a specified file
        """
        if len(args) == 1:
            sysout("Expecting: run <filename>")
        elif not os.path.exists(args[1]):
            sysout("File %s was not found" % args[1])
        else:
            filecommands = open(args[1]).read().split('\n')
            for x in range(len(filecommands)):
                split = filecommands[x].split()
                if len(split) < 1:
                    continue
                elif split[0] in self.__commands:
                    getattr(self, split[0])(split)
                else:
                    sysout('Unrecognized command "%s" at line %s' % (split[0], x))
                    break

    def help(self, args):
        """
        Usage:
            help [command]

        Description:
            Display info on a specified command, or a list of available commands
        """
        if len(args) == 1:
            sysout('Usage: help <command>, where command is one of the following:')
            sysout('    ')
            for f in self.__commands:
                sysout('%s ' % f, end='')
            sysout('')
            return
        elif not hasattr(self, args[1]):
            sysout('There is no command "%s"' % args[1])
            return

        func = getattr(self, args[1])
        if hasattr(func, '__doc__') and getattr(func, '__doc__') is not None:
            sysout(func.__doc__)
        else:
            sysout('There is no help for command "%s"' % args[1])
    man = help

    def version(self, args):
        sysout('Stomp.py Version %s.%s' % internal.__version__)
    ver = version
    
    def quit(self, args):
        pass
    exit = quit


def main():
    commands = get_commands()
    
    parser = OptionParser()
    
    parser.add_option('-H', '--host', type = 'string', dest = 'host', default = 'localhost',
                      help = 'Hostname or IP to connect to. Defaults to localhost if not specified.')
    parser.add_option('-P', '--port', type = int, dest = 'port', default = 61613,
                      help = 'Port providing stomp protocol connections. Defaults to 61613 if not specified.')
    parser.add_option('-U', '--user', type = 'string', dest = 'user', default = None,
                      help = 'Username for the connection')
    parser.add_option('-W', '--password', type = 'string', dest = 'password', default = None,
                      help = 'Password for the connection')
    parser.add_option('-F', '--file', type = 'string', dest = 'filename',
                      help = 'File containing commands to be executed, instead of prompting from the command prompt.')
                      
    (options, args) = parser.parse_args()

    st = StompCLI(options.host, options.port, options.user, options.password)
    try:
        if not options.filename:
            # If the readline module is available, make command input easier
            try:
                import readline
                def stomp_completer(text, state):
                    for command in commands[state:]:
                        if command.startswith(text):
                            return "%s " % command
                    return None

                readline.parse_and_bind("tab: complete")
                readline.set_completer(stomp_completer)
                readline.set_completer_delims("")
            except ImportError:
                pass # ignore unavailable readline module
            
            while True:
                line = input_prompt("\r> ")
                if not line or line.lstrip().rstrip() == '':
                    continue
                line = line.lstrip().rstrip()
                if line.startswith('quit') or line.startswith('exit') or line.startswith('disconnect'):
                    break
                split = line.split()
                command = split[0]
                if command in commands:
                    getattr(st, command)(split)
                else:
                    sysout('Unrecognized command')
        else:
            st.run(['run', options.filename])
    except EOFError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        st.disconnect(None)



#
# command line testing
#
if __name__ == '__main__':
    main()

