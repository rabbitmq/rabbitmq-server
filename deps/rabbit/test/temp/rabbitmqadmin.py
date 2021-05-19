#!/usr/bin/env python

## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

import sys
if sys.version_info[0] < 2 or sys.version_info[1] < 6:
    print "Sorry, rabbitmqadmin requires at least Python 2.6."
    sys.exit(1)

from ConfigParser import ConfigParser, NoSectionError
from optparse import OptionParser, TitledHelpFormatter
import httplib
import urllib
import urlparse
import base64
import json
import os
import socket

VERSION = '0.0.0'

LISTABLE = {'connections': {'vhost': False},
            'channels':    {'vhost': False},
            'consumers':   {'vhost': True},
            'exchanges':   {'vhost': True},
            'queues':      {'vhost': True},
            'bindings':    {'vhost': True},
            'users':       {'vhost': False},
            'vhosts':      {'vhost': False},
            'permissions': {'vhost': False},
            'nodes':       {'vhost': False},
            'parameters':  {'vhost': False,
                            'json':  ['value']},
            'policies':    {'vhost': False,
                            'json':  ['definition']}}

SHOWABLE = {'overview': {'vhost': False}}

PROMOTE_COLUMNS = ['vhost', 'name', 'type',
                   'source', 'destination', 'destination_type', 'routing_key']

URIS = {
    'exchange':   '/exchanges/{vhost}/{name}',
    'queue':      '/queues/{vhost}/{name}',
    'binding':    '/bindings/{vhost}/e/{source}/{destination_char}/{destination}',
    'binding_del':'/bindings/{vhost}/e/{source}/{destination_char}/{destination}/{properties_key}',
    'vhost':      '/vhosts/{name}',
    'user':       '/users/{name}',
    'permission': '/permissions/{vhost}/{user}',
    'parameter':  '/parameters/{component}/{vhost}/{name}',
    'policy':     '/policies/{vhost}/{name}'
    }

DECLARABLE = {
    'exchange':   {'mandatory': ['name', 'type'],
                   'json':      ['arguments'],
                   'optional':  {'auto_delete': 'false', 'durable': 'true',
                                 'internal': 'false', 'arguments': {}}},
    'queue':      {'mandatory': ['name'],
                   'json':      ['arguments'],
                   'optional':  {'auto_delete': 'false', 'durable': 'true',
                                 'arguments': {}, 'node': None}},
    'binding':    {'mandatory': ['source', 'destination'],
                   'json':      ['arguments'],
                   'optional':  {'destination_type': 'queue',
                                 'routing_key': '', 'arguments': {}}},
    'vhost':      {'mandatory': ['name'],
                   'optional':  {'tracing': None}},
    'user':       {'mandatory': ['name', 'password', 'tags'],
                   'optional':  {}},
    'permission': {'mandatory': ['vhost', 'user', 'configure', 'write', 'read'],
                   'optional':  {}},
    'parameter':  {'mandatory': ['component', 'name', 'value'],
                   'json':      ['value'],
                   'optional':  {}},
    # Priority is 'json' to convert to int
    'policy':     {'mandatory': ['name', 'pattern', 'definition'],
                   'json':      ['definition', 'priority'],
                   'optional':  {'priority' : 0, 'apply-to': None}}
    }

DELETABLE = {
    'exchange':   {'mandatory': ['name']},
    'queue':      {'mandatory': ['name']},
    'binding':    {'mandatory': ['source', 'destination_type', 'destination',
                                 'properties_key']},
    'vhost':      {'mandatory': ['name']},
    'user':       {'mandatory': ['name']},
    'permission': {'mandatory': ['vhost', 'user']},
    'parameter':  {'mandatory': ['component', 'name']},
    'policy':     {'mandatory': ['name']}
    }

CLOSABLE = {
    'connection': {'mandatory': ['name'],
                   'optional':  {},
                   'uri':       '/connections/{name}'}
    }

PURGABLE = {
    'queue': {'mandatory': ['name'],
              'optional':  {},
              'uri':       '/queues/{vhost}/{name}/contents'}
    }

EXTRA_VERBS = {
    'publish': {'mandatory': ['routing_key'],
                'optional':  {'payload': None,
                              'exchange': 'amq.default',
                              'payload_encoding': 'string'},
                'uri':       '/exchanges/{vhost}/{exchange}/publish'},
    'get':     {'mandatory': ['queue'],
                'optional':  {'count': '1', 'requeue': 'true',
                              'payload_file': None, 'encoding': 'auto'},
                'uri':       '/queues/{vhost}/{queue}/get'}
}

for k in DECLARABLE:
    DECLARABLE[k]['uri'] = URIS[k]

for k in DELETABLE:
    DELETABLE[k]['uri'] = URIS[k]
    DELETABLE[k]['optional'] = {}
DELETABLE['binding']['uri'] = URIS['binding_del']

def short_usage():
    return "rabbitmqadmin [options] subcommand"

def title(name):
    return "\n%s\n%s\n\n" % (name, '=' * len(name))

def subcommands_usage():
    usage = """Usage
=====
  """ + short_usage() + """

  where subcommand is one of:
""" + title("Display")

    for l in LISTABLE:
        usage += "  list {0} [<column>...]\n".format(l)
    for s in SHOWABLE:
        usage += "  show {0} [<column>...]\n".format(s)
    usage += title("Object Manipulation")
    usage += fmt_usage_stanza(DECLARABLE,  'declare')
    usage += fmt_usage_stanza(DELETABLE,   'delete')
    usage += fmt_usage_stanza(CLOSABLE,    'close')
    usage += fmt_usage_stanza(PURGABLE,    'purge')
    usage += title("Broker Definitions")
    usage += """  export <file>
  import <file>
"""
    usage += title("Publishing and Consuming")
    usage += fmt_usage_stanza(EXTRA_VERBS, '')
    usage += """
  * If payload is not specified on publish, standard input is used

  * If payload_file is not specified on get, the payload will be shown on
    standard output along with the message metadata

  * If payload_file is specified on get, count must not be set
"""
    return usage

def config_usage():
    usage = "Usage\n=====\n" + short_usage()
    usage += "\n" + title("Configuration File")
    usage += """  It is possible to specify a configuration file from the command line.
  Hosts can be configured easily in a configuration file and called
  from the command line.
"""
    usage += title("Example")
    usage += """  # rabbitmqadmin.conf.example START

  [host_normal]
  hostname = localhost
  port = 15672
  username = guest
  password = guest
  declare_vhost = / # Used as default for declare / delete only
  vhost = /         # Used as default for declare / delete / list

  [host_ssl]
  hostname = otherhost
  port = 15672
  username = guest
  password = guest
  ssl = True
  ssl_key_file = /path/to/key.pem
  ssl_cert_file = /path/to/cert.pem

  # rabbitmqadmin.conf.example END
"""
    usage += title("Use")
    usage += """  rabbitmqadmin -c rabbitmqadmin.conf.example -N host_normal ..."""
    return usage

def more_help():
    return """
More Help
=========

For more help use the help subcommand:

  rabbitmqadmin help subcommands  # For a list of available subcommands
  rabbitmqadmin help config       # For help with the configuration file
"""

def fmt_usage_stanza(root, verb):
    def fmt_args(args):
        res = " ".join(["{0}=...".format(a) for a in args['mandatory']])
        opts = " ".join("{0}=...".format(o) for o in args['optional'].keys())
        if opts != "":
            res += " [{0}]".format(opts)
        return res

    text = ""
    if verb != "":
        verb = " " + verb
    for k in root.keys():
        text += " {0} {1} {2}\n".format(verb, k, fmt_args(root[k]))
    return text

default_options = { "hostname"        : "localhost",
                    "port"            : "15672",
                    "declare_vhost"   : "/",
                    "username"        : "guest",
                    "password"        : "guest",
                    "ssl"             : False,
                    "verbose"         : True,
                    "format"          : "table",
                    "depth"           : 1,
                    "bash_completion" : False }


class MyFormatter(TitledHelpFormatter):
    def format_epilog(self, epilog):
        return epilog

parser = OptionParser(usage=short_usage(),
                      formatter=MyFormatter(),
                      epilog=more_help())

def make_parser():
    def add(*args, **kwargs):
        key = kwargs['dest']
        if key in default_options:
            default = " [default: %s]" % default_options[key]
            kwargs['help'] = kwargs['help'] + default
        parser.add_option(*args, **kwargs)

    add("-c", "--config", dest="config",
        help="configuration file [default: ~/.rabbitmqadmin.conf]",
        metavar="CONFIG")
    add("-N", "--node", dest="node",
        help="node described in the configuration file [default: 'default'" + \
             " only if configuration file is specified]",
        metavar="NODE")
    add("-H", "--host", dest="hostname",
        help="connect to host HOST" ,
        metavar="HOST")
    add("-P", "--port", dest="port",
        help="connect to port PORT",
        metavar="PORT")
    add("-V", "--vhost", dest="vhost",
        help="connect to vhost VHOST [default: all vhosts for list, '/' for declare]",
        metavar="VHOST")
    add("-u", "--username", dest="username",
        help="connect using username USERNAME",
        metavar="USERNAME")
    add("-p", "--password", dest="password",
        help="connect using password PASSWORD",
        metavar="PASSWORD")
    add("-q", "--quiet", action="store_false", dest="verbose",
        help="suppress status messages")
    add("-s", "--ssl", action="store_true", dest="ssl",
        help="connect with ssl")
    add("--ssl-key-file", dest="ssl_key_file",
        help="PEM format key file for SSL")
    add("--ssl-cert-file", dest="ssl_cert_file",
        help="PEM format certificate file for SSL")
    add("-f", "--format", dest="format",
        help="format for listing commands - one of [" + ", ".join(FORMATS.keys())  + "]")
    add("-S", "--sort", dest="sort", help="sort key for listing queries")
    add("-R", "--sort-reverse", action="store_true", dest="sort_reverse",
        help="reverse the sort order")
    add("-d", "--depth", dest="depth",
        help="maximum depth to recurse for listing tables")
    add("--bash-completion", action="store_true",
        dest="bash_completion",
        help="Print bash completion script")
    add("--version", action="store_true",
        dest="version",
        help="Display version and exit")

def default_config():
    home = os.getenv('USERPROFILE') or os.getenv('HOME')
    if home is not None:
        config_file = home + os.sep + ".rabbitmqadmin.conf"
        if os.path.isfile(config_file):
            return config_file
    return None

def make_configuration():
    make_parser()
    (options, args) = parser.parse_args()
    setattr(options, "declare_vhost", None)
    if options.version:
        print_version()
    if options.config is None:
        config_file = default_config()
        if config_file is not None:
            setattr(options, "config", config_file)
    else:
        if not os.path.isfile(options.config):
            assert_usage(False,
                "Could not read config file '%s'" % options.config)

    if options.node is None and options.config:
        options.node = "default"
    else:
        options.node = options.node
    for (key, val) in default_options.items():
        if getattr(options, key) is None:
            setattr(options, key, val)

    if options.config is not None:
        config = ConfigParser()
        try:
            config.read(options.config)
            new_conf = dict(config.items(options.node))
        except NoSectionError, error:
            if options.node == "default":
                pass
            else:
                assert_usage(False, ("Could not read section '%s' in config file"  +
                             " '%s':\n   %s") %
                             (options.node, options.config, error))
        else:
            for key, val in new_conf.items():
                setattr(options, key, val)

    return (options, args)

def assert_usage(expr, error):
    if not expr:
        output("\nERROR: {0}\n".format(error))
        output("{0} --help for help\n".format(os.path.basename(sys.argv[0])))
        sys.exit(1)

def print_version():
    output("rabbitmqadmin {0}".format(VERSION))
    sys.exit(0)

def column_sort_key(col):
    if col in PROMOTE_COLUMNS:
        return (1, PROMOTE_COLUMNS.index(col))
    else:
        return (2, col)

def main():
    (options, args) = make_configuration()
    if options.bash_completion:
        print_bash_completion()
        exit(0)
    assert_usage(len(args) > 0, 'Action not specified')
    mgmt = Management(options, args[1:])
    mode = "invoke_" + args[0]
    assert_usage(hasattr(mgmt, mode),
                 'Action {0} not understood'.format(args[0]))
    method = getattr(mgmt, "invoke_%s" % args[0])
    method()

def output(s):
    print maybe_utf8(s, sys.stdout)

def die(s):
    sys.stderr.write(maybe_utf8("*** {0}\n".format(s), sys.stderr))
    exit(1)

def maybe_utf8(s, stream):
    if stream.isatty():
        # It will have an encoding, which Python will respect
        return s
    else:
        # It won't have an encoding, and Python will pick ASCII by default
        return s.encode('utf-8')

class Management:
    def __init__(self, options, args):
        self.options = options
        self.args = args

    def get(self, path):
        return self.http("GET", "/api%s" % path, "")

    def put(self, path, body):
        return self.http("PUT", "/api%s" % path, body)

    def post(self, path, body):
        return self.http("POST", "/api%s" % path, body)

    def delete(self, path):
        return self.http("DELETE", "/api%s" % path, "")

    def http(self, method, path, body):
        if self.options.ssl:
            conn = httplib.HTTPSConnection(self.options.hostname,
                                           self.options.port,
                                           self.options.ssl_key_file,
                                           self.options.ssl_cert_file)
        else:
            conn = httplib.HTTPConnection(self.options.hostname,
                                          self.options.port)
        headers = {"Authorization":
                       "Basic " + base64.b64encode(self.options.username + ":" +
                                                   self.options.password)}
        if body != "":
            headers["Content-Type"] = "application/json"
        try:
            conn.request(method, path, body, headers)
        except socket.error, e:
            die("Could not connect: {0}".format(e))
        resp = conn.getresponse()
        if resp.status == 400:
            die(json.loads(resp.read())['reason'])
        if resp.status == 401:
            die("Access refused: {0}".format(path))
        if resp.status == 404:
            die("Not found: {0}".format(path))
        if resp.status == 301:
            url = urlparse.urlparse(resp.getheader('location'))
            [host, port] = url.netloc.split(':')
            self.options.hostname = host
            self.options.port = int(port)
            return self.http(method, url.path + '?' + url.query, body)
        if resp.status < 200 or resp.status > 400:
            raise Exception("Received %d %s for path %s\n%s"
                            % (resp.status, resp.reason, path, resp.read()))
        return resp.read()

    def verbose(self, string):
        if self.options.verbose:
            output(string)

    def get_arg(self):
        assert_usage(len(self.args) == 1, 'Exactly one argument required')
        return self.args[0]

    def invoke_help(self):
        if len(self.args) == 0:
            parser.print_help()
        else:
            help_cmd = self.get_arg()
            if help_cmd == 'subcommands':
                usage = subcommands_usage()
            elif help_cmd == 'config':
                usage = config_usage()
            else:
                assert_usage(False, """help topic must be one of:
  subcommands
  config""")
            print usage
        exit(0)

    def invoke_publish(self):
        (uri, upload) = self.parse_args(self.args, EXTRA_VERBS['publish'])
        upload['properties'] = {} # TODO do we care here?
        if not 'payload' in upload:
            data = sys.stdin.read()
            upload['payload'] = base64.b64encode(data)
            upload['payload_encoding'] = 'base64'
        resp = json.loads(self.post(uri, json.dumps(upload)))
        if resp['routed']:
            self.verbose("Message published")
        else:
            self.verbose("Message published but NOT routed")

    def invoke_get(self):
        (uri, upload) = self.parse_args(self.args, EXTRA_VERBS['get'])
        payload_file = 'payload_file' in upload and upload['payload_file'] or None
        assert_usage(not payload_file or upload['count'] == '1',
                     'Cannot get multiple messages using payload_file')
        result = self.post(uri, json.dumps(upload))
        if payload_file:
            write_payload_file(payload_file, result)
            columns = ['routing_key', 'exchange', 'message_count',
                       'payload_bytes', 'redelivered']
            format_list(result, columns, {}, self.options)
        else:
            format_list(result, [], {}, self.options)

    def invoke_export(self):
        path = self.get_arg()
        definitions = self.get("/definitions")
        f = open(path, 'w')
        f.write(definitions)
        f.close()
        self.verbose("Exported definitions for %s to \"%s\""
                     % (self.options.hostname, path))

    def invoke_import(self):
        path = self.get_arg()
        f = open(path, 'r')
        definitions = f.read()
        f.close()
        self.post("/definitions", definitions)
        self.verbose("Imported definitions for %s from \"%s\""
                     % (self.options.hostname, path))

    def invoke_list(self):
        cols = self.args[1:]
        (uri, obj_info) = self.list_show_uri(LISTABLE, 'list', cols)
        format_list(self.get(uri), cols, obj_info, self.options)

    def invoke_show(self):
        cols = self.args[1:]
        (uri, obj_info) = self.list_show_uri(SHOWABLE, 'show', cols)
        format_list('[{0}]'.format(self.get(uri)), cols, obj_info, self.options)

    def list_show_uri(self, obj_types, verb, cols):
        obj_type = self.args[0]
        assert_usage(obj_type in obj_types,
                     "Don't know how to {0} {1}".format(verb, obj_type))
        obj_info = obj_types[obj_type]
        uri = "/%s" % obj_type
        query = []
        if obj_info['vhost'] and self.options.vhost:
            uri += "/%s" % urllib.quote_plus(self.options.vhost)
        if cols != []:
            query.append("columns=" + ",".join(cols))
        sort = self.options.sort
        if sort:
            query.append("sort=" + sort)
        if self.options.sort_reverse:
            query.append("sort_reverse=true")
        query = "&".join(query)
        if query != "":
            uri += "?" + query
        return (uri, obj_info)

    def invoke_declare(self):
        (obj_type, uri, upload) = self.declare_delete_parse(DECLARABLE)
        if obj_type == 'binding':
            self.post(uri, json.dumps(upload))
        else:
            self.put(uri, json.dumps(upload))
        self.verbose("{0} declared".format(obj_type))

    def invoke_delete(self):
        (obj_type, uri, upload) = self.declare_delete_parse(DELETABLE)
        self.delete(uri)
        self.verbose("{0} deleted".format(obj_type))

    def invoke_close(self):
        (obj_type, uri, upload) = self.declare_delete_parse(CLOSABLE)
        self.delete(uri)
        self.verbose("{0} closed".format(obj_type))

    def invoke_purge(self):
        (obj_type, uri, upload) = self.declare_delete_parse(PURGABLE)
        self.delete(uri)
        self.verbose("{0} purged".format(obj_type))

    def declare_delete_parse(self, root):
        assert_usage(len(self.args) > 0, 'Type not specified')
        obj_type = self.args[0]
        assert_usage(obj_type in root,
                     'Type {0} not recognised'.format(obj_type))
        obj = root[obj_type]
        (uri, upload) = self.parse_args(self.args[1:], obj)
        return (obj_type, uri, upload)

    def parse_args(self, args, obj):
        mandatory =  obj['mandatory']
        optional = obj['optional']
        uri_template = obj['uri']
        upload = {}
        for k in optional.keys():
            if optional[k]:
                upload[k] = optional[k]
        for arg in args:
            assert_usage("=" in arg,
                         'Argument "{0}" not in format name=value'.format(arg))
            (name, value) = arg.split("=", 1)
            assert_usage(name in mandatory or name in optional.keys(),
                         'Argument "{0}" not recognised'.format(name))
            if 'json' in obj and name in obj['json']:
                upload[name] = self.parse_json(value)
            else:
                upload[name] = value
        for m in mandatory:
            assert_usage(m in upload.keys(),
                         'mandatory argument "{0}" required'.format(m))
        if 'vhost' not in mandatory:
            upload['vhost'] = self.options.vhost or self.options.declare_vhost
        uri_args = {}
        for k in upload:
            v = upload[k]
            if v and isinstance(v, basestring):
                uri_args[k] = urllib.quote_plus(v)
                if k == 'destination_type':
                    uri_args['destination_char'] = v[0]
        uri = uri_template.format(**uri_args)
        return (uri, upload)

    def parse_json(self, text):
        try:
            return json.loads(text)
        except ValueError:
            print "Could not parse JSON:\n  {0}".format(text)
            sys.exit(1)

def format_list(json_list, columns, args, options):
    format = options.format
    formatter = None
    if format == "raw_json":
        output(json_list)
        return
    elif format == "pretty_json":
        enc = json.JSONEncoder(False, False, True, True, True, 2)
        output(enc.encode(json.loads(json_list)))
        return
    else:
        formatter = FORMATS[format]
    assert_usage(formatter != None,
                 "Format {0} not recognised".format(format))
    formatter_instance = formatter(columns, args, options)
    formatter_instance.display(json_list)

class Lister:
    def verbose(self, string):
        if self.options.verbose:
            output(string)

    def display(self, json_list):
        depth = sys.maxint
        if len(self.columns) == 0:
            depth = int(self.options.depth)
        (columns, table) = self.list_to_table(json.loads(json_list), depth)
        if len(table) > 0:
            self.display_list(columns, table)
        else:
            self.verbose("No items")

    def list_to_table(self, items, max_depth):
        columns = {}
        column_ix = {}
        row = None
        table = []

        def add(prefix, depth, item, fun):
            for key in item:
                column = prefix == '' and key or (prefix + '.' + key)
                subitem = item[key]
                if type(subitem) == dict:
                    if self.obj_info.has_key('json') and key in self.obj_info['json']:
                        fun(column, json.dumps(subitem))
                    else:
                        if depth < max_depth:
                            add(column, depth + 1, subitem, fun)
                elif type(subitem) == list:
                    # The first branch has mirror nodes in queues in
                    # mind (which come out looking decent); the second
                    # one has applications in nodes (which look less
                    # so, but what would look good?).
                    if [x for x in subitem if type(x) != unicode] == []:
                        serialised = " ".join(subitem)
                    else:
                        serialised = json.dumps(subitem)
                    fun(column, serialised)
                else:
                    fun(column, subitem)

        def add_to_columns(col, val):
            columns[col] = True

        def add_to_row(col, val):
            if col in column_ix:
                row[column_ix[col]] = unicode(val)

        if len(self.columns) == 0:
            for item in items:
                add('', 1, item, add_to_columns)
            columns = columns.keys()
            columns.sort(key=column_sort_key)
        else:
            columns = self.columns

        for i in xrange(0, len(columns)):
            column_ix[columns[i]] = i
        for item in items:
            row = len(columns) * ['']
            add('', 1, item, add_to_row)
            table.append(row)

        return (columns, table)

class TSVList(Lister):
    def __init__(self, columns, obj_info, options):
        self.columns = columns
        self.obj_info = obj_info
        self.options = options

    def display_list(self, columns, table):
        head = "\t".join(columns)
        self.verbose(head)

        for row in table:
            line = "\t".join(row)
            output(line)

class LongList(Lister):
    def __init__(self, columns, obj_info, options):
        self.columns = columns
        self.obj_info = obj_info
        self.options = options

    def display_list(self, columns, table):
        sep = "\n" + "-" * 80 + "\n"
        max_width = 0
        for col in columns:
            max_width = max(max_width, len(col))
        fmt = "{0:>" + unicode(max_width) + "}: {1}"
        output(sep)
        for i in xrange(0, len(table)):
            for j in xrange(0, len(columns)):
                output(fmt.format(columns[j], table[i][j]))
            output(sep)

class TableList(Lister):
    def __init__(self, columns, obj_info, options):
        self.columns = columns
        self.obj_info = obj_info
        self.options = options

    def display_list(self, columns, table):
        total = [columns]
        total.extend(table)
        self.ascii_table(total)

    def ascii_table(self, rows):
        table = ""
        col_widths = [0] * len(rows[0])
        for i in xrange(0, len(rows[0])):
            for j in xrange(0, len(rows)):
                col_widths[i] = max(col_widths[i], len(rows[j][i]))
        self.ascii_bar(col_widths)
        self.ascii_row(col_widths, rows[0], "^")
        self.ascii_bar(col_widths)
        for row in rows[1:]:
            self.ascii_row(col_widths, row, "<")
        self.ascii_bar(col_widths)

    def ascii_row(self, col_widths, row, align):
        txt = "|"
        for i in xrange(0, len(col_widths)):
            fmt = " {0:" + align + unicode(col_widths[i]) + "} "
            txt += fmt.format(row[i]) + "|"
        output(txt)

    def ascii_bar(self, col_widths):
        txt = "+"
        for w in col_widths:
            txt += ("-" * (w + 2)) + "+"
        output(txt)

class KeyValueList(Lister):
    def __init__(self, columns, obj_info, options):
        self.columns = columns
        self.obj_info = obj_info
        self.options = options

    def display_list(self, columns, table):
        for i in xrange(0, len(table)):
            row = []
            for j in xrange(0, len(columns)):
                row.append("{0}=\"{1}\"".format(columns[j], table[i][j]))
            output(" ".join(row))

# TODO handle spaces etc in completable names
class BashList(Lister):
    def __init__(self, columns, obj_info, options):
        self.columns = columns
        self.obj_info = obj_info
        self.options = options

    def display_list(self, columns, table):
        ix = None
        for i in xrange(0, len(columns)):
            if columns[i] == 'name':
                ix = i
        if ix is not None:
            res = []
            for row in table:
                res.append(row[ix])
            output(" ".join(res))

FORMATS = {
    'raw_json'    : None, # Special cased
    'pretty_json' : None, # Ditto
    'tsv'         : TSVList,
    'long'        : LongList,
    'table'       : TableList,
    'kvp'         : KeyValueList,
    'bash'        : BashList
}

def write_payload_file(payload_file, json_list):
    result = json.loads(json_list)[0]
    payload = result['payload']
    payload_encoding = result['payload_encoding']
    f = open(payload_file, 'w')
    if payload_encoding == 'base64':
        data = base64.b64decode(payload)
    else:
        data = payload
    f.write(data)
    f.close()

def print_bash_completion():
    script = """# This is a bash completion script for rabbitmqadmin.
# Redirect it to a file, then source it or copy it to /etc/bash_completion.d
# to get tab completion. rabbitmqadmin must be on your PATH for this to work.
_rabbitmqadmin()
{
    local cur prev opts base
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    opts="list show declare delete close purge import export get publish help"
    fargs="--help --host --port --vhost --username --password --format --depth --sort --sort-reverse"

    case "${prev}" in
	list)
	    COMPREPLY=( $(compgen -W '""" + " ".join(LISTABLE) + """' -- ${cur}) )
            return 0
            ;;
	show)
	    COMPREPLY=( $(compgen -W '""" + " ".join(SHOWABLE) + """' -- ${cur}) )
            return 0
            ;;
	declare)
	    COMPREPLY=( $(compgen -W '""" + " ".join(DECLARABLE.keys()) + """' -- ${cur}) )
            return 0
            ;;
	delete)
	    COMPREPLY=( $(compgen -W '""" + " ".join(DELETABLE.keys()) + """' -- ${cur}) )
            return 0
            ;;
	close)
	    COMPREPLY=( $(compgen -W '""" + " ".join(CLOSABLE.keys()) + """' -- ${cur}) )
            return 0
            ;;
	purge)
	    COMPREPLY=( $(compgen -W '""" + " ".join(PURGABLE.keys()) + """' -- ${cur}) )
            return 0
            ;;
	export)
	    COMPREPLY=( $(compgen -f ${cur}) )
            return 0
            ;;
	import)
	    COMPREPLY=( $(compgen -f ${cur}) )
            return 0
            ;;
	help)
            opts="subcommands config"
	    COMPREPLY=( $(compgen -W "${opts}"  -- ${cur}) )
            return 0
            ;;
	-H)
	    COMPREPLY=( $(compgen -A hostname ${cur}) )
            return 0
            ;;
	--host)
	    COMPREPLY=( $(compgen -A hostname ${cur}) )
            return 0
            ;;
	-V)
            opts="$(rabbitmqadmin -q -f bash list vhosts)"
	    COMPREPLY=( $(compgen -W "${opts}"  -- ${cur}) )
            return 0
            ;;
	--vhost)
            opts="$(rabbitmqadmin -q -f bash list vhosts)"
	    COMPREPLY=( $(compgen -W "${opts}"  -- ${cur}) )
            return 0
            ;;
	-u)
            opts="$(rabbitmqadmin -q -f bash list users)"
	    COMPREPLY=( $(compgen -W "${opts}"  -- ${cur}) )
            return 0
            ;;
	--username)
            opts="$(rabbitmqadmin -q -f bash list users)"
	    COMPREPLY=( $(compgen -W "${opts}"  -- ${cur}) )
            return 0
            ;;
	-f)
	    COMPREPLY=( $(compgen -W \"""" + " ".join(FORMATS.keys()) + """\"  -- ${cur}) )
            return 0
            ;;
	--format)
	    COMPREPLY=( $(compgen -W \"""" + " ".join(FORMATS.keys()) + """\"  -- ${cur}) )
            return 0
            ;;

"""
    for l in LISTABLE:
        key = l[0:len(l) - 1]
        script += "        " + key + """)
            opts="$(rabbitmqadmin -q -f bash list """ + l + """)"
	    COMPREPLY=( $(compgen -W "${opts}"  -- ${cur}) )
            return 0
            ;;
"""
    script += """        *)
        ;;
    esac

   COMPREPLY=($(compgen -W "${opts} ${fargs}" -- ${cur}))
   return 0
}
complete -F _rabbitmqadmin rabbitmqadmin
"""
    output(script)

if __name__ == "__main__":
    main()
