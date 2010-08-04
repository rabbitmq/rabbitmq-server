%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-record(user, {username, password}).
-record(permission, {scope, configure, write, read}).
-record(user_vhost, {username, virtual_host}).
-record(user_permission, {user_vhost, permission}).

-record(vhost, {virtual_host, dummy}).

-record(connection, {protocol, user, timeout_sec, frame_max, vhost,
                     client_properties}).

-record(content,
        {class_id,
         properties, %% either 'none', or a decoded record/tuple
         properties_bin, %% either 'none', or an encoded properties binary
         %% Note: at most one of properties and properties_bin can be
         %% 'none' at once.
         protocol, %% The protocol under which properties_bin was encoded
         payload_fragments_rev %% list of binaries, in reverse order (!)
         }).

-record(resource, {virtual_host, kind, name}).

-record(exchange, {name, type, durable, auto_delete, arguments}).

-record(amqqueue, {name, durable, auto_delete, exclusive_owner = none,
                   arguments, pid}).

%% mnesia doesn't like unary records, so we add a dummy 'value' field
-record(route, {binding, value = const}).
-record(reverse_route, {reverse_binding, value = const}).

-record(binding, {exchange_name, key, queue_name, args = []}).
-record(reverse_binding, {queue_name, key, exchange_name, args = []}).

-record(listener, {node, protocol, host, port}).

-record(basic_message, {exchange_name, routing_key, content, guid,
                        is_persistent}).

-record(ssl_socket, {tcp, ssl}).
-record(delivery, {mandatory, immediate, txn, sender, message}).
-record(amqp_error, {name, explanation, method = none}).

-record(event, {type, props, timestamp}).

%%----------------------------------------------------------------------------

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007-2010 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.").
-define(INFORMATION_MESSAGE, "Licensed under the MPL.  See http://www.rabbitmq.com/").
-define(PROTOCOL_VERSION, "AMQP 0-9-1 / 0-9 / 0-8").
-define(ERTS_MINIMUM, "5.6.3").

-define(MAX_WAIT, 16#ffffffff).

-define(HIBERNATE_AFTER_MIN,        1000).
-define(DESIRED_HIBERNATE,         10000).
-define(STATS_INTERVAL,             5000).

-ifdef(debug).
-define(LOGDEBUG0(F), rabbit_log:debug(F)).
-define(LOGDEBUG(F,A), rabbit_log:debug(F,A)).
-define(LOGMESSAGE(D,C,M,Co), rabbit_log:message(D,C,M,Co)).
-else.
-define(LOGDEBUG0(F), ok).
-define(LOGDEBUG(F,A), ok).
-define(LOGMESSAGE(D,C,M,Co), ok).
-endif.
