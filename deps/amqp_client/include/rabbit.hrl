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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-record(user, {username, password}).
-record(user_vhost, {username, virtual_host}).

-record(vhost, {virtual_host, dummy}).
-record(vhost_realm, {virtual_host, realm}).

-record(realm, {name, exchanges, queues}).
-record(user_realm, {username, realm, ticket_pattern}).

-record(realm_visitor, {realm, pid}).

-record(connection, {user, timeout_sec, heartbeat_sender_pid, frame_max, vhost, reader_pid, writer_pid}).

-record(ch, {channel, tx, reader_pid, writer_pid, username, virtual_host,
             most_recently_declared_queue, consumer_mapping, next_ticket}).

-record(content, {class_id,
		  properties, %% either 'none', or a decoded record/tuple
		  properties_bin, %% either 'none', or an encoded properties binary
		  %% Note: at most one of properties and properties_bin can be 'none' at once.
		  payload_fragments_rev %% list of binaries, in reverse order (!)
		 }).

-record(resource, {virtual_host, kind, name}).

-record(ticket, {realm_name, passive_flag, active_flag, write_flag, read_flag}).

-record(exchange, {name, type, durable, auto_delete, arguments}).

-record(amqqueue, {name, durable, auto_delete, arguments, binding_specs, pid}).
-record(binding_spec, {exchange_name, routing_key, arguments}).

-record(binding, {key, handlers}).
-record(handler, {binding_spec, queue, qpid}).

-record(listener, {node, protocol, host, port}).

-record(basic_message, {exchange_name, routing_key, content, redelivered, persistent_key}).

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.").
-define(INFORMATION_MESSAGE, "Licensed under the MPL.  See http://www.rabbitmq.com/").

-ifdef(debug).
-define(LOGDEBUG0(F), rabbit_log:debug(F)).
-define(LOGDEBUG(F,A), rabbit_log:debug(F,A)).
-define(LOGMESSAGE(D,C,M,Co), rabbit_log:message(D,C,M,Co)).
-else.
-define(LOGDEBUG0(F), ok).
-define(LOGDEBUG(F,A), ok).
-define(LOGMESSAGE(D,C,M,Co), ok).
-endif.
