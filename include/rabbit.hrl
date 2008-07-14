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
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
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

-record(realm, {name, ignore}).
-record(realm_resource, {realm, resource}).

-record(user_realm, {username, realm, ticket_pattern}).

-record(realm_visitor, {realm, pid}).

-record(connection, {user, timeout_sec, frame_max, vhost}).

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

-record(basic_message, {exchange_name, routing_key, content, persistent_key}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-include("rabbit_framing_spec.hrl").

-type(maybe(T) :: T | 'none').
-type(node() :: atom()).
-type(socket() :: port()).
-type(thunk(T) :: fun(() -> T)).

%% this is really an abstract type, but dialyzer does not support them
-type(guid() :: any()).
-type(txn() :: guid()).
-type(pkey() :: guid()).
-type(r(Kind) ::
      #resource{virtual_host :: vhost(),
                kind         :: Kind,
                name         :: name()}).
-type(realm_name() :: r('realm')).
-type(queue_name() :: r('queue')).
-type(exchange_name() :: r('exchange')).
-type(user() ::
      #user{username :: username(),
            password :: password()}).
-type(ticket() ::
      #ticket{realm_name   :: realm_name(),
              passive_flag :: bool(),
              active_flag  :: bool(),
              write_flag   :: bool(),
              read_flag    :: bool()}).
-type(permission() :: 'passive' | 'active' | 'write' | 'read').      
-type(binding_spec() ::
      #binding_spec{exchange_name :: exchange_name(),
                    routing_key   :: routing_key(),
                    arguments     :: amqp_table()}).
-type(amqqueue() ::
      #amqqueue{name          :: queue_name(),
                durable       :: bool(),
                auto_delete   :: bool(),
                arguments     :: amqp_table(),
                binding_specs :: [binding_spec()],
                pid           :: maybe(pid())}).
-type(exchange() ::
      #exchange{name        :: exchange_name(),
                type        :: exchange_type(),
                durable     :: bool(),
                auto_delete :: bool(),
                arguments   :: amqp_table()}).
%% TODO: make this more precise by tying specific class_ids to
%% specific properties
-type(undecoded_content() ::
      #content{class_id              :: amqp_class_id(),
               properties            :: 'none',
               properties_bin        :: binary(),
               payload_fragments_rev :: [binary()]} |
      #content{class_id              :: amqp_class_id(),
               properties            :: amqp_properties(),
               properties_bin        :: 'none',
               payload_fragments_rev :: [binary()]}).
-type(decoded_content() ::
      #content{class_id              :: amqp_class_id(),
               properties            :: amqp_properties(),
               properties_bin        :: maybe(binary()),
               payload_fragments_rev :: [binary()]}).
-type(content() :: undecoded_content() | decoded_content()).
-type(basic_message() ::
      #basic_message{exchange_name  :: exchange_name(),
                     routing_key    :: routing_key(),
                     content        :: content(),
                     persistent_key :: maybe(pkey())}).
-type(message() :: basic_message()).
%% this really should be an abstract type
-type(msg_id() :: non_neg_integer()).
-type(msg() :: {queue_name(), pid(), msg_id(), bool(), message()}).
-type(listener() ::
      #listener{node     :: node(),
                protocol :: atom(),
                host     :: string() | atom(),
                port     :: non_neg_integer()}).
-type(not_found() :: {'error', 'not_found'}).

-endif.

%%----------------------------------------------------------------------------

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007-2008 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.").
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
