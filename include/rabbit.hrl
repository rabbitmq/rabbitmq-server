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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-record(user, {username, password}).
-record(permission, {configure, write, read}).
-record(user_vhost, {username, virtual_host}).
-record(user_permission, {user_vhost, permission}).

-record(vhost, {virtual_host, dummy}).

-record(connection, {user, timeout_sec, frame_max, vhost}).

-record(content,
        {class_id,
         properties, %% either 'none', or a decoded record/tuple
         properties_bin, %% either 'none', or an encoded properties binary
         %% Note: at most one of properties and properties_bin can be
         %% 'none' at once.
         payload_fragments_rev %% list of binaries, in reverse order (!)
         }).

-record(resource, {virtual_host, kind, name}).

-record(exchange, {name, type, durable, auto_delete, arguments}).

-record(amqqueue, {name, durable, auto_delete, arguments, pid}).

%% mnesia doesn't like unary records, so we add a dummy 'value' field
-record(route, {binding, value = const}).
-record(reverse_route, {reverse_binding, value = const}).

-record(binding, {exchange_name, key, queue_name, args = []}).
-record(reverse_binding, {queue_name, key, exchange_name, args = []}).

-record(listener, {node, protocol, host, port}).

-record(basic_message, {exchange_name, routing_key, content, persistent_key}).

-record(delivery, {mandatory, immediate, txn, sender, message}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-include("rabbit_framing_spec.hrl").

-type(maybe(T) :: T | 'none').
-type(erlang_node() :: atom()).
-type(socket() :: port()).
-type(thunk(T) :: fun(() -> T)).
-type(info_key() :: atom()).
-type(info() :: {info_key(), any()}).
-type(regexp() :: binary()).

%% this is really an abstract type, but dialyzer does not support them
-type(guid() :: any()).
-type(txn() :: guid()).
-type(pkey() :: guid()).
-type(r(Kind) ::
      #resource{virtual_host :: vhost(),
                kind         :: Kind,
                name         :: resource_name()}).
-type(queue_name() :: r('queue')).
-type(exchange_name() :: r('exchange')).
-type(user() ::
      #user{username :: username(),
            password :: password()}).
-type(permission() ::
      #permission{configure :: regexp(),
                  write     :: regexp(),
                  read      :: regexp()}).
-type(amqqueue() ::
      #amqqueue{name          :: queue_name(),
                durable       :: bool(),
                auto_delete   :: bool(),
                arguments     :: amqp_table(),
                pid           :: maybe(pid())}).
-type(exchange() ::
      #exchange{name        :: exchange_name(),
                type        :: exchange_type(),
                durable     :: bool(),
                auto_delete :: bool(),
                arguments   :: amqp_table()}).
-type(binding() ::
      #binding{exchange_name    :: exchange_name(),
               queue_name       :: queue_name(),
               key              :: binding_key()}).
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
-type(delivery() ::
      #delivery{mandatory :: bool(),
                immediate :: bool(),
                txn       :: maybe(txn()),
                sender    :: pid(),
                message   :: message()}).
%% this really should be an abstract type
-type(msg_id() :: non_neg_integer()).
-type(msg() :: {queue_name(), pid(), msg_id(), bool(), message()}).
-type(listener() ::
      #listener{node     :: erlang_node(),
                protocol :: atom(),
                host     :: string() | atom(),
                port     :: non_neg_integer()}).
-type(not_found() :: {'error', 'not_found'}).
-type(routing_result() :: 'routed' | 'unroutable' | 'not_delivered').

-endif.

%%----------------------------------------------------------------------------

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007-2009 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.").
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
