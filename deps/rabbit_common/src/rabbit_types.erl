%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_types).

-include("rabbit.hrl").

-export_type([maybe/1, info/0, infos/0, info_key/0, info_keys/0,
              message/0, msg_id/0, basic_message/0,
              delivery/0, content/0, decoded_content/0, undecoded_content/0,
              unencoded_content/0, encoded_content/0, message_properties/0,
              vhost/0, ctag/0, amqp_error/0, r/1, r2/2, r3/3, listener/0,
              binding/0, binding_source/0, binding_destination/0,
              amqqueue/0, exchange/0,
              connection/0, protocol/0, auth_user/0, user/0, internal_user/0,
              username/0, password/0, password_hash/0,
              ok/1, error/1, ok_or_error/1, ok_or_error2/2, ok_pid_or_error/0,
              channel_exit/0, connection_exit/0, mfargs/0, proc_name/0,
              proc_type_and_name/0, timestamp/0]).

-type(maybe(T) :: T | 'none').
-type(timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}).
-type(vhost() :: binary()).
-type(ctag() :: binary()).

%% TODO: make this more precise by tying specific class_ids to
%% specific properties
-type(undecoded_content() ::
        #content{class_id              :: rabbit_framing:amqp_class_id(),
                 properties            :: 'none',
                 properties_bin        :: binary(),
                 payload_fragments_rev :: [binary()]} |
        #content{class_id              :: rabbit_framing:amqp_class_id(),
                 properties            :: rabbit_framing:amqp_property_record(),
                 properties_bin        :: 'none',
                 payload_fragments_rev :: [binary()]}).
-type(unencoded_content() :: undecoded_content()).
-type(decoded_content() ::
        #content{class_id              :: rabbit_framing:amqp_class_id(),
                 properties            :: rabbit_framing:amqp_property_record(),
                 properties_bin        :: maybe(binary()),
                 payload_fragments_rev :: [binary()]}).
-type(encoded_content() ::
        #content{class_id       :: rabbit_framing:amqp_class_id(),
                 properties     :: maybe(rabbit_framing:amqp_property_record()),
                 properties_bin        :: binary(),
                 payload_fragments_rev :: [binary()]}).
-type(content() :: undecoded_content() | decoded_content()).
-type(msg_id() :: rabbit_guid:guid()).
-type(basic_message() ::
        #basic_message{exchange_name  :: rabbit_exchange:name(),
                       routing_keys   :: [rabbit_router:routing_key()],
                       content        :: content(),
                       id             :: msg_id(),
                       is_persistent  :: boolean()}).
-type(message() :: basic_message()).
-type(delivery() ::
        #delivery{mandatory :: boolean(),
                  sender    :: pid(),
                  message   :: message()}).
-type(message_properties() ::
        #message_properties{expiry :: pos_integer() | 'undefined',
                            needs_confirming :: boolean()}).

-type(info_key() :: atom()).
-type(info_keys() :: [info_key()]).

-type(info() :: {info_key(), any()}).
-type(infos() :: [info()]).

-type(amqp_error() ::
        #amqp_error{name        :: rabbit_framing:amqp_exception(),
                    explanation :: string(),
                    method      :: rabbit_framing:amqp_method_name()}).

-type(r(Kind) ::
        r2(vhost(), Kind)).
-type(r2(VirtualHost, Kind) ::
        r3(VirtualHost, Kind, rabbit_misc:resource_name())).
-type(r3(VirtualHost, Kind, Name) ::
        #resource{virtual_host :: VirtualHost,
                  kind         :: Kind,
                  name         :: Name}).

-type(listener() ::
        #listener{node     :: node(),
                  protocol :: atom(),
                  host     :: rabbit_net:hostname(),
                  port     :: rabbit_net:ip_port()}).

-type(binding_source() :: rabbit_exchange:name()).
-type(binding_destination() :: rabbit_amqqueue:name() | rabbit_exchange:name()).

-type(binding() ::
        #binding{source      :: rabbit_exchange:name(),
                 destination :: binding_destination(),
                 key         :: rabbit_binding:key(),
                 args        :: rabbit_framing:amqp_table()}).

-type(amqqueue() ::
        #amqqueue{name            :: rabbit_amqqueue:name(),
                  durable         :: boolean(),
                  auto_delete     :: boolean(),
                  exclusive_owner :: rabbit_types:maybe(pid()),
                  arguments       :: rabbit_framing:amqp_table(),
                  pid             :: rabbit_types:maybe(pid()),
                  slave_pids      :: [pid()]}).

-type(exchange() ::
        #exchange{name        :: rabbit_exchange:name(),
                  type        :: rabbit_exchange:type(),
                  durable     :: boolean(),
                  auto_delete :: boolean(),
                  arguments   :: rabbit_framing:amqp_table()}).

-type(connection() :: pid()).

-type(protocol() :: rabbit_framing:protocol()).

-type(auth_user() ::
        #auth_user{username :: username(),
                   tags     :: [atom()],
                   impl     :: any()}).

-type(user() ::
        #user{username       :: username(),
              tags           :: [atom()],
              authz_backends :: [{atom(), any()}]}).

-type(internal_user() ::
        #internal_user{username      :: username(),
                       password_hash :: password_hash(),
                       tags          :: [atom()]}).

-type(username() :: binary()).
-type(password() :: binary()).
-type(password_hash() :: binary()).

-type(ok(A) :: {'ok', A}).
-type(error(A) :: {'error', A}).
-type(ok_or_error(A) :: 'ok' | error(A)).
-type(ok_or_error2(A, B) :: ok(A) | error(B)).
-type(ok_pid_or_error() :: ok_or_error2(pid(), any())).

-type(channel_exit() :: no_return()).
-type(connection_exit() :: no_return()).

-type(mfargs() :: {atom(), atom(), [any()]}).

-type(proc_name() :: term()).
-type(proc_type_and_name() :: {atom(), proc_name()}).
