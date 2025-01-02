%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_types).

-include("rabbit.hrl").

-export_type([
              %% deprecated
              'maybe'/1,
              option/1,
              info/0, infos/0, info_key/0, info_keys/0,
              message/0, msg_id/0, basic_message/0,
              delivery/0, content/0, decoded_content/0, undecoded_content/0,
              unencoded_content/0, encoded_content/0, message_properties/0,
              vhost/0, ctag/0, amqp_error/0, r/1, r2/2, r3/3, listener/0,
              binding/0, binding_source/0, binding_destination/0,
              exchange/0,
              connection/0, connection_name/0, channel/0, channel_name/0,
              protocol/0, auth_user/0, user/0,
              username/0, password/0, password_hash/0,
              ok/1, error/1, error/2, ok_or_error/1, ok_or_error2/2, ok_pid_or_error/0,
              channel_exit/0, connection_exit/0, mfargs/0, proc_name/0,
              proc_type_and_name/0, timestamp/0, tracked_connection_id/0,
              tracked_connection/0, tracked_channel_id/0, tracked_channel/0,
              node_type/0, topic_access_context/0,
              authz_data/0, authz_context/0,
              permission_atom/0, rabbit_amqqueue_name/0, binding_key/0, channel_number/0,
              exchange_name/0, exchange_type/0, guid/0, routing_key/0,
              sup_ref/0, child/0, child_id/0]).

-type(option(T) :: T | 'none' | 'undefined').
%% Deprecated, 'maybe' is a keyword in modern Erlang
-type('maybe'(T) :: T | 'none').
-type(timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}).

-type(vhost() :: binary()).
%% An arbitrary binary tag used to distinguish between different consumers
%% set up by the same process.
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
                 properties_bin        :: option(binary()),
                 payload_fragments_rev :: [binary()]}).
-type(encoded_content() ::
        #content{class_id       :: rabbit_framing:amqp_class_id(),
                 properties     :: option(rabbit_framing:amqp_property_record()),
                 properties_bin        :: binary(),
                 payload_fragments_rev :: [binary()]}).
-type(content() :: undecoded_content() | decoded_content()).

-type(guid() :: binary()).
-type(msg_id() :: guid()).
-type(routing_key() :: binary()).
-type(basic_message() ::
        #basic_message{exchange_name  :: exchange_name(),
                       routing_keys   :: [routing_key()],
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

-type rabbit_amqqueue_name() :: rabbit_types:r('queue').

-type(binding_source() :: exchange_name()).
-type(binding_destination() :: rabbit_amqqueue_name() | exchange_name()).

-type binding_key() :: binary().

-type(binding() ::
        #binding{source      :: exchange_name(),
                 destination :: binding_destination(),
                 key         :: binding_key(),
                 args        :: rabbit_framing:amqp_table()}).

-type(exchange() ::
        #exchange{name        :: exchange_name(),
                  type        :: exchange_type(),
                  durable     :: boolean(),
                  auto_delete :: boolean(),
                  arguments   :: rabbit_framing:amqp_table()}).

-type exchange_name() :: r('exchange').
-type exchange_type() :: atom().

-type(connection_name() :: binary()).

%% used e.g. by rabbit_networking
-type(connection() :: pid()).

%% used e.g. by rabbit_connection_tracking
-type(tracked_connection_id() :: {node(), connection_name()}).

-type(tracked_connection() ::
        #tracked_connection{id           :: tracked_connection_id(),
                            node         :: node(),
                            vhost        :: vhost(),
                            name         :: connection_name(),
                            pid          :: connection(),
                            protocol     :: protocol_name(),
                            peer_host    :: rabbit_net:hostname(),
                            peer_port    :: rabbit_net:ip_port(),
                            username     :: username(),
                            connected_at :: integer()}).

-type(channel_name() :: binary()).

-type(channel() :: pid()).

-type(channel_number() :: non_neg_integer()).

%% used e.g. by rabbit_channel_tracking
-type(tracked_channel_id() :: {node(), channel_name()}).

-type(tracked_channel() ::
        #tracked_channel{ id          :: tracked_channel_id(),
                          node        :: node(),
                          vhost       :: vhost(),
                          name        :: channel_name(),
                          pid         :: channel(),
                          username    :: username(),
                          connection  :: connection()}).

%% old AMQP 0-9-1-centric type, avoid when possible
-type(protocol() :: rabbit_framing:protocol()).

-type(protocol_name() :: 'amqp0_8' | 'amqp0_9_1' | 'amqp1_0' | 'mqtt' | 'stomp' | any()).

-type(node_type() :: 'disc' | 'ram').

-type(auth_user() ::
        #auth_user{username :: username(),
                   tags     :: [atom()],
                   impl     :: any()}).

-type(authz_data() ::
        #{peeraddr := inet:ip_address() | binary(),
          _ => _      } | undefined).

-type(user() ::
        #user{username       :: username(),
              tags           :: [atom()],
              authz_backends :: [{atom(), any()}]}).

-type(username() :: binary()).
-type(password() :: binary()).
-type(password_hash() :: binary()).

-type(ok(A) :: {'ok', A}).
-type(error(A) :: {'error', A}).
-type(error(A, B) :: {'error', A, B}).
-type(ok_or_error(A) :: 'ok' | error(A)).
-type(ok_or_error2(A, B) :: ok(A) | error(B)).
-type(ok_pid_or_error() :: ok_or_error2(pid(), any())).

-type(channel_exit() :: no_return()).
-type(connection_exit() :: no_return()).

-type(mfargs() :: {atom(), atom(), [any()]}).

-type(proc_name() :: term()).
-type(proc_type_and_name() :: {atom(), proc_name()}).

-type(topic_access_context() :: #{routing_key  => routing_key(),
                                  variable_map => map(),
                                  _ => _}).

-type(authz_context() :: map()).

-type(permission_atom() :: 'configure' | 'write' | 'read').

%% not exported by OTP supervisor
-type sup_ref()   :: (Name :: atom())
                   | {Name :: atom(), Node :: node()}
                   | {'global', Name :: term()}
                   | {'via', Module :: module(), Name :: any()}
                   | pid().

-type child() :: 'undefined' | pid().
-type child_id() :: term().
