%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-ifndef(AMQP_CLIENT_HRL).
-define(AMQP_CLIENT_HRL, true).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(PROTOCOL_VERSION_MAJOR, 0).
-define(PROTOCOL_VERSION_MINOR, 9).
-define(PROTOCOL_HEADER, <<"AMQP", 0, 0, 9, 1>>).
-define(PROTOCOL, rabbit_framing_amqp_0_9_1).

-define(MAX_CHANNEL_NUMBER, 65535).
-define(DEFAULT_CONSUMER, {amqp_selective_consumer, []}).

-define(PROTOCOL_SSL_PORT, (?PROTOCOL_PORT - 1)).

-record(amqp_msg, {props = #'P_basic'{}, payload = <<>>}).

-record(amqp_params_network, {username           = <<"guest">>,
                              password           = <<"guest">>,
                              virtual_host       = <<"/">>,
                              host               = "localhost",
                              port               = undefined,
                              channel_max        = 0,
                              frame_max          = 0,
                              heartbeat          = 0,
                              connection_timeout = infinity,
                              ssl_options        = none,
                              auth_mechanisms    =
                                  [fun amqp_auth_mechanisms:plain/3,
                                   fun amqp_auth_mechanisms:amqplain/3],
                              client_properties  = [],
                              socket_options     = []}).

-record(amqp_params_direct, {username          = <<"guest">>,
                             virtual_host      = <<"/">>,
                             node              = node(),
                             adapter_info      = none,
                             client_properties = []}).

-record(adapter_info, {address         = unknown,
                       port            = unknown,
                       peer_address    = unknown,
                       peer_port       = unknown,
                       name            = unknown,
                       protocol        = unknown,
                       additional_info = []}).

-define(LOG_DEBUG(Format), error_logger:info_msg(Format)).
-define(LOG_INFO(Format, Args), error_logger:info_msg(Format, Args)).
-define(LOG_WARN(Format, Args), error_logger:warning_msg(Format, Args)).

-define(CLIENT_CAPABILITIES, [{<<"publisher_confirms">>,         bool, true},
                              {<<"exchange_exchange_bindings">>, bool, true},
                              {<<"basic.nack">>,                 bool, true},
                              {<<"consumer_cancel_notify">>,     bool, true}]).

-endif.
