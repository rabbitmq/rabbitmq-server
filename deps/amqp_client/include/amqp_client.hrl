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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-ifndef(AMQP_CLIENT_HRL).
-define(AMQP_CLIENT_HRL, true).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-record(amqp_msg, {props = #'P_basic'{}, payload = <<>>}).

-record(amqp_params_network, {username           = <<"guest">>,
                              password           = <<"guest">>,
                              virtual_host       = <<"/">>,
                              host               = "localhost",
                              port               = undefined,
                              channel_max        = 0,
                              frame_max          = 0,
                              heartbeat          = 10,
                              connection_timeout = infinity,
                              ssl_options        = none,
                              auth_mechanisms    =
                                  [fun amqp_auth_mechanisms:plain/3,
                                   fun amqp_auth_mechanisms:amqplain/3],
                              client_properties  = [],
                              socket_options     = []}).

-record(amqp_params_direct, {username          = none,
                             password          = none,
                             virtual_host      = <<"/">>,
                             node              = node(),
                             adapter_info      = none,
                             client_properties = []}).

-record(amqp_adapter_info, {host            = unknown,
                            port            = unknown,
                            peer_host       = unknown,
                            peer_port       = unknown,
                            name            = unknown,
                            protocol        = unknown,
                            additional_info = []}).

-endif.
