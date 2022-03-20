%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
                              channel_max        = 2047,
                              frame_max          = 0,
                              heartbeat          = 10,
                              connection_timeout = 60000,
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
