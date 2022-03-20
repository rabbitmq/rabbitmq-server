%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(stomp_configuration, {default_login,
                              default_passcode,
                              force_default_creds = false,
                              implicit_connect,
                              ssl_cert_login}).

-define(SUPPORTED_VERSIONS, ["1.0", "1.1", "1.2"]).

-define(INFO_ITEMS,
        [conn_name,
         connection,
         connection_state,
         session_id,
         channel,
         version,
         implicit_connect,
         auth_login,
         auth_mechanism,
         peer_addr,
         host,
         port,
         peer_host,
         peer_port,
         protocol,
         channels,
         channel_max,
         frame_max,
         client_properties,
         ssl,
         ssl_protocol,
         ssl_key_exchange,
         ssl_cipher,
         ssl_hash]).

-define(STOMP_GUIDE_URL, <<"https://rabbitmq.com/stomp.html">>).
