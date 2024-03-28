%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-record(stomp_configuration, {default_login,
                              default_passcode,
                              force_default_creds = false,
                              implicit_connect,
                              ssl_cert_login,
                              max_header_length,
                              max_headers,
                              max_body_length}).


-define(SUPPORTED_VERSIONS, ["1.0", "1.1", "1.2"]).



-define(INFO_ITEMS,
        [conn_name,
         name,
         user,
         connection,
         connection_state,
         session_id,
         version,
         implicit_connect,
         auth_login,
         auth_mechanism,
         %% peer_addr,
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

-define(STOMP_GUIDE_URL, <<"https://rabbitmq.com/docs/stomp">>).

-define(DEFAULT_MAX_FRAME_SIZE, 4 * 1024 * 1024).

-define(SIMPLE_METRICS,
        [pid,
         recv_oct,
         send_oct,
         reductions]).
-define(OTHER_METRICS,
        [recv_cnt,
         send_cnt,
         send_pend,
         garbage_collection,
         state,
         timeout]).

-type send_fun() :: fun ((async | sync, iodata()) -> ok | {atom(), any()}).
