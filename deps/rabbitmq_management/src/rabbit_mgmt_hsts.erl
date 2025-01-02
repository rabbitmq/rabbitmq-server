%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Sets HSTS header(s) on the response if configured,
%% see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Strict-Transport-Security.

-module(rabbit_mgmt_hsts).

-export([set_headers/1]).

-define(HSTS_HEADER, <<"strict-transport-security">>).

%%
%% API
%%

set_headers(ReqData) ->
    case application:get_env(rabbitmq_management, strict_transport_security) of
        undefined   -> ReqData;
        {ok, Value} ->
            cowboy_req:set_resp_header(?HSTS_HEADER,
                                       rabbit_data_coercion:to_binary(Value),
                                       ReqData)
    end.
