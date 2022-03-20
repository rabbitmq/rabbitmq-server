%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Sets CSP header(s) on the response if configured,
%% see https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP.

-module(rabbit_mgmt_csp).

-export([set_headers/1]).

-define(CSP_HEADER, <<"content-security-policy">>).

%%
%% API
%%

set_headers(ReqData) ->
    case application:get_env(rabbitmq_management, content_security_policy) of
        undefined   -> ReqData;
        {ok, Value} ->
            cowboy_req:set_resp_header(?CSP_HEADER,
                                       rabbit_data_coercion:to_binary(Value),
                                       ReqData)
    end.
