%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Sets CSP header(s) on the response if configured,
%% see https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP.

-module(rabbit_mgmt_csp).

-export([set_headers/1]).

-include_lib("kernel/include/logger.hrl").

-define(CSP_HEADER, <<"content-security-policy">>).

%%
%% API
%%

set_headers(ReqData) ->
    case application:get_env(rabbitmq_management, content_security_policy) of
        undefined   ->
            maybe_warn(ReqData),
            ReqData;
        {ok, Value} ->
            cowboy_req:set_resp_header(?CSP_HEADER,
                                       rabbit_data_coercion:to_binary(Value),
                                       ReqData)
    end.

maybe_warn(#{port := Port}) ->
    case persistent_term:get({rabbitmq_management_csp_warning, Port}, false) of
        false ->
            _ = ?LOG_WARNING("CSP disabled for management listener "
                "on port ~b; consider enabling content_security_policy option",
                [Port]),
            persistent_term:put({rabbitmq_management_csp_warning, Port}, true);
        true ->
            ok
    end;
maybe_warn(_) ->
    ok.
