%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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
