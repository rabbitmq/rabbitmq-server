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
