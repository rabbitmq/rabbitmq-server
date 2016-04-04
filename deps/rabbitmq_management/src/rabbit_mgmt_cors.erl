%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

%% Useful documentation about CORS:
%% * https://tools.ietf.org/html/rfc6454
%% * https://www.w3.org/TR/cors/
%% * https://staticapps.org/articles/cross-domain-requests-with-cors/
-module(rabbit_mgmt_cors).

-export([set_headers/2]).

%% We don't set access-control-max-age because we currently have
%% no way to know which headers apply to the whole resource. We
%% only know for the next request.
set_headers(ReqData, Module) ->
    ReqData1 = case wrq:get_resp_header("vary", ReqData) of
        undefined -> wrq:set_resp_header("vary", "origin", ReqData);
        VaryValue -> wrq:set_resp_header("vary", VaryValue ++ ", origin", ReqData)
    end,
    case match_origin(ReqData1) of
        false ->
            ReqData1;
        Origin ->
            ReqData2 = case wrq:method(ReqData1) of
                'OPTIONS' -> handle_options(ReqData1, Module);
                _         -> ReqData1
            end,
            wrq:set_resp_headers([
                {"access-control-allow-origin",      Origin},
                {"access-control-allow-credentials", "true"}
            ], ReqData2)
    end.

%% Set max-age from configuration (default: 30 minutes).
%% Set allow-methods from what is defined in Module:allowed_methods/2.
%% Set allow-headers to the same as the request (accept all headers).
handle_options(ReqData, Module) ->
    MaxAge = application:get_env(rabbitmq_management, cors_max_age, 1800),
    {Methods, _, _} = Module:allowed_methods(undefined, undefined),
    AllowMethods = string:join([atom_to_list(M) || M <- Methods], ", "),
    ReqHeaders = wrq:get_req_header("access-control-request-headers", ReqData),
    MaxAgeHd = case MaxAge of
        undefined -> [];
        _ -> {"access-control-max-age", integer_to_list(MaxAge)}
    end,
    MaybeAllowHeaders = case ReqHeaders of
        undefined -> [];
        _ -> [{"access-control-allow-headers", ReqHeaders}]
    end,
    wrq:set_resp_headers([MaxAgeHd,
        {"access-control-allow-methods", AllowMethods}
        |MaybeAllowHeaders], ReqData).

%% If the origin header is missing or "null", we disable CORS.
%% Otherwise, we only enable it if the origin is found in the
%% cors_allow_origins configuration variable, or if "*" is (it
%% allows all origins).
match_origin(ReqData) ->
    case wrq:get_req_header("origin", ReqData) of
        undefined -> false;
        "null" -> false;
        Origin ->
            AllowedOrigins = application:get_env(rabbitmq_management,
                cors_allow_origins, []),
            case lists:member(Origin, AllowedOrigins) of
                true ->
                    Origin;
                false ->
                    %% Maybe the configuration explicitly allows "*".
                    case lists:member("*", AllowedOrigins) of
                        true  -> Origin;
                        false -> false
                    end
            end
    end.
