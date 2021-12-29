%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Useful documentation about CORS:
%% * https://tools.ietf.org/html/rfc6454
%% * https://www.w3.org/TR/cors/
%% * https://staticapps.org/articles/cross-domain-requests-with-cors/
-module(rabbit_mgmt_cors).

-export([set_headers/2]).

set_headers(ReqData, Module) ->
    %% Send vary: origin by default if nothing else was set.
    ReqData1 = cowboy_req:set_resp_header(<<"vary">>, <<"origin">>, ReqData),
    case match_origin(ReqData1) of
        false ->
            ReqData1;
        Origin ->
            ReqData2 = case cowboy_req:method(ReqData1) of
                <<"OPTIONS">> -> handle_options(ReqData1, Module);
                _             -> ReqData1
            end,
            ReqData3 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>,
                                                  Origin,
                                                  ReqData2),
            cowboy_req:set_resp_header(<<"access-control-allow-credentials">>,
                                       "true",
                                       ReqData3)
    end.

%% Set max-age from configuration (default: 30 minutes).
%% Set allow-methods from what is defined in Module:allowed_methods/2.
%% Set allow-headers to the same as the request (accept all headers).
handle_options(ReqData0, Module) ->
    MaxAge = application:get_env(rabbitmq_management, cors_max_age, 1800),
    Methods = case erlang:function_exported(Module, allowed_methods, 2) of
        false -> [<<"HEAD">>, <<"GET">>, <<"OPTIONS">>];
        true  -> element(1, Module:allowed_methods(undefined, undefined))
    end,
    AllowMethods = string:join([binary_to_list(M) || M <- Methods], ", "),
    ReqHeaders = cowboy_req:header(<<"access-control-request-headers">>, ReqData0),

    ReqData1 = case MaxAge of
        undefined -> ReqData0;
        _         -> cowboy_req:set_resp_header(<<"access-control-max-age">>,
                                                integer_to_list(MaxAge),
                                                ReqData0)
    end,
    ReqData2 = case ReqHeaders of
        undefined -> ReqData1;
        _         -> cowboy_req:set_resp_header(<<"access-control-allow-headers">>,
                                                ReqHeaders,
                                                ReqData0)
    end,
    cowboy_req:set_resp_header(<<"access-control-allow-methods">>,
                               AllowMethods,
                               ReqData2).

%% If the origin header is missing or "null", we disable CORS.
%% Otherwise, we only enable it if the origin is found in the
%% cors_allow_origins configuration variable, or if "*" is (it
%% allows all origins).
match_origin(ReqData) ->
    case cowboy_req:header(<<"origin">>, ReqData) of
        undefined -> false;
        <<"null">> -> false;
        Origin     ->
            AllowedOrigins = application:get_env(rabbitmq_management,
                cors_allow_origins, []),
            case lists:member(binary_to_list(Origin), AllowedOrigins) of
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
