%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module contains helper functions that control
%% response headers related to CORS, CSP, HSTS and so on.
-module(rabbit_mgmt_headers).

-export([set_common_permission_headers/2]).
-export([set_cors_headers/2, set_hsts_headers/2, set_csp_headers/2, set_no_cache_headers/2]).

-define(X_CONTENT_TYPE_OPTIONS_HEADER, <<"X-Content-Type-Options">>).
-define(X_FRAME_OPTIONS_HEADER, <<"X-Frame-Options">>).
-define(X_XSS_PROTECTION_HEADER, <<"X-XSS-Protection">>).

%%
%% API
%%

set_cors_headers(ReqData, Module) ->
    rabbit_mgmt_cors:set_headers(ReqData, Module).

set_hsts_headers(ReqData, _Module) ->
    rabbit_mgmt_hsts:set_headers(ReqData).

set_csp_headers(ReqData, _Module) ->
    rabbit_mgmt_csp:set_headers(ReqData).

set_content_type_options_header(ReqData, _Module) ->
    maybe_set_known_configured_header(ReqData, content_type_options, ?X_CONTENT_TYPE_OPTIONS_HEADER).

set_xss_protection_header(ReqData, _Module) ->
    maybe_set_known_configured_header(ReqData, xss_protection, ?X_XSS_PROTECTION_HEADER).

set_frame_options_header(ReqData, _Module) ->
    maybe_set_known_configured_header(ReqData, frame_options, ?X_FRAME_OPTIONS_HEADER).

maybe_set_known_configured_header(ReqData, AppEnvKey, HeaderName) ->
    case application:get_env(rabbitmq_management, headers) of
        undefined      -> ReqData;
        {ok, Proplist} ->
            case proplists:get_value(AppEnvKey, Proplist) of
                undefined -> ReqData;
                Value     ->
                    cowboy_req:set_resp_header(HeaderName,
                                               rabbit_data_coercion:to_binary(Value),
                                               ReqData)
            end
    end.

set_common_permission_headers(ReqData0, EndpointModule) ->
    lists:foldl(fun(Fun, ReqData) ->
                        Fun(ReqData, EndpointModule)
                end, ReqData0,
               [fun set_csp_headers/2,
                fun set_hsts_headers/2,
                fun set_cors_headers/2,
                fun set_content_type_options_header/2,
                fun set_xss_protection_header/2,
                fun set_frame_options_header/2]).

set_no_cache_headers(ReqData0, _Module) ->
    ReqData1 = cowboy_req:set_resp_header(<<"cache-control">>, <<"no-cache, no-store, must-revalidate">>, ReqData0),
    ReqData2 = cowboy_req:set_resp_header(<<"pragma">>, <<"no-cache">>, ReqData1),
    cowboy_req:set_resp_header(<<"expires">>, rabbit_data_coercion:to_binary(0), ReqData2).
