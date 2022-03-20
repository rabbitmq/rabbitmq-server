%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module contains helper functions that control
%% response headers related to CORS, CSP, HSTS and so on.
-module(rabbit_mgmt_headers).

-export([set_common_permission_headers/2]).
-export([set_cors_headers/2, set_hsts_headers/2, set_csp_headers/2]).

%%
%% API
%%

set_cors_headers(ReqData, Module) ->
    rabbit_mgmt_cors:set_headers(ReqData, Module).

set_hsts_headers(ReqData, _Module) ->
    rabbit_mgmt_hsts:set_headers(ReqData).

set_csp_headers(ReqData, _Module) ->
    rabbit_mgmt_csp:set_headers(ReqData).

set_common_permission_headers(ReqData0, EndpointModule) ->
    lists:foldl(fun(Fun, ReqData) ->
                        Fun(ReqData, EndpointModule)
                end, ReqData0,
               [fun set_csp_headers/2,
                fun set_hsts_headers/2,
                fun set_cors_headers/2]).
