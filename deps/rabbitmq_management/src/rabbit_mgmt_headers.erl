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
