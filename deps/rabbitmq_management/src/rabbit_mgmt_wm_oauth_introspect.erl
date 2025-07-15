%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_oauth_introspect).

-export([init/2, 
        is_authorized/2, allowed_methods/2]).
-export([variances/2]).
-include("rabbit_mgmt.hrl").

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _) ->
    Ret = {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}},
    rabbit_log:debug("init rabbit_mgmt_wm_oauth_introspect"),
    Ret.
%{cowboy_rest, rabbit_mgmt_headers:set_no_cache_headers(
%        rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), ?MODULE), State}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

is_authorized(ReqData, Context) ->
    rabbit_log:debug("to_json rabbit_mgmt_wm_oauth_introspect"),
    case cowboy_req:parse_header(<<"authorization">>, ReqData) of
        {bearer, Token} ->             
            case oauth2_client:introspect_token(Token) of 
                {error, Reason} -> 
                    rabbit_log:error("Failed to introspect token due to ~p", [Reason]),
                    rabbit_mgmt_util:bad_request(<<"Cannot introspect tokenr">>, ReqData, Context);
                JwtToken -> 
                    rabbit_mgmt_util:reply(JwtToken,ReqData, Context)
            end;
        _ -> 
            rabbit_mgmt_util:bad_request(<<"Opaque token not found in authorization header">>, ReqData, Context)
    end.
