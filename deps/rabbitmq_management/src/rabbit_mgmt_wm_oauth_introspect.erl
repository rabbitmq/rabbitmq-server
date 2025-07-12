%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_oauth_introspect).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-include("rabbit_mgmt.hrl").

%%--------------------------------------------------------------------

init(Req, State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_no_cache_headers(
        rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), ?MODULE), State}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    case cowboy_req:parse_header(<<"authorization">>, ReqData) of
        {bearer, Token} ->             
            rabbit_mgmt_util:reply(
              maps:from_list(oauth2_client:introspect_token(Token)),
              ReqData, Context);
        _ -> 
            rabbit_mgmt_util:bad_request(<<"Opaque token not found in authorization header">>, ReqData, Context)
    end.
               
is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.
