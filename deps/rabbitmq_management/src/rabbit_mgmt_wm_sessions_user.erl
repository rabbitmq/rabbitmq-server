%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_wm_sessions_user).

-export([init/2, content_types_provided/2, allowed_methods/2,
         is_authorized/2, delete_resource/2]).
-export([to_json/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

to_json(ReqData, Context) ->
    Username = cowboy_req:binding(username, ReqData),
    case rabbit_mgmt_wm_sessions:parse_pagination_params(ReqData) of
        {ok, Page, PageSize} ->
            Result = rabbit_mgmt_sessions:list_sessions(Page, PageSize, Username),
            rabbit_mgmt_util:reply(Result, ReqData, Context);
        {error, Reason} ->
            rabbit_mgmt_util:bad_request(Reason, ReqData, Context)
    end.

delete_resource(ReqData, Context) ->
    Username = cowboy_req:binding(username, ReqData),
    case cowboy_req:binding(session, ReqData) of
        undefined ->
            rabbit_mgmt_sessions:terminate_sessions(Username),
            {true, ReqData, Context};
        SessionId ->
            case rabbit_mgmt_sessions:delete_session(SessionId, Username) of
                ok ->
                    {true, ReqData, Context};
                {error, not_found} ->
                    rabbit_mgmt_util:not_found(session_not_found, ReqData, Context);
                {error, forbidden} ->
                    rabbit_web_dispatch_access_control:halt_response(403, forbidden, <<"session_belongs_to_another_user">>, ReqData, Context)
            end
    end.
