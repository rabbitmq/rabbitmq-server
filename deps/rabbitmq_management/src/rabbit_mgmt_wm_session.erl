%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_wm_session).

-export([init/2, content_types_provided/2, content_types_accepted/2,
         allowed_methods/2, is_authorized/2, delete_resource/2]).
-export([to_json/2, accept_content/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(SESSION_METADATA_HEADERS, [
    <<"user-agent">>,
    <<"x-forwarded-proto">>,
    <<"host">>
]).

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

to_json(ReqData, Context) ->
    %% Never called because allowed_methods does not include GET.
    {<<"">>, ReqData, Context}.

accept_content(ReqData, Context) ->
    case cowboy_req:binding(session, ReqData) of
        undefined ->
            Username = Context#context.user#user.username,
            Metadata = build_metadata(ReqData),
            case rabbit_mgmt_sessions:create_session(Username, Metadata) of
                {ok, SessionId} ->
                    Res = #{<<"session_id">> => SessionId},
                    %% Create returns 201 Created by default when accept_xxx returns {true, _, _} if no body is explicitly sent, but we want a body. 
                    %% Actually, Cowboy will return 204 No Content for {true, Req, Ctx} if the path hasn't changed. Let's explicitly reply 201 with body.
                    ReqData2 = cowboy_req:reply(201, #{<<"content-type">> => <<"application/json">>}, rabbit_json:encode(Res), ReqData),
                    {stop, ReqData2, Context};
                {error, limit_reached} ->
                    rabbit_web_dispatch_access_control:halt_response(403, not_authorized, <<"concurrent_session_limit_reached">>, ReqData, Context)
            end;
        SessionId ->
            Username = Context#context.user#user.username,
            case rabbit_mgmt_sessions:heartbeat(SessionId, Username) of
                ok ->
                    {true, ReqData, Context};
                {error, not_found} ->
                    rabbit_web_dispatch_access_control:halt_response(401, unauthorized, <<"session_not_found">>, ReqData, Context);
                {error, forbidden} ->
                    rabbit_web_dispatch_access_control:halt_response(403, forbidden, <<"forbidden">>, ReqData, Context)
            end
    end.

delete_resource(ReqData, Context) ->
    SessionId = cowboy_req:binding(session, ReqData),
    Username = Context#context.user#user.username,
    case rabbit_mgmt_sessions:heartbeat(SessionId, Username) of
        ok ->
            rabbit_mgmt_sessions:delete_session(SessionId),
            {true, ReqData, Context};
        {error, not_found} ->
            {false, ReqData, Context};
        {error, forbidden} ->
            rabbit_web_dispatch_access_control:halt_response(403, forbidden, <<"forbidden">>, ReqData, Context)
    end.

%% Internal

peer_ip(Req) ->
    case cowboy_req:header(<<"x-forwarded-for">>, Req, undefined) of
        undefined ->
            {IP, _Port} = cowboy_req:peer(Req),
            list_to_binary(inet:ntoa(IP));
        Forwarded ->
            hd(binary:split(Forwarded, [<<",">>, <<" ">>], [global, trim_all]))
    end.

build_metadata(Req) ->
    Base = #{<<"ip">> => peer_ip(Req)},
    lists:foldl(fun(H, Acc) ->
        case cowboy_req:header(H, Req, undefined) of
            undefined -> Acc;
            Value     -> Acc#{H => Value}
        end
    end, Base, ?SESSION_METADATA_HEADERS).
