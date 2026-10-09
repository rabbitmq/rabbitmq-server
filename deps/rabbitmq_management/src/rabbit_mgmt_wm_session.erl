%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_wm_session).

-export([init/2, content_types_accepted/2,
         allowed_methods/2, is_authorized/2, delete_resource/2]).
-export([accept_content/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(SESSION_METADATA_MAX_LENGTH, 256).

%% `x-forwarded-for` is supplied by the client, so it is stored apart from `ip`.
-define(SESSION_METADATA_HEADERS, [
    {<<"user-agent">>,       <<"user-agent">>},
    {<<"x-forwarded-proto">>, <<"x-forwarded-proto">>},
    {<<"x-forwarded-for">>,  <<"forwarded_for">>},
    {<<"host">>,             <<"host">>}
]).

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

accept_content(ReqData, Context) ->
    case cowboy_req:binding(session, ReqData) of
        undefined ->
            Username = Context#context.user#user.username,
            Metadata = build_metadata(ReqData),
            TokenExpiry = rabbit_access_control:expiry_timestamp(Context#context.user),
            case rabbit_mgmt_sessions:create_session(Username, Metadata, TokenExpiry) of
                {ok, SessionId} ->
                    Res = #{<<"session_id">> => SessionId},
                    ReqData2 = cowboy_req:reply(201, #{<<"content-type">> => <<"application/json">>}, rabbit_json:encode(Res), ReqData),
                    {stop, ReqData2, Context};
                {error, limit_reached} ->
                    rabbit_web_dispatch_access_control:halt_response(403, not_authorized, <<"concurrent_session_limit_reached">>, ReqData, Context)
            end;
        SessionId ->
            Username = Context#context.user#user.username,
            TokenExpiry = rabbit_access_control:expiry_timestamp(Context#context.user),
            case rabbit_mgmt_sessions:touch(SessionId, Username, TokenExpiry) of
                ok ->
                    {true, ReqData, Context};
                {error, not_found} ->
                    rabbit_web_dispatch_access_control:halt_response(401, unauthorized, <<"session_not_found">>, ReqData, Context);
                {error, forbidden} ->
                    rabbit_web_dispatch_access_control:halt_response(403, forbidden, <<"forbidden">>, ReqData, Context)
            end
    end.

delete_resource(ReqData, Context) ->
    case cowboy_req:binding(session, ReqData) of
        undefined ->
            rabbit_mgmt_util:not_found(session_not_found, ReqData, Context);
        SessionId ->
            Username = Context#context.user#user.username,
            case rabbit_mgmt_sessions:delete_session(SessionId, Username) of
                ok ->
                    {true, ReqData, Context};
                {error, not_found} ->
                    rabbit_mgmt_util:not_found(session_not_found, ReqData, Context);
                {error, forbidden} ->
                    rabbit_web_dispatch_access_control:halt_response(403, forbidden, <<"forbidden">>, ReqData, Context)
            end
    end.

%% Internal

build_metadata(Req) ->
    {IP, _Port} = cowboy_req:peer(Req),
    Base = #{<<"ip">> => list_to_binary(inet:ntoa(IP))},
    lists:foldl(fun({Header, Key}, Acc) ->
        case metadata_value(cowboy_req:header(Header, Req, undefined)) of
            undefined -> Acc;
            Value     -> Acc#{Key => Value}
        end
    end, Base, ?SESSION_METADATA_HEADERS).

metadata_value(undefined) ->
    undefined;
metadata_value(Value) ->
    Truncated = binary:part(Value, 0, min(?SESSION_METADATA_MAX_LENGTH, byte_size(Value))),
    case unicode:characters_to_binary(Truncated, utf8) of
        Valid when is_binary(Valid) -> Valid;
        _                           -> undefined
    end.
