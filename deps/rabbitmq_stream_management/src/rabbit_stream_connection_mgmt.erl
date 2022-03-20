%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_connection_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         resource_exists/2,
         to_json/2,
         content_types_provided/2,
         is_authorized/2,
         allowed_methods/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

dispatcher() ->
    case rabbit_stream_management_utils:is_feature_flag_enabled() of
        true ->
            [{"/stream/connections/:vhost/:connection", ?MODULE, []}];
        false ->
            []
    end.

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest,
     rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData,
     Context}.

resource_exists(ReqData, Context) ->
    case conn(ReqData) of
        not_found ->
            {false, ReqData, Context};
        _Conn ->
            {true, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            rabbit_mgmt_util:reply(
                maps:from_list(
                    rabbit_mgmt_format:strip_pids(conn_stats(ReqData))),
                ReqData, Context);
        true ->
            rabbit_mgmt_util:reply([{name,
                                     rabbit_mgmt_util:id(connection, ReqData)}],
                                   ReqData, Context)
    end.

delete_resource(ReqData, Context) ->
    case conn(ReqData) of
        not_found ->
            ok;
        Conn ->
            case proplists:get_value(pid, Conn) of
                undefined ->
                    ok;
                Pid when is_pid(Pid) ->
                    force_close_connection(ReqData, Pid)
            end
    end,
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    try
        rabbit_mgmt_util:is_authorized_user(ReqData, Context, conn(ReqData))
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

%%--------------------------------------------------------------------

conn(ReqData) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            conn_stats(ReqData);
        true ->
            VHost = rabbit_mgmt_util:id(vhost, ReqData),
            case rabbit_connection_tracking:lookup(
                     rabbit_mgmt_util:id(connection, ReqData))
            of
                #tracked_connection{name = Name,
                                    pid = Pid,
                                    username = Username,
                                    type = Type,
                                    protocol = <<"stream">>,
                                    vhost = VHost} ->
                    [{name, Name}, {pid, Pid}, {user, Username}, {type, Type}];
                #tracked_connection{} ->
                    not_found;
                not_found ->
                    not_found
            end
    end.

conn_stats(ReqData) ->
    case rabbit_mgmt_db:get_connection(
             rabbit_mgmt_util:id(connection, ReqData),
             rabbit_mgmt_util:range_ceil(ReqData))
    of
        not_found ->
            not_found;
        Connection ->
            VHost = rabbit_mgmt_util:id(vhost, ReqData),
            case
                {rabbit_stream_management_utils:is_stream_connection(Connection),
                 same_vhost(VHost, Connection)}
            of
                {true, true} ->
                    Connection;
                _ ->
                    not_found
            end
    end.

same_vhost(Vhost, Connection) ->
    case lists:keyfind(vhost, 1, Connection) of
        {vhost, Vhost} ->
            true;
        _ ->
            false
    end.

force_close_connection(ReqData, Pid) ->
    Reason =
        case cowboy_req:header(<<"x-reason">>, ReqData) of
            undefined ->
                "Closed via management plugin";
            V ->
                binary_to_list(V)
        end,
    gen_server:call(Pid, {shutdown, Reason}, infinity),
    ok.
