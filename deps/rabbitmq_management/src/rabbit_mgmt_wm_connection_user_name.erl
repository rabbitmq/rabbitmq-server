%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_connection_user_name).

-export([init/2, to_json/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, _Username, UserConns} = list_user_connections(ReqData),
    FilteredConns = rabbit_mgmt_util:filter_tracked_conn_list(UserConns, ReqData, Context),
    rabbit_mgmt_util:reply_list_or_paginate(FilteredConns, ReqData, Context).

delete_resource(ReqData, Context) ->
    delete_resource(list_user_connections(ReqData), ReqData, Context).

delete_resource({ok, _Username, []}, ReqData, Context) ->
    {true, ReqData, Context};
delete_resource({ok, Username, UserConns}, ReqData, Context) ->
    ok = close_user_connections(UserConns, Username, ReqData),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    try
        UserConns = list_user_connections(ReqData),
        rabbit_mgmt_util:is_authorized_user(ReqData, Context, UserConns)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

%%--------------------------------------------------------------------

list_user_connections(ReqData) ->
    Username = rabbit_mgmt_util:id(username, ReqData),
    UserConns = rabbit_connection_tracking:list_of_user(Username),
    {ok, Username, UserConns}.

close_user_connections([], _Username, _ReqData) ->
    ok;
close_user_connections([Conn | Rest], Username, ReqData) ->
    ok = close_user_connection(Conn, Username, ReqData),
    close_user_connections(Rest, Username, ReqData).

close_user_connection(#tracked_connection{name = Name, pid = Pid, username = Username, type = Type}, Username, ReqData) when is_pid(Pid) ->
    Conn = [{name, Name}, {pid, Pid}, {user, Username}, {type, Type}],
    force_close_connection(ReqData, Conn, Pid);
close_user_connection(#tracked_connection{pid = undefined}, _Username, _ReqData) ->
    ok;
close_user_connection(UnexpectedConn, Username, _ReqData) ->
    rabbit_log:debug("~tp Username: ~tp", [?MODULE, Username]),
    rabbit_log:debug("~tp unexpected connection: ~tp", [?MODULE, UnexpectedConn]),
    ok.

force_close_connection(ReqData, Conn, Pid) ->
    Reason = case cowboy_req:header(<<"x-reason">>, ReqData) of
                 undefined -> "Closed via management plugin";
                 V -> binary_to_list(V)
             end,
    case proplists:get_value(type, Conn) of
        direct ->
            amqp_direct_connection:server_close(Pid, 320, Reason);
        network ->
            rabbit_networking:close_connection(Pid, Reason);
        _ ->
            % best effort, this will work for connections to the stream plugin
            gen_server:cast(Pid, {shutdown, Reason})
    end,
    ok.
