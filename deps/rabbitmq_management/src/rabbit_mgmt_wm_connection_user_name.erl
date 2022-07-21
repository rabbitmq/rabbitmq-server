%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_connection_user_name).

-export([init/2, resource_exists/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2, conn/1]).
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
    {[<<"HEAD">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    case conn(ReqData) of
        []    -> {false, ReqData, Context};
        _List -> {true, ReqData, Context}
    end.

delete_resource(ReqData, Context) ->
    case conn(ReqData) of
        []        -> ok;
        List      -> [close_single_connection(Conn, ReqData)|| Conn <- List]
    end,
    {true, ReqData, Context}.

close_single_connection(Data, ReqData) ->
    
    case Data of
        #tracked_connection{name = Name, pid = Pid, username = Username, type = Type} ->
            Conn =  [{name, Name}, {pid, Pid}, {user, Username}, {type, Type}],

            case proplists:get_value(pid, Conn) of
                 undefined -> ok;
 
                Pid when is_pid(Pid) ->
                     force_close_connection(ReqData, Conn, Pid)
                 end;

        not_found ->
            not_found
    end.

is_authorized(ReqData, Context) ->
    try
        rabbit_mgmt_util:is_authorized_user(ReqData, Context, conn(ReqData))
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

%%--------------------------------------------------------------------

conn(ReqData) ->
    rabbit_connection_tracking:list_by_username(rabbit_mgmt_util:id(username, ReqData)).

force_close_connection(ReqData, Conn, Pid) ->
    Reason = case cowboy_req:header(<<"x-reason">>, ReqData) of
                 undefined -> "Closed via management plugin";
                 V         -> binary_to_list(V)
             end,
            case proplists:get_value(type, Conn) of
                direct  -> amqp_direct_connection:server_close(Pid, 320, Reason);
                network -> rabbit_networking:close_connection(Pid, Reason);
                _       ->
                    % best effort, this will work for connections to the stream plugin
                    gen_server:call(Pid, {shutdown, Reason}, infinity)
            end,
    ok.
