%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_connection_sessions).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
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

resource_exists(ReqData, Context) ->
    case conn(ReqData) of
        not_found ->
            {false, ReqData, Context};
        _Conn ->
            {true, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    Conn = conn(ReqData),
    case proplists:get_value(protocol, Conn) of
        {1, 0} ->
            ConnPid = proplists:get_value(pid, Conn),
            try rabbit_amqp_reader:info(ConnPid, [session_pids]) of
                [{session_pids, Pids}] ->
                    rabbit_mgmt_util:reply_list(session_infos(Pids),
                                                ["channel_number"],
                                                ReqData,
                                                Context)
            catch Type:Reason0 ->
                      Reason = unicode:characters_to_binary(
                                 lists:flatten(
                                   io_lib:format(
                                     "failed to get sessions for connection ~p: ~s ~tp",
                                     [ConnPid, Type, Reason0]))),
                      rabbit_mgmt_util:internal_server_error(Reason, ReqData, Context)
            end;
        _ ->
            rabbit_mgmt_util:bad_request(<<"connection does not use AMQP 1.0">>,
                                         ReqData,
                                         Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_user(ReqData, Context, conn(ReqData)).

%%--------------------------------------------------------------------

conn(Req) ->
    case rabbit_connection_tracking:lookup(rabbit_mgmt_util:id(connection, Req)) of
        #tracked_connection{name = Name,
                            pid = Pid,
                            protocol = Protocol,
                            username = Username} ->
            [{name, Name},
             {pid, Pid},
             {protocol, Protocol},
             {user, Username}];
        not_found ->
            not_found
    end.

session_infos(Pids) ->
    lists:filtermap(
      fun(Pid) ->
              case rabbit_amqp_session:info(Pid) of
                  {ok, Infos} ->
                      {true, Infos};
                  {error, Reason} ->
                      rabbit_log:warning("failed to get infos for session ~p: ~tp",
                                         [Pid, Reason]),
                      false
              end
      end, Pids).
