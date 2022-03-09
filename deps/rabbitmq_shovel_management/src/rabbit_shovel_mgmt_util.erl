%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_mgmt_util).

-export([status/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbit_shovel_mgmt.hrl").

%% Allow users to see things in the vhosts they are authorised. But
%% static shovels do not have a vhost, so only allow admins (not
%% monitors) to see them.
filter_vhost_user(List, _ReqData, #context{user = User = #user{tags = Tags}}) ->
    VHosts = rabbit_mgmt_util:list_login_vhosts_names(User, undefined),
    [I || I <- List, case pget(vhost, I) of
                         undefined -> lists:member(administrator, Tags);
                         VHost     -> lists:member(VHost, VHosts)
                     end].

status(ReqData, Context) ->
    filter_vhost_user(
        lists:append([status(Node) || Node <- [node() | nodes()]]),
        ReqData, Context).

status(Node) ->
    case rpc:call(Node, rabbit_shovel_status, status, [], ?SHOVEL_CALLS_TIMEOUT_MS) of
        {badrpc, {'EXIT', _}} ->
            [];
        Status ->
            [format(Node, I) || I <- Status]
    end.

format(Node, {Name, Type, Info, TS}) ->
    [{node, Node}, {timestamp, format_ts(TS)}] ++
        format_name(Type, Name) ++
        format_info(Info).

format_name(static,  Name)          -> [{name,  Name},
    {type,  static}];
format_name(dynamic, {VHost, Name}) -> [{name,  Name},
    {vhost, VHost},
    {type,  dynamic}].

format_info(starting) ->
    [{state, starting}];

format_info({running, Props}) ->
    [{state, running}] ++ Props;

format_info({terminated, Reason}) ->
    [{state,  terminated},
        {reason, print("~p", [Reason])}].

format_ts({{Y, M, D}, {H, Min, S}}) ->
    print("~w-~2.2.0w-~2.2.0w ~w:~2.2.0w:~2.2.0w", [Y, M, D, H, Min, S]).

print(Fmt, Val) ->
    list_to_binary(io_lib:format(Fmt, Val)).