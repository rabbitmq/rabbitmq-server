%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_node_memory_ets).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, [Mode]) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {node_exists(ReqData, get_node(ReqData)), ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    rabbit_mgmt_util:reply(augment(Mode, ReqData), ReqData, {Mode, Context}).

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD, C} = rabbit_mgmt_util:is_authorized_monitor(ReqData, Context),
    {Res, RD, {Mode, C}}.

%%--------------------------------------------------------------------
get_node(ReqData) ->
    list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))).

get_filter(ReqData) ->
    case rabbit_mgmt_util:id(filter, ReqData) of
        none                        -> all;
        <<"management">>            -> rabbit_mgmt_storage;
        Other when is_binary(Other) -> list_to_atom(binary_to_list(Other));
        _                           -> all
    end.

node_exists(ReqData, Node) ->
    case [N || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData),
               proplists:get_value(name, N) == Node] of
        [] -> false;
        [_] -> true
    end.

augment(Mode, ReqData) ->
    Node = get_node(ReqData),
    Filter = get_filter(ReqData),
    case node_exists(ReqData, Node) of
        false ->
            not_found;
        true ->
            case rpc:call(Node, rabbit_vm, ets_tables_memory,
                          [Filter], infinity) of
                {badrpc, _} -> [{ets_tables_memory, not_available}];
                []          -> [{ets_tables_memory, no_tables}];
                Result      -> [{ets_tables_memory, format(Mode, Result)}]
            end
    end.

format(absolute, Result) ->
    Total = lists:sum([V || {_K,V} <- Result]),
    [{total, Total} | Result];
format(relative, Result) ->
    Total = lists:sum([V || {_K,V} <- Result]),
    [{total, 100} | [{K, percentage(V, Total)} || {K, V} <- Result]].

percentage(Part, Total) ->
    case round((Part/Total) * 100) of
        0 when Part =/= 0 ->
            1;
        Int ->
            Int
    end.
