%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_node_memory).

-export([init/3, rest_init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, [Mode]) -> {ok, Req, {Mode, #context{}}}.

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

node_exists(ReqData, Node) ->
    case [N || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData),
               proplists:get_value(name, N) == Node] of
        [] -> false;
        [_] -> true
    end.

augment(Mode, ReqData) ->
    Node = get_node(ReqData),
    case node_exists(ReqData, Node) of
        false ->
            not_found;
        true ->
            case rpc:call(Node, rabbit_vm, memory, [], infinity) of
                {badrpc, _} -> [{memory, not_available}];
                Result      -> [{memory, format(Mode, Result)}]
            end
    end.

format(absolute, Result) ->
    Result;
format(relative, Result) ->
    {[{total, Total}], Rest} = lists:splitwith(fun({Key, _}) ->
                                                       Key == total
                                               end, Result),
    [{total, 100} | [{K, percentage(V, Total)} || {K, V} <- Rest]].

percentage(Part, Total) ->
    case round((Part/Total) * 100) of
        0 when Part =/= 0 ->
            1;
        Int ->
            Int
    end.
