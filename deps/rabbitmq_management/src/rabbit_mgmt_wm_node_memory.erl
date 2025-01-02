%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_node_memory).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
%%--------------------------------------------------------------------

init(Req, [Mode]) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {rabbit_mgmt_nodes:node_exists(ReqData), ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    rabbit_mgmt_util:reply(augment(Mode, ReqData), ReqData, {Mode, Context}).

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD, C} = rabbit_mgmt_util:is_authorized_monitor(ReqData, Context),
    {Res, RD, {Mode, C}}.

%%--------------------------------------------------------------------
get_node(ReqData) ->
    list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))).

augment(Mode, ReqData) ->
    Node = get_node(ReqData),
    case rabbit_mgmt_nodes:node_exists(ReqData) of
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
    {value, {total, Totals}, Rest} = lists:keytake(total, 1, Result),
    Total = proplists:get_value(rss, Totals),
    [{total, 100} | [{K, percentage(V, Total)} || {K, V} <- Rest,
                                                  K =/= strategy]].

percentage(Part, Total) ->
    case round((Part/Total) * 100) of
        0 when Part =/= 0 ->
            1;
        Int ->
            Int
    end.
