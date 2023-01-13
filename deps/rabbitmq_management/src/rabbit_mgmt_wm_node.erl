%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_node).

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
    {case node0(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(node0(ReqData), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

%%--------------------------------------------------------------------

node0(ReqData) ->
    Node = list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))),
    [Data] = node_data(Node, ReqData),
    augment(ReqData, Node, Data).

augment(ReqData, Node, Data) ->
    lists:foldl(fun (Key, DataN) -> augment(Key, ReqData, Node, DataN) end,
                Data, [memory, binary]).

augment(Key, ReqData, Node, Data) ->
    case rabbit_mgmt_util:qs_val(list_to_binary(atom_to_list(Key)), ReqData) of
        <<"true">> -> Res = case rpc:call(Node, rabbit_vm, Key, [], infinity) of
                            {badrpc, _} -> not_available;
                            Result      -> Result
                        end,
                  [{Key, Res} | Data];
        _      -> Data
    end.

node_data(Node, ReqData) ->
    S = rabbit_db_cluster:cli_cluster_status(),
    Nodes = proplists:get_value(nodes, S),
    Running = proplists:get_value(running_nodes, S),
    Type = find_type(Node, Nodes),
    Basic = [[{name, Node}, {running, lists:member(Node, Running)}, {type, Type}]],
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            rabbit_mgmt_db:augment_nodes(Basic, rabbit_mgmt_util:range_ceil(ReqData));
        true ->
            Basic
    end.

find_type(Node, [{Type, Nodes} | Rest]) ->
    case lists:member(Node, Nodes) of
        true -> Type;
        false -> find_type(Node, Rest)
    end.
