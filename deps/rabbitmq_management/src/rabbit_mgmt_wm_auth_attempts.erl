%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_auth_attempts).

-export([init/2, to_json/2, content_types_provided/2, allowed_methods/2, is_authorized/2,
         delete_resource/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

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

resource_exists(ReqData, Context) ->
    {node_exists(ReqData, get_node(ReqData)), ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(augment(ReqData), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    Node = get_node(ReqData),
    case node_exists(ReqData, Node) of
        false ->
            {false, ReqData, Context};
        true ->
            case rpc:call(Node, rabbit_core_metrics, reset_auth_attempt_metrics, [], infinity) of
                {badrpc, _} -> {false, ReqData, Context};
                ok          -> {true, ReqData, Context}
            end
    end.
%%--------------------------------------------------------------------
get_node(ReqData) ->
    list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))).

node_exists(ReqData, Node) ->
    case [N || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData),
               proplists:get_value(name, N) == Node] of
        [] -> false;
        [_] -> true
    end.

augment(ReqData) ->
    Node = get_node(ReqData),
    case node_exists(ReqData, Node) of
        false ->
            not_found;
        true ->
            case rpc:call(Node, rabbit_core_metrics, get_auth_attempts, [], infinity) of
                {badrpc, _} -> [{auth_attempts, not_available}];
                Result      -> [{auth_attempts, Result}]
            end
    end.
