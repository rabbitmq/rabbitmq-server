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
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_node).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

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
    case [N || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData),
               proplists:get_value(name, N) == Node] of
        []     -> not_found;
        [Data] -> augment(ReqData, Node, Data)
    end.

augment(ReqData, Node, Data) ->
    lists:foldl(fun (Key, DataN) -> augment(Key, ReqData, Node, DataN) end,
                Data, [memory, binary]).

augment(Key, ReqData, Node, Data) ->
    case wrq:get_qs_value(atom_to_list(Key), ReqData) of
        "true" -> Res = case rpc:call(Node, rabbit_vm, Key, [], infinity) of
                            {badrpc, _} -> not_available;
                            Result      -> Result
                        end,
                  [{Key, Res} | Data];
        _      -> Data
    end.
