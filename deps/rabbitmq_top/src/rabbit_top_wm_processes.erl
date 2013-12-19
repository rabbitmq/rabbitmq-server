%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is VMware, Inc.
%%  Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_top_wm_processes).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    Sort = case wrq:get_qs_value("sort", ReqData) of
               undefined -> reduction_delta;
               Str       -> list_to_atom(Str)
           end,
    Node = b2a(rabbit_mgmt_util:id(node, ReqData)),
    Order = case wrq:get_qs_value("sort_reverse", ReqData) of
                "true" -> asc;
                _      -> desc
            end,
    rabbit_mgmt_util:reply(procs(Node, Sort, Order), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

b2a(B) -> list_to_atom(binary_to_list(B)).

procs(Node, Sort, Order) ->
    [fmt(P) || P <- rabbit_top_worker:procs(Node, Sort, Order, 20)].

fmt(Info) ->
    {pid, Pid} = lists:keyfind(pid, 1, Info),
    Info1 = lists:keydelete(pid, 1, Info),
    [{pid,  rabbit_top_util:fmt(Pid)},
     {name, rabbit_top_util:obtain_name(Pid)} | Info1].
