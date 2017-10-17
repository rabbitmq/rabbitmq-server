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

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    Sort     = rabbit_top_util:sort_by_param(ReqData, reduction_delta),
    Node     = rabbit_data_coercion:to_atom(rabbit_mgmt_util:id(node, ReqData)),
    Order    = rabbit_top_util:sort_order_param(ReqData),
    RowCount = rabbit_top_util:row_count_param(ReqData, 20),

    rabbit_mgmt_util:reply([{node,      Node},
                            {row_count, RowCount},
                            {processes, procs(Node, Sort, Order, RowCount)}],
                           ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

procs(Node, Sort, Order, RowCount) ->
    try
        [fmt(P) || P <- rabbit_top_worker:procs(Node, Sort, Order, RowCount)]
    catch
        exit:{noproc, _} ->
            []
    end.

fmt(Info) ->
    {pid, Pid} = lists:keyfind(pid, 1, Info),
    Info1 = lists:keydelete(pid, 1, Info),
    [{pid,  rabbit_top_util:fmt(Pid)},
     {name, rabbit_top_util:obtain_name(Pid)} | Info1].
