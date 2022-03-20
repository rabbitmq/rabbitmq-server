%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This original One True Health Check™ has been deprecated as too coarse-grained,
%% intrusive and prone to false positives under load.
-module(rabbit_mgmt_wm_healthchecks).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

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
    Node = node0(ReqData),
    Timeout = case cowboy_req:header(<<"timeout">>, ReqData) of
                  undefined -> 70000;
                  Val       -> list_to_integer(binary_to_list(Val))
              end,
    case rabbit_health_check:node(Node, Timeout) of
        ok ->
            rabbit_mgmt_util:reply([{status, ok}], ReqData, Context);
        {badrpc, timeout} ->
            ErrMsg = rabbit_mgmt_format:print("node ~p health check timed out", [Node]),
            failure(ErrMsg, ReqData, Context);
        {badrpc, Err} ->
            failure(rabbit_mgmt_format:print("~p", Err), ReqData, Context);
        {error_string, Err} ->
            S = rabbit_mgmt_format:escape_html_tags(
                  rabbit_data_coercion:to_list(rabbit_mgmt_format:print(Err))),
            failure(S, ReqData, Context)
    end.

failure(Message, ReqData, Context) ->
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply([{status, failed},
                                                            {reason, Message}],
                                                           ReqData, Context),
    {stop, cowboy_req:reply(?HEALTH_CHECK_FAILURE_STATUS, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

node0(ReqData) ->
    Node = case rabbit_mgmt_util:id(node, ReqData) of
               none ->
                   node();
               Node0 ->
                   list_to_atom(binary_to_list(Node0))
           end,
    case [N || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData),
               proplists:get_value(name, N) == Node] of
        []     -> not_found;
        [_] -> Node
    end.
