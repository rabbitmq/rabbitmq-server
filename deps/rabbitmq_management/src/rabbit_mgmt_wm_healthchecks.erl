%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_mgmt_wm_healthchecks).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

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
        timeout ->
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
    rabbit_mgmt_util:reply([{status, failed},
                            {reason, Message}],
                           ReqData, Context).

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
