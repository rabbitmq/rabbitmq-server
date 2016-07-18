%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_mgmt_wm_healthchecks).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([finish_request/2, allowed_methods/2]).
-export([encodings_provided/2]).
-export([resource_exists/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

finish_request(ReqData, Context) ->
    {ok, rabbit_mgmt_cors:set_headers(ReqData, Context), Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'OPTIONS'], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

encodings_provided(ReqData, Context) ->
    {[{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case node0(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    Node = node0(ReqData),
    try
        Timeout = case wrq:get_req_header("timeout", ReqData) of
                      undefined -> 70000;
                      Val       -> list_to_integer(Val)
                  end,
        rabbit_health_check:node(Node, Timeout),
        rabbit_mgmt_util:reply([{status, ok}], ReqData, Context)
    catch
        {node_is_ko, ErrorMsg, _ErrorCode} ->
            rabbit_mgmt_util:reply([{status, failed},
                                    {reason, rabbit_mgmt_format:print(ErrorMsg)}],
                                   ReqData, Context)
    end.

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
