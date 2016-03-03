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
-module(rabbit_health_check).

-export([node/1]).

-define(NODE_HEALTH_CHECK_TIMEOUT, 70000).

-ifdef(use_specs).
-spec(node/1 :: (node()) -> 'true' | no_return()).
-endif.

%%----------------------------------------------------------------------------
%% External functions
%%----------------------------------------------------------------------------
node(Node) ->
    node_health_check(Node, is_running),
    node_health_check(Node, list_channels),
    node_health_check(Node, list_queues),
    node_health_check(Node, alarms).

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------
node_health_check(Node, is_running) ->
    node_health_check(
      Node, {rabbit, is_running, []},
      fun(true) ->
              true;
         (false) ->
              throw({node_is_ko, "rabbit application is not running", 70})
      end);
node_health_check(Node, list_channels) ->
    node_health_check(
      Node, {rabbit_channel, info_all, [[pid]]},
      fun(L) when is_list(L) ->
              true;
         (Other) ->
              ErrorMsg = io_lib:format("list_channels unexpected output: ~p",
                                       [Other]),
              throw({node_is_ko, ErrorMsg, 70})
      end);
node_health_check(Node, list_queues) ->
    node_health_check(
      Node, {rabbit_amqqueue, info_all, [[pid]]},
      fun(L) when is_list(L) ->
              true;
         (Other) ->
              ErrorMsg = io_lib:format("list_queues unexpected output: ~p",
                                       [Other]),
              throw({node_is_ko, ErrorMsg, 70})
      end);
node_health_check(Node, alarms) ->
    node_health_check(
      Node, {rabbit, status, []},
      fun(Props) ->
              case proplists:get_value(alarms, Props) of
                  [] ->
                      true;
                  Alarms ->
                      ErrorMsg = io_lib:format("resource alarm(s) in effect:~p", [Alarms]),
                      throw({node_is_ko, ErrorMsg, 70})
              end
      end).

node_health_check(Node, {M, F, A}, Fun) ->
    case rabbit_misc:rpc_call(Node, M, F, A, ?NODE_HEALTH_CHECK_TIMEOUT) of
        {badrpc, timeout} ->
            ErrorMsg = io_lib:format(
                         "health check of node ~p fails: timed out (~p ms)",
                         [Node, ?NODE_HEALTH_CHECK_TIMEOUT]),
            throw({node_is_ko, ErrorMsg, 70});
        {badrpc, nodedown} ->
            ErrorMsg = io_lib:format(
                         "health check of node ~p fails: nodedown", [Node]),
            throw({node_is_ko, ErrorMsg, 68});
        {badrpc, Reason} ->
            ErrorMsg = io_lib:format(
                         "health check of node ~p fails: ~p", [Node, Reason]),
            throw({node_is_ko, ErrorMsg, 70});
        Other ->
            Fun(Other)
    end.

    
