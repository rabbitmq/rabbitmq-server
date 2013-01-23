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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_nodes).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([all_nodes/0]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply_list(all_nodes(), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

%%--------------------------------------------------------------------

all_nodes() ->
    S = rabbit_mnesia:status(),
    Nodes = proplists:get_value(nodes, S),
    Types = proplists:get_keys(Nodes),
    Running = proplists:get_value(running_nodes, S),
    rabbit_mgmt_db:augment_nodes(
      [[{name, Node}, {type, Type}, {running, lists:member(Node, Running)}] ||
          Type <- Types, Node <- proplists:get_value(Type, Nodes)]).
