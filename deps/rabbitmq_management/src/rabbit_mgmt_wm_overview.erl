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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_overview).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-import(rabbit_misc, [pget/2, pget/3]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context = #context{user = User = #user{tags = Tags}}) ->
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    %% NB: this duplicates what's in /nodes but we want a global idea
    %% of this. And /nodes is not accessible to non-monitor users.
    ExchangeTypes = rabbit_mgmt_external_stats:list_registry_plugins(exchange),
    Overview0 = [{management_version,  version(rabbitmq_management)},
                 {statistics_level,    StatsLevel},
                 {exchange_types,      ExchangeTypes},
                 {rabbitmq_version,    version(rabbit)},
                 {erlang_version,      erl_version(otp_release)},
                 {erlang_full_version, erl_version(system_version)}],
    Range = rabbit_mgmt_util:range(ReqData),
    Overview =
        case rabbit_mgmt_util:is_monitor(Tags) of
            true ->
                Overview0 ++
                    rabbit_mgmt_db:get_overview(Range) ++
                    [{node,               node()},
                     {statistics_db_node, stats_db_node()},
                     {listeners,          listeners()},
                     {contexts,           rabbit_web_dispatch_contexts()}];
            _ ->
                Overview0 ++
                    rabbit_mgmt_db:get_overview(User, Range)
        end,
    rabbit_mgmt_util:reply(Overview, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

stats_db_node() ->
    case global:whereis_name(rabbit_mgmt_db) of
        undefined -> not_running;
        Pid       -> node(Pid)
    end.

version(App) ->
    {ok, V} = application:get_key(App, vsn),
    list_to_binary(V).

listeners() ->
    rabbit_mgmt_util:sort_list(
      [rabbit_mgmt_format:listener(L)
       || L <- rabbit_networking:active_listeners()],
      ["protocol", "port", "node"] ).

%%--------------------------------------------------------------------

rabbit_web_dispatch_contexts() ->
    rabbit_mgmt_util:sort_list(
      lists:append(
        [rabbit_web_dispatch_contexts(N) || N <- rabbit_mgmt_wm_nodes:all_nodes()]),
      ["description", "port", "node"]).

rabbit_web_dispatch_contexts(N) ->
    [[{node, pget(name, N)} | C] || C <- pget(contexts, N, [])].

erl_version(K) ->
    list_to_binary(string:strip(erlang:system_info(K), both, $\n)).
