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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_overview).

-export([init/3, rest_init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2, pget/3]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context = #context{user = User = #user{tags = Tags}}) ->
    RatesMode = rabbit_mgmt_agent_config:get_env(rates_mode),
    %% NB: this duplicates what's in /nodes but we want a global idea
    %% of this. And /nodes is not accessible to non-monitor users.
    ExchangeTypes = rabbit_mgmt_external_stats:list_registry_plugins(exchange),
    Overview0 = [{management_version,  version(rabbitmq_management)},
                 {rates_mode,          RatesMode},
                 {exchange_types,      ExchangeTypes},
                 {rabbitmq_version,    version(rabbit)},
                 {cluster_name,        rabbit_nodes:cluster_name()},
                 {erlang_version,      erlang_version()},
                 {erlang_full_version, erlang_full_version()}],
    try
        Range = rabbit_mgmt_util:range(ReqData),
        Overview =
            case rabbit_mgmt_util:is_monitor(Tags) of
                true ->
                    Overview0 ++
                        [{K, maybe_struct(V)} ||
                            {K,V} <- rabbit_mgmt_db:get_overview(Range)] ++
                        [{node,               node()},
                         {listeners,          listeners()},
                         {contexts,           web_contexts(ReqData)}];
                _ ->
                    Overview0 ++
                        [{K, maybe_struct(V)} ||
                            {K, V} <- rabbit_mgmt_db:get_overview(User, Range)]
            end,
        rabbit_mgmt_util:reply(Overview, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

version(App) ->
    {ok, V} = application:get_key(App, vsn),
    list_to_binary(V).

listeners() ->
    rabbit_mgmt_util:sort_list(
      [rabbit_mgmt_format:listener(L)
       || L <- rabbit_networking:active_listeners()],
      ["protocol", "port", "node"] ).

maybe_struct(L) when is_list(L) -> {struct, L};
maybe_struct(V)                 -> V.

%%--------------------------------------------------------------------

web_contexts(ReqData) ->
    rabbit_mgmt_util:sort_list(
      lists:append(
        [fmt_contexts(N) || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData)]),
      ["description", "port", "node"]).

fmt_contexts(Node) ->
    [fmt_context(Node, C) || C <- pget(contexts, Node, [])].

fmt_context(Node, C) ->
  rabbit_mgmt_format:web_context([{node, pget(name, Node)} | C]).

erlang_version() -> list_to_binary(rabbit_misc:otp_release()).

erlang_full_version() ->
    list_to_binary(string:strip(erlang:system_info(system_version), both, $\n)).
