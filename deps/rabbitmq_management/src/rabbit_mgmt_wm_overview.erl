%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_overview).

-export([init/2]).
-export([to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2, pget/3]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context = #context{user = User = #user{tags = Tags}}) ->
    RatesMode = rabbit_mgmt_agent_config:get_env(rates_mode),
    SRP = get_sample_retention_policies(),
    %% NB: this duplicates what's in /nodes but we want a global idea
    %% of this. And /nodes is not accessible to non-monitor users.
    ExchangeTypes = lists:sort(
        fun(ET1, ET2) ->
            proplists:get_value(name, ET1, none)
            =<
            proplists:get_value(name, ET2, none)
        end,
        rabbit_mgmt_external_stats:list_registry_plugins(exchange)),
    Overview0 = [{management_version,        version(rabbitmq_management)},
                 {rates_mode,                RatesMode},
                 {sample_retention_policies, SRP},
                 {exchange_types,            ExchangeTypes},
                 {product_version,           list_to_binary(rabbit:product_version())},
                 {product_name,              list_to_binary(rabbit:product_name())},
                 {rabbitmq_version,          list_to_binary(rabbit:base_product_version())},
                 {cluster_name,              rabbit_nodes:cluster_name()},
                 {erlang_version,            erlang_version()},
                 {erlang_full_version,       erlang_full_version()},
                 {disable_stats,             rabbit_mgmt_util:disable_stats(ReqData)},
                 {enable_queue_totals,       rabbit_mgmt_util:enable_queue_totals(ReqData)}],
    try
        case rabbit_mgmt_util:disable_stats(ReqData) of
            false ->
                Range = rabbit_mgmt_util:range(ReqData),
                Overview =
                    case rabbit_mgmt_util:is_monitor(Tags) of
                        true ->
                            Overview0 ++
                                [{K, maybe_map(V)} ||
                                    {K,V} <- rabbit_mgmt_db:get_overview(Range)] ++
                                [{node,               node()},
                                 {listeners,          listeners()},
                                 {contexts,           web_contexts(ReqData)}];
                        _ ->
                            Overview0 ++
                                [{K, maybe_map(V)} ||
                                    {K, V} <- rabbit_mgmt_db:get_overview(User, Range)]
                    end,
                rabbit_mgmt_util:reply(Overview, ReqData, Context);
            true ->
                VHosts = case rabbit_mgmt_util:is_monitor(Tags) of
                             true -> rabbit_vhost:list_names();
                             _   -> rabbit_mgmt_util:list_visible_vhosts_names(User)
                         end,

                ObjectTotals = case rabbit_mgmt_util:is_monitor(Tags) of
                                   true ->
                                       [{queues, rabbit_amqqueue:count()},
                                        {exchanges, rabbit_exchange:count()},
                                        {connections, rabbit_connection_tracking:count()}];
                                   _   ->
                                       [{queues, length([Q || V <- VHosts, Q <- rabbit_amqqueue:list(V)])},
                                        {exchanges, length([X || V <- VHosts, X <- rabbit_exchange:list(V)])}]
                               end,
                Overview = Overview0 ++
                    [{node, node()},
                     {object_totals, ObjectTotals}],
                rabbit_mgmt_util:reply(Overview, ReqData, Context)
        end
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

maybe_map(L) when is_list(L) -> maps:from_list(L);
maybe_map(V)                 -> V.

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
    list_to_binary(rabbit_misc:otp_system_version()).

get_sample_retention_policies() ->
    P = rabbit_mgmt_agent_config:get_env(sample_retention_policies),
    get_sample_retention_policies(P).

get_sample_retention_policies(undefined) ->
    [{global, []}, {basic, []}, {detailed, []}];
get_sample_retention_policies(Policies) ->
    [transform_retention_policy(Pol, Policies) || Pol <- [global, basic, detailed]].

transform_retention_policy(Pol, Policies) ->
    case proplists:lookup(Pol, Policies) of
        none ->
            {Pol, []};
        {Pol, Intervals} ->
            {Pol, transform_retention_intervals(Intervals, [])}
    end.

transform_retention_intervals([], Acc) ->
    lists:sort(Acc);
transform_retention_intervals([{MaxAgeInSeconds, _}|Rest], Acc) ->
    %
    % Seconds | Interval
    % 60      | last minute
    % 600     | last 10 minutes
    % 3600    | last hour
    % 28800   | last 8 hours
    % 86400   | last day
    %
    % rabbitmq/rabbitmq-management#635
    %
    % We check for the max age in seconds to be within 10% of the value above.
    % The reason being that the default values are "bit higher" to accommodate
    % edge cases (see deps/rabbitmq_management_agent/Makefile)
    AccVal = if
                 MaxAgeInSeconds >= 0 andalso MaxAgeInSeconds =< 66 ->
                     60;
                 MaxAgeInSeconds >= 540 andalso MaxAgeInSeconds =< 660 ->
                     600;
                 MaxAgeInSeconds >= 3240 andalso MaxAgeInSeconds =< 3960 ->
                     3600;
                 MaxAgeInSeconds >= 25920 andalso MaxAgeInSeconds =< 31681 ->
                     28800;
                 MaxAgeInSeconds >= 77760 andalso MaxAgeInSeconds =< 95041 ->
                     86400;
                 true ->
                     0
             end,
    transform_retention_intervals(Rest, [AccVal|Acc]).
