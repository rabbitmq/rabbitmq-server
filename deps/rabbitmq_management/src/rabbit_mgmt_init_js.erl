-module(rabbit_mgmt_init_js).
-export([init/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

init(Req0, State) ->
    case rabbit_mgmt_util:is_authorized_management(Req0, State) of
        {true, Req1, Context} ->
            Req2 = rabbit_mgmt_headers:set_no_cache_headers(
                       rabbit_mgmt_headers:set_common_permission_headers(Req1, ?MODULE), ?MODULE),
            
            SettingsJSON = get_settings_json(Req2, Context),
            VhostsJSON = get_vhosts_json(Req2, Context),
            
            NodesVar = case rabbit_mgmt_util:is_monitor(Context) of
                true -> ["  window.app_nodes = ", get_nodes_json(Req2, Context), ";\n"];
                false -> ""
            end,
            
            JSContent = [
                "export function initialize(user) {\n",
                "  window.app_settings = ", SettingsJSON, ";\n",
                "  window.app_vhosts = ", VhostsJSON, ";\n",
                NodesVar,
                "}\n"
            ],
            
            Req3 = cowboy_req:reply(200, #{<<"content-type">> => <<"application/javascript; charset=utf-8">>},
                                    iolist_to_binary(JSContent), Req2),
            {ok, Req3, State};
        {false, Req1, _Context} ->
            Req2 = cowboy_req:reply(401, #{}, <<"Not authorized">>, Req1),
            {ok, Req2, State}
    end.

get_settings_json(ReqData, Context) ->
    Overview0 = [
        {management_version,        version(rabbitmq_management)},
        {rates_mode,                rabbit_mgmt_util:rates_mode(ReqData)},
        {sample_retention_policies, rabbit_mgmt_wm_overview:get_sample_retention_policies()},
        {exchange_types,            rabbit_mgmt_wm_overview:exchange_types()},
        {product_info, [
            {product_name,        version(rabbit)},
            {product_version,     version(rabbit)},
            {rabbitmq_version,    version(rabbit)},
            {management_version,  version(rabbitmq_management)},
            {erlang_version,      rabbit_mgmt_wm_overview:erlang_version()},
            {erlang_full_version, rabbit_mgmt_wm_overview:erlang_full_version()}
        ]},
        {sessions, rabbit_mgmt_features:get_sessions_settings()},
        {definitions, rabbit_mgmt_features:get_definitions_settings()},
        {cluster_name,              rabbit_nodes:cluster_name()},
        {message_rates,             rabbit_mgmt_util:message_rates(ReqData)},
        {cluster_tags,              rabbit_mgmt_wm_overview:cluster_tags()},
        {node_tags,                 rabbit_mgmt_wm_overview:node_tags()},
        {disable_stats,             rabbit_mgmt_util:disable_stats(ReqData)},
        {default_queue_type,        rabbit_queue_type:default_alias()},
        {is_op_policy_updating_enabled, not rabbit_mgmt_features:is_op_policy_updating_disabled()},
        {enable_queue_totals,       rabbit_mgmt_util:enable_queue_totals(ReqData)}
    ],

    Overview1 = case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            Range = rabbit_mgmt_util:range(ReqData),
            case rabbit_mgmt_util:is_monitor(Context) of
                true ->
                    Overview0 ++
                        [{K, maybe_map(V)} || {K,V} <- rabbit_mgmt_db:get_overview(Range)] ++
                        [{node, node()},
                         {listeners, rabbit_mgmt_wm_overview:listeners()},
                         {contexts, rabbit_mgmt_wm_overview:web_contexts(ReqData)}];
                _ ->
                    Overview0 ++
                        [{K, maybe_map(V)} || {K, V} <- rabbit_mgmt_db:get_overview(Context#context.user, Range)]
            end;
        true ->
            User = Context#context.user,
            VHosts = case rabbit_mgmt_util:is_monitor(Context) of
                         true -> rabbit_vhost:list_names();
                         _   -> rabbit_mgmt_util:list_visible_vhosts_names(User)
                     end,
            ObjectTotals = case rabbit_mgmt_util:is_monitor(Context) of
                               true ->
                                   [{queues, rabbit_amqqueue:count()},
                                    {exchanges, rabbit_exchange:count()},
                                    {connections, rabbit_connection_tracking:count()}];
                               _   ->
                                   [{queues, length([Q || V <- VHosts, Q <- rabbit_amqqueue:list(V)])},
                                    {exchanges, length([X || V <- VHosts, X <- rabbit_exchange:list(V)])}]
                           end,
            Overview0 ++ [{node, node()}, {object_totals, ObjectTotals}]
    end,
    
    rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(Overview1)).

get_vhosts_json(_ReqData, Context) ->
    VHosts = rabbit_mgmt_util:list_visible_vhosts(Context#context.user),
    rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(VHosts)).

get_nodes_json(ReqData, _Context) ->
    Nodes = rabbit_mgmt_wm_nodes:all_nodes(ReqData),
    rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(Nodes)).

%% Helpers
version(App) ->
    {ok, V} = application:get_key(App, vsn),
    list_to_binary(V).

maybe_map(L) when is_list(L) -> maps:from_list(L);
maybe_map(V)                 -> V.