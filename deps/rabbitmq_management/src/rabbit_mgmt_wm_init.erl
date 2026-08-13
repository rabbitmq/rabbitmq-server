-module(rabbit_mgmt_wm_init).
-export([init/2]).
-export([content_types_provided/2, is_authorized/2, to_json/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

init(Req0, _State) ->
    Req1 = rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE),
    Req2 = rabbit_mgmt_headers:set_no_cache_headers(Req1, ?MODULE),
    {cowboy_rest, Req2, #context{}}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    SettingsJSON = get_settings_json(ReqData, Context),
    VhostsJSON = get_vhosts_json(ReqData, Context),
    
    UserTags = (Context#context.user)#user.tags,
    NodesJSON = case rabbit_mgmt_util:is_monitor(UserTags) of
        true -> get_nodes_json(ReqData, Context);
        false -> <<"null">>
    end,
    
    JSONContent = [
        "{\"settings\": ", SettingsJSON, ",\n",
        " \"vhosts\": ", VhostsJSON, ",\n",
        " \"nodes\": ", NodesJSON, "\n",
        "}\n"
    ],
    {iolist_to_binary(JSONContent), ReqData, Context}.

get_settings_json(ReqData, _Context) ->
    Settings0 = [
        {management_version,        proplists:get_value(management_version, rabbit_mgmt_features:get_product_info())},
        {product_info,              rabbit_mgmt_features:get_product_info()},
        {cluster_name,              rabbit_nodes:cluster_name()}
    ] ++ rabbit_mgmt_features:get_settings(ReqData),

    rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(Settings0)).

get_vhosts_json(_ReqData, Context) ->
    VHosts = rabbit_mgmt_util:list_visible_vhosts(Context#context.user),
    rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(VHosts)).

get_nodes_json(ReqData, _Context) ->
    Nodes = rabbit_mgmt_wm_nodes:all_nodes(ReqData),
    rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(Nodes)).
