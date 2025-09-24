%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved. All rights reserved.
%%

-module(rabbit_peer_discovery_aws).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

-type tags() :: map().
-type filters() :: [{string(), string()}].
-type props() :: [{string(), props()}] | string().
-type path() :: [string() | integer()].

-ifdef(TEST).
-compile(export_all).
-endif.

-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(UTIL_MODULE,   rabbit_peer_discovery_util).

-define(BACKEND_CONFIG_KEY, peer_discovery_aws).

-define(CONFIG_MAPPING,
         #{
          aws_autoscaling                    => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "AWS_AUTOSCALING",
                                                   default_value = false
                                                  },
          aws_ec2_tags                       => #peer_discovery_config_entry_meta{
                                                   type          = map,
                                                   env_variable  = "AWS_EC2_TAGS",
                                                   default_value = #{}
                                                  },
          aws_access_key                     => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "AWS_ACCESS_KEY_ID",
                                                   default_value = "undefined"
                                                  },
          aws_secret_key                     => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "AWS_SECRET_ACCESS_KEY",
                                                   default_value = "undefined"
                                                  },
          aws_ec2_region                     => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "AWS_EC2_REGION",
                                                   default_value = "undefined"
                                                  },
          aws_hostname_path                 => #peer_discovery_config_entry_meta{
                                                   type          = list,
                                                   env_variable  = "AWS_HOSTNAME_PATH",
                                                   default_value = ["privateDnsName"]
                                                  },
          aws_use_private_ip                 => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "AWS_USE_PRIVATE_IP",
                                                   default_value = false
                                                  }
         }).

%%
%% API
%%

init() ->
    ?LOG_DEBUG("Peer discovery AWS: initialising..."),
    ok = application:ensure_started(inets),
    %% we cannot start this plugin yet since it depends on the rabbit app,
    %% which is in the process of being started by the time this function is called
    _ = application:load(rabbitmq_peer_discovery_common),
    rabbit_peer_discovery_httpc:maybe_configure_proxy(),
    rabbit_peer_discovery_httpc:maybe_configure_inet6().

-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} |
                      {error, Reason :: string()}.
list_nodes() ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    {ok, _} = application:ensure_all_started(rabbitmq_aws),
    ?LOG_DEBUG("Will use AWS access key of '~ts'", [get_config_key(aws_access_key, M)]),
    ok = maybe_set_region(get_config_key(aws_ec2_region, M)),
    ok = maybe_set_credentials(get_config_key(aws_access_key, M),
                               get_config_key(aws_secret_key, M)),
    case get_config_key(aws_autoscaling, M) of
        true ->
            case rabbitmq_aws_config:instance_id() of
                {ok, InstanceId} -> ?LOG_DEBUG("EC2 instance ID is determined from metadata service: ~tp", [InstanceId]),
                                    get_autoscaling_group_node_list(InstanceId, get_tags());
                _                -> {error, "Failed to determine EC2 instance ID from metadata service"}
            end;
        false ->
            get_node_list_from_tags(get_tags())
    end.

-spec supports_registration() -> boolean().

supports_registration() ->
    false.

-spec register() -> ok.
register() ->
    ok.

-spec unregister() -> ok.
unregister() ->
    ok.

-spec post_registration() -> ok | {error, Reason :: string()}.

post_registration() ->
    ok.

-spec lock(Nodes :: [node()]) ->
    {ok, {{ResourceId :: string(), LockRequesterId :: node()}, Nodes :: [node()]}} |
    {error, Reason :: string()}.

lock(Nodes) ->
    Node = node(),
    case lists:member(Node, Nodes) of
        true ->
            ?LOG_INFO("Will try to lock connecting to nodes ~tp", [Nodes]),
            LockId = rabbit_nodes:lock_id(Node),
            Retries = rabbit_nodes:lock_retries(),
            case global:set_lock(LockId, Nodes, Retries) of
                true ->
                    {ok, {LockId, Nodes}};
                false ->
                    {error, io_lib:format("Acquiring lock taking too long, bailing out after ~b retries", [Retries])}
            end;
        false ->
            %% Don't try to acquire the global lock when our own node is not discoverable by peers.
            %% We shouldn't run into this branch because our node is running and should have been discovered.
            {error, lists:flatten(io_lib:format("Local node ~ts is not part of discovered nodes ~tp", [Node, Nodes]))}
    end.

-spec unlock({{ResourceId :: string(), LockRequestedId :: atom()}, Nodes :: [atom()]}) -> 'ok'.
unlock({LockId, Nodes}) ->
  global:del_lock(LockId, Nodes),
  ok.

%%
%% Implementation
%%

-spec get_config_key(Key :: atom(), Map :: #{atom() => peer_discovery_config_value()})
                    -> peer_discovery_config_value().

get_config_key(Key, Map) ->
    ?CONFIG_MODULE:get(Key, ?CONFIG_MAPPING, Map).

-spec maybe_set_credentials(AccessKey :: string(),
                            SecretKey :: string()) -> ok.
%% @private
%% @doc Set the API credentials if they are set in configuration.
%% @end
%%
maybe_set_credentials("undefined", _) -> ok;
maybe_set_credentials(_, "undefined") -> ok;
maybe_set_credentials(AccessKey, SecretKey) ->
    ?LOG_DEBUG("Setting AWS credentials, access key: '~ts'", [AccessKey]),
    rabbitmq_aws:set_credentials(AccessKey, SecretKey).


-spec maybe_set_region(Region :: string()) -> ok.
%% @private
%% @doc Set the region from the configuration value, if it was set.
%% @end
%%
maybe_set_region("undefined") ->
    case rabbitmq_aws_config:region() of
        {ok, Region} -> maybe_set_region(Region)
    end;
maybe_set_region(Value) ->
    ?LOG_DEBUG("Setting AWS region to ~tp", [Value]),
    rabbitmq_aws:set_region(Value).

get_autoscaling_group_node_list(Instance, Tag) ->
    case get_all_autoscaling_instances([]) of
        {ok, Instances} ->
            case find_autoscaling_group(Instances, Instance) of
                {ok, Group} ->
                    ?LOG_DEBUG("Performing autoscaling group discovery, group: ~tp", [Group]),
                    Values = get_autoscaling_instances(Instances, Group, []),
                    ?LOG_DEBUG("Performing autoscaling group discovery, found instances: ~tp", [Values]),
                    case get_hostname_by_instance_ids(Values, Tag) of
                        error ->
                            Msg = "Cannot discover any nodes: DescribeInstances API call failed",
                            ?LOG_ERROR(Msg),
                            {error, Msg};
                        Names ->
                            ?LOG_DEBUG("Performing autoscaling group-based discovery, hostnames: ~tp", [Names]),
                            {ok, {[?UTIL_MODULE:node_name(N) || N <- Names], disc}}
                    end;
                error ->
                    ?LOG_WARNING("Cannot discover any nodes because no AWS "
                                       "autoscaling group could be found in "
                                       "the instance description. Make sure that this instance"
                                       " belongs to an autoscaling group.", []),
                    {ok, {[], disc}}
            end;
        _ ->
            Msg = "Cannot discover any nodes because AWS autoscaling group description API call failed",
            ?LOG_WARNING(Msg),
            {error, Msg}
    end.

get_autoscaling_instances([], _, Accum) -> Accum;
get_autoscaling_instances([H|T], Group, Accum) ->
    GroupName = proplists:get_value("AutoScalingGroupName", H),
    case GroupName == Group of
        true ->
            Node = proplists:get_value("InstanceId", H),
            get_autoscaling_instances(T, Group, lists:append([Node], Accum));
        false ->
            get_autoscaling_instances(T, Group, Accum)
    end.

get_all_autoscaling_instances(Accum) ->
    QArgs = [{"Action", "DescribeAutoScalingInstances"}, {"Version", "2011-01-01"}],
    fetch_all_autoscaling_instances(QArgs, Accum).

get_all_autoscaling_instances(Accum, 'undefined') -> {ok, Accum};
get_all_autoscaling_instances(Accum, NextToken) ->
    QArgs = [{"Action", "DescribeAutoScalingInstances"}, {"Version", "2011-01-01"},
             {"NextToken", NextToken}],
    fetch_all_autoscaling_instances(QArgs, Accum).

fetch_all_autoscaling_instances(QArgs, Accum) ->
    Path = "/?" ++ rabbitmq_aws_urilib:build_query_string(QArgs),
    case rabbitmq_aws:api_get_request("autoscaling", Path) of
        {ok, Payload} ->
            Instances = flatten_autoscaling_datastructure(Payload),
            NextToken = get_next_token(Payload),
            get_all_autoscaling_instances(lists:append(Instances, Accum), NextToken);
        {error, Reason} = Error ->
            ?LOG_ERROR("Error fetching autoscaling group instance list: ~tp", [Reason]),
            Error
    end.

flatten_autoscaling_datastructure(Value) ->
    Response = proplists:get_value("DescribeAutoScalingInstancesResponse", Value),
    Result = proplists:get_value("DescribeAutoScalingInstancesResult", Response),
    Instances = proplists:get_value("AutoScalingInstances", Result),
    [Instance || {_, Instance} <- Instances].

get_next_token(Value) ->
    Response = proplists:get_value("DescribeAutoScalingInstancesResponse", Value),
    Result = proplists:get_value("DescribeAutoScalingInstancesResult", Response),
    NextToken = proplists:get_value("NextToken", Result),
    NextToken.

-spec find_autoscaling_group(Instances :: list(), Instance :: string())
                            -> {'ok', string()} | error.
%% @private
%% @doc Attempt to find the Auto Scaling Group ID by finding the current
%%      instance in the list of instances returned by the autoscaling API
%%      endpoint.
%% @end
%%
find_autoscaling_group([], _) -> error;
find_autoscaling_group([H|T], Instance) ->
    case proplists:get_value("InstanceId", H) == Instance of
        true ->
            {ok, proplists:get_value("AutoScalingGroupName", H)};
        false ->
            find_autoscaling_group(T, Instance)
    end.

get_hostname_by_instance_ids(Instances, Tag) ->
    QArgs = build_instance_list_qargs(Instances,
                                      [{"Action", "DescribeInstances"},
                                       {"Version", "2015-10-01"}]),
    QArgs2 = lists:keysort(1, maybe_add_tag_filters(Tag, QArgs, 1)),
    Path = "/?" ++ rabbitmq_aws_urilib:build_query_string(QArgs2),
    get_hostname_names(Path).

-spec build_instance_list_qargs(Instances :: list(), Accum :: list()) -> list().
%% @private
%% @doc Build the Query args for filtering instances by InstanceID.
%% @end
%%
build_instance_list_qargs([], Accum) -> Accum;
build_instance_list_qargs([H|T], Accum) ->
    Key = "InstanceId." ++ integer_to_list(length(Accum) + 1),
    build_instance_list_qargs(T, lists:append([{Key, H}], Accum)).

-spec maybe_add_tag_filters(tags(), filters(), integer()) -> filters().
maybe_add_tag_filters(Tags, QArgs, Num) ->
    {Filters, _} =
        maps:fold(fun(Key, Value, {AccQ, AccN}) ->
                          {lists:append(
                             [{"Filter." ++ integer_to_list(AccN) ++ ".Name", "tag:" ++ Key},
                              {"Filter." ++ integer_to_list(AccN) ++ ".Value.1", Value}],
                             AccQ),
                           AccN + 1}
                  end, {QArgs, Num}, Tags),
    Filters.

-spec get_node_list_from_tags(tags()) -> {ok, {[node()], disc}}.
get_node_list_from_tags(M) when map_size(M) =:= 0 ->
    ?LOG_WARNING("Cannot discover any nodes because AWS tags are not configured!", []),
    {ok, {[], disc}};
get_node_list_from_tags(Tags) ->
    {ok, {[?UTIL_MODULE:node_name(N) || N <- get_hostname_by_tags(Tags)], disc}}.

-spec get_hostname_name_from_reservation_set(props(), [string()]) -> [string()].
get_hostname_name_from_reservation_set([], Accum) -> Accum;
get_hostname_name_from_reservation_set([{"item", RI}|T], Accum) ->
    InstancesSet = proplists:get_value("instancesSet", RI),
    Items = [Item || {"item", Item} <- InstancesSet],
    HostnamePath = get_hostname_path(),
    Hostnames = [get_hostname(HostnamePath, Item) || Item <- Items],
    Hostnames2 = [Name || Name <- Hostnames, Name =/= ""],
    get_hostname_name_from_reservation_set(T, Accum ++ Hostnames2).

get_hostname_names(Path) ->
    case rabbitmq_aws:api_get_request("ec2", Path) of
        {ok, Payload} ->
            Response = proplists:get_value("DescribeInstancesResponse", Payload),
            ReservationSet = proplists:get_value("reservationSet", Response),
            get_hostname_name_from_reservation_set(ReservationSet, []);
        {error, Reason} ->
            ?LOG_ERROR("Error fetching node list via EC2 API, request path: ~ts, error: ~tp", [Path, Reason]),
            error
    end.

get_hostname_by_tags(Tags) ->
    QArgs = [{"Action", "DescribeInstances"}, {"Version", "2015-10-01"}],
    QArgs2 = lists:keysort(1, maybe_add_tag_filters(Tags, QArgs, 1)),
    Path = "/?" ++ rabbitmq_aws_urilib:build_query_string(QArgs2),
    case get_hostname_names(Path) of
        error ->
            ?LOG_WARNING("Cannot discover any nodes because AWS "
                               "instance description with tags ~tp failed", [Tags]),
            [];
        Names ->
            Names
    end.

-spec get_hostname_path() -> path().
get_hostname_path() ->
    UsePrivateIP = get_config_key(aws_use_private_ip, ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY)),
    HostnamePath = get_config_key(aws_hostname_path, ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY)),
    FinalPath = case HostnamePath of
        ["privateDnsName"] when UsePrivateIP -> ["privateIpAddress"];
        P -> P
    end,
    ?LOG_DEBUG("AWS peer discovery using hostname path: ~tp", [FinalPath]),
    FinalPath.

-spec get_hostname(path(), props()) -> string().
get_hostname(Path, Props) ->
    List = lists:foldl(fun get_value/2, Props, Path),
    case io_lib:latin1_char_list(List) of
        true -> List;
        _ -> ""
    end.

-spec get_value(string()|integer(), props()) -> props().
get_value(_, []) ->
    [];
get_value(Key, Props) when is_integer(Key) ->
    {"item", Props2} = lists:nth(Key, Props),
    Props2;
get_value(Key, Props) ->
    Value = proplists:get_value(Key, Props),
    sort_ec2_hostname_path_set_members(Key, Value).

%% Sort AWS API responses for consistent ordering
-spec sort_ec2_hostname_path_set_members(string(), any()) -> any().
sort_ec2_hostname_path_set_members("networkInterfaceSet", NetworkInterfaces) when is_list(NetworkInterfaces) ->
    lists:sort(fun({"item", A}, {"item", B}) -> device_index(A) =< device_index(B) end, NetworkInterfaces);
sort_ec2_hostname_path_set_members("privateIpAddressesSet", PrivateIpAddresses) when is_list(PrivateIpAddresses) ->
    lists:sort(fun({"item", A}, {"item", B}) -> is_primary(A) >= is_primary(B) end, PrivateIpAddresses);
sort_ec2_hostname_path_set_members(_, Value) ->
    Value.

%% Extract deviceIndex from network interface attachment
-spec device_index(props()) -> integer().
device_index(Interface) ->
    Attachment = proplists:get_value("attachment", Interface),
    case proplists:get_value("deviceIndex", Attachment) of
        DeviceIndex when is_list(DeviceIndex) ->
            {Int, []} = string:to_integer(DeviceIndex),
            Int;
        DeviceIndex when is_integer(DeviceIndex) ->
            DeviceIndex
    end.

%% Extract primary flag from private IP address
-spec is_primary(props()) -> boolean().
is_primary(IpAddress) ->
    case proplists:get_value("primary", IpAddress) of
        "true" -> true;
        _ -> false
    end.

-spec get_tags() -> tags().
get_tags() ->
    Tags = get_config_key(aws_ec2_tags, ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY)),
    case Tags of
        Value when is_list(Value) ->
            maps:from_list(Value);
        _ -> Tags
    end.
