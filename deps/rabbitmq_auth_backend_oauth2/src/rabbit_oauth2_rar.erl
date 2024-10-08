%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

% Rich Authorization Request
-module(rabbit_oauth2_rar).

-include("oauth2.hrl").
-import(uaa_jwt, [get_scope/1, set_scope/2]).

-export([extract_scopes_from_rich_auth_request/2, has_rich_auth_request_scopes/1]).

-define(AUTHORIZATION_DETAILS_CLAIM, <<"authorization_details">>).
-define(RAR_ACTIONS_FIELD, <<"actions">>).
-define(RAR_LOCATIONS_FIELD, <<"locations">>).
-define(RAR_TYPE_FIELD, <<"type">>).

-define(RAR_CLUSTER_LOCATION_ATTRIBUTE, <<"cluster">>).
-define(RAR_VHOST_LOCATION_ATTRIBUTE, <<"vhost">>).
-define(RAR_QUEUE_LOCATION_ATTRIBUTE, <<"queue">>).
-define(RAR_EXCHANGE_LOCATION_ATTRIBUTE, <<"exchange">>).
-define(RAR_ROUTING_KEY_LOCATION_ATTRIBUTE, <<"routing-key">>).
-define(RAR_LOCATION_ATTRIBUTES, [
        ?RAR_CLUSTER_LOCATION_ATTRIBUTE, 
        ?RAR_VHOST_LOCATION_ATTRIBUTE,
        ?RAR_QUEUE_LOCATION_ATTRIBUTE, 
        ?RAR_EXCHANGE_LOCATION_ATTRIBUTE, 
        ?RAR_ROUTING_KEY_LOCATION_ATTRIBUTE]).

-define(RAR_ALLOWED_TAG_VALUES, [
        <<"monitoring">>, 
        <<"administrator">>, 
        <<"management">>, 
        <<"policymaker">> ]).
-define(RAR_ALLOWED_ACTION_VALUES, [
        <<"read">>, 
        <<"write">>, 
        <<"configure">>, 
        <<"monitoring">>,
        <<"administrator">>, 
        <<"management">>, 
        <<"policymaker">> ]).

-spec has_rich_auth_request_scopes(Payload::map()) -> boolean().
has_rich_auth_request_scopes(Payload) ->
    maps:is_key(?AUTHORIZATION_DETAILS_CLAIM, Payload).

-spec extract_scopes_from_rich_auth_request(ResourceServer :: resource_server(),
        Payload :: map()) -> map().
%% https://oauth.net/2/rich-authorization-requests/
extract_scopes_from_rich_auth_request(ResourceServer,
        #{?AUTHORIZATION_DETAILS_CLAIM := Permissions} = Payload) ->
    ResourceServerType = ResourceServer#resource_server.resource_server_type,

    FilteredPermissionsByType = lists:filter(fun(P) ->
      is_recognized_permission(P, ResourceServerType) end, Permissions),
    AdditionalScopes = map_rich_auth_permissions_to_scopes(
        ResourceServer#resource_server.id, FilteredPermissionsByType),

    ExistingScopes = get_scope(Payload),
    set_scope(AdditionalScopes ++ ExistingScopes, Payload).

put_location_attribute(Attribute, Map) ->
    put_attribute(binary:split(Attribute, <<":">>, [global, trim_all]), Map).

put_attribute([Key, Value | _], Map) ->
    case lists:member(Key, ?RAR_LOCATION_ATTRIBUTES) of
        true -> maps:put(Key, Value, Map);
        false -> Map
    end;
put_attribute([_|_], Map) -> Map.


% convert [ <<"cluster:A">>, <<"vhost:B" >>, <<"A">>, <<"unknown:C">> ] to #{ <<"cluster">> : <<"A">>, <<"vhost">> : <<"B">> }
% filtering out non-key-value-pairs and keys which are not part of LOCATION_ATTRIBUTES
convert_attribute_list_to_attribute_map(L) ->
    convert_attribute_list_to_attribute_map(L, #{}).
convert_attribute_list_to_attribute_map([H|L],Map) when is_binary(H) ->
    convert_attribute_list_to_attribute_map(L, put_location_attribute(H,Map));
convert_attribute_list_to_attribute_map([], Map) -> Map.

build_permission_resource_path(Map) ->
    Vhost = maps:get(?RAR_VHOST_LOCATION_ATTRIBUTE, Map, <<"*">>),
    Resource = maps:get(?RAR_QUEUE_LOCATION_ATTRIBUTE, Map,
        maps:get(?RAR_EXCHANGE_LOCATION_ATTRIBUTE, Map, <<"*">>)),
    RoutingKey = maps:get(?RAR_ROUTING_KEY_LOCATION_ATTRIBUTE, Map, <<"*">>),

    <<Vhost/binary,"/",Resource/binary,"/",RoutingKey/binary>>.

map_locations_to_permission_resource_paths(ResourceServerId, L) ->
    Locations = case L of
        undefined -> [];
        LocationsAsList when is_list(LocationsAsList) ->
            lists:map(fun(Location) -> convert_attribute_list_to_attribute_map(
                binary:split(Location,<<"/">>,[global,trim_all])) end, LocationsAsList);
        LocationsAsBinary when is_binary(LocationsAsBinary) ->
            [convert_attribute_list_to_attribute_map(
                binary:split(LocationsAsBinary,<<"/">>,[global,trim_all]))]
        end,

    FilteredLocations = lists:filtermap(fun(L2) ->
        case cluster_matches_resource_server_id(L2, ResourceServerId) and
          legal_queue_and_exchange_values(L2) of
            true -> { true, build_permission_resource_path(L2) };
            false -> false
        end end, Locations),

    FilteredLocations.

cluster_matches_resource_server_id(#{?RAR_CLUSTER_LOCATION_ATTRIBUTE := Cluster},
  ResourceServerId) ->
      wildcard:match(ResourceServerId, Cluster);

cluster_matches_resource_server_id(_,_) ->
    false.

legal_queue_and_exchange_values(#{?RAR_QUEUE_LOCATION_ATTRIBUTE := Queue,
    ?RAR_EXCHANGE_LOCATION_ATTRIBUTE := Exchange}) ->
        case Queue of
            <<>> ->
                case Exchange of
                    <<>> -> true;
                    _ -> false
                end;
            _ ->
                case Exchange of
                    Queue -> true;
                    _ -> false
                end
        end;
legal_queue_and_exchange_values(_) -> true.

map_rich_auth_permissions_to_scopes(ResourceServerId, Permissions) ->
    map_rich_auth_permissions_to_scopes(ResourceServerId, Permissions, []).
map_rich_auth_permissions_to_scopes(_, [], Acc) -> Acc;
map_rich_auth_permissions_to_scopes(ResourceServerId,
  [ #{?RAR_ACTIONS_FIELD := Actions, ?RAR_LOCATIONS_FIELD := Locations }  | T ], Acc) ->
    ResourcePaths = map_locations_to_permission_resource_paths(ResourceServerId, Locations),
    case ResourcePaths of
        [] -> map_rich_auth_permissions_to_scopes(ResourceServerId, T, Acc);
        _ ->
            Scopes = case Actions of
                undefined -> [];
                ActionsAsList when is_list(ActionsAsList) ->
                    build_scopes(ResourceServerId,
                        skip_unknown_actions(ActionsAsList), ResourcePaths);
                ActionsAsBinary when is_binary(ActionsAsBinary) ->
                    build_scopes(ResourceServerId,
                        skip_unknown_actions([ActionsAsBinary]), ResourcePaths)
            end,
            map_rich_auth_permissions_to_scopes(ResourceServerId, T, Acc ++ Scopes)
    end.

skip_unknown_actions(Actions) ->
    lists:filter(fun(A) -> lists:member(A, ?RAR_ALLOWED_ACTION_VALUES) end, Actions).

produce_list_of_user_tag_or_action_on_resources(ResourceServerId, ActionOrUserTag, Locations) ->
    case lists:member(ActionOrUserTag, ?RAR_ALLOWED_TAG_VALUES) of
        true -> [<< ResourceServerId/binary, ".tag:", ActionOrUserTag/binary >>];
        _ -> build_scopes_for_action(ResourceServerId, ActionOrUserTag, Locations, [])
    end.

build_scopes_for_action(ResourceServerId, Action, [Location|Locations], Acc) ->
    Scope = << ResourceServerId/binary, ".", Action/binary, ":", Location/binary >>,
    build_scopes_for_action(ResourceServerId, Action, Locations, [ Scope | Acc ] );
build_scopes_for_action(_, _, [], Acc) -> Acc.

build_scopes(ResourceServerId, Actions, Locations) ->
    lists:flatmap(fun(Action) ->
        produce_list_of_user_tag_or_action_on_resources(ResourceServerId,
            Action, Locations) end, Actions).

is_recognized_permission(#{?RAR_ACTIONS_FIELD := _, ?RAR_LOCATIONS_FIELD:= _ ,
        ?RAR_TYPE_FIELD := Type }, ResourceServerType) ->
    case ResourceServerType of
        <<>> -> false;
        V when V == Type -> true;
        _ -> false
    end;
is_recognized_permission(_, _) -> false.
