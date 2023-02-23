%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_oauth2).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([description/0]).
-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4,
         check_topic_access/4, check_token/1, state_can_expire/0, update_state/2]).

% for testing
-export([post_process_payload/1]).

-import(rabbit_data_coercion, [to_map/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

%%
%% App environment
%%

-type app_env() :: [{atom(), any()}].

-define(APP, rabbitmq_auth_backend_oauth2).
-define(RESOURCE_SERVER_ID, resource_server_id).
%% a term defined for Rich Authorization Request tokens to identify a RabbitMQ permission
-define(RESOURCE_SERVER_TYPE, resource_server_type).
%% verify server_server_id aud field is on the aud field
-define(VERIFY_AUD, verify_aud).
%% a term used by the IdentityServer community
-define(COMPLEX_CLAIM_APP_ENV_KEY, extra_scopes_source).
%% scope aliases map "role names" to a set of scopes
-define(SCOPE_MAPPINGS_APP_ENV_KEY, scope_aliases).
%% list of JWT claims (such as <<"sub">>) used to determine the username
-define(PREFERRED_USERNAME_CLAIMS, preferred_username_claims).
-define(DEFAULT_PREFERRED_USERNAME_CLAIMS, [<<"sub">>, <<"client_id">>]).

%%
%% Key JWT fields
%%

-define(AUD_JWT_FIELD, <<"aud">>).
-define(SCOPE_JWT_FIELD, <<"scope">>).
%%
%% API
%%

description() ->
    [{name, <<"OAuth 2">>},
     {description, <<"Performs authentication and authorisation using JWT tokens and OAuth 2 scopes">>}].

%%--------------------------------------------------------------------

user_login_authentication(Username, AuthProps) ->
    case authenticate(Username, AuthProps) of
	{refused, Msg, Args} = AuthResult ->
	    rabbit_log:debug(Msg, Args),
	    AuthResult;
	_ = AuthResult ->
	    AuthResult
    end.

user_login_authorization(Username, AuthProps) ->
    case authenticate(Username, AuthProps) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

check_vhost_access(#auth_user{impl = DecodedTokenFun},
                   VHost, _AuthzData) ->
    with_decoded_token(DecodedTokenFun(),
        fun() ->
            Scopes      = get_scopes(DecodedTokenFun()),
            ScopeString = rabbit_oauth2_scope:concat_scopes(Scopes, ","),
            rabbit_log:debug("Matching virtual host '~s' against the following scopes: ~s", [VHost, ScopeString]),
            rabbit_oauth2_scope:vhost_access(VHost, Scopes)
        end).

check_resource_access(#auth_user{impl = DecodedTokenFun},
                      Resource, Permission, _AuthzContext) ->
    with_decoded_token(DecodedTokenFun(),
        fun() ->
            Scopes = get_scopes(DecodedTokenFun()),
            rabbit_oauth2_scope:resource_access(Resource, Permission, Scopes)
        end).

check_topic_access(#auth_user{impl = DecodedTokenFun},
                   Resource, Permission, Context) ->
    with_decoded_token(DecodedTokenFun(),
        fun() ->
            Scopes = get_scopes(DecodedTokenFun()),
            rabbit_oauth2_scope:topic_access(Resource, Permission, Context, Scopes)
        end).

state_can_expire() -> true.

update_state(AuthUser, NewToken) ->
  case check_token(NewToken) of
      %% avoid logging the token
      {error, _} = E  -> E;
      {refused, {error, {invalid_token, error, _Err, _Stacktrace}}} ->
        {refused, "Authentication using an OAuth 2/JWT token failed: provided token is invalid"};
      {refused, Err} ->
        {refused, rabbit_misc:format("Authentication using an OAuth 2/JWT token failed: ~p", [Err])};
      {ok, DecodedToken} ->
          Tags = tags_from(DecodedToken),

          {ok, AuthUser#auth_user{tags = Tags,
                                  impl = fun() -> DecodedToken end}}
  end.

%%--------------------------------------------------------------------

authenticate(_, AuthProps0) ->
    AuthProps = to_map(AuthProps0),
    Token     = token_from_context(AuthProps),
    case check_token(Token) of
        %% avoid logging the token
        {error, _} = E  -> E;
        {refused, {error, {invalid_token, error, _Err, _Stacktrace}}} ->
          {refused, "Authentication using an OAuth 2/JWT token failed: provided token is invalid", []};
        {refused, Err} ->
          {refused, "Authentication using an OAuth 2/JWT token failed: ~p", [Err]};
        {ok, DecodedToken} ->
            Func = fun() ->
                        Username = username_from(
                          application:get_env(?APP, ?PREFERRED_USERNAME_CLAIMS, []),
                          DecodedToken),
                        Tags     = tags_from(DecodedToken),

                        {ok, #auth_user{username = Username,
                                        tags = Tags,
                                        impl = fun() -> DecodedToken end}}
                   end,
            case with_decoded_token(DecodedToken, Func) of
                {error, Err} ->
                    {refused, "Authentication using an OAuth 2/JWT token failed: ~p", [Err]};
                Else ->
                    Else
            end
    end.

with_decoded_token(DecodedToken, Fun) ->
    case validate_token_expiry(DecodedToken) of
        ok               -> Fun();
        {error, Msg} = Err ->
            rabbit_log:error(Msg),
            Err
    end.

validate_token_expiry(#{<<"exp">> := Exp}) when is_integer(Exp) ->
    Now = os:system_time(seconds),
    case Exp =< Now of
        true  -> {error, rabbit_misc:format("Provided JWT token has expired at timestamp ~p (validated at ~p)", [Exp, Now])};
        false -> ok
    end;
validate_token_expiry(#{}) -> ok.

-spec check_token(binary() | map()) ->
          {'ok', map()} |
          {'error', term() }|
          {'refused',
           'signature_invalid' |
           {'error', term()} |
           {'invalid_aud', term()}}.

check_token(DecodedToken) when is_map(DecodedToken) ->
    {ok, DecodedToken};

check_token(Token) ->
    Settings = application:get_all_env(?APP),
    case uaa_jwt:decode_and_verify(Token) of
        {error, Reason} -> {refused, {error, Reason}};
        {true, Payload} ->
            validate_payload(post_process_payload(Payload, Settings));
        {false, _}      -> {refused, signature_invalid}
    end.

post_process_payload(Payload) when is_map(Payload) ->
    post_process_payload(Payload, []).

post_process_payload(Payload, AppEnv) when is_map(Payload) ->
    Payload0 = maps:map(fun(K, V) ->
                        case K of
                            ?AUD_JWT_FIELD   when is_binary(V) -> binary:split(V, <<" ">>, [global, trim_all]);
                            ?SCOPE_JWT_FIELD when is_binary(V) -> binary:split(V, <<" ">>, [global, trim_all]);
                            _ -> V
                        end
        end,
        Payload
    ),
    Payload1 = case does_include_complex_claim_field(Payload0) of
        true  -> post_process_payload_with_complex_claim(Payload0);
        false -> Payload0
        end,

    Payload2 = case maps:is_key(<<"authorization">>, Payload1) of
        true  -> post_process_payload_in_keycloak_format(Payload1);
        false -> Payload1
        end,

    Payload3 = case has_configured_scope_aliases(AppEnv) of
        true  -> post_process_payload_with_scope_aliases(Payload2, AppEnv);
        false -> Payload2
        end,

    Payload4 = case maps:is_key(<<"authorization_details">>, Payload3) of
        true  -> post_process_payload_in_rich_auth_request_format(Payload3);
        false -> Payload3
        end,

    Payload4.

-spec has_configured_scope_aliases(AppEnv :: app_env()) -> boolean().
has_configured_scope_aliases(AppEnv) ->
    Map = maps:from_list(AppEnv),
    maps:is_key(?SCOPE_MAPPINGS_APP_ENV_KEY, Map).


-spec post_process_payload_with_scope_aliases(Payload :: map(), AppEnv :: app_env()) -> map().
%% This is for those hopeless environments where the token structure is so out of
%% messaging team's control that even the extra scopes field is no longer an option.
%%
%% This assumes that scopes can be random values that do not follow the RabbitMQ
%% convention, or any other convention, in any way. They are just random client role IDs.
%% See rabbitmq/rabbitmq-server#4588 for details.
post_process_payload_with_scope_aliases(Payload, AppEnv) ->
    %% try JWT scope field value for alias
    Payload1 = post_process_payload_with_scope_alias_in_scope_field(Payload, AppEnv),
    %% try the configurable 'extra_scopes_source' field value for alias
    Payload2 = post_process_payload_with_scope_alias_in_extra_scopes_source(Payload1, AppEnv),
    Payload2.

-spec post_process_payload_with_scope_alias_in_scope_field(Payload :: map(),
                                                           AppEnv :: app_env()) -> map().
%% First attempt: use the value in the 'scope' field for alias
post_process_payload_with_scope_alias_in_scope_field(Payload, AppEnv) ->
    ScopeMappings = proplists:get_value(?SCOPE_MAPPINGS_APP_ENV_KEY, AppEnv, #{}),
    post_process_payload_with_scope_alias_field_named(Payload, ?SCOPE_JWT_FIELD, ScopeMappings).


-spec post_process_payload_with_scope_alias_in_extra_scopes_source(Payload :: map(),
                                                                   AppEnv :: app_env()) -> map().
%% Second attempt: use the value in the configurable 'extra scopes source' field for alias
post_process_payload_with_scope_alias_in_extra_scopes_source(Payload, AppEnv) ->
    ExtraScopesField = proplists:get_value(?COMPLEX_CLAIM_APP_ENV_KEY, AppEnv, undefined),
    case ExtraScopesField of
        %% nothing to inject
        undefined -> Payload;
        _         ->
            ScopeMappings = proplists:get_value(?SCOPE_MAPPINGS_APP_ENV_KEY, AppEnv, #{}),
            post_process_payload_with_scope_alias_field_named(Payload, ExtraScopesField, ScopeMappings)
    end.


-spec post_process_payload_with_scope_alias_field_named(Payload :: map(),
                                                        Field :: binary(),
                                                        ScopeAliasMapping :: map()) -> map().
post_process_payload_with_scope_alias_field_named(Payload, undefined, _ScopeAliasMapping) ->
    Payload;
post_process_payload_with_scope_alias_field_named(Payload, FieldName, ScopeAliasMapping) ->
      Scopes0 = maps:get(FieldName, Payload, []),
      Scopes = rabbit_data_coercion:to_list_of_binaries(Scopes0),
      %% for all scopes, look them up in the scope alias map, and if they are
      %% present, add the alias to the final scope list. Note that we also preserve
      %% the original scopes, it should not hurt.
      ExpandedScopes =
          lists:foldl(fun(ScopeListItem, Acc) ->
                        case maps:get(ScopeListItem, ScopeAliasMapping, undefined) of
                            undefined ->
                                Acc;
                            MappedList when is_list(MappedList) ->
                                Binaries = rabbit_data_coercion:to_list_of_binaries(MappedList),
                                Acc ++ Binaries;
                            Value ->
                                Binaries = rabbit_data_coercion:to_list_of_binaries(Value),
                                Acc ++ Binaries
                        end
                      end, Scopes, Scopes),
       maps:put(?SCOPE_JWT_FIELD, ExpandedScopes, Payload).


-spec does_include_complex_claim_field(Payload :: map()) -> boolean().
does_include_complex_claim_field(Payload) when is_map(Payload) ->
        maps:is_key(application:get_env(?APP, ?COMPLEX_CLAIM_APP_ENV_KEY, undefined), Payload).

-spec post_process_payload_with_complex_claim(Payload :: map()) -> map().
post_process_payload_with_complex_claim(Payload) ->
    ComplexClaim = maps:get(application:get_env(?APP, ?COMPLEX_CLAIM_APP_ENV_KEY, undefined), Payload),
    ResourceServerId = rabbit_data_coercion:to_binary(application:get_env(?APP, ?RESOURCE_SERVER_ID, <<>>)),

    AdditionalScopes =
        case ComplexClaim of
            L when is_list(L) -> L;
            M when is_map(M) ->
                case maps:get(ResourceServerId, M, undefined) of
                    undefined           -> [];
                    Ks when is_list(Ks) ->
                        [erlang:iolist_to_binary([ResourceServerId, <<".">>, K]) || K <- Ks];
                    ClaimBin when is_binary(ClaimBin) ->
                        UnprefixedClaims = binary:split(ClaimBin, <<" ">>, [global, trim_all]),
                        [erlang:iolist_to_binary([ResourceServerId, <<".">>, K]) || K <- UnprefixedClaims];
                    _ -> []
                    end;
            Bin when is_binary(Bin) ->
                binary:split(Bin, <<" ">>, [global, trim_all]);
            _ -> []
            end,

    case AdditionalScopes of
        [] -> Payload;
        _  ->
            ExistingScopes = maps:get(?SCOPE_JWT_FIELD, Payload, []),
            maps:put(?SCOPE_JWT_FIELD, AdditionalScopes ++ ExistingScopes, Payload)
        end.

-spec post_process_payload_in_keycloak_format(Payload :: map()) -> map().
%% keycloak token format: https://github.com/rabbitmq/rabbitmq-auth-backend-oauth2/issues/36
post_process_payload_in_keycloak_format(#{<<"authorization">> := Authorization} = Payload) ->
    AdditionalScopes = case maps:get(<<"permissions">>, Authorization, undefined) of
        undefined   -> [];
        Permissions -> extract_scopes_from_keycloak_permissions([], Permissions)
    end,
    ExistingScopes = maps:get(?SCOPE_JWT_FIELD, Payload),
    maps:put(?SCOPE_JWT_FIELD, AdditionalScopes ++ ExistingScopes, Payload).

extract_scopes_from_keycloak_permissions(Acc, []) ->
    Acc;
extract_scopes_from_keycloak_permissions(Acc, [H | T]) when is_map(H) ->
    Scopes = case maps:get(<<"scopes">>, H, []) of
        ScopesAsList when is_list(ScopesAsList) ->
            ScopesAsList;
        ScopesAsBinary when is_binary(ScopesAsBinary) ->
            [ScopesAsBinary]
    end,
    extract_scopes_from_keycloak_permissions(Acc ++ Scopes, T);
extract_scopes_from_keycloak_permissions(Acc, [_ | T]) ->
    extract_scopes_from_keycloak_permissions(Acc, T).


-define(ACTIONS_FIELD, <<"actions">>).
-define(LOCATIONS_FIELD, <<"locations">>).
-define(TYPE_FIELD, <<"type">>).

-define(CLUSTER_LOCATION_ATTRIBUTE, <<"cluster">>).
-define(VHOST_LOCATION_ATTRIBUTE, <<"vhost">>).
-define(QUEUE_LOCATION_ATTRIBUTE, <<"queue">>).
-define(EXCHANGE_LOCATION_ATTRIBUTE, <<"exchange">>).
-define(ROUTING_KEY_LOCATION_ATTRIBUTE, <<"routing-key">>).
-define(LOCATION_ATTRIBUTES, [?CLUSTER_LOCATION_ATTRIBUTE, ?VHOST_LOCATION_ATTRIBUTE,
  ?QUEUE_LOCATION_ATTRIBUTE, ?EXCHANGE_LOCATION_ATTRIBUTE, ?ROUTING_KEY_LOCATION_ATTRIBUTE]).

-define(ALLOWED_TAG_VALUES, [<<"monitoring">>, <<"administrator">>, <<"management">>, <<"policymaker">> ]).
-define(ALLOWED_ACTION_VALUES, [<<"read">>, <<"write">>, <<"configure">>, <<"monitoring">>,
  <<"administrator">>, <<"management">>, <<"policymaker">> ]).


put_location_attribute(Attribute, Map) ->
  put_attribute(binary:split(Attribute, <<":">>, [global, trim_all]), Map).

put_attribute([Key, Value | _], Map) ->
  case lists:member(Key, ?LOCATION_ATTRIBUTES) of
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
  Vhost = maps:get(?VHOST_LOCATION_ATTRIBUTE, Map, <<"*">>),
  Resource = maps:get(?QUEUE_LOCATION_ATTRIBUTE, Map,
    maps:get(?EXCHANGE_LOCATION_ATTRIBUTE, Map, <<"*">>)),
  RoutingKey = maps:get(?ROUTING_KEY_LOCATION_ATTRIBUTE, Map, <<"*">>),

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

cluster_matches_resource_server_id(#{?CLUSTER_LOCATION_ATTRIBUTE := Cluster},
    ResourceServerId)  ->
  wildcard:match(ResourceServerId, Cluster);

cluster_matches_resource_server_id(_,_) ->
  false.

legal_queue_and_exchange_values(#{?QUEUE_LOCATION_ATTRIBUTE := Queue,
  ?EXCHANGE_LOCATION_ATTRIBUTE := Exchange}) ->
  case Queue of
    <<>> -> case Exchange of
      <<>> -> true;
      _ -> false
    end;
    _ -> case Exchange of
      Queue -> true;
      _ -> false
    end
  end;
legal_queue_and_exchange_values(_) -> true.

map_rich_auth_permissions_to_scopes(ResourceServerId, Permissions) ->
  map_rich_auth_permissions_to_scopes(ResourceServerId, Permissions, []).
map_rich_auth_permissions_to_scopes(_, [], Acc) -> Acc;
map_rich_auth_permissions_to_scopes(ResourceServerId,
  [ #{?ACTIONS_FIELD := Actions, ?LOCATIONS_FIELD := Locations }  | T ], Acc) ->
  ResourcePaths = map_locations_to_permission_resource_paths(ResourceServerId, Locations),
  case ResourcePaths of
    [] -> map_rich_auth_permissions_to_scopes(ResourceServerId, T, Acc);
    _ -> Scopes = case Actions of
          undefined -> [];
          ActionsAsList when is_list(ActionsAsList) ->
            build_scopes(ResourceServerId, skip_unknown_actions(ActionsAsList), ResourcePaths);
          ActionsAsBinary when is_binary(ActionsAsBinary) ->
            build_scopes(ResourceServerId, skip_unknown_actions([ActionsAsBinary]), ResourcePaths)
        end,
        map_rich_auth_permissions_to_scopes(ResourceServerId, T, Acc ++ Scopes)
  end.

skip_unknown_actions(Actions) ->
  lists:filter(fun(A) -> lists:member(A, ?ALLOWED_ACTION_VALUES) end, Actions).

produce_list_of_user_tag_or_action_on_resources(ResourceServerId, ActionOrUserTag, Locations) ->
  case lists:member(ActionOrUserTag, ?ALLOWED_TAG_VALUES) of
    true -> [<< ResourceServerId/binary, ".tag:", ActionOrUserTag/binary >>];
    _ -> build_scopes_for_action(ResourceServerId, ActionOrUserTag, Locations, [])
  end.

build_scopes_for_action(ResourceServerId, Action, [Location|Locations], Acc) ->
  Scope = << ResourceServerId/binary, ".", Action/binary, ":", Location/binary >>,
  build_scopes_for_action(ResourceServerId, Action, Locations, [ Scope | Acc ] );
build_scopes_for_action(_, _, [], Acc) -> Acc.

build_scopes(ResourceServerId, Actions, Locations) -> lists:flatmap(
  fun(Action) ->
    produce_list_of_user_tag_or_action_on_resources(ResourceServerId, Action, Locations) end, Actions).

is_recognized_permission(#{?ACTIONS_FIELD := _, ?LOCATIONS_FIELD:= _ , ?TYPE_FIELD := Type }, ResourceServerType) ->
  case ResourceServerType of
    <<>> -> false;
    V when V == Type -> true;
    _ -> false
  end;
is_recognized_permission(_, _) -> false.


-spec post_process_payload_in_rich_auth_request_format(Payload :: map()) -> map().
%% https://oauth.net/2/rich-authorization-requests/
post_process_payload_in_rich_auth_request_format(#{<<"authorization_details">> := Permissions} = Payload) ->
  ResourceServerId = rabbit_data_coercion:to_binary(
    application:get_env(?APP, ?RESOURCE_SERVER_ID, <<>>)),
  ResourceServerType = rabbit_data_coercion:to_binary(
    application:get_env(?APP, ?RESOURCE_SERVER_TYPE, <<>>)),

  FilteredPermissionsByType = lists:filter(fun(P) ->
      is_recognized_permission(P, ResourceServerType) end, Permissions),
  AdditionalScopes = map_rich_auth_permissions_to_scopes(ResourceServerId, FilteredPermissionsByType),

  ExistingScopes = maps:get(?SCOPE_JWT_FIELD, Payload, []),
  maps:put(?SCOPE_JWT_FIELD, AdditionalScopes ++ ExistingScopes, Payload).



validate_payload(#{?SCOPE_JWT_FIELD := _Scope } = DecodedToken) ->
    ResourceServerEnv = application:get_env(?APP, ?RESOURCE_SERVER_ID, <<>>),
    ResourceServerId = rabbit_data_coercion:to_binary(ResourceServerEnv),
    validate_payload(DecodedToken, ResourceServerId).

validate_payload(#{?SCOPE_JWT_FIELD := Scope, ?AUD_JWT_FIELD := Aud} = DecodedToken, ResourceServerId) ->
    case check_aud(Aud, ResourceServerId) of
        ok           -> {ok, DecodedToken#{?SCOPE_JWT_FIELD => filter_scopes(Scope, ResourceServerId)}};
        {error, Err} -> {refused, {invalid_aud, Err}}
    end;
validate_payload(#{?SCOPE_JWT_FIELD := Scope} = DecodedToken, ResourceServerId) ->
  case application:get_env(?APP, ?VERIFY_AUD, true) of
    true -> {error, {badarg, {aud_field_is_missing}}};
    false -> {ok, DecodedToken#{?SCOPE_JWT_FIELD => filter_scopes(Scope, ResourceServerId)}}
  end.

filter_scopes(Scopes, <<"">>) -> Scopes;
filter_scopes(Scopes, ResourceServerId)  ->
    PrefixPattern = <<ResourceServerId/binary, ".">>,
    matching_scopes_without_prefix(Scopes, PrefixPattern).

check_aud(_, <<>>)    -> ok;
check_aud(Aud, ResourceServerId) ->
  case application:get_env(?APP, ?VERIFY_AUD, true) of
    true ->
      case Aud of
        List when is_list(List) ->
            case lists:member(ResourceServerId, Aud) of
                true  -> ok;
                false -> {error, {resource_id_not_found_in_aud, ResourceServerId, Aud}}
            end;
        _ -> {error, {badarg, {aud_is_not_a_list, Aud}}}
      end;
    false -> ok
  end.

%%--------------------------------------------------------------------

get_scopes(#{?SCOPE_JWT_FIELD := Scope}) -> Scope.

%% A token may be present in the password credential or in the rabbit_auth_backend_oauth2
%% credential.  The former is the most common scenario for the first time authentication.
%% However, there are scenarios where the same user (on the same connection) is authenticated
%% more than once. When this scenario occurs, we extract the token from the credential
%% called rabbit_auth_backend_oauth2 whose value is the Decoded token returned during the
%% first authentication. 

-spec token_from_context(map()) -> binary() | undefined.
token_from_context(AuthProps) ->
    case maps:get(password, AuthProps, undefined) of
        undefined ->
            case maps:get(rabbit_auth_backend_oauth2, AuthProps, undefined) of
                undefined -> undefined;
                Impl -> Impl()
            end;
        Token -> Token
    end.

%% Decoded tokens look like this:
%%
%% #{<<"aud">>         => [<<"rabbitmq">>, <<"rabbit_client">>],
%%   <<"authorities">> => [<<"rabbitmq.read:*/*">>, <<"rabbitmq.write:*/*">>, <<"rabbitmq.configure:*/*">>],
%%   <<"azp">>         => <<"rabbit_client">>,
%%   <<"cid">>         => <<"rabbit_client">>,
%%   <<"client_id">>   => <<"rabbit_client">>,
%%   <<"exp">>         => 1530849387,
%%   <<"grant_type">>  => <<"client_credentials">>,
%%   <<"iat">>         => 1530806187,
%%   <<"iss">>         => <<"http://localhost:8080/uaa/oauth/token">>,
%%   <<"jti">>         => <<"df5d50a1cdcb4fa6bf32e7e03acfc74d">>,
%%   <<"rev_sig">>     => <<"2f880d5b">>,
%%   <<"scope">>       => [<<"rabbitmq.read:*/*">>, <<"rabbitmq.write:*/*">>, <<"rabbitmq.configure:*/*">>],
%%   <<"sub">>         => <<"rabbit_client">>,
%%   <<"zid">>         => <<"uaa">>}

-spec username_from(list(), map()) -> binary() | undefined.
username_from(PreferredUsernameClaims, DecodedToken) ->
    UsernameClaims = append_or_return_default(PreferredUsernameClaims, ?DEFAULT_PREFERRED_USERNAME_CLAIMS),
    ResolvedUsernameClaims = lists:filtermap(fun(Claim) -> find_claim_in_token(Claim, DecodedToken) end, UsernameClaims),
    Username = case ResolvedUsernameClaims of
      [ ] -> <<"unknown">>;
      [ _One ] -> _One;
      [ _One | _ ] -> _One
    end,
    rabbit_log:debug("Computing username from client's JWT token: ~ts -> ~ts ",
      [lists:flatten(io_lib:format("~p",[ResolvedUsernameClaims])), Username]),
    Username.

append_or_return_default(ListOrBinary, Default) ->
  case ListOrBinary of
    VarList when is_list(VarList) -> VarList ++ Default;
    VarBinary when is_binary(VarBinary) -> [VarBinary] ++ Default;
    _ -> Default
  end.

find_claim_in_token(Claim, Token) ->
  case maps:get(Claim, Token, undefined) of
    undefined -> false;
    ClaimValue when is_binary(ClaimValue) -> {true, ClaimValue};
    _ -> false
  end.

-define(TAG_SCOPE_PREFIX, <<"tag:">>).

-spec tags_from(map()) -> list(atom()).
tags_from(DecodedToken) ->
    Scopes    = maps:get(?SCOPE_JWT_FIELD, DecodedToken, []),
    TagScopes = matching_scopes_without_prefix(Scopes, ?TAG_SCOPE_PREFIX),
    lists:usort(lists:map(fun rabbit_data_coercion:to_atom/1, TagScopes)).

matching_scopes_without_prefix(Scopes, PrefixPattern) ->
    PatternLength = byte_size(PrefixPattern),
    lists:filtermap(
        fun(ScopeEl) ->
            case binary:match(ScopeEl, PrefixPattern) of
                {0, PatternLength} ->
                    ElLength = byte_size(ScopeEl),
                    {true,
                     binary:part(ScopeEl,
                                 {PatternLength, ElLength - PatternLength})};
                _ -> false
            end
        end,
        Scopes).
