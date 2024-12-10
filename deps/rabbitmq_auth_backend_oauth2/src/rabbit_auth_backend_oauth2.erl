%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_oauth2).

-include_lib("rabbit_common/include/rabbit.hrl").
<<<<<<< HEAD
=======
-include("oauth2.hrl").
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([description/0]).
-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4,
<<<<<<< HEAD
         check_topic_access/4, check_token/1, update_state/2,
         expiry_timestamp/1]).

% for testing
-export([post_process_payload/2, get_expanded_scopes/2]).
-import(uaa_jwt, [resolve_resource_server_id/1]).
-import(rabbit_data_coercion, [to_map/1]).
-import(rabbit_oauth2_config, [get_preferred_username_claims/1]).
=======
         check_topic_access/4, update_state/2,
         expiry_timestamp/1]).

%% for testing
-export([normalize_token_scope/2, get_expanded_scopes/2]).

-import(rabbit_data_coercion, [to_map/1]).
-import(uaa_jwt, [
    decode_and_verify/3,
    get_scope/1, set_scope/2,
    resolve_resource_server/1]).

-import(rabbit_oauth2_keycloak, [has_keycloak_scopes/1, extract_scopes_from_keycloak_format/1]).
-import(rabbit_oauth2_rar, [extract_scopes_from_rich_auth_request/2, has_rich_auth_request_scopes/1]).

-import(rabbit_oauth2_scope, [filter_matching_scope_prefix_and_drop_it/2]).
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)

-ifdef(TEST).
-compile(export_all).
-endif.

%%
<<<<<<< HEAD
%% App environment
%%


-define(RESOURCE_SERVER_ID, resource_server_id).
%% a term defined for Rich Authorization Request tokens to identify a RabbitMQ permission
%% verify server_server_id aud field is on the aud field
%% a term used by the IdentityServer community
%% scope aliases map "role names" to a set of scopes


%%
%% Key JWT fields
%%

-define(AUD_JWT_FIELD, <<"aud">>).
-define(SCOPE_JWT_FIELD, <<"scope">>).
=======
%% Types
%%

-type ok_extracted_auth_user() :: {ok, rabbit_types:auth_user()}.
-type auth_user_extraction_fun() :: fun((decoded_jwt_token()) -> any()).

>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
%%
%% API
%%

description() ->
    [{name, <<"OAuth 2">>},
     {description, <<"Performs authentication and authorisation using JWT tokens and OAuth 2 scopes">>}].

%%--------------------------------------------------------------------

<<<<<<< HEAD
=======
-spec user_login_authentication(rabbit_types:username(), [term()] | map()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
user_login_authentication(Username, AuthProps) ->
    case authenticate(Username, AuthProps) of
	{refused, Msg, Args} = AuthResult ->
	    rabbit_log:debug(Msg, Args),
	    AuthResult;
	_ = AuthResult ->
	    AuthResult
    end.

<<<<<<< HEAD
=======
-spec user_login_authorization(rabbit_types:username(), [term()] | map()) ->
    {'ok', any()} |
    {'ok', any(), any()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
user_login_authorization(Username, AuthProps) ->
    case authenticate(Username, AuthProps) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

<<<<<<< HEAD
=======
-spec check_vhost_access(AuthUser :: rabbit_types:auth_user(),
                         VHost :: rabbit_types:vhost(),
                         AuthzData :: rabbit_types:authz_data()) -> boolean() | {'error', any()}.
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
check_vhost_access(#auth_user{impl = DecodedTokenFun},
                   VHost, _AuthzData) ->
    with_decoded_token(DecodedTokenFun(),
        fun(_Token) ->
            DecodedToken = DecodedTokenFun(),
<<<<<<< HEAD
            Scopes = get_scopes(DecodedToken),
=======
            Scopes = get_scope(DecodedToken),
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
            ScopeString = rabbit_oauth2_scope:concat_scopes(Scopes, ","),
            rabbit_log:debug("Matching virtual host '~ts' against the following scopes: ~ts", [VHost, ScopeString]),
            rabbit_oauth2_scope:vhost_access(VHost, Scopes)
        end).

check_resource_access(#auth_user{impl = DecodedTokenFun},
                      Resource, Permission, _AuthzContext) ->
    with_decoded_token(DecodedTokenFun(),
        fun(Token) ->
<<<<<<< HEAD
            Scopes = get_scopes(Token),
=======
            Scopes = get_scope(Token),
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
            rabbit_oauth2_scope:resource_access(Resource, Permission, Scopes)
        end).

check_topic_access(#auth_user{impl = DecodedTokenFun},
                   Resource, Permission, Context) ->
    with_decoded_token(DecodedTokenFun(),
        fun(Token) ->
            Scopes = get_expanded_scopes(Token, Resource),
            rabbit_oauth2_scope:topic_access(Resource, Permission, Context, Scopes)
        end).

update_state(AuthUser, NewToken) ->
<<<<<<< HEAD
    case check_token(NewToken) of
        %% avoid logging the token
        {error, _} = E  -> E;
        {refused, {error, {invalid_token, error, _Err, _Stacktrace}}} ->
            {refused, "Authentication using an OAuth 2/JWT token failed: provided token is invalid"};
        {refused, Err} ->
            {refused, rabbit_misc:format("Authentication using an OAuth 2/JWT token failed: ~tp", [Err])};
        {ok, DecodedToken} ->
            ResourceServerId = resolve_resource_server_id(DecodedToken),        
            CurToken = AuthUser#auth_user.impl,
            case ensure_same_username(
                            get_preferred_username_claims(ResourceServerId),
                            CurToken(), DecodedToken) of           
                ok -> 
                    Tags = tags_from(DecodedToken),
                    {ok, AuthUser#auth_user{tags = Tags,
                                            impl = fun() -> DecodedToken end}};
                {error, mismatch_username_after_token_refresh} -> 
                    {refused, 
                        "Not allowed to change username on refreshed token"}
=======
    case resolve_resource_server(NewToken) of
        {error, _} = Err0 -> Err0;
        {ResourceServer, _} = Tuple ->
            case check_token(NewToken, Tuple) of
                %% avoid logging the token
                {refused, {error, {invalid_token, error, _Err, _Stacktrace}}} ->
                    {refused, "Authentication using an OAuth 2/JWT token failed: provided token is invalid"};
                {refused, Err} ->
                    {refused, rabbit_misc:format("Authentication using an OAuth 2/JWT token failed: ~tp", [Err])};
                {ok, DecodedToken} ->
                    CurToken = AuthUser#auth_user.impl,
                    case ensure_same_username(
                            ResourceServer#resource_server.preferred_username_claims,
                            CurToken(), DecodedToken) of 
                        ok ->
                            Tags = tags_from(DecodedToken),
                            {ok, AuthUser#auth_user{tags = Tags,
                                                    impl = fun() -> DecodedToken end}};
                        {error, mismatch_username_after_token_refresh} -> 
                            {refused, 
                                "Not allowed to change username on refreshed token"}
                    end
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
            end
    end.

expiry_timestamp(#auth_user{impl = DecodedTokenFun}) ->
    case DecodedTokenFun() of
        #{<<"exp">> := Exp} when is_integer(Exp) ->
            Exp;
        _ ->
            never
    end.

%%--------------------------------------------------------------------

<<<<<<< HEAD
authenticate(_, AuthProps0) ->
    AuthProps = to_map(AuthProps0),
    Token     = token_from_context(AuthProps),

    case check_token(Token) of
        %% avoid logging the token
        {error, _} = E  -> E;
        {refused, {error, {invalid_token, error, _Err, _Stacktrace}}} ->
          {refused, "Authentication using an OAuth 2/JWT token failed: provided token is invalid", []};
        {refused, Err} ->
          {refused, "Authentication using an OAuth 2/JWT token failed: ~tp", [Err]};
        {ok, DecodedToken} ->
            Func = fun(Token0) ->
                ResourceServerId = resolve_resource_server_id(Token0),
                Username = username_from(
                    get_preferred_username_claims(ResourceServerId), 
                    Token0),
                Tags     = tags_from(Token0),
                {ok, #auth_user{username = Username,
                                tags = Tags,
                                impl = fun() -> Token0 end}}
            end,
            case with_decoded_token(DecodedToken, Func) of
                {error, Err} ->
                    {refused, "Authentication using an OAuth 2/JWT token failed: ~tp", [Err]};
                Else ->
                    Else
            end
    end.

=======
-spec authenticate(Username, Props) -> Result
    when Username :: rabbit_types:username(),
         Props :: list() | map(),
         Result :: {ok, any()} | {refused, list(), list()} | {refused, {error, any()}}.

authenticate(_, AuthProps0) ->
    AuthProps = to_map(AuthProps0),
    Token     = token_from_context(AuthProps),
    case resolve_resource_server(Token) of
        {error, _} = Err0 ->
            {refused, "Authentication using OAuth 2/JWT token failed: ~tp", [Err0]};
        {ResourceServer, _} = Tuple ->
            case check_token(Token, Tuple) of
                {refused, {error, {invalid_token, error, _Err, _Stacktrace}}} ->
                    {refused, "Authentication using an OAuth 2/JWT token failed: provided token is invalid", []};
                {refused, Err} ->
                    {refused, "Authentication using an OAuth 2/JWT token failed: ~tp", [Err]};
                {ok, DecodedToken} ->
                    case with_decoded_token(DecodedToken, fun(In) -> auth_user_from_token(In, ResourceServer) end) of
                        {error, Err} ->
                            {refused, "Authentication using an OAuth 2/JWT token failed: ~tp", [Err]};
                        Else ->
                            Else
                    end
            end
    end.

-spec with_decoded_token(Token, Fun) -> Result
    when Token :: decoded_jwt_token(),
         Fun :: auth_user_extraction_fun(),
         Result :: {ok, any()} | {'error', any()}.
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
with_decoded_token(DecodedToken, Fun) ->
    case validate_token_expiry(DecodedToken) of
        ok               -> Fun(DecodedToken);
        {error, Msg} = Err ->
            rabbit_log:error(Msg),
            Err
    end.
<<<<<<< HEAD
=======

%% This is a helper function used with HOFs that may return errors.
-spec auth_user_from_token(Token, ResourceServer) -> Result
    when Token :: decoded_jwt_token(),
         ResourceServer :: resource_server(),
         Result :: ok_extracted_auth_user().
auth_user_from_token(Token0, ResourceServer) ->
    Username = username_from(
        ResourceServer#resource_server.preferred_username_claims,
        Token0),
    Tags     = tags_from(Token0),
    {ok, #auth_user{username = Username,
                    tags = Tags,
                    impl = fun() -> Token0 end}}.

>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
ensure_same_username(PreferredUsernameClaims, CurrentDecodedToken, NewDecodedToken) ->
    CurUsername = username_from(PreferredUsernameClaims, CurrentDecodedToken),
    case {CurUsername, username_from(PreferredUsernameClaims, NewDecodedToken)} of 
        {CurUsername, CurUsername} -> ok;
        _ -> {error, mismatch_username_after_token_refresh}
    end. 

validate_token_expiry(#{<<"exp">> := Exp}) when is_integer(Exp) ->
    Now = os:system_time(seconds),
    case Exp =< Now of
        true  -> {error, rabbit_misc:format("Provided JWT token has expired at timestamp ~tp (validated at ~tp)", [Exp, Now])};
        false -> ok
    end;
validate_token_expiry(#{}) -> ok.

<<<<<<< HEAD
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
    case uaa_jwt:decode_and_verify(Token) of
        {error, Reason} ->
          {refused, {error, Reason}};
        {true, TargetResourceServerId, Payload} ->
          Payload0 = post_process_payload(TargetResourceServerId, Payload),
          validate_payload(TargetResourceServerId, Payload0);
        {false, _, _} -> {refused, signature_invalid}
    end.

post_process_payload(ResourceServerId, Payload) when is_map(Payload) ->
    Payload0 = maps:map(fun(K, V) ->
                  case K of
                      ?AUD_JWT_FIELD   when is_binary(V) -> binary:split(V, <<" ">>, [global, trim_all]);
                      ?SCOPE_JWT_FIELD when is_binary(V) -> binary:split(V, <<" ">>, [global, trim_all]);
                      _ -> V
                  end
      end,
      Payload
    ),

    Payload1 = case does_include_complex_claim_field(ResourceServerId, Payload0) of
        true  -> post_process_payload_with_complex_claim(ResourceServerId, Payload0);
        false -> Payload0
        end,

    Payload2 = case maps:is_key(<<"authorization">>, Payload1) of
        true  -> post_process_payload_in_keycloak_format(Payload1);
        false -> Payload1
        end,

    Payload3 = case rabbit_oauth2_config:has_scope_aliases(ResourceServerId) of
        true  -> post_process_payload_with_scope_aliases(ResourceServerId, Payload2);
        false -> Payload2
        end,

    Payload4 = case maps:is_key(<<"authorization_details">>, Payload3) of
        true  -> post_process_payload_in_rich_auth_request_format(ResourceServerId, Payload3);
        false -> Payload3
        end,

    Payload4.


-spec post_process_payload_with_scope_aliases(ResourceServerId :: binary(), Payload :: map()) -> map().
%% This is for those hopeless environments where the token structure is so out of
%% messaging team's control that even the extra scopes field is no longer an option.
%%
%% This assumes that scopes can be random values that do not follow the RabbitMQ
%% convention, or any other convention, in any way. They are just random client role IDs.
%% See rabbitmq/rabbitmq-server#4588 for details.
post_process_payload_with_scope_aliases(ResourceServerId, Payload) ->
    %% try JWT scope field value for alias
    Payload1 = post_process_payload_with_scope_alias_in_scope_field(ResourceServerId, Payload),
    %% try the configurable 'extra_scopes_source' field value for alias
    post_process_payload_with_scope_alias_in_extra_scopes_source(ResourceServerId, Payload1).


-spec post_process_payload_with_scope_alias_in_scope_field(ResourceServerId :: binary(), Payload :: map()) -> map().
%% First attempt: use the value in the 'scope' field for alias
post_process_payload_with_scope_alias_in_scope_field(ResourceServerId, Payload) ->
    ScopeMappings = rabbit_oauth2_config:get_scope_aliases(ResourceServerId),
    post_process_payload_with_scope_alias_field_named(Payload, ?SCOPE_JWT_FIELD, ScopeMappings).


-spec post_process_payload_with_scope_alias_in_extra_scopes_source(ResourceServerId :: binary(), Payload :: map()) -> map().
%% Second attempt: use the value in the configurable 'extra scopes source' field for alias
post_process_payload_with_scope_alias_in_extra_scopes_source(ResourceServerId, Payload) ->
    ExtraScopesField = rabbit_oauth2_config:get_additional_scopes_key(ResourceServerId),
    case ExtraScopesField of
        %% nothing to inject
        {error, not_found} -> Payload;
        {ok, ExtraScopes} ->
            ScopeMappings = rabbit_oauth2_config:get_scope_aliases(ResourceServerId),
            post_process_payload_with_scope_alias_field_named(Payload, ExtraScopes, ScopeMappings)
    end.


-spec post_process_payload_with_scope_alias_field_named(Payload :: map(),
                                                        Field :: binary(),
                                                        ScopeAliasMapping :: map()) -> map().
post_process_payload_with_scope_alias_field_named(Payload, FieldName, ScopeAliasMapping) ->
      Scopes0 = maps:get(FieldName, Payload, []),
=======
-spec check_token(raw_jwt_token(), {resource_server(), internal_oauth_provider()}) ->
          {'ok', decoded_jwt_token()} |
          {'error', term() } |
          {'refused', 'signature_invalid' | {'error', term()} | {'invalid_aud', term()}}.

check_token(DecodedToken, _) when is_map(DecodedToken) ->
    {ok, DecodedToken};

check_token(Token, {ResourceServer, InternalOAuthProvider}) ->
    case decode_and_verify(Token, ResourceServer, InternalOAuthProvider) of
        {error, Reason} -> {refused, {error, Reason}};
        {true, Payload} -> {ok, normalize_token_scope(ResourceServer, Payload)};
        {false, _} -> {refused, signature_invalid}
    end.

-spec normalize_token_scope(
    ResourceServer :: resource_server(), DecodedToken :: decoded_jwt_token()) -> map().
normalize_token_scope(ResourceServer, Payload) ->
    Payload0 = maps:map(fun(K, V) ->
        case K of
            ?SCOPE_JWT_FIELD when is_binary(V) ->
                binary:split(V, <<" ">>, [global, trim_all]);
             _ -> V
         end
       end, Payload),

    Payload1 = case has_additional_scopes_key(ResourceServer, Payload0) of
        true  -> extract_scopes_from_additional_scopes_key(ResourceServer, Payload0);
        false -> Payload0
        end,

    Payload2 = case has_keycloak_scopes(Payload1) of
        true  -> extract_scopes_from_keycloak_format(Payload1);
        false -> Payload1
        end,

    Payload3 = case ResourceServer#resource_server.scope_aliases of
        undefined -> Payload2;
        ScopeAliases  -> extract_scopes_using_scope_aliases(ScopeAliases, Payload2)
        end,

    Payload4 = case has_rich_auth_request_scopes(Payload3) of
        true  -> extract_scopes_from_rich_auth_request(ResourceServer, Payload3);
        false -> Payload3
        end,

    FilteredScopes = filter_matching_scope_prefix_and_drop_it(
        get_scope(Payload4), ResourceServer#resource_server.scope_prefix),
    set_scope(FilteredScopes, Payload4).


-spec extract_scopes_using_scope_aliases(
    ScopeAliasMapping :: map(), Payload :: map()) -> map().
extract_scopes_using_scope_aliases(ScopeAliasMapping, Payload) ->
      Scopes0 = get_scope(Payload),
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
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
<<<<<<< HEAD
       maps:put(?SCOPE_JWT_FIELD, ExpandedScopes, Payload).


-spec does_include_complex_claim_field(ResourceServerId :: binary(), Payload :: map()) -> boolean().
does_include_complex_claim_field(ResourceServerId, Payload) when is_map(Payload) ->
  case rabbit_oauth2_config:get_additional_scopes_key(ResourceServerId) of
    {ok, ScopeKey} -> maps:is_key(ScopeKey, Payload);
    {error, not_found} -> false
  end.

-spec post_process_payload_with_complex_claim(ResourceServerId :: binary(), Payload :: map()) -> map().
post_process_payload_with_complex_claim(ResourceServerId, Payload) ->
    case rabbit_oauth2_config:get_additional_scopes_key(ResourceServerId) of
      {ok, ScopesKey} ->
        ComplexClaim = maps:get(ScopesKey, Payload),
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
            end;
      {error, not_found} -> Payload
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
  ResourceServerId) ->
      wildcard:match(ResourceServerId, Cluster);

cluster_matches_resource_server_id(_,_) ->
    false.

legal_queue_and_exchange_values(#{?QUEUE_LOCATION_ATTRIBUTE := Queue,
    ?EXCHANGE_LOCATION_ATTRIBUTE := Exchange}) ->
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
  [ #{?ACTIONS_FIELD := Actions, ?LOCATIONS_FIELD := Locations }  | T ], Acc) ->
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

build_scopes(ResourceServerId, Actions, Locations) ->
    lists:flatmap(fun(Action) ->
        produce_list_of_user_tag_or_action_on_resources(ResourceServerId,
            Action, Locations) end, Actions).

is_recognized_permission(#{?ACTIONS_FIELD := _, ?LOCATIONS_FIELD:= _ , ?TYPE_FIELD := Type }, ResourceServerType) ->
    case ResourceServerType of
        <<>> -> false;
        V when V == Type -> true;
        _ -> false
    end;
is_recognized_permission(_, _) -> false.


-spec post_process_payload_in_rich_auth_request_format(ResourceServerId :: binary(), Payload :: map()) -> map().
%% https://oauth.net/2/rich-authorization-requests/
post_process_payload_in_rich_auth_request_format(ResourceServerId, #{<<"authorization_details">> := Permissions} = Payload) ->
    ResourceServerType = rabbit_oauth2_config:get_resource_server_type(ResourceServerId),

    FilteredPermissionsByType = lists:filter(fun(P) ->
      is_recognized_permission(P, ResourceServerType) end, Permissions),
    AdditionalScopes = map_rich_auth_permissions_to_scopes(ResourceServerId, FilteredPermissionsByType),

    ExistingScopes = maps:get(?SCOPE_JWT_FIELD, Payload, []),
    maps:put(?SCOPE_JWT_FIELD, AdditionalScopes ++ ExistingScopes, Payload).

validate_payload(ResourceServerId, DecodedToken) ->
    ScopePrefix = rabbit_oauth2_config:get_scope_prefix(ResourceServerId),
    validate_payload(ResourceServerId, DecodedToken, ScopePrefix).

validate_payload(ResourceServerId, #{?SCOPE_JWT_FIELD := Scope, ?AUD_JWT_FIELD := Aud} = DecodedToken, ScopePrefix) ->
    case check_aud(Aud, ResourceServerId) of
        ok           -> {ok, DecodedToken#{?SCOPE_JWT_FIELD => filter_scopes(Scope, ScopePrefix)}};
        {error, Err} -> {refused, {invalid_aud, Err}}
    end;
validate_payload(ResourceServerId, #{?AUD_JWT_FIELD := Aud} = DecodedToken, _ScopePrefix) ->
    case check_aud(Aud, ResourceServerId) of
        ok           -> {ok, DecodedToken};
        {error, Err} -> {refused, {invalid_aud, Err}}
    end;
validate_payload(ResourceServerId, #{?SCOPE_JWT_FIELD := Scope} = DecodedToken, ScopePrefix) ->
    case rabbit_oauth2_config:is_verify_aud(ResourceServerId) of
        true -> {error, {badarg, {aud_field_is_missing}}};
        false -> {ok, DecodedToken#{?SCOPE_JWT_FIELD => filter_scopes(Scope, ScopePrefix)}}
    end.

filter_scopes(Scopes, <<"">>) -> Scopes;
filter_scopes(Scopes, ScopePrefix)  ->
    matching_scopes_without_prefix(Scopes, ScopePrefix).

check_aud(_, <<>>)    -> ok;
check_aud(Aud, ResourceServerId) ->
  case rabbit_oauth2_config:is_verify_aud(ResourceServerId) of
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

get_scopes(#{?SCOPE_JWT_FIELD := Scope}) -> Scope;
get_scopes(#{}) -> [].

-spec get_expanded_scopes(map(), #resource{}) -> [binary()].
get_expanded_scopes(Token, #resource{virtual_host = VHost}) ->
    Context = #{ token => Token , vhost => VHost},
    case maps:get(?SCOPE_JWT_FIELD, Token, []) of
        [] -> [];
        Scopes -> lists:map(fun(Scope) -> list_to_binary(parse_scope(Scope, Context)) end, Scopes)
    end.

parse_scope(Scope, Context) ->
    { Acc0, _} = lists:foldl(fun(Elem, { Acc, Stage }) -> parse_scope_part(Elem, Acc, Stage, Context) end,
        { [], undefined }, re:split(Scope,"([\{.*\}])",[{return,list},trim])),
    Acc0.

parse_scope_part(Elem, Acc, Stage, Context) ->
    case Stage of
        error -> {Acc, error};
        undefined ->
            case Elem of
                "{" -> { Acc, fun capture_var_name/3};
                Value -> { Acc ++ Value, Stage}
            end;
        _ -> Stage(Elem, Acc, Context)
    end.

capture_var_name(Elem, Acc, #{ token := Token, vhost := Vhost}) ->
    { Acc ++ resolve_scope_var(Elem, Token, Vhost), fun expect_closing_var/3}.

expect_closing_var("}" , Acc, _Context) -> { Acc , undefined };
expect_closing_var(_ , _Acc, _Context) -> {"", error}.

resolve_scope_var(Elem, Token, Vhost) ->
    case Elem of
        "vhost" -> binary_to_list(Vhost);
        _ ->
            ElemAsBinary = list_to_binary(Elem),
            binary_to_list(case maps:get(ElemAsBinary, Token, ElemAsBinary) of
                          Value when is_binary(Value) -> Value;
                          _ -> ElemAsBinary
                        end)
    end.
=======
       set_scope(ExpandedScopes, Payload).

-spec has_additional_scopes_key(
    ResourceServer :: resource_server(), Payload :: map()) -> boolean().
has_additional_scopes_key(ResourceServer, Payload) when is_map(Payload) ->
    case ResourceServer#resource_server.additional_scopes_key of
        undefined -> false;
        ScopeKey -> maps:is_key(ScopeKey, Payload)
    end.

-spec extract_scopes_from_additional_scopes_key(
    ResourceServer :: resource_server(), Payload :: map()) -> map().
extract_scopes_from_additional_scopes_key(ResourceServer, Payload) ->
    Claim = maps:get(ResourceServer#resource_server.additional_scopes_key, Payload),
    AdditionalScopes = extract_additional_scopes(ResourceServer, Claim),
    set_scope(AdditionalScopes ++ get_scope(Payload), Payload).

extract_additional_scopes(ResourceServer, ComplexClaim) ->
    ResourceServerId = ResourceServer#resource_server.id,
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
    end.

>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)

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
    ResolvedUsernameClaims = lists:filtermap(fun(Claim) -> find_claim_in_token(Claim, DecodedToken) end, PreferredUsernameClaims),
    Username = case ResolvedUsernameClaims of
      [ ] -> <<"unknown">>;
      [ _One ] -> _One;
      [ _One | _ ] -> _One
    end,
    rabbit_log:debug("Computing username from client's JWT token: ~ts -> ~ts ",
      [lists:flatten(io_lib:format("~p",[ResolvedUsernameClaims])), Username]),
    Username.

find_claim_in_token(Claim, Token) ->
    case maps:get(Claim, Token, undefined) of
        undefined -> false;
        ClaimValue when is_binary(ClaimValue) -> {true, ClaimValue};
        _ -> false
    end.

<<<<<<< HEAD
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
=======
-spec get_expanded_scopes(map(), #resource{}) -> [binary()].
get_expanded_scopes(Token, #resource{virtual_host = VHost}) ->
    Context = #{ token => Token , vhost => VHost},
    case get_scope(Token) of
        [] -> [];
        Scopes -> lists:map(fun(Scope) -> list_to_binary(parse_scope(Scope, Context)) end, Scopes)
    end.


parse_scope(Scope, Context) ->
    { Acc0, _} = lists:foldl(fun(Elem, { Acc, Stage }) -> parse_scope_part(Elem, Acc, Stage, Context) end,
        { [], undefined }, re:split(Scope,"([\{.*\}])",[{return,list},trim])),
    Acc0.

parse_scope_part(Elem, Acc, Stage, Context) ->
    case Stage of
        error -> {Acc, error};
        undefined ->
            case Elem of
                "{" -> { Acc, fun capture_var_name/3};
                Value -> { Acc ++ Value, Stage}
            end;
        _ -> Stage(Elem, Acc, Context)
    end.

capture_var_name(Elem, Acc, #{ token := Token, vhost := Vhost}) ->
    { Acc ++ resolve_scope_var(Elem, Token, Vhost), fun expect_closing_var/3}.

expect_closing_var("}" , Acc, _Context) -> { Acc , undefined };
expect_closing_var(_ , _Acc, _Context) -> {"", error}.

resolve_scope_var(Elem, Token, Vhost) ->
    case Elem of
        "vhost" -> binary_to_list(Vhost);
        _ ->
            ElemAsBinary = list_to_binary(Elem),
            binary_to_list(case maps:get(ElemAsBinary, Token, ElemAsBinary) of
                          Value when is_binary(Value) -> Value;
                          _ -> ElemAsBinary
                        end)
    end.

-spec tags_from(decoded_jwt_token()) -> list(atom()).
tags_from(DecodedToken) ->
    Scopes    = maps:get(?SCOPE_JWT_FIELD, DecodedToken, []),
    TagScopes = filter_matching_scope_prefix_and_drop_it(Scopes, ?TAG_SCOPE_PREFIX),
    lists:usort(lists:map(fun rabbit_data_coercion:to_atom/1, TagScopes)).
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
