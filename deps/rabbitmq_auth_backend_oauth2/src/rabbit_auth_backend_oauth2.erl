%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
%%--------------------------------------------------------------------

-define(APP, rabbitmq_auth_backend_oauth2).
-define(RESOURCE_SERVER_ID, resource_server_id).
-define(COMPLEX_CLAIM, extra_permissions_source).

description() ->
    [{name, <<"UAA">>},
     {description, <<"Performs authentication and authorisation using JWT tokens and OAuth 2 scopes">>}].

%%--------------------------------------------------------------------

user_login_authentication(Username0, AuthProps0) ->
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
                        Username = username_from(Username0, DecodedToken),
                        Tags     = tags_from(DecodedToken),

                        {ok, #auth_user{username = Username,
                                        tags = Tags,
                                        impl = DecodedToken}}
                   end,
            case with_decoded_token(DecodedToken, Func) of
                {error, Err} ->
                    {refused, "Authentication using an OAuth 2/JWT token failed: ~p", [Err]};
                Else ->
                    Else
            end
    end.

user_login_authorization(Username, AuthProps) ->
    case user_login_authentication(Username, AuthProps) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

check_vhost_access(#auth_user{impl = DecodedToken},
                   VHost, _AuthzData) ->
    with_decoded_token(DecodedToken,
        fun() ->
            Scopes      = get_scopes(DecodedToken),
            ScopeString = rabbit_oauth2_scope:concat_scopes(Scopes, ","),
            rabbit_log:debug("Matching virtual host '~s' against the following scopes: ~s", [VHost, ScopeString]),
            rabbit_oauth2_scope:vhost_access(VHost, Scopes)
        end).

check_resource_access(#auth_user{impl = DecodedToken},
                      Resource, Permission, _AuthzContext) ->
    with_decoded_token(DecodedToken,
        fun() ->
            Scopes = get_scopes(DecodedToken),
            rabbit_oauth2_scope:resource_access(Resource, Permission, Scopes)
        end).

check_topic_access(#auth_user{impl = DecodedToken},
                   Resource, Permission, Context) ->
    with_decoded_token(DecodedToken,
        fun() ->
            Scopes = get_scopes(DecodedToken),
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
                                  impl = DecodedToken}}
  end.

%%--------------------------------------------------------------------

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

-spec check_token(binary()) -> {ok, map()} | {error, term()}.
check_token(Token) ->
    case uaa_jwt:decode_and_verify(Token) of
        {error, Reason} -> {refused, {error, Reason}};
        {true, Payload} -> validate_payload(post_process_payload(Payload));
        {false, _}      -> {refused, signature_invalid}
    end.

post_process_payload(Payload) when is_map(Payload) ->
    Payload0 = case is_complex_claim(Payload) of
        true ->
            post_process_payload_complex_claim(Payload);
        false ->
            Payload
        end,

    Payload1 = case maps:is_key(<<"authorization">>, Payload0) of
        true ->
            post_process_payload_keycloak(Payload0);
        false ->
            Payload0
        end,
        
    maps:map(fun(K, V) ->
            case K of
                <<"aud">> when is_binary(V) ->
                    binary:split(V, <<" ">>, [global]);
                <<"scope">> when is_binary(V) ->
                    binary:split(V, <<" ">>, [global]);
                _ -> V
            end
        end,
        Payload1
    ).
    
is_complex_claim(Payload) when is_map(Payload) ->
        ComplexClaimName = application:get_env(?APP, ?COMPLEX_CLAIM, <<>>),
        maps:is_key(ComplexClaimName, Payload).

post_process_payload_complex_claim(Payload) ->
    ComplexClaimName = application:get_env(?APP, ?COMPLEX_CLAIM, <<>>),
    ComplexClaimContent = maps:get(ComplexClaimName, Payload),

    ResourceServerEnv = application:get_env(?APP, ?RESOURCE_SERVER_ID, <<>>),
    ResourceServerId = rabbit_data_coercion:to_binary(ResourceServerEnv),

    Scopes = 
        case ComplexClaimContent of
            Content when is_list(Content) ->
                Content;
            Content when is_map(Content) ->
                case maps:get(ResourceServerId, Content, empty) of
                    empty ->
                        [];
                    Permissions when is_list(Permissions) ->
                        case Permissions of
                            [] ->
                                [];
                            _ ->
                                [erlang:iolist_to_binary([ResourceServerId, <<".">>, K]) || K <- Permissions]
                            end;
                    Permission when is_binary(Permission) ->
                        case Permission of
                            <<>> ->
                                [];
                            _ ->
                                RawClaims = binary:split(Permission, <<" ">>, [global]),
                                [erlang:iolist_to_binary([ResourceServerId, <<".">>, K]) || K <- RawClaims]
                            end;
                    _ -> 
                        []
                    end;
            Content when is_binary(Content) ->
                case Content of
                    <<>> ->
                        [];
                    _ ->
                        binary:split(Content, <<" ">>, [global])
                    end;
            _ ->
                []
            end,

    case Scopes of
        [] ->
            Payload;
        _ -> 
            TokenScopes = maps:get(<<"scope">>, Payload),
            case TokenScopes of
                [] ->
                    maps:put(<<"scope">>, Scopes, Payload);
                _ ->                            
                    maps:put(<<"scope">>, [TokenScopes, Scopes], Payload)
            end
        end.

%% keycloak token format: https://github.com/rabbitmq/rabbitmq-auth-backend-oauth2/issues/36
post_process_payload_keycloak(#{<<"authorization">> := Authorization} = Payload) ->
    Scopes = case maps:get(<<"permissions">>, Authorization, no_permissions) of
        no_permissions ->
            [];
        Permissions ->
            extract_scopes_from_keycloak_permissions([], Permissions)
    end,
    case Scopes of
        [] ->
            Payload;
        _  ->            
            TokenScopes = maps:get(<<"scope">>, Payload),
            case TokenScopes of
                [] ->
                    maps:put(<<"scope">>, Scopes, Payload);
                _ ->                            
                    maps:put(<<"scope">>, [TokenScopes, Scopes], Payload)
            end
    end.

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

validate_payload(#{<<"scope">> := _Scope, <<"aud">> := _Aud} = DecodedToken) ->
    ResourceServerEnv = application:get_env(?APP, ?RESOURCE_SERVER_ID, <<>>),
    ResourceServerId = rabbit_data_coercion:to_binary(ResourceServerEnv),
    validate_payload(DecodedToken, ResourceServerId).

validate_payload(#{<<"scope">> := Scope, <<"aud">> := Aud} = DecodedToken, ResourceServerId) ->
    case check_aud(Aud, ResourceServerId) of
        ok           -> {ok, DecodedToken#{<<"scope">> => filter_scopes(Scope, ResourceServerId)}};
        {error, Err} -> {refused, {invalid_aud, Err}}
    end.

filter_scopes(Scopes, <<"">>) -> Scopes;
filter_scopes(Scopes, ResourceServerId)  ->
    PrefixPattern = <<ResourceServerId/binary, ".">>,
    matching_scopes_without_prefix(Scopes, PrefixPattern).

check_aud(_, <<>>)    -> ok;
check_aud(Aud, ResourceServerId) ->
    case Aud of
        List when is_list(List) ->
            case lists:member(ResourceServerId, Aud) of
                true  -> ok;
                false -> {error, {resource_id_not_found_in_aud, ResourceServerId, Aud}}
            end;
        _ -> {error, {badarg, {aud_is_not_a_list, Aud}}}
    end.

%%--------------------------------------------------------------------

get_scopes(#{<<"scope">> := Scope}) -> Scope.

-spec token_from_context(map()) -> binary() | undefined.
token_from_context(AuthProps) ->
    maps:get(password, AuthProps, undefined).

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

-spec username_from(binary(), map()) -> binary() | undefined.
username_from(ClientProvidedUsername, DecodedToken) ->
    ClientId = uaa_jwt:client_id(DecodedToken, undefined),
    Sub      = uaa_jwt:sub(DecodedToken, undefined),

    rabbit_log:debug("Computing username from client's JWT token, client ID: '~s', sub: '~s'",
                     [ClientId, Sub]),

    case uaa_jwt:client_id(DecodedToken, Sub) of
        undefined ->
            case ClientProvidedUsername of
                undefined -> undefined;
                <<>>      -> undefined;
                _Other    -> ClientProvidedUsername
            end;
        Value     ->
            Value
    end.

-spec tags_from(map()) -> list(atom()).
tags_from(DecodedToken) ->
    Scopes    = maps:get(<<"scope">>, DecodedToken, []),
    TagScopes = matching_scopes_without_prefix(Scopes, <<"tag:">>),
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
