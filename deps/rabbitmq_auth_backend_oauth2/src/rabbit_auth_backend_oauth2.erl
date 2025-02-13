%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_oauth2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("oauth2.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([description/0]).
-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4,
         check_topic_access/4, update_state/2,
         expiry_timestamp/1]).

%% for testing
-export([normalize_token_scope/2, get_expanded_scopes/2]).

-import(rabbit_data_coercion, [to_map/1]).
-import(uaa_jwt, [
    decode_and_verify/3,
    get_scope/1, set_scope/2,
    resolve_resource_server/1]).

-import(rabbit_oauth2_rar, [extract_scopes_from_rich_auth_request/2]).

-import(rabbit_oauth2_scope, [
    filter_matching_scope_prefix/2,
    filter_matching_scope_prefix_and_drop_it/2]).

-ifdef(TEST).
-compile(export_all).
-endif.

%%
%% Types
%%

-type ok_extracted_auth_user() :: {ok, rabbit_types:auth_user()}.
-type auth_user_extraction_fun() :: fun((decoded_jwt_token()) -> any()).

%%
%% API
%%

description() ->
    [{name, <<"OAuth 2">>},
     {description, <<"Performs authentication and authorisation using JWT tokens and OAuth 2 scopes">>}].

%%--------------------------------------------------------------------

-spec user_login_authentication(rabbit_types:username(), [term()] | map()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

user_login_authentication(Username, AuthProps) ->
    case authenticate(Username, AuthProps) of
	{refused, Msg, Args} = AuthResult ->
	    rabbit_log:debug(Msg, Args),
	    AuthResult;
	_ = AuthResult ->
	    AuthResult
    end.

-spec user_login_authorization(rabbit_types:username(), [term()] | map()) ->
    {'ok', any()} |
    {'ok', any(), any()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

user_login_authorization(Username, AuthProps) ->
    case authenticate(Username, AuthProps) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

-spec check_vhost_access(AuthUser :: rabbit_types:auth_user(),
                         VHost :: rabbit_types:vhost(),
                         AuthzData :: rabbit_types:authz_data()) -> boolean() | {'error', any()}.
check_vhost_access(#auth_user{impl = DecodedTokenFun},
                   VHost, _AuthzData) ->
    with_decoded_token(DecodedTokenFun(),
        fun(_Token) ->
            DecodedToken = DecodedTokenFun(),
            Scopes = get_scope(DecodedToken),
            ScopeString = rabbit_oauth2_scope:concat_scopes(Scopes, ","),
            rabbit_log:debug("Matching virtual host '~ts' against the following scopes: ~ts", [VHost, ScopeString]),
            rabbit_oauth2_scope:vhost_access(VHost, Scopes)
        end).

check_resource_access(#auth_user{impl = DecodedTokenFun},
                      Resource, Permission, _AuthzContext) ->
    with_decoded_token(DecodedTokenFun(),
        fun(Token) ->
            Scopes = get_scope(Token),
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
with_decoded_token(DecodedToken, Fun) ->
    case validate_token_expiry(DecodedToken) of
        ok               -> Fun(DecodedToken);
        {error, Msg} = Err ->
            rabbit_log:error(Msg),
            Err
    end.

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

extract_scopes_from_scope_claim(Payload) -> 
    case maps:find(?SCOPE_JWT_FIELD, Payload) of
        {ok, Bin} when is_binary(Bin) -> 
            maps:put(?SCOPE_JWT_FIELD, 
                    binary:split(Bin, <<" ">>, [global, trim_all]),
                    Payload);
        _ -> Payload
    end.

-spec normalize_token_scope(
    ResourceServer :: resource_server(), DecodedToken :: decoded_jwt_token()) -> map().
normalize_token_scope(ResourceServer, Payload) ->

    filter_duplicates(   
        filter_matching_scope_prefix(ResourceServer,
            extract_scopes_from_rich_auth_request(ResourceServer,
                extract_scopes_using_scope_aliases(ResourceServer, 
                    extract_scopes_from_additional_scopes_key(ResourceServer, 
                        extract_scopes_from_requesting_party_token(ResourceServer,
                            extract_scopes_from_scope_claim(Payload))))))).

filter_duplicates(#{?SCOPE_JWT_FIELD := Scopes} = Payload) -> 
    set_scope(lists:usort(Scopes), Payload);
filter_duplicates(Payload) -> Payload.

-spec extract_scopes_from_requesting_party_token(
    ResourceServer :: resource_server(), DecodedToken :: decoded_jwt_token()) -> map().
extract_scopes_from_requesting_party_token(ResourceServer, Payload) ->
    Path = ?SCOPES_LOCATION_IN_REQUESTING_PARTY_TOKEN,
    case extract_token_value(ResourceServer, Payload, Path, 
        fun extract_scope_list_from_token_value/2) of
        [] -> 
            Payload;
        AdditionalScopes -> 
            set_scope(lists:flatten(AdditionalScopes) ++ get_scope(Payload), Payload)
    end.

-spec extract_scopes_using_scope_aliases(
     ResourceServer :: resource_server(), Payload :: map()) -> map().
extract_scopes_using_scope_aliases( 
        #resource_server{scope_aliases = ScopeAliasMapping}, Payload) 
        when is_map(ScopeAliasMapping) ->
    Scopes0 = get_scope(Payload),
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
    set_scope(ExpandedScopes, Payload);
extract_scopes_using_scope_aliases(_, Payload) -> Payload.

%% Path is a binary expression which is a plain word like <<"roles">>
%% or +1 word separated by . like <<"authorization.permissions.scopes">>
%% The Payload is a map.
%% Using the path <<"authorization.permissions.scopes">> as an example
%% 1. lookup the key <<"authorization">> in the Payload
%% 2. if it is found, the next map to use as payload is the value found from the key <<"authorization">>
%% 3. lookup the key <<"permissions">> in the previous map 
%% 4. if it is found, it may be a map or a list of maps. 
%% 5. if it is a list of maps, iterate each element in the list 
%% 6. for each element in the list, which should be a map, find the key <<"scopes">>
%% 7. because there are no more words/keys, return a list of all the values found
%%    associated to the word <<"scopes">>
extract_token_value(R, Payload, Path, ValueMapperFun) 
        when is_map(Payload), is_binary(Path), is_function(ValueMapperFun) ->
    extract_token_value_from_map(R, Payload, [], split_path(Path), ValueMapperFun);
extract_token_value(_, _, _, _) ->
    [].

extract_scope_list_from_token_value(_R, List) when is_list(List) -> List;
extract_scope_list_from_token_value(_R, Binary) when is_binary(Binary) -> 
    binary:split(Binary, <<" ">>, [global, trim_all]);
extract_scope_list_from_token_value(#resource_server{id = ResourceServerId}, Map) when is_map(Map) -> 
    case maps:get(ResourceServerId, Map, undefined) of
        undefined           -> [];
        Ks when is_list(Ks) ->
            [erlang:iolist_to_binary([ResourceServerId, <<".">>, K]) || K <- Ks];
        ClaimBin when is_binary(ClaimBin) ->
            UnprefixedClaims = binary:split(ClaimBin, <<" ">>, [global, trim_all]),
            [erlang:iolist_to_binary([ResourceServerId, <<".">>, K]) || K <- UnprefixedClaims];
        _ -> []
    end;
extract_scope_list_from_token_value(_, _) -> [].

extract_token_value_from_map(_, _Map, Acc, [], _Mapper) ->
    Acc;
extract_token_value_from_map(R, Map, Acc, [KeyStr], Mapper) when is_map(Map) ->
    case maps:find(KeyStr, Map) of
        {ok, Value} -> Acc ++ Mapper(R, Value);
        error -> Acc
    end;
extract_token_value_from_map(R, Map, Acc, [KeyStr | Rest], Mapper) when is_map(Map) ->
    case maps:find(KeyStr, Map) of
        {ok, M} when is_map(M) -> extract_token_value_from_map(R, M, Acc, Rest, Mapper);
        {ok, L} when is_list(L) -> extract_token_value_from_list(R, L, Acc, Rest, Mapper); 
        {ok, Value} when Rest =:= [] -> Acc ++ Mapper(R, Value);
        _ -> Acc
    end.

extract_token_value_from_list(_, [], Acc, [], _Mapper) -> 
    Acc;
extract_token_value_from_list(_, [], Acc, [_KeyStr | _Rest], _Mapper) -> 
    Acc;
extract_token_value_from_list(R, [H | T], Acc, [KeyStr | Rest] = KeyList, Mapper) when is_map(H) ->
    NewAcc = case maps:find(KeyStr, H) of
        {ok, Map} when is_map(Map) -> extract_token_value_from_map(R, Map, Acc, Rest, Mapper);
        {ok, List} when is_list(List) -> extract_token_value_from_list(R, List, Acc, Rest, Mapper);
        {ok, Value} -> Acc++Mapper(R, Value);
        _ -> Acc
    end,
    extract_token_value_from_list(R, T, NewAcc, KeyList, Mapper);

extract_token_value_from_list(R, [E | T], Acc, [], Mapper) ->        
    extract_token_value_from_list(R, T, Acc++Mapper(R, E), [], Mapper);
extract_token_value_from_list(R, [E | _T] = L, Acc, KeyList, Mapper) when is_map(E) ->    
    extract_token_value_from_list(R, L, Acc, KeyList, Mapper);
extract_token_value_from_list(R, [_ | T], Acc, KeyList, Mapper) ->
    extract_token_value_from_list(R, T, Acc, KeyList, Mapper).


split_path(Path) when is_binary(Path) ->
    binary:split(Path, <<".">>, [global, trim_all]).


-spec extract_scopes_from_additional_scopes_key(
    ResourceServer :: resource_server(), Payload :: map()) -> map().
extract_scopes_from_additional_scopes_key(
        #resource_server{additional_scopes_key = Key} = ResourceServer, Payload) 
          when is_binary(Key) ->
    Paths = binary:split(Key, <<" ">>, [global, trim_all]),
    AdditionalScopes = [ extract_token_value(ResourceServer, 
        Payload, Path, fun extract_scope_list_from_token_value/2) || Path <- Paths],    
    set_scope(lists:flatten(AdditionalScopes) ++ get_scope(Payload), Payload);
extract_scopes_from_additional_scopes_key(_, Payload) -> Payload.


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
