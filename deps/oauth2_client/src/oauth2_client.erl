%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(oauth2_client).
-export([get_access_token/2, get_expiration_time/1,
        refresh_access_token/2,
        get_oauth_provider/1, get_oauth_provider/2,
        get_openid_configuration/2, get_openid_configuration/3,
        merge_openid_configuration/2,
        merge_oauth_provider/2,
        extract_ssl_options_as_list/1,
        format_ssl_options/1, format_oauth_provider/1, format_oauth_provider_id/1
        ]).

-include("oauth2_client.hrl").
-spec get_access_token(oauth_provider(), access_token_request()) ->
    {ok, successful_access_token_response()} | {error, unsuccessful_access_token_response() | any()}.
get_access_token(OAuthProvider, Request) ->
    rabbit_log:debug("get_access_token using OAuthProvider:~p and client_id:~p",
        [OAuthProvider, Request#access_token_request.client_id]),
        URL = OAuthProvider#oauth_provider.token_endpoint,
    Header = [],
    Type = ?CONTENT_URLENCODED,
    Body = build_access_token_request_body(Request),
    HTTPOptions = get_ssl_options_if_any(OAuthProvider) ++
        get_timeout_of_default(Request#access_token_request.timeout),
    Options = [],
    Response = httpc:request(post, {URL, Header, Type, Body}, HTTPOptions, Options),
    parse_access_token_response(Response).

-spec refresh_access_token(oauth_provider(), refresh_token_request()) ->
    {ok, successful_access_token_response()} | {error, unsuccessful_access_token_response() | any()}.
refresh_access_token(OAuthProvider, Request) ->
    URL = OAuthProvider#oauth_provider.token_endpoint,
    Header = [],
    Type = ?CONTENT_URLENCODED,
    Body = build_refresh_token_request_body(Request),
    HTTPOptions = get_ssl_options_if_any(OAuthProvider) ++
        get_timeout_of_default(Request#refresh_token_request.timeout),
    Options = [],
    Response = httpc:request(post, {URL, Header, Type, Body}, HTTPOptions, Options),
    parse_access_token_response(Response).

append_paths(Path1, Path2) ->
    erlang:iolist_to_binary([Path1, Path2]).

-spec get_openid_configuration(uri_string:uri_string(), erlang:iodata() | <<>>,
    ssl:tls_option() | []) -> {ok, openid_configuration()} | {error, term()}.
get_openid_configuration(IssuerURI, OpenIdConfigurationPath, TLSOptions) ->
    URLMap = uri_string:parse(IssuerURI),
    Path = case maps:get(path, URLMap) of
        "/" -> OpenIdConfigurationPath;
        "" -> OpenIdConfigurationPath;
        P -> append_paths(P, OpenIdConfigurationPath)
    end,
    URL = uri_string:resolve(Path, IssuerURI),
    rabbit_log:debug("get_openid_configuration issuer URL ~p (~p)", [URL,
        format_ssl_options(TLSOptions)]),
    Options = [],
    Response = httpc:request(get, {URL, []}, TLSOptions, Options),
    parse_openid_configuration_response(Response).

-spec get_openid_configuration(uri_string:uri_string(), ssl:tls_option() | []) ->
    {ok, openid_configuration()} | {error, term()}.
get_openid_configuration(IssuerURI, TLSOptions) ->
    get_openid_configuration(IssuerURI, ?DEFAULT_OPENID_CONFIGURATION_PATH, TLSOptions).
% Returns {ok, with_modidified_oauth_provider} or {ok} if oauth_provider was
% not modified
-spec merge_openid_configuration(openid_configuration(), oauth_provider()) ->
    oauth_provider().
merge_openid_configuration(OpendIdConfiguration, OAuthProvider) ->
    OAuthProvider0 = case OpendIdConfiguration#openid_configuration.issuer of
        undefined -> OAuthProvider;
        Issuer ->
            OAuthProvider#oauth_provider{issuer = Issuer}
    end,
    OAuthProvider1 = case OpendIdConfiguration#openid_configuration.token_endpoint of
        undefined -> OAuthProvider0;
        TokenEndpoint ->
            OAuthProvider0#oauth_provider{token_endpoint = TokenEndpoint}
    end,
    OAuthProvider2 = case OpendIdConfiguration#openid_configuration.authorization_endpoint of
        undefined -> OAuthProvider1;
        AuthorizationEndpoint ->
            OAuthProvider1#oauth_provider{authorization_endpoint = AuthorizationEndpoint}
    end,
    OAuthProvider3 = case OpendIdConfiguration#openid_configuration.end_session_endpoint of
        undefined -> OAuthProvider2;
        EndSessionEndpoint ->
            OAuthProvider2#oauth_provider{end_session_endpoint = EndSessionEndpoint}
    end,
    case OpendIdConfiguration#openid_configuration.jwks_uri of
        undefined -> OAuthProvider3;
        JwksUri ->
            OAuthProvider3#oauth_provider{jwks_uri = JwksUri}
    end.

-spec merge_oauth_provider(oauth_provider(), proplists:proplist()) ->
    proplists:proplist().
merge_oauth_provider(OAuthProvider, Proplist) ->
    Proplist0 = case OAuthProvider#oauth_provider.token_endpoint of
        undefined ->  Proplist;
        TokenEndpoint -> [{token_endpoint, TokenEndpoint} |
            proplists:delete(token_endpoint, Proplist)]
    end,
    Proplist1 = case OAuthProvider#oauth_provider.authorization_endpoint of
        undefined ->  Proplist0;
        AuthzEndpoint -> [{authorization_endpoint, AuthzEndpoint} |
            proplists:delete(authorization_endpoint, Proplist0)]
    end,
    Proplist2 = case OAuthProvider#oauth_provider.end_session_endpoint of
        undefined ->  Proplist1;
        EndSessionEndpoint -> [{end_session_endpoint, EndSessionEndpoint} |
            proplists:delete(end_session_endpoint, Proplist1)]
    end,
    case OAuthProvider#oauth_provider.jwks_uri of
        undefined ->  Proplist2;
        JwksEndPoint -> [{jwks_uri, JwksEndPoint} |
            proplists:delete(jwks_uri, Proplist2)]
    end.

parse_openid_configuration_response({error, Reason}) ->
    {error, Reason};
parse_openid_configuration_response({ok,{{_,Code,Reason}, Headers, Body}}) ->
    map_response_to_openid_configuration(Code, Reason, Headers, Body).
map_response_to_openid_configuration(Code, Reason, Headers, Body) ->
    case decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body) of
        {error, {error, InternalError}} ->
            {error, InternalError};
        {error, _} = Error ->
            Error;
        Value ->
            case Code of
                200 -> {ok, map_to_openid_configuration(Value)};
                201 -> {ok, map_to_openid_configuration(Value)};
                _ ->   {error, Reason}
            end
    end.
map_to_openid_configuration(Map) ->
    #openid_configuration{
        issuer = maps:get(?RESPONSE_ISSUER, Map),
        token_endpoint = maps:get(?RESPONSE_TOKEN_ENDPOINT, Map, undefined),
        authorization_endpoint = maps:get(?RESPONSE_AUTHORIZATION_ENDPOINT, Map, undefined),
        end_session_endpoint = maps:get(?RESPONSE_END_SESSION_ENDPOINT, Map, undefined),
        jwks_uri = maps:get(?RESPONSE_JWKS_URI, Map, undefined)
    }.

-spec get_expiration_time(successful_access_token_response()) ->
    {ok, [{expires_in, integer() }| {exp, integer() }]} | {error, missing_exp_field}.
get_expiration_time(#successful_access_token_response{expires_in = ExpiresInSec,
        access_token = AccessToken}) ->
    case ExpiresInSec of
        undefined ->
            case jwt_helper:get_expiration_time(jwt_helper:decode(AccessToken)) of
                {ok, Exp} -> {ok, [{exp, Exp}]};
                {error, _} = Error -> Error
            end;
        _ -> {ok, [{expires_in, ExpiresInSec}]}
    end.

update_oauth_provider_endpoints_configuration(OAuthProvider) ->
    LockId = lock(),
    try do_update_oauth_provider_endpoints_configuration(OAuthProvider) of
        V -> V
    after
        unlock(LockId)
    end.

update_oauth_provider_endpoints_configuration(OAuthProviderId, OAuthProvider) ->
    LockId = lock(),
    try do_update_oauth_provider_endpoints_configuration(OAuthProviderId, OAuthProvider) of
        V -> V
    after
        unlock(LockId)
    end.

do_update_oauth_provider_endpoints_configuration(OAuthProvider) ->
    case OAuthProvider#oauth_provider.token_endpoint of
        undefined ->
            do_nothing;
        TokenEndpoint ->
            application:set_env(rabbitmq_auth_backend_oauth2, token_endpoint, TokenEndpoint)
    end,
    case OAuthProvider#oauth_provider.authorization_endpoint of
        undefined ->
            do_nothing;
        AuthzEndpoint ->
            application:set_env(rabbitmq_auth_backend_oauth2, authorization_endpoint, AuthzEndpoint)
    end,
    case OAuthProvider#oauth_provider.end_session_endpoint of
        undefined ->
            do_nothing;
        EndSessionEndpoint ->
            application:set_env(rabbitmq_auth_backend_oauth2, end_session_endpoint, EndSessionEndpoint)
    end,
    List = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    ModifiedList = case OAuthProvider#oauth_provider.jwks_uri of
        undefined ->  List;
        JwksEndPoint -> [{jwks_url, JwksEndPoint} | proplists:delete(jwks_url, List)]
    end,
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, ModifiedList),
    rabbit_log:debug("Updated oauth_provider details: ~p ", [ format_oauth_provider(OAuthProvider)]),
    OAuthProvider.

do_update_oauth_provider_endpoints_configuration(OAuthProviderId, OAuthProvider) ->
    OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
    Proplist = maps:get(OAuthProviderId, OAuthProviders),
    ModifiedOAuthProviders = maps:put(OAuthProviderId,
        merge_oauth_provider(OAuthProvider, Proplist), OAuthProviders),
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, ModifiedOAuthProviders),
    rabbit_log:debug("Replaced oauth_providers "),
    OAuthProvider.

use_global_locks_on_all_nodes() ->
    case application:get_env(rabbitmq_auth_backend_oauth2, use_global_locks, true) of
        true -> {rabbit_nodes:list_running(), rabbit_nodes:lock_retries()};
        _ -> {}
    end.

lock() ->
    case use_global_locks_on_all_nodes() of
      {} ->
        case global:set_lock({oauth2_config_lock, rabbitmq_auth_backend_oauth2}) of
          true  -> rabbitmq_auth_backend_oauth2;
          false -> undefined
        end;
      {Nodes, Retries} ->
        case global:set_lock({oauth2_config_lock, rabbitmq_auth_backend_oauth2}, Nodes, Retries) of
          true  -> rabbitmq_auth_backend_oauth2;
          false -> undefined
        end
    end.

unlock(LockId) ->
    case LockId of
        undefined -> ok;
        Value ->
            case use_global_locks_on_all_nodes() of
                {} -> global:del_lock({oauth2_config_lock, Value});
                {Nodes, _Retries} -> global:del_lock({oauth2_config_lock, Value}, Nodes)
            end
    end.

-spec get_oauth_provider(list()) -> {ok, oauth_provider()} | {error, any()}.
get_oauth_provider(ListOfRequiredAttributes) ->
    case application:get_env(rabbitmq_auth_backend_oauth2, default_oauth_provider) of
        undefined -> get_oauth_provider_from_keyconfig(ListOfRequiredAttributes);
        {ok, DefaultOauthProviderId} ->
            rabbit_log:debug("Using default_oauth_provider ~p", [DefaultOauthProviderId]),
            get_oauth_provider(DefaultOauthProviderId, ListOfRequiredAttributes)
    end.

get_oauth_provider_from_keyconfig(ListOfRequiredAttributes) ->
    OAuthProvider = lookup_oauth_provider_from_keyconfig(),
    rabbit_log:debug("Using oauth_provider ~p from keyconfig", [format_oauth_provider(OAuthProvider)]),
    case find_missing_attributes(OAuthProvider, ListOfRequiredAttributes) of
        [] ->
            {ok, OAuthProvider};
        _ = MissingAttributes ->
            rabbit_log:debug("OauthProvider has following missing attributes ~p", [MissingAttributes]),
            Result2 = case OAuthProvider#oauth_provider.issuer of
                undefined -> {error, {missing_oauth_provider_attributes, [issuer]}};
                Issuer ->
                    rabbit_log:debug("Downloading oauth_provider using issuer ~p", [Issuer]),
                    case get_openid_configuration(Issuer, get_ssl_options_if_any(OAuthProvider)) of
                        {ok, OpenIdConfiguration} ->
                            {ok, update_oauth_provider_endpoints_configuration(
                                merge_openid_configuration(OpenIdConfiguration, OAuthProvider))};
                        {error, _} = Error2 -> Error2
                    end
                end,
            case Result2 of
                {ok, OAuthProvider2} ->
                    case find_missing_attributes(OAuthProvider2, ListOfRequiredAttributes) of
                        [] ->
                            rabbit_log:debug("Resolved oauth_provider ~p", [format_oauth_provider(OAuthProvider)]),
                            {ok, OAuthProvider2};
                        _ = Attrs->
                            {error, {missing_oauth_provider_attributes, Attrs}}
                    end;
                {error, _} = Error3 -> Error3
            end
  end.


-spec get_oauth_provider(oauth_provider_id(), list()) -> {ok, oauth_provider()} | {error, any()}.
get_oauth_provider(root, ListOfRequiredAttributes) ->
    get_oauth_provider(ListOfRequiredAttributes);

get_oauth_provider(OAuth2ProviderId, ListOfRequiredAttributes) when is_list(OAuth2ProviderId) ->
    get_oauth_provider(list_to_binary(OAuth2ProviderId), ListOfRequiredAttributes);

get_oauth_provider(OAuthProviderId, ListOfRequiredAttributes) when is_binary(OAuthProviderId) ->
    rabbit_log:debug("get_oauth_provider ~p with at least these attributes: ~p", [OAuthProviderId, ListOfRequiredAttributes]),
    case lookup_oauth_provider_config(OAuthProviderId) of
        {error, _} = Error0 ->
            rabbit_log:debug("Failed to find oauth_provider ~p configuration due to ~p",
                [OAuthProviderId, Error0]),
            Error0;
        Config ->
            rabbit_log:debug("Found oauth_provider configuration ~p", [Config]),
            OAuthProvider = map_to_oauth_provider(Config),
            rabbit_log:debug("Resolved oauth_provider ~p", [format_oauth_provider(OAuthProvider)]),
            case find_missing_attributes(OAuthProvider, ListOfRequiredAttributes) of
                [] ->
                    {ok, OAuthProvider};
                _ = MissingAttributes ->
                    rabbit_log:debug("OauthProvider has following missing attributes ~p", [MissingAttributes]),
                    Result2 = case OAuthProvider#oauth_provider.issuer of
                        undefined -> {error, {missing_oauth_provider_attributes, [issuer]}};
                        Issuer ->
                            rabbit_log:debug("Downloading oauth_provider ~p using issuer ~p",
                                [OAuthProviderId, Issuer]),
                            case get_openid_configuration(Issuer, get_ssl_options_if_any(OAuthProvider)) of
                                {ok, OpenIdConfiguration} ->
                                    {ok, update_oauth_provider_endpoints_configuration(OAuthProviderId,
                                        merge_openid_configuration(OpenIdConfiguration, OAuthProvider))};
                                {error, _} = Error2 -> Error2
                            end
                    end,
                    case Result2 of
                        {ok, OAuthProvider2} ->
                            case find_missing_attributes(OAuthProvider2, ListOfRequiredAttributes) of
                                [] ->
                                    rabbit_log:debug("Resolved oauth_provider ~p", [format_oauth_provider(OAuthProvider)]),
                                    {ok, OAuthProvider2};
                                _ = Attrs->
                                    {error, {missing_oauth_provider_attributes, Attrs}}
                            end;
                        {error, _} = Error3 -> Error3
                    end
            end
    end.

%% HELPER functions


oauth_provider_to_proplists(#oauth_provider{} = OAuthProvider) ->
    lists:zip(record_info(fields, oauth_provider), tl(tuple_to_list(OAuthProvider))).
filter_undefined_props(PropList) ->
    lists:foldl(fun(Prop, Acc) ->
        case Prop of
            {Name, undefined} -> Acc ++ [Name];
            _ -> Acc
        end end, [], PropList).

-spec intersection(list(), list()) -> list().
intersection(L1, L2) ->
    S1 = sets:from_list(L1),
    S2 = sets:from_list(L2),
    lists:usort(sets:to_list(sets:intersection(S1, S2))).

find_missing_attributes(#oauth_provider{} = OAuthProvider, RequiredAttributes) ->
    PropList = oauth_provider_to_proplists(OAuthProvider),
    Filtered = filter_undefined_props(PropList),
    intersection(Filtered, RequiredAttributes).

lookup_oauth_provider_from_keyconfig() ->
    Issuer = application:get_env(rabbitmq_auth_backend_oauth2, issuer, undefined),
    TokenEndpoint = application:get_env(rabbitmq_auth_backend_oauth2, token_endpoint, undefined),
    AuthorizationEndpoint = application:get_env(rabbitmq_auth_backend_oauth2, authorization_endpoint, undefined),
    EndSessionEndpoint = application:get_env(rabbitmq_auth_backend_oauth2, end_session_endpoint, undefined),
    Map = maps:from_list(application:get_env(rabbitmq_auth_backend_oauth2, key_config, [])),
    #oauth_provider{
        id = root,
        issuer = Issuer,
        jwks_uri = maps:get(jwks_url, Map, undefined), %% jwks_url not uri . _url is the legacy name
        token_endpoint = TokenEndpoint,
        authorization_endpoint = AuthorizationEndpoint,
        end_session_endpoint = EndSessionEndpoint,
        ssl_options = extract_ssl_options_as_list(Map)
    }.

-spec extract_ssl_options_as_list(#{atom() => any()}) -> proplists:proplist().
extract_ssl_options_as_list(Map) ->
    {Verify, CaCerts, CaCertFile} = case get_verify_or_peer_verification(Map, verify_peer) of
        verify_peer ->
            case maps:get(cacertfile, Map, undefined) of
                undefined ->
                    case public_key:cacerts_get() of
                        [] -> {verify_none, undefined, undefined};
                        Certs -> {verify_peer, Certs, undefined}
                    end;
                CaCert -> {verify_peer, undefined, CaCert}
            end;
        verify_none -> {verify_none, undefined, undefined}
    end,
    [ {verify, Verify} ]
    ++
    case Verify of
        verify_none -> [];
        _ ->
            [
            {depth, maps:get(depth, Map, 10)},
            {crl_check, maps:get(crl_check, Map, false)},
            {fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Map, false)}
            ]
    end
    ++
    case Verify of
        verify_none -> [];
        _ ->
            case {CaCerts, CaCertFile} of
                {_, undefined} -> [{cacerts, CaCerts}];
                {undefined, _} -> [{cacertfile, CaCertFile}]
            end
    end
    ++
    case maps:get(hostname_verification, Map, none) of
        wildcard ->
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}];
        none ->
            []
    end.

% Replace peer_verification with verify to make it more consistent with other
% ssl_options in RabbitMQ and Erlang's ssl options
% Eventually, peer_verification will be removed. For now, both are allowed
-spec get_verify_or_peer_verification(#{atom() => any()}, verify_none | verify_peer ) -> verify_none | verify_peer.
get_verify_or_peer_verification(Ssl_options, Default) ->
    case maps:get(verify, Ssl_options, undefined) of
        undefined ->
            case maps:get(peer_verification, Ssl_options, undefined) of
                undefined -> Default;
                PeerVerification -> PeerVerification
            end;
        Verify -> Verify
    end.

lookup_oauth_provider_config(OAuth2ProviderId) ->
    case application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers) of
        undefined -> {error, oauth_providers_not_found};
        {ok, MapOfProviders} when is_map(MapOfProviders) ->
            case maps:get(OAuth2ProviderId, MapOfProviders, undefined) of
                undefined ->
                    {error, {oauth_provider_not_found, OAuth2ProviderId}};
                OAuthProvider ->
                    ensure_oauth_provider_has_id_property(OAuth2ProviderId, OAuthProvider)
            end;
        _ ->  {error, invalid_oauth_provider_configuration}
    end.
ensure_oauth_provider_has_id_property(OAuth2ProviderId, OAuth2Provider) ->
    case proplists:is_defined(id, OAuth2Provider) of
        true -> OAuth2Provider;
        false -> OAuth2Provider ++ [{id, OAuth2ProviderId}]
    end.

build_access_token_request_body(Request) ->
    uri_string:compose_query([
        grant_type_request_parameter(?CLIENT_CREDENTIALS_GRANT_TYPE),
        client_id_request_parameter(Request#access_token_request.client_id),
        client_secret_request_parameter(Request#access_token_request.client_secret)]
        ++ scope_request_parameter_or_default(Request#access_token_request.scope, [])).

build_refresh_token_request_body(Request) ->
    uri_string:compose_query([
        grant_type_request_parameter(?REFRESH_TOKEN_GRANT_TYPE),
        refresh_token_request_parameter(Request#refresh_token_request.refresh_token),
        client_id_request_parameter(Request#refresh_token_request.client_id),
        client_secret_request_parameter(Request#refresh_token_request.client_secret)]
        ++ scope_request_parameter_or_default(Request#refresh_token_request.scope, [])).

grant_type_request_parameter(Type) ->
    {?REQUEST_GRANT_TYPE, Type}.
client_id_request_parameter(Client_id) ->
    {?REQUEST_CLIENT_ID, binary_to_list(Client_id)}.
client_secret_request_parameter(Client_secret) ->
    {?REQUEST_CLIENT_SECRET, binary_to_list(Client_secret)}.
refresh_token_request_parameter(RefreshToken) ->
    {?REQUEST_REFRESH_TOKEN, RefreshToken}.
scope_request_parameter_or_default(Scope, Default) ->
    case Scope of
        undefined -> Default;
        <<>> -> Default;
        Scope -> [{?REQUEST_SCOPE, Scope}]
    end.

get_ssl_options_if_any(OAuthProvider) ->
    case OAuthProvider#oauth_provider.ssl_options of
        undefined -> [];
        Options ->  [{ssl, Options}]
    end.
get_timeout_of_default(Timeout) ->
    case Timeout of
        undefined -> [{timeout, ?DEFAULT_HTTP_TIMEOUT}];
        Timeout -> [{timeout, Timeout}]
    end.

is_json(?CONTENT_JSON) -> true;
is_json(_) -> false.

-spec decode_body(string(), string() | binary() | term()) -> 'false' | 'null' | 'true' |
                                                              binary() | [any()] | number() | map() | {error, term()}.

decode_body(_, []) -> [];
decode_body(?CONTENT_JSON, Body) ->
    case rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body)) of
        {ok, Value} ->
            Value;
        {error, _} = Error  ->
            Error
    end;
decode_body(MimeType, Body) ->
    Items = string:split(MimeType, ";"),
    case lists:any(fun is_json/1, Items) of
        true -> decode_body(?CONTENT_JSON, Body);
        false -> {error, mime_type_is_not_json}
    end.
map_to_successful_access_token_response(Map) ->
    #successful_access_token_response{
        access_token = maps:get(?RESPONSE_ACCESS_TOKEN, Map),
        token_type = maps:get(?RESPONSE_TOKEN_TYPE, Map, undefined),
        refresh_token = maps:get(?RESPONSE_REFRESH_TOKEN, Map, undefined),
        expires_in = maps:get(?RESPONSE_EXPIRES_IN, Map, undefined)
    }.
map_to_unsuccessful_access_token_response(Map) ->
    #unsuccessful_access_token_response{
        error = maps:get(?RESPONSE_ERROR, Map),
        error_description = maps:get(?RESPONSE_ERROR_DESCRIPTION, Map, undefined)
    }.
map_to_oauth_provider(PropList) when is_list(PropList) ->
    #oauth_provider{
        id = proplists:get_value(id, PropList),
        issuer = proplists:get_value(issuer, PropList),
        token_endpoint = proplists:get_value(token_endpoint, PropList),
        authorization_endpoint = proplists:get_value(authorization_endpoint, PropList, undefined),
        end_session_endpoint = proplists:get_value(end_session_endpoint, PropList, undefined),
        jwks_uri = proplists:get_value(jwks_uri, PropList, undefined),
        ssl_options = extract_ssl_options_as_list(maps:from_list(proplists:get_value(https, PropList, [])))
    }.
map_to_access_token_response(Code, Reason, Headers, Body) ->
    case decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body) of
        {error, {error, InternalError}} ->
            {error, InternalError};
        {error, _} = Error ->
            Error;
        Value ->
            case Code of
                200 -> {ok, map_to_successful_access_token_response(Value)};
                201 -> {ok, map_to_successful_access_token_response(Value)};
                204 -> {ok, []};
                400 -> {error, map_to_unsuccessful_access_token_response(Value)};
                401 -> {error, map_to_unsuccessful_access_token_response(Value)};
                _ ->   {error, Reason}
            end
    end.
parse_access_token_response({error, Reason}) ->
    {error, Reason};
parse_access_token_response({ok,{{_,Code,Reason}, Headers, Body}}) ->
    map_to_access_token_response(Code, Reason, Headers, Body).

-spec format_ssl_options([ssl:tls_client_option()]) -> string().
format_ssl_options(TlsOptions) ->
    CaCertsCount = case proplists:get_value(cacerts, TlsOptions, []) of
        [] -> 0;
        Certs -> length(Certs)
    end,
    lists:flatten(io_lib:format("{verify: ~p, fail_if_no_peer_cert: ~p, crl_check: ~p, depth: ~p, cacertfile: ~p, cacerts(count): ~p }", [
        proplists:get_value(verify, TlsOptions),
        proplists:get_value(fail_if_no_peer_cert, TlsOptions),
        proplists:get_value(crl_check, TlsOptions),
        proplists:get_value(depth, TlsOptions),
        proplists:get_value(cacertfile, TlsOptions),
        CaCertsCount])).

format_oauth_provider_id(root) -> "<from keyconfig>";
format_oauth_provider_id(Id) -> binary_to_list(Id).

-spec format_oauth_provider(oauth_provider()) -> string().
format_oauth_provider(OAuthProvider) ->
    lists:flatten(io_lib:format("{id: ~p, issuer: ~p, token_endpoint: ~p, " ++
        "authorization_endpoint: ~p, end_session_endpoint: ~p, " ++
        "jwks_uri: ~p, ssl_options: ~p }", [
        format_oauth_provider_id(OAuthProvider#oauth_provider.id),
        OAuthProvider#oauth_provider.issuer,
        OAuthProvider#oauth_provider.token_endpoint,
        OAuthProvider#oauth_provider.authorization_endpoint,
        OAuthProvider#oauth_provider.end_session_endpoint,
        OAuthProvider#oauth_provider.jwks_uri,
        format_ssl_options(OAuthProvider#oauth_provider.ssl_options)])).
