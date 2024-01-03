-module(oauth2_client).
-export([get_access_token/2,
        refresh_access_token/2,
        get_oauth_provider/1,get_oauth_provider/2
        ]).

-include("oauth2_client.hrl").
-define(APP, auth_aouth2).

-spec get_access_token(oauth_provider_id() | oauth_provider(), access_token_request()) ->
  {ok, successful_access_token_response()} | {error, unsuccessful_access_token_response() | any()}.
get_access_token(OAuth2ProviderId, Request) when is_binary(OAuth2ProviderId) ->
  rabbit_log:debug("get_access_token using OAuth2ProviderId:~p and client_id:~p",
    [OAuth2ProviderId, Request#access_token_request.client_id]),
  case get_oauth_provider(OAuth2ProviderId, [token_endpoint]) of
    {error, _Error } = Error0 -> Error0;
    {ok, Provider} -> get_access_token(Provider, Request)
  end;

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

-spec get_openid_configuration(uri_string:uri_string(), erlang:iodata() | <<>>, ssl:tls_option() | []) -> {ok, oauth_provider()} | {error, term()}.
get_openid_configuration(IssuerURI, OpenIdConfigurationPath, TLSOptions) ->
  URLMap = uri_string:parse(IssuerURI),
  Path = append_paths(maps:get(path, URLMap), OpenIdConfigurationPath),
  URL = uri_string:resolve(Path, IssuerURI),
  rabbit_log:debug("get_openid_configuration issuer URL ~p (~p)", [URL, TLSOptions]),
  Options = [],
  Response = httpc:request(get, {URL, []}, TLSOptions, Options),
  enrich_oauth_provider(parse_openid_configuration_response(Response), TLSOptions).

-spec get_openid_configuration(uri_string:uri_string(), ssl:tls_option() | []) ->  {ok, oauth_provider()} | {error, term()}.
get_openid_configuration(IssuerURI, TLSOptions) ->
  get_openid_configuration(IssuerURI, ?DEFAULT_OPENID_CONFIGURATION_PATH, TLSOptions).

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
    undefined ->  do_nothing;
    TokenEndPoint -> application:set_env(rabbitmq_auth_backend_oauth2, token_endpoint, TokenEndPoint)
  end,
  case OAuthProvider#oauth_provider.authorization_endpoint of
    undefined ->  do_nothing;
    AuthzEndPoint -> application:set_env(rabbitmq_auth_backend_oauth2, authorization_endpoint, AuthzEndPoint)
  end,
  List = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
  ModifiedList = case OAuthProvider#oauth_provider.jwks_uri of
    undefined ->  List;
    JwksEndPoint -> [{jwks_url, JwksEndPoint} | List]
  end,
  application:set_env(rabbitmq_auth_backend_oauth2, key_config, ModifiedList),
  rabbit_log:debug("Updated oauth_provider details: ~p ", [ OAuthProvider]),
  OAuthProvider.


do_update_oauth_provider_endpoints_configuration(OAuthProviderId, OAuthProvider) ->
  OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
  LookupProviderPropList = maps:get(OAuthProviderId, OAuthProviders),
  ModifiedList0 = case OAuthProvider#oauth_provider.token_endpoint of
    undefined ->  LookupProviderPropList;
    TokenEndPoint -> [{token_endpoint, TokenEndPoint} | LookupProviderPropList]
  end,
  ModifiedList1 = case OAuthProvider#oauth_provider.authorization_endpoint of
    undefined ->  ModifiedList0;
    AuthzEndPoint -> [{authorization_endpoint, AuthzEndPoint} | ModifiedList0]
  end,
  ModifiedList2 = case OAuthProvider#oauth_provider.jwks_uri of
    undefined ->  ModifiedList1;
    JwksEndPoint -> [{jwks_uri, JwksEndPoint} | ModifiedList1]
  end,
  ModifiedOAuthProviders = maps:put(OAuthProviderId, ModifiedList2, OAuthProviders),
  application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, ModifiedOAuthProviders),
  rabbit_log:debug("Replacing oauth_providers  ~p", [ ModifiedOAuthProviders]),
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
    {ok, DefaultOauthProvider} ->
      rabbit_log:debug("Using default_oauth_provider ~p", [DefaultOauthProvider]),
      get_oauth_provider(DefaultOauthProvider, ListOfRequiredAttributes)
  end.

get_oauth_provider_from_keyconfig(ListOfRequiredAttributes) ->
  OAuthProvider = lookup_oauth_provider_from_keyconfig(),
  rabbit_log:debug("Using oauth_provider ~p from keyconfig", [OAuthProvider]),
  case find_missing_attributes(OAuthProvider, ListOfRequiredAttributes) of
    [] -> {ok, OAuthProvider};
    _ -> Result2 = case OAuthProvider#oauth_provider.issuer of
            undefined -> {error, {missing_oauth_provider_attributes, [issuer]}};
            Issuer ->
                rabbit_log:debug("Downloading oauth_provider using issuer ~p", [Issuer]),
                case get_openid_configuration(Issuer, get_ssl_options_if_any(OAuthProvider)) of
                  {ok, OauthProvider} -> {ok, update_oauth_provider_endpoints_configuration(OauthProvider)};
                  {error, _} = Error2 -> Error2
                end
        end,
        case Result2 of
          {ok, OAuthProvider2} ->
              case find_missing_attributes(OAuthProvider2, ListOfRequiredAttributes) of
                [] ->
                  rabbit_log:debug("Resolved oauth_provider ~p", [OAuthProvider]),
                  {ok, OAuthProvider2};
                _ = Attrs->
                  {error, {missing_oauth_provider_attributes, Attrs}}
              end;
          {error, _} = Error3 -> Error3
        end
  end.


-spec get_oauth_provider(oauth_provider_id(), list()) -> {ok, oauth_provider()} | {error, any()}.
get_oauth_provider(OAuth2ProviderId, ListOfRequiredAttributes) when is_list(OAuth2ProviderId) ->
  get_oauth_provider(list_to_binary(OAuth2ProviderId), ListOfRequiredAttributes);

get_oauth_provider(OAuth2ProviderId, ListOfRequiredAttributes) when is_binary(OAuth2ProviderId) ->
  rabbit_log:debug("get_oauth_provider ~p with at least these attributes: ~p", [OAuth2ProviderId, ListOfRequiredAttributes]),
  case lookup_oauth_provider_config(OAuth2ProviderId) of
    {error, _} = Error0 ->
    rabbit_log:debug("Failed to find oauth_provider ~p configuration due to ~p", [OAuth2ProviderId, Error0]),
      Error0;
    Config ->
      rabbit_log:debug("Found oauth_provider configuration ~p", [Config]),
      OAuthProvider = case Config of
        {error,_} = Error -> Error;
        _ -> map_to_oauth_provider(Config)
      end,
      rabbit_log:debug("Resolved oauth_provider ~p", [OAuthProvider]),
      case find_missing_attributes(OAuthProvider, ListOfRequiredAttributes) of
        [] -> {ok, OAuthProvider};
        _ -> Result2 = case OAuthProvider#oauth_provider.issuer of
                undefined -> {error, {missing_oauth_provider_attributes, [issuer]}};
                Issuer ->
                    rabbit_log:debug("Downloading oauth_provider ~p using issuer ~p", [OAuth2ProviderId, Issuer]),
                    case get_openid_configuration(Issuer, get_ssl_options_if_any(OAuthProvider)) of
                      {ok, OauthProvider} -> {ok, update_oauth_provider_endpoints_configuration(OAuth2ProviderId, OauthProvider)};
                      {error, _} = Error2 -> Error2
                    end
            end,
            case Result2 of
              {ok, OAuthProvider2} ->
                  case find_missing_attributes(OAuthProvider2, ListOfRequiredAttributes) of
                    [] ->
                      rabbit_log:debug("Resolved oauth_provider ~p", [OAuthProvider]),
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

intersection(S1, S2) -> intersection(S1, S2, []).
is_element(H, [H|_])   -> true;
is_element(H, [_|Set]) -> is_element(H, Set);
is_element(_, [])      -> false.

intersection([], _, S) -> S;
intersection([H|T], S1, S) ->
    case is_element(H,S1) of
        true  -> intersection(T, S1, [H|S]);
        false -> intersection(T, S1, S)
    end.

find_missing_attributes(#oauth_provider{} = OAuthProvider, RequiredAttributes) ->
  PropList = oauth_provider_to_proplists(OAuthProvider),
  Filtered = filter_undefined_props(PropList),
  intersection(Filtered, RequiredAttributes).

lookup_oauth_provider_from_keyconfig() ->
  Issuer = application:get_env(rabbitmq_auth_backend_oauth2, issuer, undefined),
  TokenEndpoint = application:get_env(rabbitmq_auth_backend_oauth2, token_endpoint, undefined),
  Map = maps:from_list(application:get_env(rabbitmq_auth_backend_oauth2, key_config, [])),
  #oauth_provider{
    issuer=Issuer,
    jwks_uri=maps:get(jwks_url, Map, undefined), %% jwks_url not uri . _url is the legacy name
    token_endpoint=TokenEndpoint,
    ssl_options=extract_ssl_options_as_list(Map)
  }.

extract_ssl_options_as_list(Map) ->
  Verify = case maps:get(cacertfile, Map, undefined) of
    undefined -> verify_none;
    _ -> maps:get(peer_verification, Map, verify_peer)
  end,
  [ {verify, Verify},
    {cacertfile, maps:get(cacertfile, Map, "")},
    {depth, maps:get(depth, Map, 10)},
    {crl_check, maps:get(crl_check, Map, false)},
    {fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Map, false)}
  ] ++
  case maps:get(hostname_verification, Map, none) of
      wildcard ->
          [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}];
      none ->
          []
  end.

lookup_oauth_provider_config(OAuth2ProviderId) ->
  case application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers) of
    undefined -> {error, oauth_providers_not_found};
    {ok, MapOfProviders} when is_map(MapOfProviders) ->
        case maps:get(OAuth2ProviderId, MapOfProviders, undefined) of
          undefined ->
            {error, {oauth_provider_not_found, OAuth2ProviderId}};
          Value -> Value
        end;
    _ ->  {error, invalid_oauth_provider_configuration}
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


map_to_successful_access_token_response(Json) ->
  #successful_access_token_response{
    access_token=maps:get(?RESPONSE_ACCESS_TOKEN, Json),
    token_type=maps:get(?RESPONSE_TOKEN_TYPE, Json, undefined),
    refresh_token=maps:get(?RESPONSE_REFRESH_TOKEN, Json, undefined),
    expires_in=maps:get(?RESPONSE_EXPIRES_IN, Json, undefined)
  }.

map_to_unsuccessful_access_token_response(Json) ->
  #unsuccessful_access_token_response{
    error=maps:get(?RESPONSE_ERROR, Json),
    error_description=maps:get(?RESPONSE_ERROR_DESCRIPTION, Json, undefined)
  }.


map_to_oauth_provider(Map) when is_map(Map) ->
  #oauth_provider{
    issuer=maps:get(?RESPONSE_ISSUER, Map),
    token_endpoint=maps:get(?RESPONSE_TOKEN_ENDPOINT, Map, undefined),
    authorization_endpoint=maps:get(?RESPONSE_AUTHORIZATION_ENDPOINT, Map, undefined),
    jwks_uri=maps:get(?RESPONSE_JWKS_URI, Map, undefined)
  };

map_to_oauth_provider(PropList) when is_list(PropList) ->
  #oauth_provider{
    issuer=proplists:get_value(issuer, PropList),
    token_endpoint=proplists:get_value(token_endpoint, PropList),
    authorization_endpoint=proplists:get_value(authorization_endpoint, PropList, undefined),
    jwks_uri=proplists:get_value(jwks_uri, PropList, undefined),
    ssl_options=map_ssl_options(proplists:get_value(https, PropList, undefined))
    }.

map_ssl_options(undefined) ->
  [{verify, verify_none},
      {depth, 10},
      {fail_if_no_peer_cert, false},
      {crl_check, false},
      {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}}];
map_ssl_options(Ssl_options) ->
  Ssl_options1 = [{verify, proplists:get_value(verify, Ssl_options, verify_none)},
    {depth, proplists:get_value(depth, Ssl_options, 10)},
    {fail_if_no_peer_cert, proplists:get_value(fail_if_no_peer_cert, Ssl_options, false)},
    {crl_check, proplists:get_value(crl_check, Ssl_options, false)},
    {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}} | cacertfile(Ssl_options)],
  case proplists:get_value(hostname_verification, Ssl_options, none) of
      wildcard ->
          [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | Ssl_options1];
      none ->
          Ssl_options1
  end.

cacertfile(Ssl_options) ->
  case proplists:get_value(cacertfile, Ssl_options) of
    undefined -> [];
    CaCertFile -> [{cacertfile, CaCertFile}]
  end.

enrich_oauth_provider({ok, OAuthProvider}, TLSOptions) ->
  {ok, OAuthProvider#oauth_provider{ssl_options=TLSOptions}};
enrich_oauth_provider(Response, _) ->
  Response.

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

map_response_to_oauth_provider(Code, Reason, Headers, Body) ->
  case decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body) of
    {error, {error, InternalError}} ->
      {error, InternalError};
    {error, _} = Error ->
      Error;
    Value ->
      case Code of
        200 -> {ok, map_to_oauth_provider(Value)};
        201 -> {ok, map_to_oauth_provider(Value)};
        _ ->   {error, Reason}
      end
  end.


parse_access_token_response({error, Reason}) ->
  {error, Reason};
parse_access_token_response({ok,{{_,Code,Reason}, Headers, Body}}) ->
  map_to_access_token_response(Code, Reason, Headers, Body).

parse_openid_configuration_response({error, Reason}) ->
  {error, Reason};
parse_openid_configuration_response({ok,{{_,Code,Reason}, Headers, Body}}) ->
  map_response_to_oauth_provider(Code, Reason, Headers, Body).
