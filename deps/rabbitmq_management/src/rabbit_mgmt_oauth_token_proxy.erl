%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_token_proxy).

-export([init/2]).
%% exported for testing
-export([inject_client_secret/2, rewrite_token_endpoint/2]).

-include_lib("oauth2_client/include/oauth2_client.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% Server-side proxy for the OAuth 2 token endpoint.
%%
%% It exists so that management.oauth_client_secret is never sent to the
%% browser. Resource servers that need a client secret point the OAuth 2
%% client at this proxy instead of the identity provider. The proxy adds the
%% secret and forwards the request to the provider's real token endpoint.
%%
%% The token endpoint the browser must use is advertised through a rewritten
%% OpenID configuration document (the metadata operation), which is the real
%% document with only token_endpoint replaced.
%%
%% The provider is resolved the same way the browser resolves it, from the
%% management resource server settings, so the proxy always targets the
%% identity provider the browser was told to use. The proxy is an HTTPS client
%% towards that provider, so it verifies the peer using the OAuth 2 backend's
%% TLS options, which default to the system trust store.
%%--------------------------------------------------------------------

init(Req, #{op := metadata} = State) ->
    handle_metadata(Req, State);
init(Req, #{op := token} = State) ->
    handle_token(Req, State).

handle_metadata(Req0, State) ->
    Id = cowboy_req:binding(id, Req0),
    case resolve(Id) of
        {ok, _Secret, MetadataURL, HttpOpts} ->
            case http_get(MetadataURL, HttpOpts) of
                {ok, 200, _Headers, Body} ->
                    ProxyTokenURL = proxy_token_url(Req0, Id),
                    Rewritten = rewrite_token_endpoint(Body, ProxyTokenURL),
                    {ok, reply_json(200, Rewritten, Req0), State};
                Other ->
                    ?LOG_ERROR("OAuth 2 token proxy could not fetch ~ts: ~tp",
                               [MetadataURL, Other]),
                    {ok, cowboy_req:reply(502, Req0), State}
            end;
        {error, _} ->
            {ok, cowboy_req:reply(404, Req0), State}
    end.

handle_token(Req0, State) ->
    case cowboy_req:method(Req0) of
        <<"POST">> ->
            Id = cowboy_req:binding(id, Req0),
            case resolve(Id) of
                {ok, Secret, MetadataURL, HttpOpts} ->
                    forward_token_request(Req0, State, Secret, MetadataURL,
                                          HttpOpts);
                {error, _} ->
                    {ok, cowboy_req:reply(404, Req0), State}
            end;
        _ ->
            {ok, cowboy_req:reply(405, #{<<"allow">> => <<"POST">>}, Req0), State}
    end.

forward_token_request(Req0, State, Secret, MetadataURL, HttpOpts) ->
    case token_endpoint(MetadataURL, HttpOpts) of
        {ok, TokenEndpoint} ->
            {ok, Params, Req1} = cowboy_req:read_urlencoded_body(Req0),
            Body = uri_string:compose_query(inject_client_secret(Params, Secret)),
            case http_post(TokenEndpoint, Body, HttpOpts) of
                {ok, Status, Headers, RespBody} ->
                    {ok, reply_json(Status, content_type(Headers), RespBody,
                                    Req1), State};
                {error, Reason} ->
                    ?LOG_ERROR("OAuth 2 token proxy request to ~ts failed: ~tp",
                               [TokenEndpoint, Reason]),
                    {ok, cowboy_req:reply(502, Req1), State}
            end;
        {error, Reason} ->
            ?LOG_ERROR("OAuth 2 token proxy could not resolve token endpoint "
                       "from ~ts: ~tp", [MetadataURL, Reason]),
            {ok, cowboy_req:reply(502, Req0), State}
    end.

%% Only add the secret when the request does not already carry one, so an
%% explicit client_secret_post from the client is never overwritten.
inject_client_secret(Params, Secret) ->
    case lists:keymember(<<"client_secret">>, 1, Params) of
        true -> Params;
        false -> Params ++ [{<<"client_secret">>, Secret}]
    end.

rewrite_token_endpoint(MetadataJson, ProxyTokenURL) ->
    Map = rabbit_json:decode(MetadataJson),
    rabbit_json:encode(maps:put(<<"token_endpoint">>, ProxyTokenURL, Map)).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

%% A resource server is only served by the proxy when a secret is configured
%% for it, which also prevents the proxy from relaying to arbitrary providers.
resolve(Id) ->
    ManagementProps = application:get_all_env(rabbitmq_management),
    ResourceServers = proplists:get_value(oauth_resource_servers,
                                          ManagementProps, #{}),
    case maps:find(Id, ResourceServers) of
        {ok, Props} ->
            build(Id,
                  secret([proplists:get_value(oauth_client_secret, Props),
                          proplists:get_value(oauth_client_secret, ManagementProps)]),
                  proplists:get_value(oauth_provider_id, Props));
        error ->
            case is_root_resource_server(Id) of
                true ->
                    build(Id,
                          secret([proplists:get_value(oauth_client_secret,
                                                      ManagementProps)]),
                          undefined);
                false ->
                    {error, unknown_resource_server}
            end
    end.

build(_Id, undefined, _ProviderId) ->
    {error, no_client_secret};
build(Id, Secret, ProviderId) ->
    case metadata_url(Id) of
        undefined ->
            {error, no_provider_url};
        MetadataURL ->
            case tls_options(ProviderId) of
                {ok, HttpOpts} ->
                    {ok, rabbit_data_coercion:to_binary(Secret), MetadataURL,
                     HttpOpts};
                {error, _} = Error ->
                    Error
            end
    end.

%% Resolve the provider the same way the browser does, from the settings served
%% to it, so the proxy targets the identity provider the browser was told to use.
metadata_url(Id) ->
    Settings = rabbit_mgmt_wm_auth:authSettings(),
    case proplists:get_value(oauth_resource_servers, Settings, #{}) of
        ResourceServers when is_map(ResourceServers) ->
            case maps:find(Id, ResourceServers) of
                {ok, ResourceServer} -> metadata_url_of(ResourceServer);
                error -> undefined
            end;
        _ ->
            undefined
    end.

metadata_url_of(ResourceServer) ->
    case proplists:get_value(oauth_metadata_url, ResourceServer) of
        undefined ->
            case proplists:get_value(oauth_provider_url, ResourceServer) of
                undefined -> undefined;
                ProviderURL -> well_known_url(ProviderURL)
            end;
        MetadataURL ->
            MetadataURL
    end.

well_known_url(ProviderURL) ->
    Trimmed = string:trim(rabbit_data_coercion:to_binary(ProviderURL),
                          trailing, "/"),
    <<Trimmed/binary, "/.well-known/openid-configuration">>.

%% The OAuth 2 backend provider's TLS options, as an `httpc` `{ssl, _}` option.
%% `get_oauth_provider/2` with no required attributes returns them without
%% contacting the provider; they default to the system trust store.
tls_options(ProviderId) ->
    Result = case ProviderId of
                 undefined -> oauth2_client:get_oauth_provider([]);
                 _ -> oauth2_client:get_oauth_provider(ProviderId, [])
             end,
    case Result of
        {ok, #oauth_provider{ssl_options = undefined}} -> {ok, []};
        {ok, #oauth_provider{ssl_options = SslOptions}} -> {ok, [{ssl, SslOptions}]};
        {error, _} = Error -> Error
    end.

token_endpoint(MetadataURL, HttpOpts) ->
    case oauth2_client:get_openid_configuration(MetadataURL, HttpOpts) of
        {ok, #openid_configuration{token_endpoint = TokenEndpoint}} ->
            {ok, TokenEndpoint};
        {error, _} = Error ->
            Error
    end.

secret(Candidates) ->
    case [V || V <- Candidates, is_valid(V)] of
        [Secret | _] -> Secret;
        [] -> undefined
    end.

is_valid(undefined) -> false;
is_valid("") -> false;
is_valid(<<>>) -> false;
is_valid(_) -> true.

is_root_resource_server(Id) ->
    Id =:= rabbit_data_coercion:to_binary(
        application:get_env(rabbitmq_auth_backend_oauth2, resource_server_id,
                            undefined)).

proxy_token_url(Req, Id) ->
    Scheme = cowboy_req:scheme(Req),
    Host = cowboy_req:host(Req),
    Prefix = rabbit_mgmt_util:get_path_prefix(),
    iolist_to_binary([Scheme, "://", Host, port(Req, Scheme), Prefix,
                      "/js/oidc-oauth/token-endpoint/",
                      cow_uri:urlencode(Id)]).

port(Req, Scheme) ->
    case {cowboy_req:port(Req), Scheme} of
        {80, <<"http">>} -> "";
        {443, <<"https">>} -> "";
        {Port, _} -> [":", integer_to_binary(Port)]
    end.

http_get(URL, HttpOpts) ->
    request(get, {URL, []}, HttpOpts).

http_post(URL, Body, HttpOpts) ->
    request(post, {URL, [], "application/x-www-form-urlencoded", Body},
            HttpOpts).

request(Method, Request, HttpOpts) ->
    case httpc:request(Method, Request, HttpOpts, [{body_format, binary}]) of
        {ok, {{_, Status, _}, Headers, Body}} -> {ok, Status, Headers, Body};
        {error, _} = Error -> Error
    end.

content_type(Headers) ->
    case lists:keyfind("content-type", 1, Headers) of
        {_, Value} -> list_to_binary(Value);
        false -> <<"application/json">>
    end.

reply_json(Status, Body, Req) ->
    reply_json(Status, <<"application/json">>, Body, Req).

reply_json(Status, ContentType, Body, Req) ->
    cowboy_req:reply(Status, #{<<"content-type">> => ContentType,
                               <<"cache-control">> => <<"no-store">>}, Body, Req).
