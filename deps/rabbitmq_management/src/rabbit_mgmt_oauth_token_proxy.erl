%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_token_proxy).

-export([init/2]).
%% exported for testing
-export([inject_client_secret/2, rewrite_token_endpoint/2, external_authority/2]).

-include_lib("oauth2_client/include/oauth2_client.hrl").
-include_lib("kernel/include/logger.hrl").

-type connection_authority() :: #{scheme := binary(),
                                  host := binary(),
                                  port := inet:port_number()}.
-type forwarded_headers() :: #{proto => binary(),
                               host => binary(),
                               port => binary()}.

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
                    Rewritten = rewrite_token_endpoint(Body, proxy_token_url(Req0, Id)),
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
-spec inject_client_secret([{binary(), binary() | true}], binary()) ->
    [{binary(), binary() | true}].
inject_client_secret(Params, Secret) ->
    case lists:keymember(<<"client_secret">>, 1, Params) of
        true -> Params;
        false -> Params ++ [{<<"client_secret">>, Secret}]
    end.

-spec rewrite_token_endpoint(binary(), binary()) -> binary().
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
    Authority = external_authority(
                  #{scheme => cowboy_req:scheme(Req),
                    host => cowboy_req:host(Req),
                    port => cowboy_req:port(Req)},
                  forwarded_headers(Req)),
    iolist_to_binary([Authority, rabbit_mgmt_util:get_path_prefix(),
                      "/js/oidc-oauth/token-endpoint/",
                      cow_uri:urlencode(Id)]).

%% Only headers are consulted: the same values in the query string would be
%% settable from a link, which would let a third party choose the token
%% endpoint a victim's browser posts the authorization code to.
forwarded_headers(Req) ->
    maps:filtermap(
      fun(_, Name) ->
              case cowboy_req:header(Name, Req) of
                  undefined -> false;
                  Value -> {true, Value}
              end
      end,
      #{proto => <<"x-forwarded-proto">>,
        host => <<"x-forwarded-host">>,
        port => <<"x-forwarded-port">>}).

-spec external_authority(connection_authority(), forwarded_headers()) -> iodata().
external_authority(Connection, Forwarded) ->
    {Scheme, DefaultPort} = scheme_and_default_port(Forwarded, Connection),
    {Host, Port} = host_and_port(Forwarded, Connection, DefaultPort),
    [Scheme, "://", Host, port_suffix(Scheme, Port)].

%% An accepted forwarded scheme means the browser reached a proxy rather than
%% this listener, so that scheme's default port is a better guess than the port
%% this node listens on.
scheme_and_default_port(#{proto := Proto}, Connection) ->
    case ascii_lowercase(last_value(Proto)) of
        <<"http">> -> {<<"http">>, 80};
        <<"https">> -> {<<"https">>, 443};
        _ -> connection_scheme_and_port(Connection)
    end;
scheme_and_default_port(_, Connection) ->
    connection_scheme_and_port(Connection).

connection_scheme_and_port(#{scheme := Scheme, port := Port}) ->
    {Scheme, Port}.

host_and_port(Forwarded, #{host := ConnHost}, DefaultPort) ->
    {Host, HostPort} = case forwarded_host(Forwarded) of
                           undefined -> {ConnHost, undefined};
                           Accepted -> Accepted
                       end,
    case {forwarded_port(Forwarded), HostPort} of
        {undefined, undefined} -> {Host, DefaultPort};
        {undefined, Port} -> {Host, Port};
        {Port, _} -> {Host, Port}
    end.

forwarded_host(#{host := Value}) ->
    %% uri_string:parse/1 raises on bytes that are not valid UTF-8.
    try uri_string:parse(<<"//", (last_value(Value))/binary>>) of
        #{host := Host, path := <<>>} = Parsed
          when Host =/= <<>>,
               not is_map_key(userinfo, Parsed),
               not is_map_key(query, Parsed),
               not is_map_key(fragment, Parsed) ->
            {brackets(Host), valid_port(maps:get(port, Parsed, undefined))};
        _ ->
            undefined
    catch
        _:_ ->
            undefined
    end;
forwarded_host(_) ->
    undefined.

forwarded_port(#{port := Value}) ->
    try valid_port(binary_to_integer(last_value(Value)))
    catch
        _:_ -> undefined
    end;
forwarded_port(_) ->
    undefined.

valid_port(Port) when is_integer(Port), Port > 0, Port < 65536 -> Port;
valid_port(_) -> undefined.

%% uri_string:parse/1 strips the brackets an IPv6 literal needs in a URL.
brackets(Host) ->
    case binary:match(Host, <<":">>) of
        nomatch -> Host;
        _ -> <<"[", Host/binary, "]">>
    end.

%% The rightmost value is the one written by the proxy closest to this node;
%% anything to its left may have been supplied by the client.
last_value(Value) ->
    trim_ows(lists:last(binary:split(Value, <<",">>, [global]))).

%% Hand-rolled because `string:trim/1` raises on header bytes that are not
%% valid UTF-8.
trim_ows(<<C, Rest/binary>>) when C =:= $\s; C =:= $\t ->
    trim_ows(Rest);
trim_ows(Value) ->
    case byte_size(Value) - 1 of
        -1 ->
            Value;
        Last ->
            case binary:at(Value, Last) of
                C when C =:= $\s; C =:= $\t ->
                    trim_ows(binary:part(Value, 0, Last));
                _ ->
                    Value
            end
    end.

ascii_lowercase(Value) ->
    << <<(lowercase_byte(C))>> || <<C>> <= Value >>.

lowercase_byte(C) when C >= $A, C =< $Z -> C + 32;
lowercase_byte(C) -> C.

port_suffix(<<"http">>, 80) -> "";
port_suffix(<<"https">>, 443) -> "";
port_suffix(_, Port) -> [":", integer_to_binary(Port)].

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
