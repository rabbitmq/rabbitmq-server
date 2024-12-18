-module(uaa_jwks).
-export([get/2, get/3]).

-import(oauth2_client, [
    get_jwks/2, get_jwks/3]).

-spec get(uri_string:uri_string(), list()) -> {ok, term()} | {error, term()}.
get(JwksUrl, SslOptions) ->
    get_jwks(JwksUrl, SslOptions).

-spec get(uri_string:uri_string(), list(), 
    oauth2_client:proxy_options() | undefined | 'none') -> {ok, term()} | {error, term()}.
get(JwksUrl, SslOptions, undefined) ->
    get_jwks(JwksUrl, SslOptions);
get(JwksUrl, SslOptions, ProxyOptions) ->
    get_jwks(JwksUrl, SslOptions, ProxyOptions).    
