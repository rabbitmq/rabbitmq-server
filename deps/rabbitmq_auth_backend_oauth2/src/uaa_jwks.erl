-module(uaa_jwks).
-export([get/2, get/3]).

-import(oauth2_client, [
    map_ssl_options_to_httpc_option/1,
    map_timeout_to_httpc_option/1,
    map_proxy_auth_to_httpc_option/1,
    map_proxy_to_httpc_option/1]).

-spec get(uri_string:uri_string(), list()) -> {ok, term()} | {error, term()}.
get(JwksUrl, SslOptions) ->
    http_get(JwksUrl, SslOptions, undefined).

-spec get(uri_string:uri_string(), list(), oauth2_client:proxy_options() | undefined | 'none') -> 
        {ok, term()} | {error, term()}.
get(JwksUrl, SslOptions, undefined) ->
    get(JwksUrl, SslOptions);
get(JwksUrl, SslOptions, ProxyOptions) ->
    http_get(JwksUrl, SslOptions, ProxyOptions).    

http_get(URL, SslOptions, ProxyOptions) ->
    HttpOptions = map_timeout_to_httpc_option(60000) 
                    ++ map_ssl_options_to_httpc_option(SslOptions),
    {HttpProxyOptions, SetOptions} = 
        case ProxyOptions of 
            undefined -> {[], ok};
        _ ->
            case httpc:set_options(map_proxy_to_httpc_option(ProxyOptions)) of 
                ok -> {map_proxy_auth_to_httpc_option(ProxyOptions), ok};
                {error, _} = Error -> {undefined, Error}
            end
    end,
    case SetOptions of 
        ok -> httpc:request(get, {URL, []}, HttpOptions ++ HttpProxyOptions, []);
        {error, _} -> SetOptions
    end.
        