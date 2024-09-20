-module(uaa_jwks).
-export([get/2]).

-spec get(uri_string:uri_string(), list()) -> {ok, term()} | {error, term()}.
get(JwksUrl, SslOptions) ->
    Options = [{timeout, 60000}] ++ [{ssl, SslOptions}],
    httpc:request(get, {JwksUrl, []}, Options, []).
