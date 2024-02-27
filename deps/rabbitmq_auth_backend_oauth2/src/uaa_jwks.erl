-module(uaa_jwks).
-export([get/2]).

-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
get(JwksUrl, KeyConfig) ->
    httpc:request(get, {JwksUrl, []}, [{ssl, ssl_options(KeyConfig)}, {timeout, 60000}], []).
