-module(uaa_jwks).
-export([get/2]).

<<<<<<< HEAD
-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
=======
-spec get(uri_string:uri_string(), list()) -> {ok, term()} | {error, term()}.
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)
get(JwksUrl, SslOptions) ->
    Options = [{timeout, 60000}] ++ [{ssl, SslOptions}],
    httpc:request(get, {JwksUrl, []}, Options, []).
