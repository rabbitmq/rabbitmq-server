-module(uaa_jwks).
-export([get/2]).

<<<<<<< HEAD
-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
=======
-spec get(uri_string:uri_string(), list()) -> {ok, term()} | {error, term()}.
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)
get(JwksUrl, SslOptions) ->
    Options = [{timeout, 60000}] ++ [{ssl, SslOptions}],
    httpc:request(get, {JwksUrl, []}, Options, []).
