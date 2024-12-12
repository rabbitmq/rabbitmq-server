-module(uaa_jwks).
-export([get/2]).

<<<<<<< HEAD
-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
=======
-spec get(uri_string:uri_string(), list()) -> {ok, term()} | {error, term()}.
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)
get(JwksUrl, SslOptions) ->
    Options = [{timeout, 60000}] ++ [{ssl, SslOptions}],
    httpc:request(get, {JwksUrl, []}, Options, []).
