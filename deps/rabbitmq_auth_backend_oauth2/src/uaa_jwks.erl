-module(uaa_jwks).
<<<<<<< HEAD
<<<<<<< HEAD
-export([get/1, ssl_options/0]).

-spec get(string() | binary()) -> {ok, term()} | {error, term()}.
get(JwksUrl) ->
    httpc:request(get, {JwksUrl, []}, [{ssl, ssl_options()}, {timeout, 60000}], []).

-spec ssl_options() -> list().
ssl_options() ->
    UaaEnv = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    PeerVerification = proplists:get_value(peer_verification, UaaEnv, verify_none),
    Depth = proplists:get_value(depth, UaaEnv, 10),
    FailIfNoPeerCert = proplists:get_value(fail_if_no_peer_cert, UaaEnv, false),
    CrlCheck = proplists:get_value(crl_check, UaaEnv, false),
    SslOpts0 = [{verify, PeerVerification},
                {depth, Depth},
                {fail_if_no_peer_cert, FailIfNoPeerCert},
                {crl_check, CrlCheck},
                {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}} | cacertfile(UaaEnv)],

    case proplists:get_value(hostname_verification, UaaEnv, none) of
        wildcard ->
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | SslOpts0];
        none ->
            SslOpts0
    end.

cacertfile(UaaEnv) ->
  case proplists:get_value(cacertfile, UaaEnv) of
    undefined -> [];
    CaCertFile -> [{cacertfile, CaCertFile}]
  end.
=======
-export([get/2]).

-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
get(JwksUrl, KeyConfig) ->
    httpc:request(get, {JwksUrl, []}, [{ssl, ssl_options(KeyConfig)}, {timeout, 60000}], []).
>>>>>>> 5e66d25b45 (Remove obsolete function)
=======
-export([get/2, ssl_options/1]).

-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
get(JwksUrl, SslOptions) ->
    Options = [{timeout, 60000}] ++ [{ssl, SslOptions}],
    rabbit_log:debug("get signing keys using options ~p", Options),
    httpc:request(get, {JwksUrl, []}, Options, []).

-spec ssl_options(term()) -> list().
ssl_options(KeyConfig) ->
    PeerVerification = proplists:get_value(peer_verification, KeyConfig, verify_none),
    Depth = proplists:get_value(depth, KeyConfig, 10),
    FailIfNoPeerCert = proplists:get_value(fail_if_no_peer_cert, KeyConfig, false),
    CrlCheck = proplists:get_value(crl_check, KeyConfig, false),
    SslOpts0 = [{verify, PeerVerification},
                {depth, Depth},
                {fail_if_no_peer_cert, FailIfNoPeerCert},
                {crl_check, CrlCheck},
                {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}} | cacertfile(KeyConfig)],

    case proplists:get_value(hostname_verification, KeyConfig, none) of
        wildcard ->
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | SslOpts0];
        none ->
            SslOpts0
    end.

cacertfile(KeyConfig) ->
    case proplists:get_value(cacertfile, KeyConfig) of
        undefined -> [];
        CaCertFile -> [{cacertfile, CaCertFile}]
    end.
>>>>>>> 1fa7f40277 (Fix issue introduced while removing ssl_options function)
