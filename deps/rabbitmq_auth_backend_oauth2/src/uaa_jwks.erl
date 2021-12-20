-module(uaa_jwks).
-export([get/1]).

-spec get(string() | binary()) -> {ok, term()} | {error, term()}.
get(JwksUrl) ->
    httpc:request(get, {JwksUrl, []}, [{ssl, ssl_options()}, {timeout, 60000}], []).

-spec ssl_options() -> list().
ssl_options() ->
    UaaEnv = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    PeerVerification = proplists:get_value(peer_verification, UaaEnv, verify_none),
    CaCertFile = proplists:get_value(cacertfile, UaaEnv),
    Depth = proplists:get_value(depth, UaaEnv, 10),
    FailIfNoPeerCert = proplists:get_value(fail_if_no_peer_cert, UaaEnv, false),
    CrlCheck = proplists:get_value(crl_check, UaaEnv, false),
    SslOpts0 = [{verify, PeerVerification},
                {cacertfile, CaCertFile},
                {depth, Depth},
                {fail_if_no_peer_cert, FailIfNoPeerCert},
                {crl_check, CrlCheck},
                {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}}],
    case proplists:get_value(hostname_verification, UaaEnv, none) of
        wildcard ->
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | SslOpts0];
        none ->
            SslOpts0
    end.