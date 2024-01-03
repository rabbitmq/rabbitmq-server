-module(uaa_jwks).
-export([get/2, ssl_options/1]).

-spec get(string() | binary(), term()) -> {ok, term()} | {error, term()}.
get(JwksUrl, KeyConfig) ->
    httpc:request(get, {JwksUrl, []}, [{ssl, ssl_options(KeyConfig)}, {timeout, 60000}], []).

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
