-module(rabbit_trust_store_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


%% ...

library_test() ->

    %% Given: Makefile.

    {_Root, _Certificate, _Key} = ct_helper:make_certs().

success_test() ->

    %% Given: an authority and a certificate rooted with that authority.
    AuthorityInfo = {X = Root, _AuthorityKey} = erl_make_certs:make_cert([{key, dsa}]),
    {Certificate, Key} = chain(AuthorityInfo), {Y, Z} = chain(AuthorityInfo),

    %% When: Rabbit accepts just this one authority's certificate
    %% (i.e. these are options that'd be in the configuration file).
    rabbit_networking:start_ssl_listener(port(), [
        {cacerts, [Root]},
        {cert, Y}, {key, Z},
        {verify, verify_peer}, {fail_if_no_peer_cert, true}], 1),

    %% Then: a client presenting a certifcate rooted at the same
    %% authority connects successfully.
    {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1", port = port(), ssl_options = [
        {cacerts, [X]},
        {cert, Certificate}, {key, Key}]}),
    ok = amqp_connection:close(Con).


%% Test Constants

port() -> 4096.

chain(Issuer) ->
    %% Theses are DER encoded.
    {Certificate, {Kind, Key, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Issuer}]),
    {Certificate, {Kind, Key}}.
