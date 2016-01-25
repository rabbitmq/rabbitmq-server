-module(rabbit_trust_store_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


%% ...

library_test() ->

    %% Given: Makefile.

    {_Root, _Certificate, _Key} = ct_helper:make_certs().

invasive_SSL_option_change_test() ->

    %% Given: Rabbit is started with the boot-steps in the
    %% Trust-Store's OTP Application file.

    %% When: we get Rabbit's SSL options.
    Options = cfg(),

    %% Then: all necessary settings are correct.
    {_, verify_peer} = lists:keyfind(verify,               1, Options),
    {_, true}        = lists:keyfind(fail_if_no_peer_cert, 1, Options),
    {_, {F, []}}     = lists:keyfind(verify_fun,           1, Options),

    {module, rabbit_trust_store} = erlang:fun_info(F, module),
    {name,   interface}          = erlang:fun_info(F, name).

validation_success_for_AMQP_client_test_() ->

    {timeout,
     15,
     fun () ->

         %% Given: an authority and a certificate rooted with that
         %% authority.
         AuthorityInfo = {_X = Root, _AuthorityKey} = erl_make_certs:make_cert([{key, dsa}]),
         {Certificate, Key} = chain(AuthorityInfo),
         {_Y, _Z} = chain(AuthorityInfo),

         %% When: Rabbit accepts just this one authority's certificate
         %% (i.e. these are options that'd be in the configuration
         %% file).
         ok = rabbit_networking:start_ssl_listener(port(), [
             {cacerts, [Root]}, {cert, _Y}, {key, _Z}|cfg()], 1),

         %% Then: a client presenting a certifcate rooted at the same
         %% authority connects successfully.
         {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
             port = port(), ssl_options = [{cert, Certificate}, {key, Key},
                 {cacerts, [_X]}]}),

         %% Clean: client & server TLS/TCP.
         ok = amqp_connection:close(Con),
         ok = rabbit_networking:stop_tcp_listener(port())

     end
    }.

validation_failure_for_AMQP_client_test_() ->

    {timeout,
     15,
     fun () ->

        %% Given: a root certificate and a certificate rooted with another
        %% authority.
        {R, _X, _Y} = ct_helper:make_certs(),
        {_S, C, _Z} = ct_helper:make_certs(),

        %% When: Rabbit accepts certificates rooted with just one
        %% particular authority.
        ok = rabbit_networking:start_ssl_listener(port(), [
            {cacerts, [R]}, {cert, _X}, {key, _Y}|cfg()], 1),

        %% Then: a client presenting a certificate rooted with another
        %% authority is REJECTED.
        {error, _} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
            port = port(), ssl_options = [{cacerts, [_S]}, {cert, C}, {key, _Z}]}),

        %% Clean: server TLS/TCP.
        ok = rabbit_networking:stop_tcp_listener(port())

     end
    }.

whitelisted_certificate_accepted_regardless_of_root_test_() ->

    {timeout,
     15,
     fun () ->

        %% Given: a certificate `C` AND that it is whitelisted.

        {R, _U,           _V} = ct_helper:make_certs(),
        {_S, C, {_A,_B} = _Y} = ct_helper:make_certs(),

        ok = erl_make_certs:write_pem(friendlies(), "alice", {C, {_A, _B, not_encrypted}}),
        ok = application:set_env(trust_store, whitelist, friendlies(), [{persistent, true}]),

        %% When: Rabbit validates paths with a different root `R` than
        %% that of the certificate `C`.
        ok = rabbit_networking:start_ssl_listener(port(), [
            {cacerts, [R]}, {cert, _U}, {key, _V}|cfg()], 1),

        %% Then: a client presenting the whitelisted certificate `C`
        %% is allowed.
        {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
            port = port(), ssl_options = [{cacerts, [_S]}, {cert, C}, {key, _Y}]}),

        %% Clean: client & server TLS/TCP
        ok = amqp_connection:close(Con),
        ok = rabbit_networking:stop_tcp_listener(port())

     end
    }.


%% Test Constants

port() -> 4096.

friendlies() ->
    os:getenv("TMPDIR") ++ "friendlies".

cfg() ->
    {ok, Cfg} = application:get_env(rabbit, ssl_options),
    Cfg.


%% Ancillary

chain(Issuer) ->
    %% Theses are DER encoded.
    {Certificate, {Kind, Key, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Issuer}]),
    {Certificate, {Kind, Key}}.
