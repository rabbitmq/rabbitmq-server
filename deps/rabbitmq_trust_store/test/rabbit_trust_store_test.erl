-module(rabbit_trust_store_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


%% ...

library_test() ->

    %% Given: Makefile.

    {_Root, _Certificate, _Key} = ct_helper:make_certs().

validation_success_for_AMQP_client_test_() ->

    {timeout,
     15,
     fun () ->

             %% Given: an authority and a certificate rooted with that authority.
             AuthorityInfo = {_X = Root, _AuthorityKey} = erl_make_certs:make_cert([{key, dsa}]),
             {Certificate, Key} = chain(AuthorityInfo),
             {_Y, _Z} = chain(AuthorityInfo),

             %% When: Rabbit accepts just this one authority's certificate
             %% (i.e. these are options that'd be in the configuration file).
             rabbit_networking:start_ssl_listener(port(), [
                 {verify, verify_peer}, {fail_if_no_peer_cert, true},
                 {cacerts, [Root]}, {cert, _Y}, {key, _Z}], 1),

             %% Then: a client presenting a certifcate rooted at the same
             %% authority connects successfully.
             {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                 port = port(), ssl_options = [{cert, Certificate}, {key, Key},
                     {cacerts, [_X]}]}),

             %% Clean: client & server TLS/TCP.
             ok = amqp_connection:close(Con),
             rabbit_networking:stop_tcp_listener(port())

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
        rabbit_networking:start_ssl_listener(port(), [
            {cacerts, [R]},
            {cert, _X}, {key, _Y},
            {verify, verify_peer}, {fail_if_no_peer_cert, true}], 1),

        %% Then: a client presenting a certificate rooted with another
        %% authority is REJECTED.
        {error, _} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
            port = port(), ssl_options = [
                {cacerts, [_S]},
                {cert, C}, {key, _Z}]}),

        %% Clean: server TLS/TCP.
        rabbit_networking:stop_tcp_listener(port())

     end
    }.


%% Test Constants

port() -> 4096.


%% Ancillary

chain(Issuer) ->
    %% Theses are DER encoded.
    {Certificate, {Kind, Key, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Issuer}]),
    {Certificate, {Kind, Key}}.
