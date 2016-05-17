-module(rabbit_trust_store_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(SERVER_REJECT_CLIENT, {tls_alert,"unknown ca"}).


%% ...

library_test() ->

    {timeout,
     10,
     fun () ->
             %% Given: Makefile.
             {_Root, _Certificate, _Key} = ct_helper:make_certs()
     end
    }.

invasive_SSL_option_change_test() ->

    %% Given: Rabbit is started with the boot-steps in the
    %% Trust-Store's OTP Application file.

    %% When: we get Rabbit's SSL options.
    Options = cfg(),

    %% Then: all necessary settings are correct.
    verify_peer             = proplists:get_value(verify, Options),
    true                    = proplists:get_value(fail_if_no_peer_cert, Options),
    {Verifyfun, _UserState} = proplists:get_value(verify_fun, Options),

    {module, rabbit_trust_store} = erlang:fun_info(Verifyfun, module),
    {name,   whitelisted}        = erlang:fun_info(Verifyfun, name).

validation_success_for_AMQP_client_test_() ->

    {timeout,
     15,
     fun () ->

             %% Given: an authority and a certificate rooted with that
             %% authority.
             AuthorityInfo = {Root, _AuthorityKey} = erl_make_certs:make_cert([{key, dsa}]),
             {Certificate, Key} = chain(AuthorityInfo),
             {Certificate2, Key2} = chain(AuthorityInfo),

             %% When: Rabbit accepts just this one authority's certificate
             %% (i.e. these are options that'd be in the configuration
             %% file).
             ok = rabbit_networking:start_ssl_listener(port(), [{cacerts, [Root]},
                                                                {cert, Certificate2},
                                                                {key, Key2} | cfg()], 1),

             %% Then: a client presenting a certifcate rooted at the same
             %% authority connects successfully.
             {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                                    port = port(),
                                                                    ssl_options = [{cert, Certificate},
                                                                                   {key, Key}]}),

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
             {Root, Cert, Key}      = ct_helper:make_certs(),
             {_,  CertOther, KeyOther}    = ct_helper:make_certs(),

             %% When: Rabbit accepts certificates rooted with just one
             %% particular authority.
             ok = rabbit_networking:start_ssl_listener(port(), [{cacerts, [Root]},
                                                                {cert, Cert},
                                                                {key, Key} | cfg()], 1),

             %% Then: a client presenting a certificate rooted with another
             %% authority is REJECTED.
             {error, ?SERVER_REJECT_CLIENT} =
                 amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                            port = port(),
                                                            ssl_options = [{cert, CertOther},
                                                                           {key, KeyOther}]}),

             %% Clean: server TLS/TCP.
             ok = rabbit_networking:stop_tcp_listener(port())

     end
    }.

whitelisted_certificate_accepted_from_AMQP_client_regardless_of_validation_to_root_test_() ->

    {setup,
     fun() ->
             ok = force_delete_entire_directory(friendlies()),
             ok = build_directory_tree(friendlies())
     end,
     fun(_) ->
             ok = force_delete_entire_directory(friendlies())
     end,
     [{timeout,
       15,
       fun () ->

               %% Given: a certificate `CertTrusted` AND that it is whitelisted.

               {Root, Cert, Key} = ct_helper:make_certs(),
               {_,  CertTrusted, KeyTrusted} = ct_helper:make_certs(),

               ok = whitelist(friendlies(), "alice", CertTrusted,  KeyTrusted),
               ok = change_configuration(rabbitmq_trust_store, [{directory, friendlies()}]),

               %% When: Rabbit validates paths with a different root `R` than
               %% that of the certificate `CertTrusted`.
               ok = rabbit_networking:start_ssl_listener(port(), [{cacerts, [Root]},
                                                                  {cert, Cert},
                                                                  {key, Key} | cfg()], 1),

               %% Then: a client presenting the whitelisted certificate `C`
               %% is allowed.
               {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                                      port = port(),
                                                                      ssl_options = [{cert, CertTrusted},
                                                                                     {key, KeyTrusted}]}),

               %% Clean: client & server TLS/TCP
               ok = delete("alice.pem"),
               ok = amqp_connection:close(Con),
               ok = rabbit_networking:stop_tcp_listener(port())

       end
      }]}.

removed_certificate_denied_from_AMQP_client_test_() ->

    {setup,
     fun() ->
             ok = build_directory_tree(friendlies())
     end,
     fun(_) ->
             ok = force_delete_entire_directory(friendlies())
     end,
     [{timeout,
       15,
       fun () ->

               %% Given: a certificate `CertOther` AND that it is whitelisted.

               {Root, Cert, Key} = ct_helper:make_certs(),
               {_,  CertOther, KeyOther} = ct_helper:make_certs(),

               ok = whitelist(friendlies(), "bob", CertOther,  KeyOther),
               ok = change_configuration(rabbitmq_trust_store, [{directory, friendlies()},
                                                                {refresh_interval,
                                                                    {seconds, interval()}}]),

               %% When: we wait for at least one second (the accuracy of the
               %% file system's time), remove the whitelisted certificate,
               %% then wait for the trust-store to refresh the whitelist.
               ok = rabbit_networking:start_ssl_listener(port(), [{cacerts, [Root]},
                                                                  {cert, Cert},
                                                                  {key, Key} | cfg()], 1),

               wait_for_file_system_time(),
               ok = delete("bob.pem"),
               wait_for_trust_store_refresh(),

               %% Then: a client presenting the removed whitelisted
               %% certificate `CertOther` is denied.
               {error, ?SERVER_REJECT_CLIENT} =
                   amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                              port = port(),
                                                              ssl_options = [{cert, CertOther},
                                                                             {key, KeyOther}]}),

               %% Clean: server TLS/TCP
               ok = rabbit_networking:stop_tcp_listener(port())
       end
      }]}.


installed_certificate_accepted_from_AMQP_client_test_() ->

    {setup,
     fun() ->
             ok = build_directory_tree(friendlies())
     end,
     fun(_) ->
             ok = force_delete_entire_directory(friendlies())
     end,
     [{timeout,
       15,
       fun () ->

               %% Given: a certificate `CertOther` which is NOT yet whitelisted.

               {Root, Cert, Key} = ct_helper:make_certs(),
               {_,  CertOther, KeyOther} = ct_helper:make_certs(),

               ok = change_configuration(rabbitmq_trust_store, [{directory, friendlies()},
                                                                {refresh_interval,
                                                                    {seconds, interval()}}]),

               %% When: we wait for at least one second (the accuracy of the
               %% file system's time), add a certificate to the directory,
               %% then wait for the trust-store to refresh the whitelist.
               ok = rabbit_networking:start_ssl_listener(port(), [{cacerts, [Root]},
                                                                  {cert, Cert},
                                                                  {key, Key} | cfg()], 1),

               wait_for_file_system_time(),
               ok = whitelist(friendlies(), "charlie", CertOther,  KeyOther),
               wait_for_trust_store_refresh(),

               %% Then: a client presenting the whitelisted certificate `CertOther`
               %% is allowed.
               {ok, Con} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                                      port = port(),
                                                                      ssl_options = [{cert, CertOther},
                                                                                     {key, KeyOther}]}),

               %% Clean: Client & server TLS/TCP
               ok = delete("charlie.pem"),
               ok = amqp_connection:close(Con),
               ok = rabbit_networking:stop_tcp_listener(port())

       end
      }]}.

whitelist_directory_DELTA_test_() ->

    {setup,
     fun() ->
             ok = build_directory_tree(friendlies())
     end,
     fun(_) ->
             ok = force_delete_entire_directory(friendlies())
     end,
     [{timeout,
       20,
       fun () ->

               %% Given: a certificate `Root` which Rabbit can use as a
               %% root certificate to validate agianst AND three
               %% certificates which clients can present (the first two
               %% of which are whitelisted).

               {Root, Cert, Key} = ct_helper:make_certs(),

               {_,  CertListed1, KeyListed1} = ct_helper:make_certs(),
               {_,  CertRevoked, KeyRevoked} = ct_helper:make_certs(),
               {_,  CertListed2, KeyListed2} = ct_helper:make_certs(),

               ok = whitelist(friendlies(), "foo", CertListed1,  KeyListed1),
               ok = whitelist(friendlies(), "bar", CertRevoked,  KeyRevoked),
               ok = change_configuration(rabbitmq_trust_store, [{directory, friendlies()},
                                                                {refresh_interval,
                                                                 {seconds, interval()}}]),

               %% When: we wait for at least one second (the accuracy
               %% of the file system's time), delete a certificate and
               %% a certificate to the directory, then wait for the
               %% trust-store to refresh the whitelist.
               ok = rabbit_networking:start_ssl_listener(port(), [{cacerts, [Root]},
                                                                  {cert, Cert},
                                                                  {key, Key} | cfg()], 1),

               wait_for_file_system_time(),
               ok = delete("bar.pem"),
               ok = whitelist(friendlies(), "baz", CertListed2,  KeyListed2),
               wait_for_trust_store_refresh(),

               %% Then: connectivity to Rabbit is as it should be.
               {ok, Conn1} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                                        port = port(),
                                                                        ssl_options = [{cert, CertListed1},
                                                                                       {key, KeyListed1}]}),
               {error, ?SERVER_REJECT_CLIENT} =
                    amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                               port = port(),
                                                               ssl_options = [{cert, CertRevoked},
                                                                              {key, KeyRevoked}]}),

               {ok, Conn2} = amqp_connection:start(#amqp_params_network{host = "127.0.0.1",
                                                                        port = port(),
                                                                        ssl_options = [{cert, CertListed2},
                                                                                       {key, KeyListed2}]}),

               %% Clean: delete certificate file, close client & server
               %% TLS/TCP
               ok = delete("foo.pem"),
               ok = delete("baz.pem"),

               ok = amqp_connection:close(Conn1),
               ok = amqp_connection:close(Conn2),

               ok = rabbit_networking:stop_tcp_listener(port())
       end
      }]}.


ensure_configuration_using_binary_strings_is_handled_test_() ->
    {setup,
     fun() ->
             ok = build_directory_tree(friendlies())
     end,
     fun(_) ->
             ok = force_delete_entire_directory(friendlies())
     end,
    [{timeout,
     15,
     fun () ->
           ok = change_configuration(rabbitmq_trust_store, [{directory, list_to_binary(friendlies())},
                                                            {refresh_interval,
                                                                {seconds, interval()}}])
     end }]}.

%% Test Constants

port() -> 4096.

data_directory() ->
    Path = os:getenv("TMPDIR"),
    %% Ensure TMPDIR exists
    true = (false =/= Path),
    Path.

friendlies() ->
    filename:join([data_directory(), "friendlies"]).

build_directory_tree(Path) ->
    ok = filelib:ensure_dir(Path),
    file:make_dir(Path).

force_delete_entire_directory(Path) ->
    rabbit_file:recursive_delete([Path]).

interval() ->
    1.

wait_for_file_system_time() ->
    timer:sleep(timer:seconds(1)).

wait_for_trust_store_refresh() ->
    timer:sleep(2 * timer:seconds(interval())).

cfg() ->
    {ok, Cfg} = application:get_env(rabbit, ssl_options),
    Cfg.


%% Ancillary

chain(Issuer) ->
    %% Theses are DER encoded.
    {Certificate, {Kind, Key, _}} = erl_make_certs:make_cert([{key, dsa}, {issuer, Issuer}]),
    {Certificate, {Kind, Key}}.

change_configuration(App, Props) ->
    ok = application:stop(App),
    ok = change_cfg(App, Props),
    application:start(App).

change_cfg(_, []) ->
    ok;
change_cfg(App, [{Name,Value}|Rest]) ->
    ok = application:set_env(App, Name, Value),
    change_cfg(App, Rest).

whitelist(Path, Filename, Certificate, {A, B} = _Key) ->
    ok = erl_make_certs:write_pem(Path, Filename, {Certificate, {A, B, not_encrypted}}),
    lists:foreach(fun delete/1, filelib:wildcard("*_key.pem", friendlies())).

delete(Name) ->
    file:delete(filename:join([friendlies(), Name])).
