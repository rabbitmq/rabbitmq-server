%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests},
      {group, sequential_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          auth_backend_internal_expand_topic_permission,
          {basic_header_handling, [parallel], [
              write_table_with_invalid_existing_type,
              invalid_existing_headers,
              disparate_invalid_header_entries_accumulate_separately,
              corrupt_or_invalid_headers_are_overwritten,
              invalid_same_header_entry_accumulation
            ]},
          content_framing,
          content_transcoding,
          decrypt_config,
          listing_plugins_from_multiple_directories,
          rabbitmqctl_encode,
          pmerge,
          plmerge,
          priority_queue,
          rabbit_direct_extract_extra_auth_props,
          {resource_monitor, [parallel], [
              parse_information_unit
            ]},
          {supervisor2, [], [
              check_shutdown_stop,
              check_shutdown_ignored
            ]},
          table_codec,
          unfold,
          {vm_memory_monitor, [parallel], [
              parse_line_linux
            ]}
        ]},
      {sequential_tests, [], [
          pg_local,
          pg_local_with_unexpected_deaths1,
          pg_local_with_unexpected_deaths2,
          decrypt_start_app,
          decrypt_start_app_file,
          decrypt_start_app_undefined,
          decrypt_start_app_wrong_passphrase
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(TC, Config) when TC =:= decrypt_start_app;
                                   TC =:= decrypt_start_app_file;
                                   TC =:= decrypt_start_app_undefined ->
    application:load(rabbit),
    application:set_env(rabbit, feature_flags_file, ""),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(TC, _Config) when TC =:= decrypt_start_app;
                                   TC =:= decrypt_start_app_file;
                                   TC =:= decrypt_start_app_undefined ->
    application:unload(rabbit),
    application:unload(rabbit_shovel_test);
end_per_testcase(decrypt_config, _Config) ->
    application:unload(rabbit);
end_per_testcase(_TC, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% basic_header_handling.
%% -------------------------------------------------------------------
-define(XDEATH_TABLE,
        [{<<"reason">>,       longstr,   <<"blah">>},
         {<<"queue">>,        longstr,   <<"foo.bar.baz">>},
         {<<"exchange">>,     longstr,   <<"my-exchange">>},
         {<<"routing-keys">>, array,     []}]).

-define(ROUTE_TABLE, [{<<"redelivered">>, bool, <<"true">>}]).

-define(BAD_HEADER(K), {<<K>>, longstr, <<"bad ", K>>}).
-define(BAD_HEADER2(K, Suf), {<<K>>, longstr, <<"bad ", K, Suf>>}).
-define(FOUND_BAD_HEADER(K), {<<K>>, array, [{longstr, <<"bad ", K>>}]}).

write_table_with_invalid_existing_type(_Config) ->
    prepend_check(<<"header1">>, ?XDEATH_TABLE, [?BAD_HEADER("header1")]).

invalid_existing_headers(_Config) ->
    Headers =
        prepend_check(<<"header2">>, ?ROUTE_TABLE, [?BAD_HEADER("header2")]),
    {array, [{table, ?ROUTE_TABLE}]} =
        rabbit_misc:table_lookup(Headers, <<"header2">>),
    passed.

disparate_invalid_header_entries_accumulate_separately(_Config) ->
    BadHeaders = [?BAD_HEADER("header2")],
    Headers = prepend_check(<<"header2">>, ?ROUTE_TABLE, BadHeaders),
    Headers2 = prepend_check(<<"header1">>, ?XDEATH_TABLE,
                             [?BAD_HEADER("header1") | Headers]),
    {table, [?FOUND_BAD_HEADER("header1"),
             ?FOUND_BAD_HEADER("header2")]} =
        rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    passed.

corrupt_or_invalid_headers_are_overwritten(_Config) ->
    Headers0 = [?BAD_HEADER("header1"),
                ?BAD_HEADER("x-invalid-headers")],
    Headers1 = prepend_check(<<"header1">>, ?XDEATH_TABLE, Headers0),
    {table,[?FOUND_BAD_HEADER("header1"),
            ?FOUND_BAD_HEADER("x-invalid-headers")]} =
        rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY),
    passed.

invalid_same_header_entry_accumulation(_Config) ->
    BadHeader1 = ?BAD_HEADER2("header1", "a"),
    Headers = prepend_check(<<"header1">>, ?ROUTE_TABLE, [BadHeader1]),
    Headers2 = prepend_check(<<"header1">>, ?ROUTE_TABLE,
                             [?BAD_HEADER2("header1", "b") | Headers]),
    {table, InvalidHeaders} =
        rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    {array, [{longstr,<<"bad header1b">>},
             {longstr,<<"bad header1a">>}]} =
        rabbit_misc:table_lookup(InvalidHeaders, <<"header1">>),
    passed.

prepend_check(HeaderKey, HeaderTable, Headers) ->
    Headers1 = rabbit_basic:prepend_table_header(
                 HeaderKey, HeaderTable, Headers),
    {table, Invalid} =
        rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY),
    {Type, Value} = rabbit_misc:table_lookup(Headers, HeaderKey),
    {array, [{Type, Value} | _]} =
        rabbit_misc:table_lookup(Invalid, HeaderKey),
    Headers1.

decrypt_config(_Config) ->
    %% Take all available block ciphers.
    Hashes = rabbit_pbe:supported_hashes(),
    Ciphers = rabbit_pbe:supported_ciphers(),
    Iterations = [1, 10, 100, 1000],
    %% Loop through all hashes, ciphers and iterations.
    _ = [begin
        PassPhrase = crypto:strong_rand_bytes(16),
        do_decrypt_config({C, H, I, PassPhrase})
    end || H <- Hashes, C <- Ciphers, I <- Iterations],
    ok.

do_decrypt_config(Algo = {C, H, I, P}) ->
    application:load(rabbit),
    RabbitConfig = application:get_all_env(rabbit),
    %% Encrypt a few values in configuration.
    %% Common cases.
    _ = [encrypt_value(Key, Algo) || Key <- [
        tcp_listeners,
        num_tcp_acceptors,
        ssl_options,
        vm_memory_high_watermark,
        default_pass,
        default_permissions,
        cluster_nodes,
        auth_mechanisms,
        msg_store_credit_disc_bound]],
    %% Special case: encrypt a value in a list.
    {ok, [LoopbackUser]} = application:get_env(rabbit, loopback_users),
    EncLoopbackUser = rabbit_pbe:encrypt_term(C, H, I, P, LoopbackUser),
    application:set_env(rabbit, loopback_users, [{encrypted, EncLoopbackUser}]),
    %% Special case: encrypt a value in a key/value list.
    {ok, TCPOpts} = application:get_env(rabbit, tcp_listen_options),
    {_, Backlog} = lists:keyfind(backlog, 1, TCPOpts),
    {_, Linger} = lists:keyfind(linger, 1, TCPOpts),
    EncBacklog = rabbit_pbe:encrypt_term(C, H, I, P, Backlog),
    EncLinger = rabbit_pbe:encrypt_term(C, H, I, P, Linger),
    TCPOpts1 = lists:keyreplace(backlog, 1, TCPOpts, {backlog, {encrypted, EncBacklog}}),
    TCPOpts2 = lists:keyreplace(linger, 1, TCPOpts1, {linger, {encrypted, EncLinger}}),
    application:set_env(rabbit, tcp_listen_options, TCPOpts2),
    %% Decrypt configuration.
    rabbit:decrypt_config([rabbit], Algo),
    %% Check that configuration was decrypted properly.
    RabbitConfig = application:get_all_env(rabbit),
    application:unload(rabbit),
    ok.

encrypt_value(Key, {C, H, I, P}) ->
    {ok, Value} = application:get_env(rabbit, Key),
    EncValue = rabbit_pbe:encrypt_term(C, H, I, P, Value),
    application:set_env(rabbit, Key, {encrypted, EncValue}).

decrypt_start_app(Config) ->
    do_decrypt_start_app(Config, "hello").

decrypt_start_app_file(Config) ->
    do_decrypt_start_app(Config, {file, ?config(data_dir, Config) ++ "/rabbit_shovel_test.passphrase"}).

do_decrypt_start_app(Config, Passphrase) ->
    %% Configure rabbit for decrypting configuration.
    application:set_env(rabbit, config_entry_decoder, [
        {cipher, aes_cbc256},
        {hash, sha512},
        {iterations, 1000},
        {passphrase, Passphrase}
    ]),
    %% Add the path to our test application.
    code:add_path(?config(data_dir, Config) ++ "/lib/rabbit_shovel_test/ebin"),
    %% Attempt to start our test application.
    %%
    %% We expect a failure *after* the decrypting has been done.
    try
        rabbit:start_apps([rabbit_shovel_test], #{rabbit => temporary})
    catch _:_ ->
        ok
    end,
    %% Check if the values have been decrypted.
    {ok, Shovels} = application:get_env(rabbit_shovel_test, shovels),
    {_, FirstShovel} = lists:keyfind(my_first_shovel, 1, Shovels),
    {_, Sources} = lists:keyfind(sources, 1, FirstShovel),
    {_, Brokers} = lists:keyfind(brokers, 1, Sources),
    ["amqp://fred:secret@host1.domain/my_vhost",
     "amqp://john:secret@host2.domain/my_vhost"] = Brokers,
    ok.

decrypt_start_app_undefined(Config) ->
    %% Configure rabbit for decrypting configuration.
    application:set_env(rabbit, config_entry_decoder, [
        {cipher, aes_cbc256},
        {hash, sha512},
        {iterations, 1000}
        %% No passphrase option!
    ]),
    %% Add the path to our test application.
    code:add_path(?config(data_dir, Config) ++ "/lib/rabbit_shovel_test/ebin"),
    %% Attempt to start our test application.
    %%
    %% We expect a failure during decryption because the passphrase is missing.
    try
        rabbit:start_apps([rabbit_shovel_test], #{rabbit => temporary})
    catch
        exit:{bad_configuration, config_entry_decoder} -> ok;
        _:Exception -> exit({unexpected_exception, Exception})
    end.

decrypt_start_app_wrong_passphrase(Config) ->
    %% Configure rabbit for decrypting configuration.
    application:set_env(rabbit, config_entry_decoder, [
        {cipher, aes_cbc256},
        {hash, sha512},
        {iterations, 1000},
        {passphrase, "wrong passphrase"}
    ]),
    %% Add the path to our test application.
    code:add_path(?config(data_dir, Config) ++ "/lib/rabbit_shovel_test/ebin"),
    %% Attempt to start our test application.
    %%
    %% We expect a failure during decryption because the passphrase is wrong.
    try
        rabbit:start_apps([rabbit_shovel_test], #{rabbit => temporary})
    catch
        exit:{decryption_error,_,_} -> ok;
        _:Exception -> exit({unexpected_exception, Exception})
    end.

rabbitmqctl_encode(_Config) ->
    % list ciphers and hashes
    {ok, _} = rabbit_control_pbe:list_ciphers(),
    {ok, _} = rabbit_control_pbe:list_hashes(),
    % incorrect ciphers, hashes and iteration number
    {error, _} = rabbit_control_pbe:encode(funny_cipher, undefined, undefined, undefined),
    {error, _} = rabbit_control_pbe:encode(undefined, funny_hash, undefined, undefined),
    {error, _} = rabbit_control_pbe:encode(undefined, undefined, -1, undefined),
    {error, _} = rabbit_control_pbe:encode(undefined, undefined, 0, undefined),
    % incorrect number of arguments
    {error, _} = rabbit_control_pbe:encode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        []
    ),
    {error, _} = rabbit_control_pbe:encode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [undefined]
    ),
    {error, _} = rabbit_control_pbe:encode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [undefined, undefined, undefined]
    ),

    % encrypt/decrypt
    % string
    rabbitmqctl_encode_encrypt_decrypt("foobar"),
    % binary
    rabbitmqctl_encode_encrypt_decrypt("<<\"foobar\">>"),
    % tuple
    rabbitmqctl_encode_encrypt_decrypt("{password,<<\"secret\">>}"),

    ok.

rabbitmqctl_encode_encrypt_decrypt(Secret) ->
    PassPhrase = "passphrase",
    {ok, Output} = rabbit_control_pbe:encode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [Secret, PassPhrase]
    ),
    {encrypted, Encrypted} = rabbit_control_pbe:evaluate_input_as_term(lists:flatten(Output)),

    {ok, Result} = rabbit_control_pbe:decode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [lists:flatten(io_lib:format("~p", [Encrypted])), PassPhrase]
    ),
    Secret = lists:flatten(Result),
    % decrypt with {encrypted, ...} form as input
    {ok, Result} = rabbit_control_pbe:decode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [lists:flatten(io_lib:format("~p", [{encrypted, Encrypted}])), PassPhrase]
    ),

    % wrong passphrase
    {error, _} = rabbit_control_pbe:decode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [lists:flatten(io_lib:format("~p", [Encrypted])), PassPhrase ++ " "]
    ),
    {error, _} = rabbit_control_pbe:decode(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(),
        [lists:flatten(io_lib:format("~p", [{encrypted, Encrypted}])), PassPhrase ++ " "]
    )
    .

rabbit_direct_extract_extra_auth_props(_Config) ->
    {ok, CSC} = code_server_cache:start_link(),
    % no protocol to extract
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{name,<<"127.0.0.1:52366 -> 127.0.0.1:1883">>}]),
    % protocol to extract, but no module to call
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{protocol, {'PROTOCOL_WITHOUT_MODULE', "1.0"}}]),
    % see rabbit_dummy_protocol_connection_info module
    % protocol to extract, module that returns a client ID
    [{client_id, <<"DummyClientId">>}] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, 1,
        [{protocol, {'DUMMY_PROTOCOL', "1.0"}}]),
    % protocol to extract, but error thrown in module
    [] = rabbit_direct:extract_extra_auth_props(
        {<<"guest">>, <<"guest">>}, <<"/">>, -1,
        [{protocol, {'DUMMY_PROTOCOL', "1.0"}}]),
    gen_server:stop(CSC),
    ok.

%% -------------------------------------------------------------------
%% pg_local.
%% -------------------------------------------------------------------

pg_local(_Config) ->
    [P, Q] = [spawn(fun () -> receive X -> X end end) || _ <- lists:seq(0, 1)],
    check_pg_local(ok, [], []),
    %% P joins group a, then b, then a again
    check_pg_local(pg_local:join(a, P), [P], []),
    check_pg_local(pg_local:join(b, P), [P], [P]),
    check_pg_local(pg_local:join(a, P), [P, P], [P]),
    %% Q joins group a, then b, then b again
    check_pg_local(pg_local:join(a, Q), [P, P, Q], [P]),
    check_pg_local(pg_local:join(b, Q), [P, P, Q], [P, Q]),
    check_pg_local(pg_local:join(b, Q), [P, P, Q], [P, Q, Q]),
    %% P leaves groups a and a
    check_pg_local(pg_local:leave(a, P), [P, Q], [P, Q, Q]),
    check_pg_local(pg_local:leave(b, P), [P, Q], [Q, Q]),
    %% leave/2 is idempotent
    check_pg_local(pg_local:leave(a, P), [Q], [Q, Q]),
    check_pg_local(pg_local:leave(a, P), [Q], [Q, Q]),
    %% clean up all processes
    [begin X ! done,
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [P, Q]],
    %% ensure the groups are empty
    check_pg_local(ok, [], []),
    passed.

pg_local_with_unexpected_deaths1(_Config) ->
    [A, B] = [spawn(fun () -> receive X -> X end end) || _ <- lists:seq(0, 1)],
    check_pg_local(ok, [], []),
    %% A joins groups a and b
    check_pg_local(pg_local:join(a, A), [A], []),
    check_pg_local(pg_local:join(b, A), [A], [A]),
    %% B joins group b
    check_pg_local(pg_local:join(b, B), [A], [A, B]),

    [begin erlang:exit(X, sleep_now_in_a_fire),
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [A, B]],
    %% ensure the groups are empty
    check_pg_local(ok, [], []),
    ?assertNot(erlang:is_process_alive(A)),
    ?assertNot(erlang:is_process_alive(B)),

    passed.

pg_local_with_unexpected_deaths2(_Config) ->
    [A, B] = [spawn(fun () -> receive X -> X end end) || _ <- lists:seq(0, 1)],
    check_pg_local(ok, [], []),
    %% A joins groups a and b
    check_pg_local(pg_local:join(a, A), [A], []),
    check_pg_local(pg_local:join(b, A), [A], [A]),
    %% B joins group b
    check_pg_local(pg_local:join(b, B), [A], [A, B]),

    %% something else yanks a record (or all of them) from the pg_local
    %% bookkeeping table
    ok = pg_local:clear(),

    [begin erlang:exit(X, sleep_now_in_a_fire),
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [A, B]],
    %% ensure the groups are empty
    check_pg_local(ok, [], []),
    ?assertNot(erlang:is_process_alive(A)),
    ?assertNot(erlang:is_process_alive(B)),

    passed.

check_pg_local(ok, APids, BPids) ->
    ok = pg_local:sync(),
    ?assertEqual([true, true], [lists:sort(Pids) == lists:sort(pg_local:get_members(Key)) ||
                                   {Key, Pids} <- [{a, APids}, {b, BPids}]]).

%% -------------------------------------------------------------------
%% priority_queue.
%% -------------------------------------------------------------------

priority_queue(_Config) ->

    false = priority_queue:is_queue(not_a_queue),

    %% empty Q
    Q = priority_queue:new(),
    {true, true, 0, [], []} = test_priority_queue(Q),

    %% 1-4 element no-priority Q
    true = lists:all(fun (X) -> X =:= passed end,
                     lists:map(fun test_simple_n_element_queue/1,
                               lists:seq(1, 4))),

    %% 1-element priority Q
    Q1 = priority_queue:in(foo, 1, priority_queue:new()),
    {true, false, 1, [{1, foo}], [foo]} =
        test_priority_queue(Q1),

    %% 2-element same-priority Q
    Q2 = priority_queue:in(bar, 1, Q1),
    {true, false, 2, [{1, foo}, {1, bar}], [foo, bar]} =
        test_priority_queue(Q2),

    %% 2-element different-priority Q
    Q3 = priority_queue:in(bar, 2, Q1),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q3),

    %% 1-element negative priority Q
    Q4 = priority_queue:in(foo, -1, priority_queue:new()),
    {true, false, 1, [{-1, foo}], [foo]} = test_priority_queue(Q4),

    %% merge 2 * 1-element no-priority Qs
    Q5 = priority_queue:join(priority_queue:in(foo, Q),
                             priority_queue:in(bar, Q)),
    {true, false, 2, [{0, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q5),

    %% merge 1-element no-priority Q with 1-element priority Q
    Q6 = priority_queue:join(priority_queue:in(foo, Q),
                             priority_queue:in(bar, 1, Q)),
    {true, false, 2, [{1, bar}, {0, foo}], [bar, foo]} =
        test_priority_queue(Q6),

    %% merge 1-element priority Q with 1-element no-priority Q
    Q7 = priority_queue:join(priority_queue:in(foo, 1, Q),
                             priority_queue:in(bar, Q)),
    {true, false, 2, [{1, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q7),

    %% merge 2 * 1-element same-priority Qs
    Q8 = priority_queue:join(priority_queue:in(foo, 1, Q),
                             priority_queue:in(bar, 1, Q)),
    {true, false, 2, [{1, foo}, {1, bar}], [foo, bar]} =
        test_priority_queue(Q8),

    %% merge 2 * 1-element different-priority Qs
    Q9 = priority_queue:join(priority_queue:in(foo, 1, Q),
                             priority_queue:in(bar, 2, Q)),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q9),

    %% merge 2 * 1-element different-priority Qs (other way around)
    Q10 = priority_queue:join(priority_queue:in(bar, 2, Q),
                              priority_queue:in(foo, 1, Q)),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q10),

    %% merge 2 * 2-element multi-different-priority Qs
    Q11 = priority_queue:join(Q6, Q5),
    {true, false, 4, [{1, bar}, {0, foo}, {0, foo}, {0, bar}],
     [bar, foo, foo, bar]} = test_priority_queue(Q11),

    %% and the other way around
    Q12 = priority_queue:join(Q5, Q6),
    {true, false, 4, [{1, bar}, {0, foo}, {0, bar}, {0, foo}],
     [bar, foo, bar, foo]} = test_priority_queue(Q12),

    %% merge with negative priorities
    Q13 = priority_queue:join(Q4, Q5),
    {true, false, 3, [{0, foo}, {0, bar}, {-1, foo}], [foo, bar, foo]} =
        test_priority_queue(Q13),

    %% and the other way around
    Q14 = priority_queue:join(Q5, Q4),
    {true, false, 3, [{0, foo}, {0, bar}, {-1, foo}], [foo, bar, foo]} =
        test_priority_queue(Q14),

    %% joins with empty queues:
    Q1 = priority_queue:join(Q, Q1),
    Q1 = priority_queue:join(Q1, Q),

    %% insert with priority into non-empty zero-priority queue
    Q15 = priority_queue:in(baz, 1, Q5),
    {true, false, 3, [{1, baz}, {0, foo}, {0, bar}], [baz, foo, bar]} =
        test_priority_queue(Q15),

    %% 1-element infinity priority Q
    Q16 = priority_queue:in(foo, infinity, Q),
    {true, false, 1, [{infinity, foo}], [foo]} = test_priority_queue(Q16),

    %% add infinity to 0-priority Q
    Q17 = priority_queue:in(foo, infinity, priority_queue:in(bar, Q)),
    {true, false, 2, [{infinity, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q17),

    %% and the other way around
    Q18 = priority_queue:in(bar, priority_queue:in(foo, infinity, Q)),
    {true, false, 2, [{infinity, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q18),

    %% add infinity to mixed-priority Q
    Q19 = priority_queue:in(qux, infinity, Q3),
    {true, false, 3, [{infinity, qux}, {2, bar}, {1, foo}], [qux, bar, foo]} =
        test_priority_queue(Q19),

    %% merge the above with a negative priority Q
    Q20 = priority_queue:join(Q19, Q4),
    {true, false, 4, [{infinity, qux}, {2, bar}, {1, foo}, {-1, foo}],
     [qux, bar, foo, foo]} = test_priority_queue(Q20),

    %% merge two infinity priority queues
    Q21 = priority_queue:join(priority_queue:in(foo, infinity, Q),
                              priority_queue:in(bar, infinity, Q)),
    {true, false, 2, [{infinity, foo}, {infinity, bar}], [foo, bar]} =
        test_priority_queue(Q21),

    %% merge two mixed priority with infinity queues
    Q22 = priority_queue:join(Q18, Q20),
    {true, false, 6, [{infinity, foo}, {infinity, qux}, {2, bar}, {1, foo},
                      {0, bar}, {-1, foo}], [foo, qux, bar, foo, bar, foo]} =
        test_priority_queue(Q22),

    passed.

priority_queue_in_all(Q, L) ->
    lists:foldl(fun (X, Acc) -> priority_queue:in(X, Acc) end, Q, L).

priority_queue_out_all(Q) ->
    case priority_queue:out(Q) of
        {empty, _}       -> [];
        {{value, V}, Q1} -> [V | priority_queue_out_all(Q1)]
    end.

test_priority_queue(Q) ->
    {priority_queue:is_queue(Q),
     priority_queue:is_empty(Q),
     priority_queue:len(Q),
     priority_queue:to_list(Q),
     priority_queue_out_all(Q)}.

test_simple_n_element_queue(N) ->
    Items = lists:seq(1, N),
    Q = priority_queue_in_all(priority_queue:new(), Items),
    ToListRes = [{0, X} || X <- Items],
    {true, false, N, ToListRes, Items} = test_priority_queue(Q),
    passed.

%% ---------------------------------------------------------------------------
%% resource_monitor.
%% ---------------------------------------------------------------------------

parse_information_unit(_Config) ->
    lists:foreach(fun ({S, V}) ->
                          V = rabbit_resource_monitor_misc:parse_information_unit(S)
                  end,
                  [
                   {"1000", {ok, 1000}},

                   {"10kB", {ok, 10000}},
                   {"10MB", {ok, 10000000}},
                   {"10GB", {ok, 10000000000}},

                   {"10kiB", {ok, 10240}},
                   {"10MiB", {ok, 10485760}},
                   {"10GiB", {ok, 10737418240}},

                   {"10k", {ok, 10240}},
                   {"10M", {ok, 10485760}},
                   {"10G", {ok, 10737418240}},

                   {"10KB", {ok, 10000}},
                   {"10K",  {ok, 10240}},
                   {"10m",  {ok, 10485760}},
                   {"10Mb", {ok, 10000000}},

                   {"0MB",  {ok, 0}},

                   {"10 k", {error, parse_error}},
                   {"MB", {error, parse_error}},
                   {"", {error, parse_error}},
                   {"0.5GB", {error, parse_error}},
                   {"10TB", {error, parse_error}}
                  ]),
    passed.

%% ---------------------------------------------------------------------------
%% supervisor2.
%% ---------------------------------------------------------------------------

check_shutdown_stop(_Config) ->
    ok = check_shutdown(stop,    200, 200, 2000).

check_shutdown_ignored(_Config) ->
    ok = check_shutdown(ignored,   1,   2, 2000).

check_shutdown(SigStop, Iterations, ChildCount, SupTimeout) ->
    {ok, Sup} = supervisor2:start_link(dummy_supervisor2, [SupTimeout]),
    Res = lists:foldl(
            fun (I, ok) ->
                    TestSupPid = erlang:whereis(dummy_supervisor2),
                    ChildPids =
                        [begin
                             {ok, ChildPid} =
                                 supervisor2:start_child(TestSupPid, []),
                             ChildPid
                         end || _ <- lists:seq(1, ChildCount)],
                    MRef = erlang:monitor(process, TestSupPid),
                    [P ! SigStop || P <- ChildPids],
                    ok = supervisor2:terminate_child(Sup, test_sup),
                    {ok, _} = supervisor2:restart_child(Sup, test_sup),
                    receive
                        {'DOWN', MRef, process, TestSupPid, shutdown} ->
                            ok;
                        {'DOWN', MRef, process, TestSupPid, Reason} ->
                            {error, {I, Reason}}
                    end;
                (_, R) ->
                    R
            end, ok, lists:seq(1, Iterations)),
    unlink(Sup),
    MSupRef = erlang:monitor(process, Sup),
    exit(Sup, shutdown),
    receive
        {'DOWN', MSupRef, process, Sup, _Reason} ->
            ok
    end,
    Res.

%% ---------------------------------------------------------------------------
%% vm_memory_monitor.
%% ---------------------------------------------------------------------------

parse_line_linux(_Config) ->
    lists:foreach(fun ({S, {K, V}}) ->
                          {K, V} = vm_memory_monitor:parse_line_linux(S)
                  end,
                  [{"MemTotal:      0 kB",        {'MemTotal', 0}},
                   {"MemTotal:      502968 kB  ", {'MemTotal', 515039232}},
                   {"MemFree:         178232 kB", {'MemFree',  182509568}},
                   {"MemTotal:         50296888", {'MemTotal', 50296888}},
                   {"MemTotal         502968 kB", {'MemTotal', 515039232}},
                   {"MemTotal     50296866   ",   {'MemTotal', 50296866}}]),
    ok.

%% ---------------------------------------------------------------------------
%% Unordered tests (originally from rabbit_tests.erl).
%% ---------------------------------------------------------------------------

%% Test that content frames don't exceed frame-max
content_framing(_Config) ->
    %% no content
    passed = test_content_framing(4096, <<>>),
    %% easily fit in one frame
    passed = test_content_framing(4096, <<"Easy">>),
    %% exactly one frame (empty frame = 8 bytes)
    passed = test_content_framing(11, <<"One">>),
    %% more than one frame
    passed = test_content_framing(11, <<"More than one frame">>),
    passed.

test_content_framing(FrameMax, BodyBin) ->
    [Header | Frames] =
        rabbit_binary_generator:build_simple_content_frames(
          1,
          rabbit_binary_generator:ensure_content_encoded(
            rabbit_basic:build_content(#'P_basic'{}, BodyBin),
            rabbit_framing_amqp_0_9_1),
          FrameMax,
          rabbit_framing_amqp_0_9_1),
    %% header is formatted correctly and the size is the total of the
    %% fragments
    <<_FrameHeader:7/binary, _ClassAndWeight:4/binary,
      BodySize:64/unsigned, _Rest/binary>> = list_to_binary(Header),
    BodySize = size(BodyBin),
    true = lists:all(
             fun (ContentFrame) ->
                     FrameBinary = list_to_binary(ContentFrame),
                     %% assert
                     <<_TypeAndChannel:3/binary,
                       Size:32/unsigned, _Payload:Size/binary, 16#CE>> =
                         FrameBinary,
                     size(FrameBinary) =< FrameMax
             end, Frames),
    passed.

content_transcoding(_Config) ->
    %% there are no guarantees provided by 'clear' - it's just a hint
    ClearDecoded = fun rabbit_binary_parser:clear_decoded_content/1,
    ClearEncoded = fun rabbit_binary_generator:clear_encoded_content/1,
    EnsureDecoded =
        fun (C0) ->
                C1 = rabbit_binary_parser:ensure_content_decoded(C0),
                true = C1#content.properties =/= none,
                C1
        end,
    EnsureEncoded =
        fun (Protocol) ->
                fun (C0) ->
                        C1 = rabbit_binary_generator:ensure_content_encoded(
                               C0, Protocol),
                        true = C1#content.properties_bin =/= none,
                        C1
                end
        end,
    %% Beyond the assertions in Ensure*, the only testable guarantee
    %% is that the operations should never fail.
    %%
    %% If we were using quickcheck we'd simply stuff all the above
    %% into a generator for sequences of operations. In the absence of
    %% quickcheck we pick particularly interesting sequences that:
    %%
    %% - execute every op twice since they are idempotent
    %% - invoke clear_decoded, clear_encoded, decode and transcode
    %%   with one or both of decoded and encoded content present
    [begin
         sequence_with_content([Op]),
         sequence_with_content([ClearEncoded, Op]),
         sequence_with_content([ClearDecoded, Op])
     end || Op <- [ClearDecoded, ClearEncoded, EnsureDecoded,
                   EnsureEncoded(rabbit_framing_amqp_0_9_1),
                   EnsureEncoded(rabbit_framing_amqp_0_8)]],
    passed.

sequence_with_content(Sequence) ->
    lists:foldl(fun (F, V) -> F(F(V)) end,
                rabbit_binary_generator:ensure_content_encoded(
                  rabbit_basic:build_content(#'P_basic'{}, <<>>),
                  rabbit_framing_amqp_0_9_1),
                Sequence).

pmerge(_Config) ->
    P = [{a, 1}, {b, 2}],
    P = rabbit_misc:pmerge(a, 3, P),
    [{c, 3} | P] = rabbit_misc:pmerge(c, 3, P),
    passed.

plmerge(_Config) ->
    P1 = [{a, 1}, {b, 2}, {c, 3}],
    P2 = [{a, 2}, {d, 4}],
    [{a, 1}, {b, 2}, {c, 3}, {d, 4}] = rabbit_misc:plmerge(P1, P2),
    passed.

table_codec(_Config) ->
    %% FIXME this does not test inexact numbers (double and float) yet,
    %% because they won't pass the equality assertions
    Table = [{<<"longstr">>,   longstr,   <<"Here is a long string">>},
             {<<"signedint">>, signedint, 12345},
             {<<"decimal">>,   decimal,   {3, 123456}},
             {<<"timestamp">>, timestamp, 109876543209876},
             {<<"table">>,     table,     [{<<"one">>, signedint, 54321},
                                           {<<"two">>, longstr,
                                            <<"A long string">>}]},
             {<<"byte">>,      byte,      -128},
             {<<"long">>,      long,      1234567890},
             {<<"short">>,     short,     655},
             {<<"bool">>,      bool,      true},
             {<<"binary">>,    binary,    <<"a binary string">>},
             {<<"unsignedbyte">>, unsignedbyte, 250},
             {<<"unsignedshort">>, unsignedshort, 65530},
             {<<"unsignedint">>, unsignedint, 4294967290},
             {<<"void">>,      void,      undefined},
             {<<"array">>,     array,     [{signedint, 54321},
                                           {longstr, <<"A long string">>}]}
            ],
    Binary = <<
               7,"longstr",   "S", 21:32, "Here is a long string",
               9,"signedint", "I", 12345:32/signed,
               7,"decimal",   "D", 3, 123456:32,
               9,"timestamp", "T", 109876543209876:64,
               5,"table",     "F", 31:32, % length of table
               3,"one",       "I", 54321:32,
               3,"two",       "S", 13:32, "A long string",
               4,"byte",      "b", -128:8/signed,
               4,"long",      "l", 1234567890:64,
               5,"short",     "s", 655:16,
               4,"bool",      "t", 1,
               6,"binary",    "x", 15:32, "a binary string",
               12,"unsignedbyte", "B", 250:8/unsigned,
               13,"unsignedshort", "u", 65530:16/unsigned,
               11,"unsignedint", "i", 4294967290:32/unsigned,
               4,"void",      "V",
               5,"array",     "A", 23:32,
               "I", 54321:32,
               "S", 13:32, "A long string"
             >>,
    Binary = rabbit_binary_generator:generate_table(Table),
    Table  = rabbit_binary_parser:parse_table(Binary),
    passed.

unfold(_Config) ->
    {[], test} = rabbit_misc:unfold(fun (_V) -> false end, test),
    List = lists:seq(2,20,2),
    {List, 0} = rabbit_misc:unfold(fun (0) -> false;
                                       (N) -> {true, N*2, N-1}
                                   end, 10),
    passed.

listing_plugins_from_multiple_directories(Config) ->
    %% Generate some fake plugins in .ez files
    FirstDir = filename:join([?config(priv_dir, Config), "listing_plugins_from_multiple_directories-1"]),
    SecondDir = filename:join([?config(priv_dir, Config), "listing_plugins_from_multiple_directories-2"]),
    ok = file:make_dir(FirstDir),
    ok = file:make_dir(SecondDir),
    lists:foreach(fun({Dir, AppName, Vsn}) ->
                          EzName = filename:join([Dir, io_lib:format("~s-~s.ez", [AppName, Vsn])]),
                          AppFileName = lists:flatten(io_lib:format("~s-~s/ebin/~s.app", [AppName, Vsn, AppName])),
                          AppFileContents = list_to_binary(io_lib:format("~p.", [{application, AppName, [{vsn, Vsn}]}])),
                          {ok, {_, EzData}} = zip:zip(EzName, [{AppFileName, AppFileContents}], [memory]),
                          ok = file:write_file(EzName, EzData)
                  end,
                  [{FirstDir, plugin_first_dir, "3"},
                   {SecondDir, plugin_second_dir, "4"},
                   {FirstDir, plugin_both, "1"},
                   {SecondDir, plugin_both, "2"}]),

    %% Everything was collected from both directories, plugin with higher version should take precedence
    Path = FirstDir ++ ":" ++ SecondDir,
    Got = lists:sort([{Name, Vsn} || #plugin{name = Name, version = Vsn} <- rabbit_plugins:list(Path)]),
    Expected = [{plugin_both, "2"}, {plugin_first_dir, "3"}, {plugin_second_dir, "4"}],
    case Got of
        Expected ->
            ok;
        _ ->
            ct:pal("Got ~p~nExpected: ~p", [Got, Expected]),
            exit({wrong_plugins_list, Got})
    end,
    ok.

auth_backend_internal_expand_topic_permission(_Config) ->
    ExpandMap = #{<<"username">> => <<"guest">>, <<"vhost">> => <<"default">>},
    %% simple case
    <<"services/default/accounts/guest/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            ExpandMap
        ),
    %% replace variable twice
    <<"services/default/accounts/default/guest/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{vhost}/{username}/notifications">>,
            ExpandMap
        ),
    %% nothing to replace
    <<"services/accounts/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/accounts/notifications">>,
            ExpandMap
        ),
    %% the expand map isn't defined
    <<"services/{vhost}/accounts/{username}/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            undefined
        ),
    %% the expand map is empty
    <<"services/{vhost}/accounts/{username}/notifications">> =
        rabbit_auth_backend_internal:expand_topic_permission(
            <<"services/{vhost}/accounts/{username}/notifications">>,
            #{}
        ),
    ok.
