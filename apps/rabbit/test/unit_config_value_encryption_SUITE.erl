%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_config_value_encryption_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%% This cipher is listed as supported, but doesn't actually work.
%% OTP bug: https://bugs.erlang.org/browse/ERL-1478
-define(SKIPPED_CIPHERS, [aes_ige256]).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          decrypt_start_app,
          decrypt_start_app_file,
          decrypt_start_app_undefined,
          decrypt_start_app_wrong_passphrase,
          decrypt_config,
          rabbitmqctl_encode
        ]}
    ].

init_per_testcase(TC, Config) when TC =:= decrypt_start_app;
                                   TC =:= decrypt_start_app_file;
                                   TC =:= decrypt_start_app_undefined;
                                   TC =:= decrypt_start_app_wrong_passphrase ->
    application:set_env(rabbit, feature_flags_file, "", [{persistent, true}]),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

decrypt_config(_Config) ->
    %% Take all available block ciphers.
    Hashes = rabbit_pbe:supported_hashes(),
    Ciphers = rabbit_pbe:supported_ciphers() -- ?SKIPPED_CIPHERS,
    Iterations = [1, 10, 100, 1000],
    %% Loop through all hashes, ciphers and iterations.
    _ = [begin
        PassPhrase = crypto:strong_rand_bytes(16),
        do_decrypt_config({C, H, I, PassPhrase})
    end || H <- Hashes, C <- Ciphers, I <- Iterations],
    ok.

do_decrypt_config(Algo = {C, H, I, P}) ->
    ok = application:load(rabbit),
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
    {encrypted, EncLoopbackUser} = rabbit_pbe:encrypt_term(C, H, I, P, LoopbackUser),
    application:set_env(rabbit, loopback_users, [{encrypted, EncLoopbackUser}]),
    %% Special case: encrypt a value in a key/value list.
    {ok, TCPOpts} = application:get_env(rabbit, tcp_listen_options),
    {_, Backlog} = lists:keyfind(backlog, 1, TCPOpts),
    {_, Linger} = lists:keyfind(linger, 1, TCPOpts),
    {encrypted, EncBacklog} = rabbit_pbe:encrypt_term(C, H, I, P, Backlog),
    {encrypted, EncLinger} = rabbit_pbe:encrypt_term(C, H, I, P, Linger),
    TCPOpts1 = lists:keyreplace(backlog, 1, TCPOpts, {backlog, {encrypted, EncBacklog}}),
    TCPOpts2 = lists:keyreplace(linger, 1, TCPOpts1, {linger, {encrypted, EncLinger}}),
    application:set_env(rabbit, tcp_listen_options, TCPOpts2),
    %% Decrypt configuration.
    rabbit_prelaunch_conf:decrypt_config([rabbit], Algo),
    %% Check that configuration was decrypted properly.
    RabbitConfig = application:get_all_env(rabbit),
    ok = application:unload(rabbit),
    ok.

encrypt_value(Key, {C, H, I, P}) ->
    {ok, Value} = application:get_env(rabbit, Key),
    {encrypted, EncValue} = rabbit_pbe:encrypt_term(C, H, I, P, Value),
    application:set_env(rabbit, Key, {encrypted, EncValue}).

decrypt_start_app(Config) ->
    do_decrypt_start_app(Config, "hello").

decrypt_start_app_file(Config) ->
    do_decrypt_start_app(Config, {file, ?config(data_dir, Config) ++ "/rabbit_shovel_test.passphrase"}).

do_decrypt_start_app(Config, Passphrase) ->
    %% Configure rabbit for decrypting configuration.
    application:set_env(rabbit, config_entry_decoder, [
        {cipher, aes_256_cbc},
        {hash, sha512},
        {iterations, 1000},
        {passphrase, Passphrase}
    ], [{persistent, true}]),
    %% Add the path to our test application.
    code:add_path(?config(data_dir, Config) ++ "/lib/rabbit_shovel_test/ebin"),
    %% Attempt to start our test application.
    %%
    %% We expect a failure *after* the decrypting has been done.
    try
        rabbit:start_apps([rabbit_shovel_test], #{rabbit => temporary})
    catch _:Err ->
              ct:pal("~s: start_apps failed with ~p", [Err]),
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
    ], [{persistent, true}]),
    %% Add the path to our test application.
    code:add_path(?config(data_dir, Config) ++ "/lib/rabbit_shovel_test/ebin"),
    %% Attempt to start our test application.
    %%
    %% We expect a failure during decryption because the passphrase is missing.
    try
        rabbit:start_apps([rabbit_shovel_test], #{rabbit => temporary})
    catch
        throw:{bad_config_entry_decoder, missing_passphrase} -> ok;
        _:Exception -> exit({unexpected_exception, Exception})
    end.

decrypt_start_app_wrong_passphrase(Config) ->
    %% Configure rabbit for decrypting configuration.
    application:set_env(rabbit, config_entry_decoder, [
        {cipher, aes_cbc256},
        {hash, sha512},
        {iterations, 1000},
        {passphrase, "wrong passphrase"}
    ], [{persistent, true}]),
    %% Add the path to our test application.
    code:add_path(?config(data_dir, Config) ++ "/lib/rabbit_shovel_test/ebin"),
    %% Attempt to start our test application.
    %%
    %% We expect a failure during decryption because the passphrase is wrong.
    try
        rabbit:start_apps([rabbit_shovel_test], #{rabbit => temporary})
    catch
        throw:{config_decryption_error, _, _} -> ok;
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
    ).
