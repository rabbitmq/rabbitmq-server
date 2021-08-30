%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(add_uaa_key_command_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand').

all() ->
    [validate_arguments,
     validate_json_key,
     validate_pem_key,
     validate_pem_file_key
    ].


init_per_suite(Config) ->
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, []).


validate_arguments(_) ->
    {validation_failure, too_many_args} =
        ?COMMAND:validate([<<"one">>, <<"two">>], #{json => <<"{}">>}),
    {validation_failure, not_enough_args} =
        ?COMMAND:validate([], #{json => <<"{}">>}),
    {validation_failure, {bad_argument, <<"No key specified">>}} =
        ?COMMAND:validate([<<"foo">>], #{}),
    {validation_failure, {bad_argument, <<"There can be only one key type">>}} =
        ?COMMAND:validate([<<"foo">>], #{json => <<"{}">>, pem => <<"pem">>}),
    {validation_failure, {bad_argument, <<"There can be only one key type">>}} =
        ?COMMAND:validate([<<"foo">>], #{json => <<"{}">>, pem_file => <<"/tmp/key.pem">>}),
    {validation_failure, {bad_argument, <<"There can be only one key type">>}} =
        ?COMMAND:validate([<<"foo">>], #{pem => <<"pem">>, pem_file => <<"/tmp/key.pem">>}).

validate_json_key(_) ->
    {validation_failure, {bad_argument, <<"Invalid JSON">>}} =
        ?COMMAND:validate([<<"foo">>], #{json => <<"foobar">>}),
    {validation_failure, {bad_argument, <<"Json key should contain \"kty\" field">>}} =
        ?COMMAND:validate([<<"foo">>], #{json => <<"{}">>}),
    {validation_failure, {bad_argument, _}} =
        ?COMMAND:validate([<<"foo">>], #{json => <<"{\"kty\": \"oct\"}">>}),
    ValidJson = <<"{\"alg\":\"HS256\",\"k\":\"dG9rZW5rZXk\",\"kid\":\"token-key\",\"kty\":\"oct\",\"use\":\"sig\",\"value\":\"tokenkey\"}">>,
    ok = ?COMMAND:validate([<<"foo">>], #{json => ValidJson}).

validate_pem_key(Config) ->
    {validation_failure, <<"Unable to read a key from the PEM string">>} =
        ?COMMAND:validate([<<"foo">>], #{pem => <<"not a key">>}),
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, <<"client">>, <<"key.pem">>]),
    {ok, Key} = file:read_file(Keyfile),
    ok = ?COMMAND:validate([<<"foo">>], #{pem => Key}).

validate_pem_file_key(Config) ->
    {validation_failure, {bad_argument, <<"PEM file not found">>}} =
        ?COMMAND:validate([<<"foo">>], #{pem_file => <<"non_existent_file">>}),
    file:write_file("empty.pem", <<"">>),
    {validation_failure, <<"Unable to read a key from the PEM file">>} =
        ?COMMAND:validate([<<"foo">>], #{pem_file => <<"empty.pem">>}),
    file:write_file("not_pem.pem", <<"">>),
    {validation_failure, _} =
        ?COMMAND:validate([<<"foo">>], #{pem_file => <<"not_pem.pem">>}),
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, <<"client">>, <<"key.pem">>]),
    ok = ?COMMAND:validate([<<"foo">>], #{pem_file => Keyfile}).

