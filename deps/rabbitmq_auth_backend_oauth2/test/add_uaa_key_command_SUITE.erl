-module(add_uaa_key_command_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand').

all() ->
    [validate_arguments,
     validate_json_key,
     validate_pem_key
    ].


init_per_suite(Config) ->
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, []).


validate_arguments(_) ->
    {validation_failure, {bad_argument, too_many_args}} =
        ?COMMAND:validate([<<"one">>, <<"two">>], #{json => <<"{}">>}),
    {validation_failure, {bad_argument, not_enough_args}} =
        ?COMMAND:validate([], #{json => <<"{}">>}),
    {validation_failure, {bad_argument, <<"No key specified">>}} =
        ?COMMAND:validate([<<"foo">>], #{}),
    {validation_failure, {bad_argument, <<"Key type should be either json or pem">>}} =
        ?COMMAND:validate([<<"foo">>], , #{json => <<"{}">>, pem => "/tmp/key.pem"}).

validate_json_key(_) ->
    {validation_failure, {bad_argument, <<"Invalid JSON">>}} =
        ?COMMAND:validate([<<"foo">>, #{json => <<"foobar">>}]),
    {validation_failure, {bad_argument, <<"JSON key should contain \"kty\" field">>}} =
        ?COMMAND:validate([<<"foo">>, #{json => <<"{}">>}]),
    {validation_failure, {bad_argument, _}} =
        ?COMMAND:validate([<<"foo">>, #{json => <<"{\"kty\": \"oct\"}">>}]),
    VlidJson = <<"{\"alg\":\"HS256\",\"k\":\"dG9rZW5rZXk\",\"kid\":\"token-key\",\"kty\":\"oct\",\"use\":\"sig\",\"value\":\"tokenkey\"}">>,
    ok = ?COMMAND:validate([<<"foo">>], #{json => ValidJson}).

validate_pem_key(_) ->
    {validation_failure, {bad_argument, <<"PEM file not found">>}} =
        ?COMMAND:validate([<<"foo">>], #{pem => <<"non_existent_file">>}),
    file:write_file("empty.pem", <<"">>),
    {validation_failure, <<"Unable to read a key from the PEM file">>} =
        ?COMMAND:validate([<<"foo">>], #{pem => <<"empty.pem">>}),
    file:write_file("not_pem.pem", <<"">>),
    {validation_failure, _} =
        ?COMMAND:validate([<<"foo">>], #{pem => <<"not_pem.pem">>}),
    Keyfile = filename:join([CertsDir, "client", "key.pem"]),
    ok = ?COMMAND:validate([<<"foo">>], #{pem => Keyfile}).

