%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_definition_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, unit_tests}
    ].

groups() ->
    [
     {unit_tests, [parallel], [
        extract_amqp091_queue_source,
        extract_amqp091_exchange_source,
        extract_amqp091_empty_queue_falls_through,
        extract_amqp091_none_queue_falls_through,
        extract_amqp091_none_exchange_returns_unknown,
        extract_amqp10_address_source,
        extract_amqp10_queues_format,
        extract_amqp10_queues_empty_name,
        extract_amqp10_amq_queue_format,
        extract_amqp10_exchanges_format,
        extract_amqp10_exchanges_with_key_format,
        extract_amqp10_empty_address,
        extract_amqp10_missing_address,
        extract_local_queue_source,
        extract_local_exchange_source,
        extract_empty_definition,
        extract_map_input,
        vhost_from_uri_default,
        vhost_from_uri_custom,
        vhost_from_uri_encoded,
        vhost_local_protocol_fallback,
        vhost_cross_vhost_consumption,
        vhost_malformed_uri_fallback,
        vhost_unencrypted_uri_parsed,
        vhost_uri_no_path,
        vhost_uri_trailing_slash,
        vhost_uri_bare_binary,
        vhost_uri_double_slash
     ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%%
%% Unit Tests
%%

extract_amqp091_queue_source(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp091_queue_source_0, []).

extract_amqp091_queue_source_0() ->
    Def = [{<<"src-queue">>, <<"my-queue">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := queue, queue := <<"my-queue">>, protocol := amqp091}, Result).

extract_amqp091_exchange_source(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp091_exchange_source_0, []).

extract_amqp091_exchange_source_0() ->
    Def = [{<<"src-exchange">>, <<"my-exchange">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := exchange, exchange := <<"my-exchange">>, queue := undefined}, Result).

extract_amqp091_empty_queue_falls_through(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp091_empty_queue_falls_through_0, []).

extract_amqp091_empty_queue_falls_through_0() ->
    Def = [{<<"src-queue">>, <<>>}, {<<"src-exchange">>, <<"x">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := exchange, exchange := <<"x">>}, Result).

extract_amqp091_none_queue_falls_through(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp091_none_queue_falls_through_0, []).

extract_amqp091_none_queue_falls_through_0() ->
    Def = [{<<"src-queue">>, none}, {<<"src-exchange">>, <<"x">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := exchange, exchange := <<"x">>}, Result).

extract_amqp091_none_exchange_returns_unknown(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp091_none_exchange_returns_unknown_0, []).

extract_amqp091_none_exchange_returns_unknown_0() ->
    Def = [{<<"src-exchange">>, none}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined}, Result).

extract_amqp10_address_source(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_address_source_0, []).

extract_amqp10_address_source_0() ->
    %% Bare address (not v2 format) returns unknown type
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"my-address">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_amqp10_queues_format(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_queues_format_0, []).

extract_amqp10_queues_format_0() ->
    %% Address v2: /queues/:queue
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"/queues/my-queue">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := queue, queue := <<"my-queue">>, protocol := amqp10}, Result),
    %% With percent-encoding
    Def2 = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"/queues/my%2Fqueue">>}],
    Result2 = rabbit_shovel_definition:extract_source_info(Def2, <<"/">>),
    ?assertMatch(#{type := queue, queue := <<"my/queue">>}, Result2).

extract_amqp10_queues_empty_name(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_queues_empty_name1, []).

extract_amqp10_queues_empty_name1() ->
    %% /queues/ with an empty queue name returns 'unknown'
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"/queues/">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_amqp10_amq_queue_format(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_amq_queue_format_0, []).

extract_amqp10_amq_queue_format_0() ->
    %% Address v1 format /amq/queue/:queue is not recognized as v2, returns unknown
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"/amq/queue/my-queue">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_amqp10_exchanges_format(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_exchanges_format_0, []).

extract_amqp10_exchanges_format_0() ->
    %% /exchanges/ is for targets (publishing), not sources (consuming).
    %% For source addresses, only /queues/ is valid in v2.
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"/exchanges/my-exchange">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_amqp10_exchanges_with_key_format(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_exchanges_with_key_format_0, []).

extract_amqp10_exchanges_with_key_format_0() ->
    %% /exchanges/:exchange/:key is for targets only
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<"/exchanges/my-exchange/my-key">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_amqp10_empty_address(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_empty_address_0, []).

extract_amqp10_empty_address_0() ->
    Def = [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, <<>>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_amqp10_missing_address(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_amqp10_missing_address_0, []).

extract_amqp10_missing_address_0() ->
    Def = [{<<"src-protocol">>, <<"amqp10">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := unknown, queue := undefined, protocol := amqp10}, Result).

extract_local_queue_source(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_local_queue_source_0, []).

extract_local_queue_source_0() ->
    Def = [{<<"src-protocol">>, <<"local">>}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"production">>),
    ?assertMatch(#{type := queue, queue := <<"q">>, protocol := local, vhost := <<"production">>}, Result).

extract_local_exchange_source(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_local_exchange_source_0, []).

extract_local_exchange_source_0() ->
    Def = [{<<"src-protocol">>, <<"local">>}, {<<"src-exchange">>, <<"x">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := exchange, exchange := <<"x">>, protocol := local}, Result).

extract_empty_definition(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_empty_definition_0, []).

extract_empty_definition_0() ->
    Result = rabbit_shovel_definition:extract_source_info([], <<"/">>),
    ?assertMatch(#{type := unknown, protocol := amqp091, vhost := <<"/">>}, Result).

extract_map_input(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, extract_map_input_0, []).

extract_map_input_0() ->
    Def = #{<<"src-queue">> => <<"q">>},
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{type := queue, queue := <<"q">>}, Result).

vhost_from_uri_default(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_from_uri_default_0, []).

vhost_from_uri_default_0() ->
    URI = <<"amqp://localhost/%2F">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"other">>),
    ?assertMatch(#{vhost := <<"/">>}, Result).

vhost_from_uri_custom(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_from_uri_custom_0, []).

vhost_from_uri_custom_0() ->
    URI = <<"amqp://localhost/my-vhost">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{vhost := <<"my-vhost">>}, Result).

vhost_from_uri_encoded(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_from_uri_encoded_0, []).

vhost_from_uri_encoded_0() ->
    URI = <<"amqp://localhost/my%2Fvhost">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"/">>),
    ?assertMatch(#{vhost := <<"my/vhost">>}, Result).

vhost_local_protocol_fallback(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_local_protocol_fallback_0, []).

vhost_local_protocol_fallback_0() ->
    Def = [{<<"src-protocol">>, <<"local">>}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"production">>),
    ?assertMatch(#{vhost := <<"production">>}, Result).

vhost_cross_vhost_consumption(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_cross_vhost_consumption_0, []).

vhost_cross_vhost_consumption_0() ->
    URI = <<"amqp://localhost/other-vhost">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"shovel-vhost">>),
    ?assertMatch(#{vhost := <<"other-vhost">>}, Result).

vhost_malformed_uri_fallback(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_malformed_uri_fallback_0, []).

vhost_malformed_uri_fallback_0() ->
    %% Test that malformed src-uri values (non-binary, non-list) fall back to shovel vhost
    Def1 = [{<<"src-uri">>, some_atom}, {<<"src-queue">>, <<"q">>}],
    Result1 = rabbit_shovel_definition:extract_source_info(Def1, <<"fallback">>),
    ?assertMatch(#{vhost := <<"fallback">>}, Result1),

    Def2 = [{<<"src-uri">>, 12345}, {<<"src-queue">>, <<"q">>}],
    Result2 = rabbit_shovel_definition:extract_source_info(Def2, <<"fallback">>),
    ?assertMatch(#{vhost := <<"fallback">>}, Result2).

vhost_unencrypted_uri_parsed(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_unencrypted_uri_parsed_0, []).

vhost_unencrypted_uri_parsed_0() ->
    %% Unencrypted URIs are parsed successfully (deobfuscate returns them as-is).
    %% This verifies the code handles both encrypted and unencrypted URIs gracefully.
    Def = [{<<"src-uri">>, [<<"amqp://localhost/some-vhost">>]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"fallback">>),
    ?assertMatch(#{vhost := <<"some-vhost">>}, Result).

vhost_uri_no_path(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_uri_no_path_0, []).

vhost_uri_no_path_0() ->
    %% Test that URI with no path returns default vhost "/"
    URI = <<"amqp://localhost">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"other">>),
    ?assertMatch(#{vhost := <<"/">>}, Result).

vhost_uri_trailing_slash(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_uri_trailing_slash_0, []).

vhost_uri_trailing_slash_0() ->
    %% Test that URI with trailing slash only returns default vhost "/"
    URI = <<"amqp://localhost/">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"other">>),
    ?assertMatch(#{vhost := <<"/">>}, Result).

vhost_uri_bare_binary(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_uri_bare_binary_0, []).

vhost_uri_bare_binary_0() ->
    %% Test that src-uri as a bare binary (not wrapped in list) works correctly
    URI = <<"amqp://localhost/my-vhost">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, Encrypted}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"fallback">>),
    ?assertMatch(#{vhost := <<"my-vhost">>}, Result).

vhost_uri_double_slash(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, vhost_uri_double_slash_0, []).

vhost_uri_double_slash_0() ->
    %% Test that URI with double slash (vhost "/" without percent-encoding) works correctly.
    %% amqp://localhost// means vhost "/"
    URI = <<"amqp://localhost//">>,
    Encrypted = credentials_obfuscation:encrypt(URI),
    Def = [{<<"src-uri">>, [Encrypted]}, {<<"src-queue">>, <<"q">>}],
    Result = rabbit_shovel_definition:extract_source_info(Def, <<"other">>),
    ?assertMatch(#{vhost := <<"/">>}, Result).
