%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_parsing_and_validation_SUITE).

-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [parallel], [
          parse_amqp091,
          parse_amqp10_mixed,
          parse_local,
          source_without_declarations_is_arity_compatible,
          source_with_declarations_is_arity_compatible,
          destination_without_declarations_is_arity_compatible,
          prop_source_decl_fun_is_arity_compatible
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(credentials_obfuscation),
    ok = credentials_obfuscation:set_secret(crypto:strong_rand_bytes(128)),
    Config.

end_per_suite(Config) ->
    ok = application:stop(credentials_obfuscation),
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) -> Config.

end_per_testcase(_Testcase, Config) -> Config.


%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

parse_amqp091(_Config) ->
    Amqp091Src = {source, [{protocol, amqp091},
                           {uris, ["ampq://myhost:5672/vhost"]},
                           {declarations, []},
                           {queue, <<"the-queue">>},
                           {delete_after, never},
                           {prefetch_count, 10}]},
    Amqp091Dst = {destination, [{protocol, amqp091},
                                {uris, ["ampq://myhost:5672"]},
                                {declarations, []},
                                {publish_properties, [{delivery_mode, 1}]},
                                {publish_fields, []},
                                {add_forward_headers, true}]},
    In = [Amqp091Src,
          Amqp091Dst,
          {ack_mode, on_confirm},
          {reconnect_delay, 2}],

    {ok, Parsed} = rabbit_shovel_config:parse(my_shovel, In),
    ?assertMatch(
       #{name := my_shovel,
         ack_mode := on_confirm,
         reconnect_delay := 2,
         dest := #{module := rabbit_amqp091_shovel,
                   uris := [{encrypted, _}],
                   fields_fun := _PubFields,
                   props_fun := _PubProps,
                   resource_decl := {rabbit_amqp091_shovel, decl_fun, [[]]},
                   add_timestamp_header := false,
                   add_forward_headers := true},
         source := #{module := rabbit_amqp091_shovel,
                     uris := [{encrypted, _}],
                     queue := <<"the-queue">>,
                     prefetch_count := 10,
                     delete_after := never,
                     resource_decl := {rabbit_amqp091_shovel, decl_fun, [[]]}}},
        Parsed),
    assert_uris_round_trip(Parsed,
                           ["ampq://myhost:5672/vhost"],
                           ["ampq://myhost:5672"]),
    ok.

parse_amqp10_mixed(_Config) ->
    Amqp10Src = {source, [{protocol, amqp10},
                          {uris, ["ampq://myotherhost:5672"]},
                          {source_address, <<"the-queue">>}
                         ]},
    Amqp10Dst = {destination, [{protocol, amqp10},
                               {uris, ["ampq://myhost:5672"]},
                               {target_address, <<"targe-queue">>},
                               {message_annotations, [{soma_ann, <<"some-info">>}]},
                               {properties, [{user_id, <<"some-user">>}]},
                               {application_properties, [{app_prop_key, <<"app_prop_value">>}]},
                               {add_forward_headers, true}
                              ]},
    In = [Amqp10Src,
          Amqp10Dst,
          {ack_mode, on_confirm},
          {reconnect_delay, 2}],

    {ok, Parsed} = rabbit_shovel_config:parse(my_shovel, In),
    ?assertMatch(
       #{name := my_shovel,
         ack_mode := on_confirm,
         source := #{module := rabbit_amqp10_shovel,
                     uris := [{encrypted, _}],
                     source_address := <<"the-queue">>
                     },
         dest := #{module := rabbit_amqp10_shovel,
                   uris := [{encrypted, _}],
                   target_address := <<"targe-queue">>,
                   properties := #{user_id := <<"some-user">>},
                   application_properties := #{app_prop_key := <<"app_prop_value">>},
                   message_annotations := #{soma_ann := <<"some-info">>},
                   add_forward_headers := true}},
        Parsed),
    assert_uris_round_trip(Parsed,
                           ["ampq://myotherhost:5672"],
                           ["ampq://myhost:5672"]),
    ok.

parse_local(_Config) ->
    Amqp091Src = {source, [
        {protocol, local},
        {uris, ["ampq://myhost:5672/vhost"]},
        {declarations, []},
        {queue, <<"the-queue">>},
        {delete_after, never},
        {prefetch_count, 10}]},
    Amqp091Dst = {destination, [
        {protocol, local},
        {uris, ["ampq://myhost:5672"]},
        {declarations, []},
        {publish_properties, [{delivery_mode, 1}]},
        {publish_fields, []},
        {add_forward_headers, true}]},
    In = [Amqp091Src,
        Amqp091Dst,
        {ack_mode, on_confirm},
        {reconnect_delay, 2}],

    {ok, Parsed} = rabbit_shovel_config:parse(my_shovel, In),
    ?assertMatch(
        #{name := my_shovel,
            ack_mode := on_confirm,
            reconnect_delay := 2,
            shovel_type := static,
            dest := #{
                module := rabbit_local_shovel,
                uris := [{encrypted, _}],
                exchange := none,
                routing_key := none,
                resource_decl := {rabbit_local_shovel, decl_fun, [[]]},
                add_timestamp_header := false,
                add_forward_headers := true},
            source := #{
                module := rabbit_local_shovel,
                uris := [{encrypted, _}],
                queue := <<"the-queue">>,
                consumer_args := [],
                delete_after := never,
                resource_decl := {rabbit_local_shovel, decl_fun, [[]]}}},
        Parsed),
    assert_uris_round_trip(Parsed,
                           ["ampq://myhost:5672/vhost"],
                           ["ampq://myhost:5672"]),
    ok.

assert_uris_round_trip(#{source := #{uris := SrcUris},
                         dest := #{uris := DestUris}},
                       ExpectedSrcUris, ExpectedDestUris) ->
    ?assertEqual(ExpectedSrcUris, rabbit_shovel_util:deobfuscate_uris(SrcUris)),
    ?assertEqual(ExpectedDestUris, rabbit_shovel_util:deobfuscate_uris(DestUris)).

%% Regression: an empty argument list resolved to a lower, non-existent arity
%% once the connection context was appended.
source_without_declarations_is_arity_compatible(_Config) ->
    [begin
         Endpoint = {source, [{queue, <<"a-queue">>}]},
         MFA = rabbit_shovel_util:decl_fun(Mod, Endpoint),
         ?assertEqual({Mod, decl_fun, [[]]}, MFA),
         assert_applicable(MFA)
     end || Mod <- [rabbit_amqp091_shovel, rabbit_local_shovel]],
    ok.

source_with_declarations_is_arity_compatible(_Config) ->
    [begin
         Endpoint = {source, [{queue, <<"a-queue">>},
                              {declarations,
                               [{'queue.declare', [{queue, <<"a-queue">>}]}]}]},
         MFA = rabbit_shovel_util:decl_fun(Mod, Endpoint),
         ?assertMatch({Mod, decl_fun, [[_ | _]]}, MFA),
         assert_applicable(MFA)
     end || Mod <- [rabbit_amqp091_shovel, rabbit_local_shovel]],
    ok.

destination_without_declarations_is_arity_compatible(_Config) ->
    [begin
         Endpoint = {destination, []},
         MFA = rabbit_shovel_util:decl_fun(Mod, Endpoint),
         ?assertEqual({Mod, decl_fun, [[]]}, MFA),
         assert_applicable(MFA)
     end || Mod <- [rabbit_amqp091_shovel, rabbit_local_shovel]],
    ok.

%% Any declaration count resolves to a single-argument MFA, exported at the
%% arity the backend applies.
prop_source_decl_fun_is_arity_compatible(_Config) ->
    Prop =
        ?FORALL({Mod, Decls},
                {oneof([rabbit_amqp091_shovel, rabbit_local_shovel]),
                 declarations_gen()},
                begin
                    Endpoint = {source, [{queue, <<"a-queue">>},
                                         {declarations, Decls}]},
                    {M, F, Args} = rabbit_shovel_util:decl_fun(Mod, Endpoint),
                    length(Args) =:= 1 andalso
                        lists:member({F, length(Args) + context_arity(M)},
                                     M:module_info(exports))
                end),
    ?assert(proper:quickcheck(Prop, [{numtests, 200}, {to_file, user}])).

declarations_gen() ->
    list(oneof([{'queue.declare',    [{queue, non_empty_binary()}]},
                {'exchange.declare', [{exchange, non_empty_binary()}]}])).

non_empty_binary() ->
    ?LET(Chars, non_empty(list(range($a, $z))), list_to_binary(Chars)).

%% Backends append their connection context, so the target must be exported at
%% length(Args) plus that context's arity.
assert_applicable({Mod, Fun, Args}) ->
    Arity = length(Args) + context_arity(Mod),
    ?assert(lists:member({Fun, Arity}, Mod:module_info(exports))).

context_arity(rabbit_amqp091_shovel) -> 2;
context_arity(rabbit_local_shovel)   -> 3.
