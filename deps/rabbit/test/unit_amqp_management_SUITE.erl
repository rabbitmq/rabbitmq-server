%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Unit and property tests for rabbit_amqp_management. rabbit_access_control is
%% mocked so that argument permission checks can be exercised without a broker.
-module(unit_amqp_management_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(VHOST, <<"test vhost">>).
-define(ITERATIONS, 500).

all() ->
    [
     absent_argument_skips_checks,
     invalid_argument_type_rejected,
     both_permissions_granted,
     source_read_denied_skips_target_write,
     target_write_denied,
     wrapper_uses_matching_argument_key,
     prop_read_then_write
    ].

init_per_testcase(_Testcase, Config) ->
    ok = meck:new(rabbit_access_control, [no_link]),
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok = meck:unload(rabbit_access_control).

absent_argument_skips_checks(_Config) ->
    ok = mock_permitted([]),
    Cache = [sentinel],
    ?assertEqual(
       Cache,
       rabbit_amqp_management:check_routing_arg(
         source(<<"x.1">>), [], <<"alternate-exchange">>, user(), Cache)),
    ?assertEqual(0, meck:num_calls(rabbit_access_control, check_resource_access, ['_', '_', '_', '_'])).

invalid_argument_type_rejected(_Config) ->
    ok = mock_permitted([]),
    Args = [{<<"alternate-exchange">>, long, 42}],
    ?assertThrow(
       {rabbit_amqp_management, <<"400">>, _},
       rabbit_amqp_management:check_routing_arg(
         source(<<"x.1">>), Args, <<"alternate-exchange">>, user(), [])).

both_permissions_granted(_Config) ->
    Source = source(<<"x.1">>),
    Target = target(<<"ae.1">>),
    ok = mock_permitted([{Source, read}, {Target, write}]),
    Args = [{<<"alternate-exchange">>, longstr, <<"ae.1">>}],
    Cache = rabbit_amqp_management:check_routing_arg(
              Source, Args, <<"alternate-exchange">>, user(), []),
    ?assert(lists:member({Source, read}, Cache)),
    ?assert(lists:member({Target, write}, Cache)),
    ?assertEqual(1, num_calls(Source, read)),
    ?assertEqual(1, num_calls(Target, write)).

source_read_denied_skips_target_write(_Config) ->
    Source = source(<<"x.1">>),
    Target = target(<<"ae.1">>),
    ok = mock_permitted([{Target, write}]),
    Args = [{<<"alternate-exchange">>, longstr, <<"ae.1">>}],
    ?assertExit(
       #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS},
       rabbit_amqp_management:check_routing_arg(
         Source, Args, <<"alternate-exchange">>, user(), [])),
    ?assertEqual(1, num_calls(Source, read)),
    ?assertEqual(0, num_calls(Target, write)).

target_write_denied(_Config) ->
    Source = source(<<"x.1">>),
    Target = target(<<"ae.1">>),
    ok = mock_permitted([{Source, read}]),
    Args = [{<<"alternate-exchange">>, longstr, <<"ae.1">>}],
    ?assertExit(
       #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS},
       rabbit_amqp_management:check_routing_arg(
         Source, Args, <<"alternate-exchange">>, user(), [])),
    ?assertEqual(1, num_calls(Source, read)),
    ?assertEqual(1, num_calls(Target, write)).

wrapper_uses_matching_argument_key(_Config) ->
    ok = mock_permitted([]),
    Source = source(<<"x.1">>),
    DlxArg = [{<<"x-dead-letter-exchange">>, longstr, <<"dlx.1">>}],
    AeArg = [{<<"alternate-exchange">>, longstr, <<"ae.1">>}],
    ?assertEqual(
       [], rabbit_amqp_management:check_alternate_exchange(Source, DlxArg, user(), [])),
    ?assertEqual(
       [], rabbit_amqp_management:check_dead_letter_exchange(Source, AeArg, user(), [])),
    ?assertEqual(0, meck:num_calls(rabbit_access_control, check_resource_access, ['_', '_', '_', '_'])).

%%%===================================================================
%%% Property
%%%===================================================================

%% Success only when both are granted, and the target write is attempted only
%% after the source read passes.
prop_read_then_write(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_read_then_write_/0, [], ?ITERATIONS).

prop_read_then_write_() ->
    ?FORALL(
       {ReadOk, WriteOk, SrcName, TgtName, Key},
       {boolean(), boolean(), resource_name(), resource_name(), argument_key()},
       begin
           Source = source(SrcName),
           Target = target(TgtName),
           Permitted = [{Source, read} || ReadOk] ++ [{Target, write} || WriteOk],
           ok = mock_permitted(Permitted),
           ok = meck:reset(rabbit_access_control),
           Args = [{Key, longstr, TgtName}],
           Result = (catch rabbit_amqp_management:check_routing_arg(
                             Source, Args, Key, user(), [])),
           Reads = num_calls(Source, read),
           Writes = num_calls(Target, write),
           case {ReadOk, WriteOk} of
               {true, true} ->
                   is_list(Result)
                       andalso lists:member({Source, read}, Result)
                       andalso lists:member({Target, write}, Result)
                       andalso Reads =:= 1 andalso Writes =:= 1;
               {false, _} ->
                   is_unauthorized(Result)
                       andalso Reads =:= 1 andalso Writes =:= 0;
               {true, false} ->
                   is_unauthorized(Result)
                       andalso Reads =:= 1 andalso Writes =:= 1
           end
       end).

resource_name() ->
    ?LET(N, non_empty(list(choose($a, $z))), list_to_binary(N)).

argument_key() ->
    oneof([<<"alternate-exchange">>, <<"x-dead-letter-exchange">>]).

%%%===================================================================
%%% Helpers
%%%===================================================================

mock_permitted(Permitted) ->
    ok = meck:expect(
           rabbit_access_control, check_resource_access,
           fun(_User, Resource, Perm, _Context) ->
                   case lists:member({Resource, Perm}, Permitted) of
                       true ->
                           ok;
                       false ->
                           exit(#amqp_error{name = access_refused,
                                            explanation = "access refused"})
                   end
           end).

num_calls(Resource, Perm) ->
    meck:num_calls(rabbit_access_control, check_resource_access,
                   ['_', Resource, Perm, '_']).

is_unauthorized({'EXIT', #'v1_0.error'{
                            condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS}}) ->
    true;
is_unauthorized(_) ->
    false.

source(Name) ->
    #resource{virtual_host = ?VHOST, kind = exchange, name = Name}.

target(Name) ->
    #resource{virtual_host = ?VHOST, kind = exchange, name = Name}.

user() ->
    #user{username = <<"test user">>}.
