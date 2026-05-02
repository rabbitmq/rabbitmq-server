%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(unit_stream_arg_validation_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, unit},
     {group, integration}
    ].

groups() ->
    [
     {unit, [parallel],
      [check_max_age_empty_string,
       check_max_age_no_leading_digits,
       check_max_age_zero_count,
       check_max_age_valid_units,
       check_max_age_invalid_unit]},
     {integration, [parallel],
      [filter_size_lower_bound,
       filter_size_upper_bound,
       filter_size_below_lower_bound,
       filter_size_zero,
       filter_size_float,
       filter_size_on_classic_queue]}
    ].

%% -------------------------------------------------------------------
%% Suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unit, Config) ->
    Config;
init_per_group(integration, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, integration},
                                            {rmq_nodes_count, 1}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(unit, _Config) ->
    ok;
end_per_group(integration, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Q = rabbit_data_coercion:to_binary(Testcase),
    rabbit_ct_helpers:set_config(Config1, [{queue_name, Q}]).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Unit tests: rabbit_amqqueue:check_max_age/1.
%% Pure function; no broker required.
%% -------------------------------------------------------------------

check_max_age_empty_string(_Config) ->
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<>>)).

check_max_age_no_leading_digits(_Config) ->
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"D">>)),
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"M">>)),
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"Year">>)),
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"-1D">>)).

check_max_age_zero_count(_Config) ->
    lists:foreach(
      fun(Unit) ->
              ?assertEqual({error, invalid_max_age},
                           rabbit_amqqueue:check_max_age(list_to_binary("0" ++ Unit)))
      end,
      ["Y", "M", "D", "h", "m", "s"]).

check_max_age_valid_units(_Config) ->
    lists:foreach(
      fun(Unit) ->
              Result = rabbit_amqqueue:check_max_age(list_to_binary("10" ++ Unit)),
              ?assert(is_integer(Result)),
              ?assert(Result > 0)
      end,
      ["Y", "M", "D", "h", "m", "s"]).

check_max_age_invalid_unit(_Config) ->
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"1A">>)),
    %% Lowercase y is not a valid unit; only uppercase Y is
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"1y">>)),
    %% Multiple trailing characters are not a valid unit
    ?assertEqual({error, invalid_max_age}, rabbit_amqqueue:check_max_age(<<"1D2">>)).

%% -------------------------------------------------------------------
%% Integration tests: x-stream-filter-size-bytes validation.
%% These tests require a running broker.
%% -------------------------------------------------------------------

filter_size_lower_bound(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare_stream(Config, Server, Q,
                                [{<<"x-stream-filter-size-bytes">>, long, 16}])),
    delete_queue(Config, Q).

filter_size_upper_bound(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare_stream(Config, Server, Q,
                                [{<<"x-stream-filter-size-bytes">>, long, 255}])),
    delete_queue(Config, Q).

filter_size_below_lower_bound(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),
    ExpectedError = <<"PRECONDITION_FAILED - Invalid value for x-stream-filter-size-bytes">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare_stream(Config, Server, Q,
                      [{<<"x-stream-filter-size-bytes">>, long, 15}])).

filter_size_zero(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),
    %% 0 satisfies `check_non_neg_int_arg/2` but still falls below the valid range
    ExpectedError = <<"PRECONDITION_FAILED - Invalid value for x-stream-filter-size-bytes">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare_stream(Config, Server, Q,
                      [{<<"x-stream-filter-size-bytes">>, long, 0}])).

filter_size_float(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),
    %% Before the fix, a float in [16.0, 255.0] was silently accepted because Erlang
    %% compares floats and integers numerically, bypassing the `x-stream-filter-size-bytes`
    %% range guard.
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare_stream(Config, Server, Q,
                      [{<<"x-stream-filter-size-bytes">>, float, 32.0}])).

filter_size_on_classic_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),
    %% `x-stream-filter-size-bytes` is now in stream capabilities and therefore in the
    %% global queue_arguments union, so classic queues must reject it.
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare_classic(Config, Server, Q,
                       [{<<"x-stream-filter-size-bytes">>, long, 32}])).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

declare_stream(Config, Server, Q, ExtraArgs) ->
    call_declare(Config, Server,
                 #'queue.declare'{queue     = Q,
                                  durable   = true,
                                  arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}
                                               | ExtraArgs]}).

declare_classic(Config, Server, Q, ExtraArgs) ->
    call_declare(Config, Server,
                 #'queue.declare'{queue     = Q,
                                  durable   = true,
                                  arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}
                                               | ExtraArgs]}).

call_declare(Config, Server, Cmd) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Reply = amqp_channel:call(Ch, Cmd),
    rabbit_ct_client_helpers:close_channel(Ch),
    Reply.

delete_queue(Config, Q) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, do_delete_queue, [Q]).

do_delete_queue(Name) ->
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.
