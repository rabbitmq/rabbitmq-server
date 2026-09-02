%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(unit_queue_type_discover_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_ct_broker_helpers, [rpc/5]).

all() ->
    [
     discover_raises_protocol_error_for_unknown_type,
     declare_with_unknown_queue_type_closes_channel_cleanly
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 1}
    ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, Testcase).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

%% `discover/1` must turn an unknown type into a protocol error, not let a
%% `{error, not_found}` result badmatch the caller. `rabbit_ct_broker_helpers`
%% RPCs via `erpc`, which re-raises a remote `exit/1` locally as
%% `exit({exception, Reason})`.
discover_raises_protocol_error_for_unknown_type(Config) ->
    ?assertExit(
       {exception, #amqp_error{name = precondition_failed}},
       rpc(Config, 0, rabbit_queue_type, discover, [<<"totally-bogus-type">>])).

%% End-to-end: declaring a queue with an unknown `x-queue-type` must close
%% only that channel with a clean 406 precondition_failed, and must not take
%% down the connection or crash anything else reachable from it.
declare_with_unknown_queue_type_closes_channel_cleanly(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    expect_shutdown_due_to_precondition_failed(
      fun () ->
              amqp_channel:call(
                Ch, #'queue.declare'{
                       queue     = <<>>,
                       arguments = [{<<"x-queue-type">>, longstr,
                                     <<"totally-bogus-type">>}]})
      end),
    %% The connection, and a fresh channel on it, must still be usable.
    {ok, Ch2} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch2, #'queue.declare'{queue = <<>>, exclusive = true}),
    #'queue.delete_ok'{} =
        amqp_channel:call(Ch2, #'queue.delete'{queue = Q}),
    amqp_channel:close(Ch2),
    amqp_connection:close(Conn).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

expect_shutdown_due_to_precondition_failed(Thunk) ->
    try
        Thunk(),
        ct:fail("expected the channel to be closed with precondition_failed")
    catch _:{{shutdown, {server_initiated_close, 406, _}}, _} ->
              ok
    end.
