%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(link_startup_shutdown_SUITE).

%% Verifies that queue federation links start and stop in response to
%% policy changes and upstream removal. Each test case uses an isolated
%% pair of virtual hosts on a single node.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(QUEUE_NAME, <<"fed.q">>).
-define(POLICY_NAME, <<"federate-queues">>).
-define(UPSTREAM_NAME, <<"the-upstream">>).

all() ->
    [
     {group, link_behavior}
    ].

groups() ->
    [
     {link_behavior, [], [
                          link_stops_after_policy_disables_federation,
                          link_starts_after_policy_enables_federation,
                          link_stops_after_policy_removal,
                          link_stops_after_upstream_removal
                         ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(link_behavior, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps());
init_per_group(_, Config) ->
    Config.

end_per_group(link_behavior, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    N = erlang:unique_integer([positive]),
    NBin = integer_to_binary(N),
    DownVHost = <<"lss.", NBin/binary, ".downstream">>,
    UpVHost = <<"lss.", NBin/binary, ".upstream">>,
    setup_vhosts(Config1, DownVHost, UpVHost),
    rabbit_ct_helpers:set_config(Config1, [
        {down_vhost, DownVHost},
        {up_vhost, UpVHost}
    ]).

end_per_testcase(Testcase, Config) ->
    DownVHost = ?config(down_vhost, Config),
    UpVHost = ?config(up_vhost, Config),
    cleanup_vhosts(Config, DownVHost, UpVHost),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

link_stops_after_policy_disables_federation(Config) ->
    declare_queues(Config),
    setup_upstream(Config),
    setup_federation_policy(Config),
    await_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),

    %% Replace the federation policy with a non-federation one
    set_non_federation_policy(Config),
    await_no_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),
    ok.

link_starts_after_policy_enables_federation(Config) ->
    declare_queues(Config),
    setup_upstream(Config),
    set_non_federation_policy(Config),

    timer:sleep(3000),
    assert_no_running_link_in_vhost(Config, ?config(down_vhost, Config)),

    %% Update the policy to add federation keys
    setup_federation_policy(Config),
    await_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),
    ok.

link_stops_after_policy_removal(Config) ->
    declare_queues(Config),
    setup_upstream(Config),
    setup_federation_policy(Config),
    await_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),

    delete_policy(Config),
    await_no_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),
    ok.

link_stops_after_upstream_removal(Config) ->
    declare_queues(Config),
    setup_upstream(Config),
    setup_federation_policy(Config),
    await_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),

    delete_upstream(Config),
    await_no_running_link_in_vhost(Config, ?config(down_vhost, Config), 30000),
    ok.

%% -------------------------------------------------------------------
%% Vhost helpers
%% -------------------------------------------------------------------

setup_vhosts(Config, DownVHost, UpVHost) ->
    lists:foreach(
      fun(VHost) ->
              catch rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
              ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
              ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost)
      end, [DownVHost, UpVHost]).

cleanup_vhosts(Config, DownVHost, UpVHost) ->
    catch rabbit_ct_broker_helpers:delete_vhost(Config, DownVHost),
    catch rabbit_ct_broker_helpers:delete_vhost(Config, UpVHost),
    ok.

%% -------------------------------------------------------------------
%% Resource declaration helpers
%% -------------------------------------------------------------------

declare_queues(Config) ->
    DownVHost = ?config(down_vhost, Config),
    UpVHost = ?config(up_vhost, Config),
    lists:foreach(
      fun(VHost) ->
              {Conn, Ch} = open_channel_to_vhost(Config, VHost),
              try
                  #'queue.declare_ok'{} =
                      amqp_channel:call(Ch, #'queue.declare'{
                                               queue = ?QUEUE_NAME,
                                               durable = true,
                                               arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]})
              after
                  close_connection_and_channel(Conn, Ch)
              end
      end, [DownVHost, UpVHost]).

%% -------------------------------------------------------------------
%% Channel helpers
%% -------------------------------------------------------------------

open_channel_to_vhost(Config, VHost) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection_direct(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {Conn, Ch}.

close_connection_and_channel(Conn, Ch) ->
    catch amqp_channel:close(Ch),
    catch amqp_connection:close(Conn).

%% -------------------------------------------------------------------
%% URI helpers
%% -------------------------------------------------------------------

uri_for_vhost(BaseUri, VHost) ->
    EncodedVHost = binary:replace(VHost, <<".">>, <<"%2e">>, [global]),
    <<BaseUri/binary, "/", EncodedVHost/binary>>.

%% -------------------------------------------------------------------
%% Federation setup helpers
%% -------------------------------------------------------------------

setup_upstream(Config) ->
    DownVHost = ?config(down_vhost, Config),
    UpVHost = ?config(up_vhost, Config),
    BaseUri = rabbit_ct_broker_helpers:node_uri(Config, 0),
    UpstreamUri = uri_for_vhost(BaseUri, UpVHost),
    rabbit_ct_broker_helpers:set_parameter(
      Config, 0, DownVHost,
      <<"federation-upstream">>, ?UPSTREAM_NAME,
      [{<<"uri">>, UpstreamUri}]).

setup_federation_policy(Config) ->
    DownVHost = ?config(down_vhost, Config),
    rabbit_ct_broker_helpers:set_policy_in_vhost(
      Config, 0, DownVHost,
      ?POLICY_NAME, <<"^fed\\.">>, <<"all">>,
      [{<<"federation-upstream-set">>, <<"all">>}]).

set_non_federation_policy(Config) ->
    DownVHost = ?config(down_vhost, Config),
    rabbit_ct_broker_helpers:set_policy_in_vhost(
      Config, 0, DownVHost,
      ?POLICY_NAME, <<"^fed\\.">>, <<"all">>,
      [{<<"max-length">>, 1000}]).

delete_policy(Config) ->
    DownVHost = ?config(down_vhost, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_policy, delete,
           [DownVHost, ?POLICY_NAME, <<"acting-user">>]).

delete_upstream(Config) ->
    DownVHost = ?config(down_vhost, Config),
    rabbit_ct_broker_helpers:clear_parameter(
      Config, 0, DownVHost,
      <<"federation-upstream">>, ?UPSTREAM_NAME).

%% -------------------------------------------------------------------
%% Federation status helpers
%% -------------------------------------------------------------------

await_running_link_in_vhost(Config, VHost, Timeout) ->
    rabbit_ct_helpers:await_condition_ignoring_exceptions(
      fun() ->
              count_running_links_in_vhost(Config, VHost) > 0
      end, Timeout).

await_no_running_link_in_vhost(Config, VHost, Timeout) ->
    rabbit_ct_helpers:await_condition_ignoring_exceptions(
      fun() ->
              count_running_links_in_vhost(Config, VHost) =:= 0
      end, Timeout).

assert_no_running_link_in_vhost(Config, VHost) ->
    ?assertEqual(0, count_running_links_in_vhost(Config, VHost)).

count_running_links_in_vhost(Config, VHost) ->
    Status = rabbit_ct_broker_helpers:rpc(
               Config, 0, rabbit_federation_status, status, []),
    case Status of
        {badrpc, _} -> 0;
        List ->
            length([S || S <- List,
                         proplists:get_value(vhost, S) =:= VHost,
                         proplists:get_value(status, S) =:= running])
    end.
