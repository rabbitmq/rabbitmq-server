%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([nowarn_export_all,
          export_all]).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       authorization_failure_disclosure
      ]
     }].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(rabbitmq_amqp_client),
    start_http_auth_server(),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    ok = stop_http_auth_server(),
    Config.

init_per_group(_Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Suffix}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                [
                 {rabbit, [{auth_backends, [rabbit_auth_backend_http]}]},
                 {rabbitmq_auth_backend_http, [{authorization_failure_disclosure, true}]}
                ]),
    rabbit_ct_helpers:run_setup_steps(
      Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

start_http_auth_server() ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(cowboy),
    Dispatch = cowboy_router:compile([{'_', [{'_', auth_http_server, #{}}]}]),
    {ok, _} = cowboy:start_clear(auth_http_listener,
                                 [{port, 8000}],
                                 #{env => #{dispatch => Dispatch}}).

stop_http_auth_server() ->
    cowboy:stop_listener(auth_http_listener).

authorization_failure_disclosure(Config) ->
    OpnConf = amqp_connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"pair">>),

    QName = <<"my-queue">>,
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),

    XName = <<"my-exchange">>,
    {error, {session_ended, Error}} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    %% We expect to receive the full denial reason as sent by the HTTP auth server.
    ExpectedReason = <<"configure access to exchange 'my-exchange' in vhost '/' refused for user "
                       "'guest' by backend rabbit_auth_backend_http: Creating or deleting "
                       "exchanges is forbidden for all client apps ❌"/utf8>>,
    ?assertEqual(#'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                               description = {utf8, ExpectedReason}},
                 Error),

    ok = amqp10_client:close_connection(Connection).

amqp_connection_config(Config) ->
    Host = proplists:get_value(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.
