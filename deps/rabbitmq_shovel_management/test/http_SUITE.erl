%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [http_get/3, http_get/5, http_put/4, http_post/4, http_delete/3, http_delete/4, http_get_fails/2]).
-import(rabbit_ct_helpers, [await_condition/2]).

-compile(export_all).

all() ->
    [
     {group, dynamic_shovels},
     {group, static_shovels},
     {group, plugin_management}
    ].

groups() ->
    [
     {dynamic_shovels, [], [
                  start_and_list_a_dynamic_amqp10_shovel,
                  start_and_get_a_dynamic_amqp10_shovel,
                  create_and_delete_a_dynamic_shovel_that_successfully_connects,
                  create_and_delete_a_dynamic_shovel_that_fails_to_connect
                 ]},

    {static_shovels, [], [
                    start_static_shovels
                 ]},

    {plugin_management, [], [
                    dynamic_plugin_enable_disable
                  ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(static_shovels, Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1, [
        fun configure_shovels/1,
        fun start_inets/1
    ] ++ rabbit_ct_broker_helpers:setup_steps() ++
         rabbit_ct_client_helpers:setup_steps());
init_per_group(_Group, Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1, [
        fun start_inets/1
      ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(start_static_shovels, Config) ->
    http_delete(Config, "/vhosts/v", ?NO_CONTENT),
    http_delete(Config, "/users/admin", ?NO_CONTENT),
    http_delete(Config, "/users/mon", ?NO_CONTENT),

    remove_all_dynamic_shovels(Config, <<"/">>),

    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    remove_all_dynamic_shovels(Config, <<"/">>),
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).


init_per_testcase(create_and_delete_a_dynamic_shovel_that_fails_to_connect = Testcase, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            rabbit_ct_helpers:testcase_started(Config, Testcase)
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

configure_shovels(Config) ->
    rabbit_ct_helpers:merge_app_env(Config,
      {rabbitmq_shovel, [
          {shovels,
            [{'my-static',
                [{sources, [
                      {broker, "amqp://"},
                      {declarations, [
                          {'queue.declare', [{queue, <<"static">>}]}]}
                    ]},
                  {destinations, [{broker, "amqp://"}]},
                  {queue, <<"static">>},
                  {publish_fields, [
                      {exchange, <<"">>},
                      {routing_key, <<"static2">>}
                    ]}
                ]}
            ]}
        ]}).

start_inets(Config) ->
    _ = application:start(inets),
    Config.

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

start_and_list_a_dynamic_amqp10_shovel(Config) ->
    remove_all_dynamic_shovels(Config, <<"/">>),
    Name = <<"dynamic-amqp10-await-startup-1">>,
    ID = {<<"/">>, Name},
    await_shovel_removed(Config, ID),

    declare_shovel(Config, Name),
    await_shovel_startup(Config, ID),
    Shovels = list_shovels(Config),
    ?assert(lists:any(
        fun(M) ->
            maps:get(name, M) =:= Name
        end, Shovels)),
    delete_shovel(Config, <<"dynamic-amqp10-await-startup-1">>),

    ok.

start_and_get_a_dynamic_amqp10_shovel(Config) ->
    remove_all_dynamic_shovels(Config, <<"/">>),
    Name = <<"dynamic-amqp10-get-shovel-1">>,
    ID = {<<"/">>, Name},
    await_shovel_removed(Config, ID),

    declare_shovel(Config, Name),
    await_shovel_startup(Config, ID),
    Sh = get_shovel(Config, Name),
    ?assertEqual(Name, maps:get(name, Sh)),
    delete_shovel(Config, <<"dynamic-amqp10-await-startup-1">>),

    ok.

-define(StaticPattern, #{name := <<"my-static">>,
                         type := <<"static">>}).

-define(Dynamic1Pattern, #{name  := <<"my-dynamic">>,
                           vhost := <<"/">>,
                           type  := <<"dynamic">>}).

-define(Dynamic2Pattern, #{name  := <<"my-dynamic">>,
                           vhost := <<"v">>,
                           type  := <<"dynamic">>}).

start_static_shovels(Config) ->
    http_put(Config, "/users/admin",
	     #{password => <<"admin">>, tags => <<"administrator">>}, ?CREATED),
    http_put(Config, "/users/mon",
	     #{password => <<"mon">>, tags => <<"monitoring">>}, ?CREATED),
    http_put(Config, "/vhosts/v", none, ?CREATED),
    Perms = #{configure => <<".*">>,
              write     => <<".*">>,
              read      => <<".*">>},
    http_put(Config, "/permissions/v/guest",  Perms, ?NO_CONTENT),
    http_put(Config, "/permissions/v/admin",  Perms, ?CREATED),
    http_put(Config, "/permissions/v/mon",    Perms, ?CREATED),

    [http_put(Config, "/parameters/shovel/" ++ V ++ "/my-dynamic",
              #{value => #{'src-protocol' => <<"amqp091">>,
                           'src-uri'    => <<"amqp://">>,
                           'src-queue'  => <<"test">>,
                           'dest-protocol' => <<"amqp091">>,
                           'dest-uri'   => <<"amqp://">>,
                           'dest-queue' => <<"test2">>}}, ?CREATED)
     || V <- ["%2f", "v"]],

     [http_put(Config, "/parameters/shovel/" ++ V ++ "/my-dynamic",
              #{value => #{'src-protocol' => <<"amqp091">>,
                           'src-uri'    => <<"amqp://">>,
                           'src-queue'  => <<"test">>,
                           'dest-protocol' => <<"amqp091">>,
                           'dest-uri'   => <<"amqp://">>,
                           'dest-queue' => list_to_binary(
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                                           "test2qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq")}},
               ?BAD_REQUEST)
     || V <- ["%2f", "v"]],

    ?assertMatch([?StaticPattern, ?Dynamic1Pattern, ?Dynamic2Pattern],
		 http_get(Config, "/shovels",     "guest", "guest", ?OK)),
    ?assertMatch([?Dynamic1Pattern],
		 http_get(Config, "/shovels/%2f", "guest", "guest", ?OK)),
    ?assertMatch([?Dynamic2Pattern],
		 http_get(Config, "/shovels/v",   "guest", "guest", ?OK)),

    ?assertMatch([?StaticPattern, ?Dynamic2Pattern],
		 http_get(Config, "/shovels",     "admin", "admin", ?OK)),
    ?assertMatch([],
		 http_get(Config, "/shovels/%2f", "admin", "admin", ?OK)),
    ?assertMatch([?Dynamic2Pattern],
		 http_get(Config, "/shovels/v",   "admin", "admin", ?OK)),

    ?assertMatch([?Dynamic2Pattern],
		 http_get(Config, "/shovels",     "mon",   "mon", ?OK)),
    ?assertMatch([],
		 http_get(Config, "/shovels/%2f", "mon",   "mon", ?OK)),
    ?assertMatch([?Dynamic2Pattern],
		 http_get(Config, "/shovels/v",   "mon",   "mon", ?OK)),
    ok.

create_and_delete_a_dynamic_shovel_that_successfully_connects(Config) ->
    Port = integer_to_binary(
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)),

    remove_all_dynamic_shovels(Config, <<"/">>),
    Name = <<"dynamic-amqp10-to-delete-1">>,
    ID = {<<"/">>, Name},
    await_shovel_removed(Config, ID),

    http_put(Config, "/parameters/shovel/%2f/dynamic-amqp10-to-delete-1",
        #{value => #{'src-protocol' => <<"amqp10">>,
            'src-uri' => <<"amqp://localhost:", Port/binary>>,
            'src-address'  => <<"test">>,
            'dest-protocol' => <<"amqp10">>,
            'dest-uri' => <<"amqp://localhost:", Port/binary>>,
            'dest-address' => <<"test2">>,
            'dest-properties' => #{},
            'dest-application-properties' => #{},
            'dest-message-annotations' => #{}}}, ?CREATED),

    await_shovel_startup(Config, ID),
    timer:sleep(3_000),
    delete_shovel(Config, Name),
    await_shovel_removed(Config, ID).

create_and_delete_a_dynamic_shovel_that_fails_to_connect(Config) ->
    Port = integer_to_binary(
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)),

    remove_all_dynamic_shovels(Config, <<"/">>),
    Name = <<"dynamic-amqp10-to-delete-2">>,
    ID = {<<"/">>, Name},
    await_shovel_removed(Config, ID),

    http_put(Config, "/parameters/shovel/%2f/dynamic-amqp10-to-delete-2",
        #{value => #{'src-protocol' => <<"amqp10">>,
            'src-uri' => <<"amqp://non-existing-hostname.lolz.wut:", Port/binary>>,
            'src-address'  => <<"test">>,
            'dest-protocol' => <<"amqp10">>,
            'dest-uri' => <<"amqp://non-existing-hostname.lolz.wut:", Port/binary>>,
            'dest-address' => <<"test2">>,
            'dest-properties' => #{},
            'dest-application-properties' => #{},
            'dest-message-annotations' => #{}}}, ?CREATED),

    await_shovel_startup(Config, ID),
    timer:sleep(3_000),
    delete_shovel(Config, Name),
    await_shovel_removed(Config, ID).

dynamic_plugin_enable_disable(Config) ->
    http_get(Config, "/shovels", ?OK),
    rabbit_ct_broker_helpers:disable_plugin(Config, 0,
      "rabbitmq_shovel_management"),
    http_get(Config, "/shovels", ?NOT_FOUND),
    http_get(Config, "/overview", ?OK),
    rabbit_ct_broker_helpers:disable_plugin(Config, 0,
      "rabbitmq_management"),
    http_get_fails(Config, "/shovels"),
    http_get_fails(Config, "/overview"),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0,
      "rabbitmq_management"),
    http_get(Config, "/shovels", ?NOT_FOUND),
    http_get(Config, "/overview", ?OK),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0,
      "rabbitmq_shovel_management"),
    http_get(Config, "/shovels", ?OK),
    http_get(Config, "/overview", ?OK),
    passed.

%%
%% Implementation
%%

cleanup(L) when is_list(L) ->
    [cleanup(I) || I <- L];
cleanup(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K, latin1) => cleanup(V)}
    end, #{}, M);
cleanup(I) ->
    I.

auth_header(Username, Password) ->
    {"Authorization",
     "Basic " ++ binary_to_list(base64:encode(Username ++ ":" ++ Password))}.

assert_list(Exp, Act) ->
    _ = [assert_item(ExpI, ActI) || {ExpI, ActI} <- lists:zip(Exp, Act)],
    ok.

assert_item(ExpI, ActI) ->
    ExpI = maps:with(maps:keys(ExpI), ActI),
    ok.

list_shovels(Config) ->
    list_shovels(Config, "%2F").

list_shovels(Config, VirtualHost) ->
    Path = io_lib:format("/shovels/~s", [VirtualHost]),
    http_get(Config, Path, ?OK).

get_shovel(Config, Name) ->
    get_shovel(Config, "%2F", Name).

get_shovel(Config, VirtualHost, Name) ->
    Path = io_lib:format("/shovels/vhost/~s/~s", [VirtualHost, Name]),
    http_get(Config, Path, ?OK).

delete_shovel(Config, Name) ->
    delete_shovel(Config, "%2F", Name).

delete_shovel(Config, VirtualHost, Name) ->
    Path = io_lib:format("/shovels/vhost/~s/~s", [VirtualHost, Name]),
    http_delete(Config, Path, ?NO_CONTENT).

remove_all_dynamic_shovels(Config, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
        rabbit_runtime_parameters, clear_vhost, [VHost, <<"CT tests">>]).

declare_shovel(Config, Name) ->
    Port = integer_to_binary(
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)),
    http_put(Config, io_lib:format("/parameters/shovel/%2f/~ts", [Name]),
        #{
            value => #{
                'src-protocol' => <<"amqp10">>,
                'src-uri' => <<"amqp://localhost:", Port/binary>>,
                'src-address'  => <<"test">>,
                'dest-protocol' => <<"amqp10">>,
                'dest-uri' => <<"amqp://localhost:", Port/binary>>,
                'dest-address' => <<"test2">>,
                'dest-properties' => #{},
                'dest-application-properties' => #{},
                'dest-message-annotations' => #{}}
        }, ?CREATED).

await_shovel_startup(Config, Name) ->
    await_shovel_startup(Config, Name, 10_000).

await_shovel_startup(Config, Name, Timeout) ->
    await_condition(
        fun() ->
            does_shovel_exist(Config, Name)
        end, Timeout).

await_shovel_removed(Config, Name) ->
    await_shovel_removed(Config, Name, 10_000).

await_shovel_removed(Config, Name, Timeout) ->
    await_condition(
        fun() ->
            not does_shovel_exist(Config, Name)
        end, Timeout).

lookup_shovel_status(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status, lookup, [Name]).

does_shovel_exist(Config, Name) ->
    case lookup_shovel_status(Config, Name) of
        not_found -> false;
        _Found    -> true
    end.