%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               amqp10_shovels,
                               shovels,
                               dynamic_plugin_enable_disable
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1, [
        fun configure_shovels/1,
        fun start_inets/1
      ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(amqp10_shovels = Testcase, Config) ->
    http_delete(Config,  "/parameters/shovel/%2f/my-dynamic-amqp10", ?NO_CONTENT),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(shovels = Testcase, Config) ->
    http_delete(Config, "/vhosts/v", ?NO_CONTENT),
    http_delete(Config, "/users/admin", ?NO_CONTENT),
    http_delete(Config, "/users/mon", ?NO_CONTENT),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
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
    ok = application:start(inets),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

amqp10_shovels(Config) ->
    Port = integer_to_binary(
             rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp)),
    http_put(Config, "/parameters/shovel/%2f/my-dynamic-amqp10",
                  #{value => #{'src-protocol' => <<"amqp10">>,
                               'src-uri' => <<"amqp://localhost:", Port/binary>>,
                               'src-address'  => <<"test">>,
                               'dest-protocol' => <<"amqp10">>,
                               'dest-uri' => <<"amqp://localhost:", Port/binary>>,
                               'dest-address' => <<"test2">>,
                               'dest-properties' => #{},
                               'dest-application-properties' => #{},
                               'dest-message-annotations' => #{}}}, ?CREATED),
    % sleep to give the shovel time to emit a full report
    % that includes the protocols used.
    wait_until(fun () ->
		       case lists:sort(fun(#{name := AName}, #{name := BName}) ->
					       AName < BName
				       end,
				       http_get(Config, "/shovels", "guest", "guest", ?OK))
		       of
			   [#{name := <<"my-dynamic-amqp10">>,
			      src_protocol := <<"amqp10">>,
			      dest_protocol := <<"amqp10">>,
			      type := <<"dynamic">>},
			    #{name := <<"my-static">>,
			      src_protocol := <<"amqp091">>,
			      dest_protocol := <<"amqp091">>,
			      type := <<"static">>}] ->
			       true;
			   _ ->
			       false
		       end
	       end, 20),
    ok.


-define(StaticPattern, #{name := <<"my-static">>,
                         type := <<"static">>}).

-define(Dynamic1Pattern, #{name  := <<"my-dynamic">>,
                           vhost := <<"/">>,
                           type  := <<"dynamic">>}).

-define(Dynamic2Pattern, #{name  := <<"my-dynamic">>,
                           vhost := <<"v">>,
                           type  := <<"dynamic">>}).

shovels(Config) ->
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

%% It's a bit arbitrary to be testing this here, but we want to be
%% able to test that mgmt extensions can be started and stopped
%% *somewhere*, and here is as good a place as any.
dynamic_plugin_enable_disable(Config) ->
    http_get(Config, "/shovels", ?OK),
    rabbit_ct_broker_helpers:disable_plugin(Config, 0,
      "rabbitmq_shovel_management"),
    http_get(Config, "/shovels", ?NOT_FOUND),
    http_get(Config, "/overview", ?OK),
    rabbit_ct_broker_helpers:disable_plugin(Config, 0,
      "rabbitmq_management"),
    http_fail(Config, "/shovels"),
    http_fail(Config, "/overview"),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0,
      "rabbitmq_management"),
    http_get(Config, "/shovels", ?NOT_FOUND),
    http_get(Config, "/overview", ?OK),
    rabbit_ct_broker_helpers:enable_plugin(Config, 0,
      "rabbitmq_shovel_management"),
    http_get(Config, "/shovels", ?OK),
    http_get(Config, "/overview", ?OK),
    passed.

%%---------------------------------------------------------------------------
%% TODO this is mostly copypasta from the mgmt tests

http_get(Config, Path) ->
    http_get(Config, Path, ?OK).

http_get(Config, Path, CodeExp) ->
    http_get(Config, Path, "guest", "guest", CodeExp).

http_get(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, get, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "GET", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_fail(Config, Path) ->
    {error, {failed_connect, _}} = req(Config, get, Path, []).

http_put(Config, Path, List, CodeExp) ->
    http_put_raw(Config, Path, format_for_upload(List), CodeExp).

http_put(Config, Path, List, User, Pass, CodeExp) ->
    http_put_raw(Config, Path, format_for_upload(List), User, Pass, CodeExp).

http_post(Config, Path, List, CodeExp) ->
    http_post_raw(Config, Path, format_for_upload(List), CodeExp).

http_post(Config, Path, List, User, Pass, CodeExp) ->
    http_post_raw(Config, Path, format_for_upload(List), User, Pass, CodeExp).

format_for_upload(none) ->
    <<"">>;
format_for_upload(Map) ->
    iolist_to_binary(rabbit_json:encode(convert_keys(Map))).

convert_keys(Map) ->
    maps:fold(fun
        (K, V, Acc) when is_map(V) ->
            Acc#{atom_to_binary(K, latin1) => convert_keys(V)};
        (K, V, Acc) ->
            Acc#{atom_to_binary(K, latin1) => V}
    end, #{}, Map).

http_put_raw(Config, Path, Body, CodeExp) ->
    http_upload_raw(Config, put, Path, Body, "guest", "guest", CodeExp).

http_put_raw(Config, Path, Body, User, Pass, CodeExp) ->
    http_upload_raw(Config, put, Path, Body, User, Pass, CodeExp).

http_post_raw(Config, Path, Body, CodeExp) ->
    http_upload_raw(Config, post, Path, Body, "guest", "guest", CodeExp).

http_post_raw(Config, Path, Body, User, Pass, CodeExp) ->
    http_upload_raw(Config, post, Path, Body, User, Pass, CodeExp).

http_upload_raw(Config, Type, Path, Body, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, Type, Path, [auth_header(User, Pass)], Body),
    assert_code(CodeExp, CodeAct, Type, Path, ResBody),
    decode(CodeExp, Headers, ResBody).

http_delete(Config, Path, CodeExp) ->
    http_delete(Config, Path, "guest", "guest", CodeExp).

http_delete(Config, Path, User, Pass, CodeExp) ->
    {ok, {{_HTTP, CodeAct, _}, Headers, ResBody}} =
        req(Config, delete, Path, [auth_header(User, Pass)]),
    assert_code(CodeExp, CodeAct, "DELETE", Path, ResBody),
    decode(CodeExp, Headers, ResBody).

assert_code(CodeExp, CodeAct, _Type, _Path, _Body) ->
    ?assertEqual(CodeExp, CodeAct).

req_uri(Config, Path) ->
    rabbit_misc:format("~s/api~s", [
        rabbit_ct_broker_helpers:node_uri(Config, 0, management),
        Path
      ]).

req(Config, Type, Path, Headers) ->
    httpc:request(Type,
      {req_uri(Config, Path), Headers},
      ?HTTPC_OPTS, []).

req(Config, Type, Path, Headers, Body) ->
    httpc:request(Type,
      {req_uri(Config, Path), Headers, "application/json", Body},
      ?HTTPC_OPTS, []).

decode(?OK, _Headers,  ResBody) -> cleanup(rabbit_json:decode(rabbit_data_coercion:to_binary(ResBody)));
decode(_,    Headers, _ResBody) -> Headers.

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

wait_until(_Fun, 0) ->
    ?assert(wait_failed);
wait_until(Fun, N) ->
    case Fun() of
	true ->
	    ok;
	false ->
	    timer:sleep(500),
	    wait_until(Fun, N - 1)
    end.
