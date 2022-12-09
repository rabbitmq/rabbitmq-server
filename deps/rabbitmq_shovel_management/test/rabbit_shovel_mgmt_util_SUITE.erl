%%% @doc Unit tests of rabbit_shovel_mgmt_util
-module(rabbit_shovel_mgmt_util_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [blocked_status].

init_per_testcase(_, Config) ->
    meck:expect(rabbit_shovel_dyn_worker_sup_sup, cleanup_specs, 0, ok),
    rabbit_shovel_status:start_link(),
    Config.

end_per_testcase(_, Config) ->
    meck:unload(rabbit_shovel_dyn_worker_sup_sup),
    Config.

blocked_status(_Config) ->
    ?assertNotEqual(undefined, whereis(rabbit_shovel_status)),
    Name = {<<"/">>, <<"test">>},
    Type = dynamic,
    Props = [{src_uri,<<"amqp://">>},
             {src_protocol,<<"amqp091">>},
             {dest_protocol,<<"amqp091">>},
             {dest_uri,<<"amqp://">>},
             {src_queue,<<"q1">>},
             {dest_queue,<<"q2">>}],
    ok = rabbit_shovel_status:report(Name, Type, starting),
    ok = rabbit_shovel_status:report(Name, Type, {running, Props}),

    ?assertEqual([{Name, running}], get_shovel_states()),

    ok = rabbit_shovel_status:report_blocked_status(Name, flow),
    ?assertEqual([{Name, flow}], get_shovel_states()),

    %% If the shovel was blocked by credit flow in the last
    %% STATE_CHANGE_INTERVAL, its state will be reported as "in flow".
    ok = rabbit_shovel_status:report_blocked_status(Name, running),
    ?assertEqual([{Name, flow}], get_shovel_states()),

    timer:sleep(1000),
    ?assertEqual([{Name, running}], get_shovel_states()),

    ok = rabbit_shovel_status:report_blocked_status(Name, flow),
    ?assertEqual([{Name, flow}], get_shovel_states()),

    ok = rabbit_shovel_status:report_blocked_status(Name, blocked),
    ?assertEqual([{Name, blocked}], get_shovel_states()),

    %% If the shovel was blocked by credit flow in the last
    %% STATE_CHANGE_INTERVAL, its state will be reported as "in flow",
    %% even if there was a blocked state in-between
    ok = rabbit_shovel_status:report_blocked_status(Name, running),
    ?assertEqual([{Name, flow}], get_shovel_states()),

    timer:sleep(1000),
    ?assertEqual([{Name, running}], get_shovel_states()),

    ok = rabbit_shovel_status:report_blocked_status(Name, blocked),
    ?assertEqual([{Name, blocked}], get_shovel_states()),

    %% Switching back from blocked to running happens immediately
    ok = rabbit_shovel_status:report_blocked_status(Name, running),
    ?assertEqual([{Name, running}], get_shovel_states()),

    %% Switching from flow to terminated happens immediately
    ok = rabbit_shovel_status:report_blocked_status(Name, flow),
    rabbit_shovel_status:report(Name, Type, {terminated, reason}),
    ?assertEqual([{Name, terminated}], get_shovel_states()),

    ok.

get_shovel_states() ->
    [{{proplists:get_value(vhost, S), proplists:get_value(name, S)},
      proplists:get_value(state, S)}
     || S <- rabbit_shovel_mgmt_util:status(node())].
