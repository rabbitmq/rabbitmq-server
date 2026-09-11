%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("rabbit_exchange_federation.hrl").
-include_lib("rabbitmq_federation_common/include/rabbit_federation.hrl").

-compile(export_all).

all() -> [
    reconnect_all_empty_scope,
    reconnect_all_broadcasts_to_members,
    adjust_when_supervisor_not_running,
    adjust_clear_upstream_when_supervisor_not_running,
    start_child_handles_already_present,
    hops_no_header_returns_max_hops,
    hops_larger_than_max_hops_is_clamped,
    hops_of_one_returns_zero,
    hops_of_zero_or_negative_returns_zero,
    hops_missing_from_head_table_returns_zero,
    hops_with_non_integer_value_returns_zero,
    hops_header_not_an_array_returns_zero,
    hops_empty_array_or_non_table_head_returns_zero,
    hops_accepts_long_and_unsignedbyte,
    hops_cycle_detection_returns_zero,
    hops_with_non_positive_max_hops_returns_zero
].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

reconnect_all_empty_scope(_Config) ->
    Scope = ?FEDERATION_PG_SCOPE,
    {ok, _} = pg:start_link(Scope),
    ?assertEqual(ok, rabbit_federation_exchange_link:reconnect_all()),
    stop_pg_scope(Scope).

reconnect_all_broadcasts_to_members(_Config) ->
    Scope = ?FEDERATION_PG_SCOPE,
    {ok, _} = pg:start_link(Scope),
    Self = self(),
    Pids = [spawn(fun() ->
        receive
            {'$gen_cast', reconnect} -> Self ! {got_reconnect, self()}
        after 5000 -> Self ! {timeout, self()}
        end
    end) || _ <- lists:seq(1, 3)],
    GroupName = rabbit_federation_util:pgname(rabbit_federation_exchanges),
    [pg:join(Scope, GroupName, Pid) || Pid <- Pids],
    ?assertEqual(ok, rabbit_federation_exchange_link:reconnect_all()),
    [receive
        {got_reconnect, Pid} -> ok;
        {timeout, Pid} -> ct:fail("Process ~p did not receive reconnect", [Pid])
    after 1000 ->
        ct:fail("Timeout waiting for process ~p", [Pid])
    end || Pid <- Pids],
    stop_pg_scope(Scope).

stop_pg_scope(Scope) ->
    case whereis(Scope) of
        Pid when is_pid(Pid) ->
            unlink(Pid),
            exit(Pid, kill);
        _ -> ok
    end,
    ok.

%% Test that adjust/1 returns ok when the supervisor is not running,
%% for example, during a node shutdown when plugin tries to adjust federation
%% links but the federation supervisor has already been stopped by the core.
adjust_when_supervisor_not_running(_Config) ->
    ?assertEqual(undefined, whereis(rabbit_federation_exchange_link_sup_sup)),
    %% adjust/1 should return ok, not crash
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust(everything)),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({upstream, <<"test">>})),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({upstream_set, <<"test">>})).

adjust_clear_upstream_when_supervisor_not_running(_Config) ->
    ?assertEqual(undefined, whereis(rabbit_federation_exchange_link_sup_sup)),
    %% adjust/1 with clear_upstream should not fail
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({clear_upstream, <<"/">>, <<"test">>})),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({clear_upstream_set, <<"test">>})).

start_child_handles_already_present(_Config) ->
    XName = #resource{virtual_host = <<"/">>, kind = exchange, name = <<"x">>},
    X = #exchange{name = XName, type = direct, durable = true,
                  auto_delete = false, internal = false, arguments = []},
    ExpectedId = (rabbit_exchange:immutable(X))#exchange{policy = X#exchange.policy},
    ok = meck:new(mirrored_supervisor, [unstick, passthrough]),
    ok = meck:expect(mirrored_supervisor, start_child,
                     fun(_Sup, _ChildSpec) -> {error, already_present} end),
    ok = meck:expect(mirrored_supervisor, delete_child,
                     fun(_Sup, _Id) -> ok end),
    try
        ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:start_child(X)),
        ?assert(meck:called(mirrored_supervisor, delete_child,
                            [rabbit_federation_exchange_link_sup_sup, ExpectedId]))
    after
        ok = meck:unload(mirrored_supervisor)
    end.

%% `hops/4` only uses the cluster name and vhost for cycle detection, so any
%% values will do.
-define(UNAME, <<"upstream-cluster">>).
-define(UVHOST, <<"/">>).

hops_no_header_returns_max_hops(_Config) ->
    ?assertEqual(5, rabbit_federation_exchange_link:hops([], 5, ?UNAME, ?UVHOST)).

hops_larger_than_max_hops_is_clamped(_Config) ->
    ?assertEqual(3, rabbit_federation_exchange_link:hops(header_with_hops(10), 3, ?UNAME, ?UVHOST)).

hops_of_one_returns_zero(_Config) ->
    ?assertEqual(0, rabbit_federation_exchange_link:hops(header_with_hops(1), 5, ?UNAME, ?UVHOST)).

hops_of_zero_or_negative_returns_zero(_Config) ->
    ?assertEqual(0, rabbit_federation_exchange_link:hops(header_with_hops(0), 5, ?UNAME, ?UVHOST)),
    ?assertEqual(0, rabbit_federation_exchange_link:hops(header_with_hops(-100), 5, ?UNAME, ?UVHOST)).

hops_missing_from_head_table_returns_zero(_Config) ->
    Args = [{?BINDING_HEADER, array, [{table, [{<<"cluster-name">>, longstr, <<"x">>}]}]}],
    ?assertEqual(0, rabbit_federation_exchange_link:hops(Args, 5, ?UNAME, ?UVHOST)).

hops_with_non_integer_value_returns_zero(_Config) ->
    Args = [{?BINDING_HEADER, array, [{table, [{<<"hops">>, longstr, <<"5">>}]}]}],
    ?assertEqual(0, rabbit_federation_exchange_link:hops(Args, 5, ?UNAME, ?UVHOST)).

hops_header_not_an_array_returns_zero(_Config) ->
    Args = [{?BINDING_HEADER, longstr, <<"nope">>}],
    ?assertEqual(0, rabbit_federation_exchange_link:hops(Args, 5, ?UNAME, ?UVHOST)).

hops_empty_array_or_non_table_head_returns_zero(_Config) ->
    ?assertEqual(0, rabbit_federation_exchange_link:hops([{?BINDING_HEADER, array, []}], 5, ?UNAME, ?UVHOST)),
    ?assertEqual(0, rabbit_federation_exchange_link:hops([{?BINDING_HEADER, array, [{longstr, <<"x">>}]}], 5, ?UNAME, ?UVHOST)).

hops_accepts_long_and_unsignedbyte(_Config) ->
    LongArgs = [{?BINDING_HEADER, array, [{table, [{<<"hops">>, long, 3}]}]}],
    ?assertEqual(2, rabbit_federation_exchange_link:hops(LongArgs, 5, ?UNAME, ?UVHOST)),
    ByteArgs = [{?BINDING_HEADER, array, [{table, [{<<"hops">>, unsignedbyte, 3}]}]}],
    ?assertEqual(2, rabbit_federation_exchange_link:hops(ByteArgs, 5, ?UNAME, ?UVHOST)).

hops_cycle_detection_returns_zero(_Config) ->
    Args = [{?BINDING_HEADER, array,
             [{table, [{<<"hops">>, short, 5},
                       {<<"cluster-name">>, longstr, ?UNAME},
                       {<<"vhost">>, longstr, ?UVHOST}]}]}],
    ?assertEqual(0, rabbit_federation_exchange_link:hops(Args, 5, ?UNAME, ?UVHOST)).

hops_with_non_positive_max_hops_returns_zero(_Config) ->
    ?assertEqual(0, rabbit_federation_exchange_link:hops([], 0, ?UNAME, ?UVHOST)),
    ?assertEqual(0, rabbit_federation_exchange_link:hops([], -1, ?UNAME, ?UVHOST)).

header_with_hops(Hops) ->
    [{?BINDING_HEADER, array, [{table, [{<<"hops">>, short, Hops}]}]}].
