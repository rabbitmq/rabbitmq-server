%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-compile(export_all).

all() -> [
    obfuscate_upstream,
    obfuscate_upstream_params_network,
    obfuscate_upstream_params_network_with_char_list_password_value,
    obfuscate_upstream_params_direct,
    shutdown_flag_defaults_to_false,
    shutdown_flag_can_be_set,
    shutdown_flag_can_be_cleared,
    reconnect_all_empty_scope,
    reconnect_all_broadcasts_to_exchange_link_members,
    reconnect_all_broadcasts_to_queue_link_members,
    exchange_adjust_when_supervisor_not_running,
    exchange_adjust_clear_upstream_when_supervisor_not_running,
    queue_adjust_when_supervisor_not_running,
    queue_adjust_clear_upstream_when_supervisor_not_running
].

init_per_suite(Config) ->
    application:ensure_all_started(credentials_obfuscation),
    Config.

end_per_suite(Config) ->
    Config.

obfuscate_upstream(_Config) ->
    Upstream = #upstream{uris = [<<"amqp://guest:password@localhost">>]},
    ObfuscatedUpstream = rabbit_federation_util:obfuscate_upstream(Upstream),
    ?assertEqual(Upstream, rabbit_federation_util:deobfuscate_upstream(ObfuscatedUpstream)),
    ok.

obfuscate_upstream_params_network(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    ?assertEqual(UpstreamParams, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

obfuscate_upstream_params_network_with_char_list_password_value(_Config) ->
    Input = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = "password"}
    },
    Output = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(Input),
    ?assertEqual(Output, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

 obfuscate_upstream_params_direct(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_direct{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    ?assertEqual(UpstreamParams, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

shutdown_flag_defaults_to_false(_Config) ->
    rabbit_federation_app_state:reset_shutting_down_marker(),
    ?assertEqual(false, rabbit_federation_app_state:is_shutting_down()),
    ok.

shutdown_flag_can_be_set(_Config) ->
    rabbit_federation_app_state:reset_shutting_down_marker(),
    ?assertEqual(false, rabbit_federation_app_state:is_shutting_down()),
    rabbit_federation_app_state:mark_as_shutting_down(),
    ?assertEqual(true, rabbit_federation_app_state:is_shutting_down()),
    ok.

shutdown_flag_can_be_cleared(_Config) ->
    rabbit_federation_app_state:reset_shutting_down_marker(),
    rabbit_federation_app_state:mark_as_shutting_down(),
    ?assertEqual(true, rabbit_federation_app_state:is_shutting_down()),
    rabbit_federation_app_state:reset_shutting_down_marker(),
    ?assertEqual(false, rabbit_federation_app_state:is_shutting_down()),
    ok.

reconnect_all_empty_scope(_Config) ->
    Scope = ?FEDERATION_PG_SCOPE,
    {ok, _} = pg:start_link(Scope),
    ?assertEqual(ok, rabbit_federation_exchange_link:reconnect_all()),
    ?assertEqual(ok, rabbit_federation_queue_link:reconnect_all()),
    stop_pg_scope(Scope).

reconnect_all_broadcasts_to_exchange_link_members(_Config) ->
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

reconnect_all_broadcasts_to_queue_link_members(_Config) ->
    Scope = ?FEDERATION_PG_SCOPE,
    {ok, _} = pg:start_link(Scope),
    Self = self(),
    Pids = [spawn(fun() ->
        receive
            {'$gen_cast', reconnect} -> Self ! {got_reconnect, self()}
        after 5000 -> Self ! {timeout, self()}
        end
    end) || _ <- lists:seq(1, 3)],
    GroupName = rabbit_federation_util:pgname(rabbit_federation_queues),
    [pg:join(Scope, GroupName, Pid) || Pid <- Pids],
    ?assertEqual(ok, rabbit_federation_queue_link:reconnect_all()),
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
exchange_adjust_when_supervisor_not_running(_Config) ->
    ?assertEqual(undefined, whereis(rabbit_federation_exchange_link_sup_sup)),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust(everything)),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({upstream, <<"test">>})),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({upstream_set, <<"test">>})).

exchange_adjust_clear_upstream_when_supervisor_not_running(_Config) ->
    ?assertEqual(undefined, whereis(rabbit_federation_exchange_link_sup_sup)),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({clear_upstream, <<"/">>, <<"test">>})),
    ?assertEqual(ok, rabbit_federation_exchange_link_sup_sup:adjust({clear_upstream_set, <<"test">>})).

queue_adjust_when_supervisor_not_running(_Config) ->
    ?assertEqual(undefined, whereis(rabbit_federation_queue_link_sup_sup)),
    ?assertEqual(ok, rabbit_federation_queue_link_sup_sup:adjust(everything)),
    ?assertEqual(ok, rabbit_federation_queue_link_sup_sup:adjust({upstream, <<"test">>})),
    ?assertEqual(ok, rabbit_federation_queue_link_sup_sup:adjust({upstream_set, <<"test">>})).

queue_adjust_clear_upstream_when_supervisor_not_running(_Config) ->
    ?assertEqual(undefined, whereis(rabbit_federation_queue_link_sup_sup)),
    ?assertEqual(ok, rabbit_federation_queue_link_sup_sup:adjust({clear_upstream, <<"/">>, <<"test">>})),
    ?assertEqual(ok, rabbit_federation_queue_link_sup_sup:adjust({clear_upstream_set, <<"test">>})).
