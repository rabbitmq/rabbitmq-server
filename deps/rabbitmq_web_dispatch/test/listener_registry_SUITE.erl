%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(listener_registry_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

-define(V4_CONTEXT, listener_registry_v4).
-define(V6_CONTEXT, listener_registry_v6).
-define(V4_PREFIX, "listener_registry_v4").
-define(V6_PREFIX, "listener_registry_v6").
-define(CONFLICT_CONTEXT, listener_registry_conflict).
-define(CONFLICT_PREFIX, "listener_registry_conflict").
-define(V4_ADDRESS, {127, 0, 0, 1}).
-define(V6_ADDRESS, {0, 0, 0, 0, 0, 0, 0, 1}).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                two_interfaces_on_one_port_serve_separately,
                                unregistering_one_interface_leaves_the_other,
                                %% Runs last: it stops the registry, which
                                %% empties the dispatch table for the whole
                                %% node.
                                one_address_spelled_two_ways_is_rejected
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_http_shared, tcp_port_http_shared_alt,
                               tcp_port_http_conflict]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, ipv6_loopback_available, []) of
        true ->
            rabbit_ct_helpers:testcase_started(Config, Testcase);
        false ->
            {skip, "IPv6 loopback is not available on this host"}
    end.

end_per_testcase(Testcase, Config) ->
    _ = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, unregister_both, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

two_interfaces_on_one_port_serve_separately(Config) ->
    Port = port(Config, tcp_port_http_shared),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, register_both, [Port]),
    ?assertEqual([?V4_PREFIX, ?V6_PREFIX], registered_prefixes(Config)),
    ?assertEqual({ok, 200}, http_status(?V4_ADDRESS, Port, ?V4_PREFIX)),
    ?assertEqual({ok, 200}, http_status(?V6_ADDRESS, Port, ?V6_PREFIX)),
    %% Each address keeps its own dispatch table, so neither one serves the
    %% other's prefix.
    ?assertEqual({ok, 404}, http_status(?V6_ADDRESS, Port, ?V4_PREFIX)),
    ?assertEqual({ok, 404}, http_status(?V4_ADDRESS, Port, ?V6_PREFIX)).

unregistering_one_interface_leaves_the_other(Config) ->
    Port = port(Config, tcp_port_http_shared_alt),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, register_both, [Port]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_web_dispatch, unregister_context, [?V4_CONTEXT]),
    ?assertEqual([?V6_PREFIX], registered_prefixes(Config)),
    ?assertEqual({ok, 200}, http_status(?V6_ADDRESS, Port, ?V6_PREFIX)),
    ?assertEqual({error, econnrefused}, http_status(?V4_ADDRESS, Port, ?V4_PREFIX)).

one_address_spelled_two_ways_is_rejected(Config) ->
    Port = port(Config, tcp_port_http_conflict),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, ?MODULE, register_v4_as_string, [Port]),
    ?assertMatch({exit, {{listener_address_in_use, _}, {gen_server, call, _}}},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, register_v4_as_tuple, [Port])).

%% -------------------------------------------------------------------
%% Helpers running on the broker node.
%% -------------------------------------------------------------------

ipv6_loopback_available() ->
    case gen_tcp:listen(0, [inet6, {ip, {0, 0, 0, 0, 0, 0, 0, 1}}]) of
        {ok, Socket} ->
            ok = gen_tcp:close(Socket),
            true;
        {error, _} ->
            false
    end.

register_both(Port) ->
    ok = register_v4_as_string(Port),
    ok = register(?V6_CONTEXT, ?V6_PREFIX, [{port, Port}, {ip, "::1"}]).

register_v4_as_string(Port) ->
    register(?V4_CONTEXT, ?V4_PREFIX, [{port, Port}, {ip, "127.0.0.1"}]).

register_v4_as_tuple(Port) ->
    try
        register(?CONFLICT_CONTEXT, ?CONFLICT_PREFIX,
                 [{port, Port}, {ip, {127, 0, 0, 1}}])
    catch
        exit:Reason -> {exit, Reason}
    end.

register(Context, Prefix, Listener) ->
    {ok, _} = rabbit_web_dispatch:register_static_context(
                Context, Listener, Prefix, ?MODULE, "test/priv/www", Prefix),
    ok.

unregister_both() ->
    _ = rabbit_web_dispatch:unregister_context(?V4_CONTEXT),
    _ = rabbit_web_dispatch:unregister_context(?V6_CONTEXT),
    ok.

own_prefixes() ->
    [Path || {Path, _Desc, _Listener} <- rabbit_web_dispatch_registry:list_all(),
             Path =:= ?V4_PREFIX orelse Path =:= ?V6_PREFIX].

%% -------------------------------------------------------------------
%% Helpers running on the CT node.
%% -------------------------------------------------------------------

port(Config, Key) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, Key).

registered_prefixes(Config) ->
    lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, own_prefixes, [])).

%% `httpc` resolves a bracketed IPv6 URL through the resolver instead of taking
%% it as a literal address, so the request is made directly.
http_status(IPAddress, Port, Prefix) ->
    Family = case tuple_size(IPAddress) of
                 4 -> inet;
                 8 -> inet6
             end,
    Options = [Family, binary, {active, false}, {packet, http_bin}],
    case gen_tcp:connect(IPAddress, Port, Options, 10_000) of
        {ok, Socket} ->
            try
                Request = ["GET /", Prefix, "/index.html HTTP/1.1\r\n",
                           "Host: localhost\r\nConnection: close\r\n\r\n"],
                ok = gen_tcp:send(Socket, Request),
                case gen_tcp:recv(Socket, 0, 10_000) of
                    {ok, {http_response, _, Status, _}} -> {ok, Status};
                    {error, Reason}                     -> {error, Reason}
                end
            after
                ok = gen_tcp:close(Socket)
            end;
        {error, Reason} ->
            {error, Reason}
    end.
