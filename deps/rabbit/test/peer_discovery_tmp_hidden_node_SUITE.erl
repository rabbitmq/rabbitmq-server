%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(peer_discovery_tmp_hidden_node_SUITE).

-include_lib("kernel/include/inet.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([suite/0,
         all/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         do_setup_test_node/1,

         no_connection_between_peers_is_opened/1,
         long_names_work/1,
         ipv6_works/1,
         inetrc_file_as_atom_works/1,
         tls_dist_works/1,
         custom_cookie_is_propagated/1,
         tmp_hidden_node_does_not_need_a_cookie_file/1,
         tmp_hidden_node_is_not_connectable/1
        ]).

suite() ->
    [{timetrap, {minutes, 15}}].

all() ->
    [no_connection_between_peers_is_opened,
     long_names_work,
     ipv6_works,
     inetrc_file_as_atom_works,
     tls_dist_works,
     custom_cookie_is_propagated,
     tmp_hidden_node_does_not_need_a_cookie_file,
     tmp_hidden_node_is_not_connectable].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

no_connection_between_peers_is_opened(_Config) ->
    PeerOptions = #{longnames => false},
    test_query_node_props(?FUNCTION_NAME, 2, PeerOptions).

long_names_work(_Config) ->
    PeerOptions = #{longnames => true},
    test_query_node_props(?FUNCTION_NAME, 2, PeerOptions).

ipv6_works(Config) ->
    PrivDir = ?config(priv_dir, Config),
    InetrcFilename = filename:join(PrivDir, "inetrc-ipv6.erl"),
    ct:log("Inetrc filename:~n~0p", [InetrcFilename]),
    Inetrc = [{inet6, true}],
    InetrcContent = [io_lib:format("~p.~n", [Param]) || Param <- Inetrc],
    ct:log("Inetrc file content:~n---8<---~n~s---8<---", [InetrcContent]),
    ok = file:write_file(InetrcFilename, InetrcContent),
    InetrcArg = rabbit_misc:format("~0p", [InetrcFilename]),

    PeerOptions = #{host => "::1",
                    args => ["-proto_dist", "inet6_tcp",
                             "-kernel", "inetrc", InetrcArg]},
    test_query_node_props(?FUNCTION_NAME, 2, PeerOptions).

inetrc_file_as_atom_works(_Config) ->
    %% We can't write the inetrc file in `privdir' like we did in
    %% `ipv6_works/1' because here we convert the filename to an atom and an
    %% atom can't be more than 255 characters. It happens that in the
    %% Buildbuddy CI worker, we reach a filename of 340+ characters.
    %%
    %% Instead, we write the file in the temporary directory.
    %%
    %% TEMP and TMP are used on Microsoft Windows, TMPDIR on Unix (but TMPDIR
    %% might not be defined).
    TmpDir = os:getenv("TEMP", os:getenv("TMP", os:getenv("TMPDIR", "/tmp"))),
    InetrcFilename = filename:join(TmpDir, "inetrc-ipv6.erl"),
    ct:log("Inetrc filename:~n~0p", [InetrcFilename]),
    Inetrc = [{inet6, true}],
    InetrcContent = [io_lib:format("~p.~n", [Param]) || Param <- Inetrc],
    ct:log("Inetrc file content:~n---8<---~n~s---8<---", [InetrcContent]),
    ok = file:write_file(InetrcFilename, InetrcContent),
    InetrcArg = rabbit_misc:format("~0p", [list_to_atom(InetrcFilename)]),

    PeerOptions = #{host => "::1",
                    args => ["-proto_dist", "inet6_tcp",
                             "-kernel", "inetrc", InetrcArg]},
    test_query_node_props(?FUNCTION_NAME, 2, PeerOptions).

tls_dist_works(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    Password = ?config(rmq_certspwd, Config),
    CACert = filename:join([CertsDir, "testca", "cacert.pem"]),
    ServerCert = filename:join([CertsDir, "server", "cert.pem"]),
    ServerKey = filename:join([CertsDir, "server", "key.pem"]),
    SslOptions = [{server,
                   [{cacertfile, CACert},
                    {certfile, ServerCert},
                    {keyfile, ServerKey},
                    {password, Password},
                    {secure_renegotiate, true},
                    {verify, verify_none},
                    {fail_if_no_peer_cert, false}]},
                  {client,
                   [{cacertfile, CACert},
                    {secure_renegotiate, true}]}],

    PrivDir = ?config(priv_dir, Config),
    SslOptFilename = filename:join(PrivDir, "ssl-options.erl"),
    ct:log("SSL options filename:~n~0p", [SslOptFilename]),
    SslOptContent = rabbit_misc:format("~p.~n", [SslOptions]),
    ct:log("SSL options file content:~n---8<---~n~s---8<---", [SslOptContent]),
    ok = file:write_file(SslOptFilename, SslOptContent),

    %% We need to read the certificate's Subject ID to see what hostname is
    %% used in the certificate and use the same to start the test Erlang nodes.
    %% We also need to pay attention if the name is short or long.
    {ok, ServerCertBin} = file:read_file(ServerCert),
    ct:log("ServerCertBin = ~p", [ServerCertBin]),
    [DecodedCert] = public_key:pem_decode(ServerCertBin),
    ct:log("DecodedCert = ~p", [DecodedCert]),
    DecodedCert1 = element(2, DecodedCert),
    {_SerialNr, {rdnSequence, IssuerAttrs}} = public_key:pkix_subject_id(
                                                DecodedCert1),
    ct:log("IssuerAttrs = ~p", [IssuerAttrs]),
    [ServerName] = [Value
                    || [#'AttributeTypeAndValue'{type = {2, 5, 4, 3},
                                                 value = {utf8String, Value}}]
                       <- IssuerAttrs],
    ct:log("ServerName = ~p", [ServerName]),
    UseLongnames = re:run(ServerName, "\\.", [{capture, none}]) =:= match,

    PeerOptions = #{host => binary_to_list(ServerName),
                    longnames => UseLongnames,
                    args => ["-proto_dist", "inet_tls",
                             "-ssl_dist_optfile", SslOptFilename]},
    test_query_node_props(?FUNCTION_NAME, 2, PeerOptions).

custom_cookie_is_propagated(_Config) ->
    PeerOptions = #{longnames => false},
    Peers = start_test_nodes(?FUNCTION_NAME, 2, PeerOptions),
    try
        %% Give both peers the same cookie, distinct from whatever
        %% `.erlang.cookie' they would otherwise share, to make sure the
        %% temporary hidden node started by `rabbit_peer_discovery' inherits
        %% the querying node's cookie instead of picking up its own default.
        maps:foreach(
          fun(_PeerName, PeerPid) ->
                  true = peer:call(PeerPid, erlang, set_cookie,
                                    [custom_test_cookie])
          end, Peers),
        do_test_query_node_props(Peers)
    after
        stop_test_nodes(Peers)
    end.

tmp_hidden_node_does_not_need_a_cookie_file(Config) ->
    PrivDir = ?config(priv_dir, Config),
    %% A directory that exists but can't be written to: if the temporary
    %% hidden node started by `rabbit_peer_discovery' ever tries to read or
    %% create `$HOME/.erlang.cookie', it fails to boot.
    UnwritableHome = filename:join(PrivDir, "unwritable_home"),
    ok = file:make_dir(UnwritableHome),
    ok = file:change_mode(UnwritableHome, 8#0500),
    PeerOptions = #{longnames => false,
                    args => ["-nocookie"],
                    env => [{"HOME", UnwritableHome}]},
    Peers = start_test_nodes(?FUNCTION_NAME, 2, PeerOptions),
    try
        %% Give both peers the same cookie so the temporary hidden node
        %% started from `NodeA' can still reach `NodeB'.
        maps:foreach(
          fun(_PeerName, PeerPid) ->
                  true = peer:call(PeerPid, erlang, set_cookie,
                                    [custom_test_cookie])
          end, Peers),
        do_test_query_node_props(Peers)
    after
        stop_test_nodes(Peers),
        ok = file:change_mode(UnwritableHome, 8#0700)
    end.

tmp_hidden_node_is_not_connectable(_Config) ->
    %% Names already registered with epmd before we start anything (e.g. the
    %% Common Test runner's own node, or nodes started by other suites
    %% running concurrently on the same host) are not part of what this
    %% test cares about; only names that show up *because of* the query
    %% below, and that could plausibly be the temporary hidden node, are.
    {ok, BaselineNames} = net_adm:names(),
    PeerOptions = #{longnames => false},
    Peers = start_test_nodes(?FUNCTION_NAME, 2, PeerOptions),
    try
        %% The temporary hidden node started internally by
        %% `rabbit_peer_discovery' must never register with epmd or accept
        %% connections, regardless of its cookie, since it only ever needs
        %% to connect out. Watch epmd for the whole duration of the query
        %% and make sure no node matching the temporary hidden node's
        %% naming scheme ever shows up (see `is_temp_hidden_node_name/2').
        KnownNames = [Name || {Name, _Port} <- BaselineNames],
        PeerPrefixes = [hd(string:split(atom_to_list(Node), "@")) ||
                           Node <- maps:keys(Peers)],
        Self = self(),
        Watcher = spawn_link(
                    fun() -> watch_epmd_names(Self, KnownNames, PeerPrefixes) end),
        do_test_query_node_props(Peers),
        Watcher ! {stop, Self},
        ExtraNames = receive
                         {names, Names} -> Names
                     after 5000 ->
                             error(epmd_watcher_timeout)
                     end,
        ?assertEqual([], ExtraNames)
    after
        stop_test_nodes(Peers)
    end.

test_query_node_props(Testcase, NodeCount, PeerOptions) ->
    Peers = start_test_nodes(Testcase, NodeCount, PeerOptions),
    try
        do_test_query_node_props(Peers)
    after
        stop_test_nodes(Peers)
    end.

do_test_query_node_props(Peers) ->
    %% Ensure no connection exists at the beginning.
    ensure_no_connections_between_test_nodes(Peers),

    %% Query the remote node's properties. The return value should have the
    %% properties of the peer node, otherwise it means that we failed to
    %% contact it.
    [NodeA, NodeB] = lists:sort(maps:keys(Peers)),
    NodeAPid = maps:get(NodeA, Peers),
    Ret = peer:call(
            NodeAPid,
            rabbit_peer_discovery, query_node_props, [[NodeB]],
            infinity),
    ct:log("Discovered nodes properties:~n~p", [Ret]),
    ?assertMatch([{NodeB, [NodeB], _, false}], Ret),

    %% Ensure no connection exists after the query.
    ensure_no_connections_between_test_nodes(Peers).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

start_test_nodes(Testcase, NodeCount, PeerOptions) ->
    PeerOptions1 = PeerOptions#{
                     %% We use an alternative connection channel, not the
                     %% regular Erlang distribution, because we want to test
                     %% the behavior of the temporary hidden node and
                     %% especially that it doesn't rely or create a connection
                     %% between the two nodes.
                     connection => standard_io,
                     wait_boot => infinity},
    TestEbin = filename:dirname(code:which(?MODULE)),
    Args0 = maps:get(args, PeerOptions1, []),
    Args1 = ["-pa", TestEbin | Args0],
    Env0 = maps:get(env, PeerOptions1, []),
    Env1 = [{"ERL_LIBS", os:getenv("ERL_LIBS")} | Env0],
    PeerOptions2 = PeerOptions1#{args => Args1,
                                 env => Env1},
    start_test_nodes(Testcase, 1, NodeCount, PeerOptions2, #{}).

start_test_nodes(Testcase, NodeNumber, NodeCount, PeerOptions, Peers)
  when NodeNumber =< NodeCount ->
    PeerName0 = rabbit_misc:format("~s-~b", [Testcase, NodeNumber]),
    PeerOptions1 = PeerOptions#{name => PeerName0},
    PeerOptions2 = case PeerOptions1 of
                       #{host := _} ->
                           PeerOptions1;
                       #{longnames := true} ->
                           %% To simulate Erlang long node names, we use a
                           %% hard-coded IP address that is likely to exist.
                           %%
                           %% We can't rely on the host proper network
                           %% configuration because it appears that several
                           %% hosts are half-configured (at least some random
                           %% GitHub workers and Broadcom-managed OSX laptops
                           %% in the team).
                           PeerOptions1#{host => "127.0.0.1"};
                       _ ->
                           PeerOptions1
                   end,
    ct:log("Starting peer with options: ~p", [PeerOptions2]),
    try peer:start(PeerOptions2) of
        {ok, PeerPid, PeerName} ->
            ct:log("Configuring peer '~ts'", [PeerName]),
            setup_test_node(PeerPid, PeerOptions2),
            Peers1 = Peers#{PeerName => PeerPid},
            start_test_nodes(
              Testcase, NodeNumber + 1, NodeCount, PeerOptions, Peers1);
        Error ->
            ct:log("Failed to started peer node:~n"
                   "Options: ~p~n"
                   "Error: ~p", [PeerOptions2, Error]),
            stop_test_nodes(Peers),
            erlang:throw(Error)
    catch
        Class:Reason:Stacktrace ->
            ct:log("Failed to started peer node:~n"
                   "Options: ~p~n"
                   "Exception: ~p:~p~n~p",
                   [PeerOptions2, Class, Reason, Stacktrace]),
            stop_test_nodes(Peers),
            erlang:raise(Class, Reason, Stacktrace)
    end;
start_test_nodes(_Testcase, _NodeNumber, _Count, _PeerOptions, Peers) ->
    ct:log("Peers: ~p", [Peers]),
    Peers.

setup_test_node(PeerPid, PeerOptions) ->
    peer:call(PeerPid, ?MODULE, do_setup_test_node, [PeerOptions]).

do_setup_test_node(PeerOptions) ->
    Context = case maps:get(longnames, PeerOptions, false) of
                  true  -> #{nodename_type => longnames};
                  false -> #{}
              end,
    logger:set_primary_config(level, debug),
    meck:new(rabbit_prelaunch, [unstick, passthrough, no_link]),
    meck:expect(rabbit_prelaunch, get_context, fun() -> Context end),
    meck:new(rabbit_nodes, [unstick, passthrough, no_link]),
    Nodes = [node()],
    meck:expect(rabbit_nodes, all, fun() -> Nodes end),
    meck:expect(rabbit_nodes, list_members, fun() -> Nodes end),
    ok.

stop_test_nodes(Peers) ->
    maps:foreach(
      fun(_PeerName, PeerPid) ->
              peer:stop(PeerPid)
      end, Peers).

ensure_no_connections_between_test_nodes(Peers) ->
    maps:foreach(
      fun(_PeerName, PeerPid) ->
              ?assertEqual([], peer:call(PeerPid, erlang, nodes, []))
      end, Peers).

watch_epmd_names(Parent, KnownNames, PeerPrefixes) ->
    watch_epmd_names(Parent, KnownNames, PeerPrefixes, sets:new([{version, 2}])).

watch_epmd_names(Parent, KnownNames, PeerPrefixes, Extra) ->
    receive
        {stop, Parent} ->
            Parent ! {names, sets:to_list(Extra)}
    after 5 ->
            NewExtra = case net_adm:names() of
                           {ok, Names} ->
                               lists:foldl(
                                 fun({Name, _Port}, Acc) ->
                                         case lists:member(Name, KnownNames) of
                                             true ->
                                                 Acc;
                                             false ->
                                                 case is_temp_hidden_node_name(
                                                        Name, PeerPrefixes) of
                                                     true ->
                                                         sets:add_element(Name, Acc);
                                                     false ->
                                                         Acc
                                                 end
                                         end
                                 end, Extra, Names);
                           {error, _} ->
                               Extra
                       end,
            watch_epmd_names(Parent, KnownNames, PeerPrefixes, NewExtra)
    end.

%% `rabbit_peer_discovery:query_node_props/1' names the temporary hidden
%% node with `peer:random_name/1', passing the querying node's own prefix
%% (the part of its name before `@'). That always produces
%% `Prefix-Uniq-OsPid'; this is the only shape of name that could possibly
%% be the temporary hidden node, as opposed to some unrelated node started
%% by another suite running concurrently on the same host.
is_temp_hidden_node_name(Name, PeerPrefixes) ->
    lists:any(
      fun(Prefix) ->
              Pattern = "^\\Q" ++ Prefix ++ "\\E-[0-9]+-[0-9]+$",
              match =:= re:run(Name, Pattern, [{capture, none}])
      end, PeerPrefixes).
