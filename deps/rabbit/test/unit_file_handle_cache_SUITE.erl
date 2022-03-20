%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_file_handle_cache_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          file_handle_cache, %% Change FHC limit.
          file_handle_cache_reserve,
          file_handle_cache_reserve_release,
          file_handle_cache_reserve_above_limit,
          file_handle_cache_reserve_monitor,
          file_handle_cache_reserve_open_file_above_limit
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 2}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun setup_file_handle_cache/1
      ]).

setup_file_handle_cache(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_file_handle_cache1, []),
    Config.

setup_file_handle_cache1() ->
    %% FIXME: Why are we doing this?
    application:set_env(rabbit, file_handles_high_watermark, 10),
    ok = file_handle_cache:set_limit(10),
    ok.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% ---------------------------------------------------------------------------
%% file_handle_cache.
%% ---------------------------------------------------------------------------

file_handle_cache(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache1, [Config]).

file_handle_cache1(_Config) ->
    %% test copying when there is just one spare handle
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5), %% 1 or 2 sockets, 2 msg_stores
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    ok = filelib:ensure_dir(filename:join(TmpDir, "nothing")),
    [Src1, Dst1, Src2, Dst2] = Files =
        [filename:join(TmpDir, Str) || Str <- ["file1", "file2", "file3", "file4"]],
    Content = <<"foo">>,
    CopyFun = fun (Src, Dst) ->
                      {ok, Hdl} = prim_file:open(Src, [binary, write]),
                      ok = prim_file:write(Hdl, Content),
                      ok = prim_file:sync(Hdl),
                      prim_file:close(Hdl),

                      {ok, SrcHdl} = file_handle_cache:open(Src, [read], []),
                      {ok, DstHdl} = file_handle_cache:open(Dst, [write], []),
                      Size = size(Content),
                      {ok, Size} = file_handle_cache:copy(SrcHdl, DstHdl, Size),
                      ok = file_handle_cache:delete(SrcHdl),
                      ok = file_handle_cache:delete(DstHdl)
              end,
    Pid = spawn(fun () -> {ok, Hdl} = file_handle_cache:open(
                                        filename:join(TmpDir, "file5"),
                                        [write], []),
                          receive {next, Pid1} -> Pid1 ! {next, self()} end,
                          file_handle_cache:delete(Hdl),
                          %% This will block and never return, so we
                          %% exercise the fhc tidying up the pending
                          %% queue on the death of a process.
                          ok = CopyFun(Src1, Dst1)
                end),
    ok = CopyFun(Src1, Dst1),
    ok = file_handle_cache:set_limit(2),
    Pid ! {next, self()},
    receive {next, Pid} -> ok end,
    timer:sleep(100),
    Pid1 = spawn(fun () -> CopyFun(Src2, Dst2) end),
    timer:sleep(100),
    erlang:monitor(process, Pid),
    erlang:monitor(process, Pid1),
    exit(Pid, kill),
    exit(Pid1, kill),
    receive {'DOWN', _MRef, process, Pid, _Reason} -> ok end,
    receive {'DOWN', _MRef1, process, Pid1, _Reason1} -> ok end,
    [file:delete(File) || File <- Files],
    ok = file_handle_cache:set_limit(Limit),
    passed.

file_handle_cache_reserve(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve1, [Config]).

file_handle_cache_reserve1(_Config) ->
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5),
    %% Reserves are always accepted, even if above the limit
    %% These are for special processes such as quorum queues
    ok = file_handle_cache:set_reservation(7),

    Self = self(),
    spawn(fun () -> ok = file_handle_cache:obtain(),
                    Self ! obtained
          end),

    Props = file_handle_cache:info([files_reserved, sockets_used]),
    ?assertEqual(7, proplists:get_value(files_reserved, Props)),
    ?assertEqual(0, proplists:get_value(sockets_used, Props)),

    %% The obtain should still be blocked, as there are no file handles
    %% available
    receive
        obtained ->
            throw(error_file_obtained)
    after 1000 ->
            %% Let's release 5 file handles, that should leave
            %% enough free for the `obtain` to go through
            file_handle_cache:set_reservation(2),
            Props0 = file_handle_cache:info([files_reserved, sockets_used]),
            ?assertEqual(2, proplists:get_value(files_reserved, Props0)),
            ?assertEqual(1, proplists:get_value(sockets_used, Props0)),
            receive
                obtained ->
                    ok = file_handle_cache:set_limit(Limit),
                    passed
            after 5000 ->
                    throw(error_file_not_released)
            end
    end.

file_handle_cache_reserve_release(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_release1, [Config]).

file_handle_cache_reserve_release1(_Config) ->
    ok = file_handle_cache:set_reservation(7),
    ?assertEqual([{files_reserved, 7}], file_handle_cache:info([files_reserved])),
    ok = file_handle_cache:set_reservation(3),
    ?assertEqual([{files_reserved, 3}], file_handle_cache:info([files_reserved])),
    ok = file_handle_cache:release_reservation(),
    ?assertEqual([{files_reserved, 0}], file_handle_cache:info([files_reserved])),
    passed.

file_handle_cache_reserve_above_limit(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_above_limit1, [Config]).

file_handle_cache_reserve_above_limit1(_Config) ->
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5),
    %% Reserves are always accepted, even if above the limit
    %% These are for special processes such as quorum queues
    ok = file_handle_cache:obtain(5),
    ?assertEqual([{file_descriptor_limit, []}],  rabbit_alarm:get_alarms()),

    ok = file_handle_cache:set_reservation(7),

    Props = file_handle_cache:info([files_reserved, sockets_used]),
    ?assertEqual(7, proplists:get_value(files_reserved, Props)),
    ?assertEqual(5, proplists:get_value(sockets_used, Props)),

    ok = file_handle_cache:set_limit(Limit),
    passed.

file_handle_cache_reserve_open_file_above_limit(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_open_file_above_limit1, [Config]).

file_handle_cache_reserve_open_file_above_limit1(_Config) ->
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5),
    %% Reserves are always accepted, even if above the limit
    %% These are for special processes such as quorum queues
    ok = file_handle_cache:set_reservation(7),

    Self = self(),
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    spawn(fun () -> {ok, _} = file_handle_cache:open(
                                filename:join(TmpDir, "file_above_limit"),
                                [write], []),
                    Self ! opened
          end),

    Props = file_handle_cache:info([files_reserved]),
    ?assertEqual(7, proplists:get_value(files_reserved, Props)),

    %% The open should still be blocked, as there are no file handles
    %% available
    receive
        opened ->
            throw(error_file_opened)
    after 1000 ->
            %% Let's release 5 file handles, that should leave
            %% enough free for the `open` to go through
            file_handle_cache:set_reservation(2),
            Props0 = file_handle_cache:info([files_reserved, total_used]),
            ?assertEqual(2, proplists:get_value(files_reserved, Props0)),
            receive
                opened ->
                    ok = file_handle_cache:set_limit(Limit),
                    passed
            after 5000 ->
                    throw(error_file_not_released)
            end
    end.

file_handle_cache_reserve_monitor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_monitor1, [Config]).

file_handle_cache_reserve_monitor1(_Config) ->
    %% Check that if the process that does the reserve dies, the file handlers are
    %% released by the cache
    Self = self(),
    Pid = spawn(fun () ->
                        ok = file_handle_cache:set_reservation(2),
                        Self ! done,
                        receive
                            stop -> ok
                        end
                end),
    receive
        done -> ok
    end,
    ?assertEqual([{files_reserved, 2}], file_handle_cache:info([files_reserved])),
    Pid ! stop,
    timer:sleep(500),
    ?assertEqual([{files_reserved, 0}], file_handle_cache:info([files_reserved])),
    passed.
