%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(worker_pool_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [{tests, [], [dispatch_async_blocks_until_task_begins]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    {ok, Server} = worker_pool_sup:start_link(2, Testcase),
    [{worker_pool_sup, Server}, {worker_pool_name, Testcase} | Config].

end_per_testcase(_Testcase, Config) ->
    Server = ?config(worker_pool_sup, Config),
    unlink(Server),
    Ref = monitor(process, Server),
    exit(Server, shutdown),
    receive
        {'DOWN', Ref, process, Server, _Reason} ->
            ok
    after 1000 ->
            error(exit_timeout)
    end.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

dispatch_async_blocks_until_task_begins(Config) ->
    Server = ?config(worker_pool_sup, Config),
    PoolName = ?config(worker_pool_name, Config),

    Self = self(),

    Waiter = fun() ->
                     Self ! {register, self()},
                     receive
                         go -> ok
                     end
             end,

    ok = worker_pool:dispatch_sync(PoolName, Waiter),
    ok = worker_pool:dispatch_sync(PoolName, Waiter),
    SomeWorker = receive
                     {register, WPid} -> WPid
                 after 250 ->
                         none
                 end,
    ?assert(is_process_alive(SomeWorker), "Dispatched tasks should be running"),
    Pid = spawn(
            fun() ->
                    ok = worker_pool:dispatch_sync(PoolName,
                                                   Waiter),
                    Self ! done_waiting,
                    exit(normal)
            end),
    DidWait = receive
                  done_waiting ->
                      false
              after 250 ->
                      true
              end,
    ?assert(DidWait, "dispatch_sync should block until there is a free worker"),
    SomeWorker ! go,
    DidFinish = receive
                    done_waiting ->
                        true
                after 250 ->
                        false
                end,
    ?assert(DidFinish, "appearance of a free worker should unblock the dispatcher").
