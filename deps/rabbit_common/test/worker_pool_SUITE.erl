%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(worker_pool_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(POOL_SIZE, 1).
-define(POOL_NAME, test_pool).

all() ->
    [
    run_code_synchronously,
    run_code_asynchronously,
    set_timeout,
    cancel_timeout,
    cancel_timeout_by_setting,
    dispatch_async_blocks_until_task_begins
    ].

init_per_testcase(_, Config) ->
    {ok, Pool} = worker_pool_sup:start_link(?POOL_SIZE, ?POOL_NAME),
    [{pool_sup, Pool} | Config].

end_per_testcase(_, Config) ->
    Pool = ?config(pool_sup, Config),
    unlink(Pool),
    exit(Pool, kill).

run_code_synchronously(_) ->
    Self = self(),
    Test = make_ref(),
    Sleep = 200,
    {Time, Result} = timer:tc(fun() ->
        worker_pool:submit(?POOL_NAME,
            fun() ->
                timer:sleep(Sleep),
                Self ! {hi, Test},
                self()
            end,
            reuse)
    end),
    % Worker run synchronously
    true = Time > Sleep,
    % Worker have sent message
    receive {hi, Test} -> ok
    after 0 -> error(no_message_from_worker)
    end,
    % Worker is a separate process
    true = (Self /= Result).

run_code_asynchronously(_) ->
    Self = self(),
    Test = make_ref(),
    Sleep = 200,
    {Time, Result} = timer:tc(fun() ->
        worker_pool:submit_async(?POOL_NAME,
            fun() ->
                timer:sleep(Sleep),
                Self ! {hi, Test},
                self()
            end)
    end),
    % Worker run synchronously
    true = Time < Sleep,
    % Worker have sent message
    receive {hi, Test} -> ok
    after Sleep + 100 -> error(no_message_from_worker)
    end,
    % Worker is a separate process
    true = (Self /= Result).

set_timeout(_) ->
    Self = self(),
    Test = make_ref(),
    Worker = worker_pool:submit(?POOL_NAME,
        fun() ->
            Worker = self(),
            timer:sleep(100),
            worker_pool_worker:set_timeout(
                my_timeout, 1000,
                fun() ->
                    Self ! {hello, self(), Test}
                end),
            Worker
        end,
        reuse),

    % Timeout will occur after 1000 ms only
    receive {hello, Worker, Test} -> exit(timeout_should_wait)
    after 0 -> ok
    end,

    timer:sleep(1000),

    receive {hello, Worker, Test} -> ok
    after 1000 -> exit(timeout_is_late)
    end.


cancel_timeout(_) ->
    Self = self(),
    Test = make_ref(),
    Worker = worker_pool:submit(?POOL_NAME,
        fun() ->
            Worker = self(),
            timer:sleep(100),
            worker_pool_worker:set_timeout(
                my_timeout, 1000,
                fun() ->
                    Self ! {hello, self(), Test}
                end),
            Worker
        end,
        reuse),

    % Timeout will occur after 1000 ms only
    receive {hello, Worker, Test} -> exit(timeout_should_wait)
    after 0 -> ok
    end,

    worker_pool_worker:next_job_from(Worker, Self),
    Worker = worker_pool_worker:submit(Worker,
        fun() ->
            worker_pool_worker:clear_timeout(my_timeout),
            Worker
        end,
        reuse),

    timer:sleep(1000),
    receive {hello, Worker, Test} -> exit(timeout_is_not_cancelled)
    after 0 -> ok
    end.

cancel_timeout_by_setting(_) ->
    Self = self(),
    Test = make_ref(),
    Worker = worker_pool:submit(?POOL_NAME,
        fun() ->
            Worker = self(),
            timer:sleep(100),
            worker_pool_worker:set_timeout(
                my_timeout, 1000,
                fun() ->
                    Self ! {hello, self(), Test}
                end),
            Worker
        end,
        reuse),

    % Timeout will occur after 1000 ms only
    receive {hello, Worker, Test} -> exit(timeout_should_wait)
    after 0 -> ok
    end,

    worker_pool_worker:next_job_from(Worker, Self),
    Worker = worker_pool_worker:submit(Worker,
        fun() ->
            worker_pool_worker:set_timeout(my_timeout, 1000,
                fun() ->
                    Self ! {hello_reset, self(), Test}
                end),
            Worker
        end,
        reuse),

    timer:sleep(1000),
    receive {hello, Worker, Test} -> exit(timeout_is_not_cancelled)
    after 0 -> ok
    end,

    receive {hello_reset, Worker, Test} -> ok
    after 1000 -> exit(timeout_is_late)
    end.

dispatch_async_blocks_until_task_begins(_) ->
    Self = self(),

    Waiter = fun() ->
                     Self ! {register, self()},
                     receive
                         go -> ok
                     end
             end,

    ok = worker_pool:dispatch_sync(?POOL_NAME, Waiter),
    SomeWorker = receive
                     {register, WPid} -> WPid
                 after 250 ->
                         none
                 end,
    ?assert(is_process_alive(SomeWorker), "Dispatched tasks should be running"),
    spawn(fun() ->
            ok = worker_pool:dispatch_sync(?POOL_NAME,
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
