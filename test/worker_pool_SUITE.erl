%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(worker_pool_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").


-define(POOL_SIZE, 1).
-define(POOL_NAME, test_pool).

all() ->
    [
    run_code_synchronously,
    run_code_asynchronously,
    set_timeout,
    cancel_timeout,
    cancel_timeout_by_setting
    ].

init_per_testcase(_, Config) ->
    {ok, Pool} = worker_pool_sup:start_link(?POOL_SIZE, ?POOL_NAME),
    rabbit_ct_helpers:set_config(Config, [{pool_sup, Pool}]).

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
