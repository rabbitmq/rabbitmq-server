
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
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(collector_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                collector_pushes_metrics,
                                collection_exits_with_server
                               ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) -> Config.

end_per_suite(Config) -> Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) -> Config.

end_per_testcase(_Testcase, Config) -> Config.


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

collector_pushes_metrics(_Config) ->
    ets:new(test_table, [set, public, named_table]),
    {ok, Coll} = gen_server:start(rabbit_mgmt_agent_collector,
                                  [self(), test_table, 100], []),
    Entry = {"one", "data"},
    ets:insert(test_table, Entry),
    receive
        {'$gen_cast', {metrics, _, Metrics}} ->
            [Entry] = Metrics
    after 1000 ->
          throw(collector_didnt_reply)
    end,
    exit(Coll, kill).

collection_exits_with_server(_Config) ->
    ets:new(test_table, [set, public, named_table]),
    Server = spawn(fun() -> receive _ -> ok end end),
    {ok, Coll} = gen_server:start(rabbit_mgmt_agent_collector,
                                  [Server, test_table, 10000], []),

    Ref = erlang:monitor(process, Coll),
    exit(Server, kill),
    receive
        {'DOWN', Ref, process, _, normal} -> ok
    after 1000 ->
              throw(collector_did_not_exit)
    end,
    exit(Coll, kill).




