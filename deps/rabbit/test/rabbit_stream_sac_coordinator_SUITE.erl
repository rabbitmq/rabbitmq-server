%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_sac_coordinator_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/src/rabbit_stream_sac_coordinator.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

%% replicate eunit like test resolution
all_tests() ->
    [F
     || {F, _} <- ?MODULE:module_info(functions),
        re:run(atom_to_list(F), "_test$") /= nomatch].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

ensure_monitors_test(_) ->
    GroupId = {<<"/">>, <<"stream">>, <<"app">>},
    Group =
        #group{partition_index = -1,
               consumers =
                   [#consumer{pid = self(),
                              subscription_id = 0,
                              active = true},
                    #consumer{pid = self(),
                              subscription_id = 1,
                              active = false}]},
    State0 =
        #rabbit_stream_sac_coordinator{groups = #{GroupId => Group},
                                       pids_groups = #{}},
    Monitors0 = #{},
    Command0 =
        {register_consumer, <<"/">>, <<"stream">>, -1, <<"app">>, self(), 1},
    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups}, Monitors1,
     Effects1} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command0,
                                                      State0,
                                                      Monitors0,
                                                      []),
    ?assertEqual(1, sets:size(PidsGroups)),
    ?assertEqual(#{self() => {self(), sac}}, Monitors1),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects1),
    ok.
