%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_mnesia_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [mqtt_retained_store_files_are_ignored].

init_per_testcase(Testcase, Config) ->
    DataDir = filename:join(?config(priv_dir, Config), Testcase),
    ok = filelib:ensure_path(DataDir),
    Envs = [{rabbit, data_dir, DataDir},
            {mnesia, dir, DataDir},
            {rabbit, feature_flags_file, filename:join(DataDir, "feature_flags")},
            {ra, data_dir, filename:join(DataDir, "quorum")}],
    [application:set_env(App, Key, Value) || {App, Key, Value} <- Envs],
    [{data_dir, DataDir} | Config].

end_per_testcase(_Testcase, Config) ->
    [application:unset_env(App, Key)
     || {App, Key} <- [{rabbit, data_dir}, {rabbit, feature_flags_file},
                  {mnesia, dir}, {ra, data_dir}]],
    Config.

mqtt_retained_store_files_are_ignored(Config) ->
    DataDir = ?config(data_dir, Config),
    Dets = filename:join(DataDir, "mqtt_retained_1a2b3c.dets"),
    ok = file:write_file(Dets, <<>>),
    ?assertEqual([], rabbit_mnesia:mnesia_and_msg_store_files()),
    ?assert(rabbit_mnesia:is_virgin_node()),

    ok = file:write_file(filename:join(DataDir, "schema.DAT"), <<>>),
    ?assertEqual(["schema.DAT"], rabbit_mnesia:mnesia_and_msg_store_files()),
    ?assertNot(rabbit_mnesia:is_virgin_node()).
