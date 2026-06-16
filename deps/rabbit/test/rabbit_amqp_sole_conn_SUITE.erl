%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_sole_conn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_amqp_sole_conn,
        [acquire/4]).

all() ->
    [
      {group, default_group}
    ].

groups() ->
    [
     {default_group, [shuffle],
      [
        refuse_connection_policy 
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(
                PrivDir,
                rabbit_misc:format("data-~ts", [node()])),
    case application:load(rabbit) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ok = application:set_env(rabbit, data_dir, DataDir),
    rabbit_khepri:setup(),

    Config.

end_per_group(_, Config) ->
    ok = khepri:stop(rabbit_khepri:get_store_id()),
    ok = ra_system:stop(coordination),
    Config.

refuse_connection_policy(_) ->
    ?assertEqual(ok, acquire(refuse_connection, <<"/">>, <<"id-1">>, self())),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, <<"/">>, <<"id-1">>, self())),
    ok.
