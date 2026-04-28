%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(auth_attempts_http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [http_get/2, http_get/3, http_get/5,
                                 http_put/4,
                                 http_delete/3, http_delete/5]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [{group, authorization}].

groups() ->
    [{authorization, [], [
        monitoring_user_can_read_auth_attempts,
        monitoring_user_cannot_delete_auth_attempts,
        admin_can_delete_auth_attempts
    ]}].

init_per_group(_Group, Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
        Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    inets:stop(),
    rabbit_ct_helpers:run_teardown_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

monitoring_user_can_read_auth_attempts(Config) ->
    try
        http_put(Config, "/users/mon",
                 #{password => <<"mon">>, tags => <<"monitoring">>},
                 {group, '2xx'}),
        Node = node_name(Config),
        http_get(Config, "/auth/attempts/" ++ Node, "mon", "mon", ?OK),
        http_get(Config, "/auth/attempts/" ++ Node ++ "/source", "mon", "mon", ?OK)
    after
        catch http_delete(Config, "/users/mon", ?NO_CONTENT)
    end.

monitoring_user_cannot_delete_auth_attempts(Config) ->
    try
        http_put(Config, "/users/mon",
                 #{password => <<"mon">>, tags => <<"monitoring">>},
                 {group, '2xx'}),
        Node = node_name(Config),
        http_delete(Config, "/auth/attempts/" ++ Node, "mon", "mon", ?NOT_AUTHORISED),
        http_delete(Config, "/auth/attempts/" ++ Node ++ "/source", "mon", "mon", ?NOT_AUTHORISED)
    after
        catch http_delete(Config, "/users/mon", ?NO_CONTENT)
    end.

admin_can_delete_auth_attempts(Config) ->
    try
        http_put(Config, "/users/admin",
                 #{password => <<"admin">>, tags => <<"administrator">>},
                 {group, '2xx'}),
        Node = node_name(Config),
        http_delete(Config, "/auth/attempts/" ++ Node, "admin", "admin", ?NO_CONTENT),
        http_delete(Config, "/auth/attempts/" ++ Node ++ "/source", "admin", "admin", ?NO_CONTENT)
    after
        catch http_delete(Config, "/users/admin", ?NO_CONTENT)
    end.

%% -------------------------------------------------------------------

node_name(Config) ->
    [NodeData] = http_get(Config, "/nodes"),
    binary_to_list(maps:get(name, NodeData)).
