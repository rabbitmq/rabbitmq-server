%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(topic_permission_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

all() ->
    [
        {group, sequential_tests}
    ].

groups() -> [
        {sequential_tests, [], [
            topic_permission_database_access,
            topic_permission_checks
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, clear_tables, []),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

clear_tables() ->
    {atomic, ok} = mnesia:clear_table(rabbit_topic_permission),
    {atomic, ok} = mnesia:clear_table(rabbit_vhost),
    {atomic, ok} = mnesia:clear_table(rabbit_user),
    ok.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

topic_permission_database_access(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, topic_permission_database_access1, [Config]).

topic_permission_database_access1(_Config) ->
    0 = length(ets:tab2list(rabbit_topic_permission)),
    rabbit_vhost:add(<<"/">>, <<"acting-user">>),
    rabbit_vhost:add(<<"other-vhost">>, <<"acting-user">>),
    rabbit_auth_backend_internal:add_user(<<"guest">>, <<"guest">>, <<"acting-user">>),
    rabbit_auth_backend_internal:add_user(<<"dummy">>, <<"dummy">>, <<"acting-user">>),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "^a", "^a", <<"acting-user">>
    ),
    1 = length(ets:tab2list(rabbit_topic_permission)),
    1 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    1 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    ),
    2 = length(ets:tab2list(rabbit_topic_permission)),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    2 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"topic1">>, "^a", "^a", <<"acting-user">>
    ),
    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"topic2">>, "^a", "^a", <<"acting-user">>
    ),

    4 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    3 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    4 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"other-vhost">>,
                                                         <<"acting-user">>),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    3 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"/">>, <<"topic1">>,
                                                         <<"acting-user">>),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"/">>,
                                                         <<"acting-user">>),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),


    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"non-existing-user">>, <<"other-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    )),

    {error, {no_such_vhost, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"non-existing-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    )),

    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"non-existing-user">>, <<"non-existing-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    )),

    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:list_user_topic_permissions(
        "non-existing-user"
    )),

    {error, {no_such_vhost, _}} = (catch rabbit_auth_backend_internal:list_vhost_topic_permissions(
        "non-existing-vhost"
    )),

    {error, {invalid_regexp, _, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "[", "^a", <<"acting-user">>
    )),
    ok.

topic_permission_checks(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, topic_permission_checks1, [Config]).

topic_permission_checks1(_Config) ->
    0 = length(ets:tab2list(rabbit_topic_permission)),
    rabbit_misc:execute_mnesia_transaction(fun() ->
        ok = mnesia:write(rabbit_vhost,
            vhost:new(<<"/">>, []),
            write),
        ok = mnesia:write(rabbit_vhost,
            vhost:new(<<"other-vhost">>, []),
            write)
                                           end),
    rabbit_auth_backend_internal:add_user(<<"guest">>, <<"guest">>, <<"acting-user">>),
    rabbit_auth_backend_internal:add_user(<<"dummy">>, <<"dummy">>, <<"acting-user">>),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "^a", "^a", <<"acting-user">>
    ),
    1 = length(ets:tab2list(rabbit_topic_permission)),
    1 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    ),
    2 = length(ets:tab2list(rabbit_topic_permission)),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),

    User = #auth_user{username = <<"guest">>},
    Topic = #resource{name = <<"amq.topic">>, virtual_host = <<"/">>,
        kind = topic},
    Context = #{routing_key => <<"a.b.c">>},
    Permissions = [write, read],
    %% user has access to exchange, routing key matches
    [true = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic,
        Perm,
        Context
    ) || Perm <- Permissions],
    %% user has access to exchange, routing key does not match
    [false = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic,
        Perm,
        #{routing_key => <<"x.y.z">>}
    ) || Perm <- Permissions],
    %% user has access to exchange but not on this vhost
    %% let pass when there's no match
    [true = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic#resource{virtual_host = <<"fancyvhost">>},
        Perm,
        Context
    ) || Perm <- Permissions],
    %% user does not have access to exchange
    %% let pass when there's no match
    [true = rabbit_auth_backend_internal:check_topic_access(
        #auth_user{username = <<"dummy">>},
        Topic,
        Perm,
        Context
    ) || Perm <- Permissions],

    %% expand variables
    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>,
        "services.{vhost}.accounts.{username}.notifications",
        "services.{vhost}.accounts.{username}.notifications", <<"acting-user">>
    ),
    %% routing key OK
    [true = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic#resource{virtual_host = <<"other-vhost">>},
        Perm,
        #{routing_key   => <<"services.other-vhost.accounts.guest.notifications">>,
          variable_map  => #{
              <<"username">> => <<"guest">>,
              <<"vhost">>    => <<"other-vhost">>
          }
        }
    ) || Perm <- Permissions],
    %% routing key KO
    [false = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic#resource{virtual_host = <<"other-vhost">>},
        Perm,
        #{routing_key   => <<"services.default.accounts.dummy.notifications">>,
          variable_map  => #{
              <<"username">> => <<"guest">>,
              <<"vhost">>    => <<"other-vhost">>
          }
        }
    ) || Perm <- Permissions],

    ok.
