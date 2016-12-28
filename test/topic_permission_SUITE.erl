%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
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

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config) when Testcase =:= topic_permission_database_access;
                                         Testcase =:= topic_permission_checks ->
    mnesia:start(),
    create_tables([rabbit_topic_permission, rabbit_user, rabbit_vhost]),
    {ok, Pool} = worker_pool_sup:start_link(1, worker_pool:default_pool()),
    {ok, Registry} = rabbit_registry:start_link(),
    {ok, Event} = rabbit_event:start_link(),
    Config1 = rabbit_ct_helpers:set_config(Config,[
        {pool_sup, Pool}, {registry_sup, Registry},
        {event_sup, Event}
    ]),
    file_handle_cache_stats:init(),
    Config1;
init_per_testcase(_Testcase, Config) ->
    Config.

create_tables(Tables) ->
    AllTables = rabbit_table:definitions(),
    [begin
         ShortDefinition = [begin
                                {Field, proplists:get_value(Field, Definition)}
                            end || Field <- [record_name, attributes]],
         mnesia:create_table(Name, ShortDefinition)
     end || {Name, Definition} <- AllTables, proplists:is_defined(Name, Tables)].

end_per_testcase(Testcase, Config)  when Testcase =:= topic_permission_database_access;
                                         Testcase =:= topic_permission_checks ->
    mnesia:stop(),
    [begin
         Sup = ?config(SupEntry, Config),
         unlink(Sup),
         exit(Sup, kill)
     end || SupEntry <- [pool_sup, registry_sup, event_sup]],
    ok;
end_per_testcase(_TC, _Config) ->
    ok.

topic_permission_database_access(_Config) ->
    0 = length(ets:tab2list(rabbit_topic_permission)),
    rabbit_misc:execute_mnesia_transaction(fun() ->
        ok = mnesia:write(rabbit_vhost,
            #vhost{virtual_host = <<"/">>},
            write),
        ok = mnesia:write(rabbit_vhost,
            #vhost{virtual_host = <<"other-vhost">>},
            write)
                                           end),
    rabbit_auth_backend_internal:add_user(<<"guest">>, <<"guest">>),
    rabbit_auth_backend_internal:add_user(<<"dummy">>, <<"dummy">>),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "^a"
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
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>, ".*"
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
        <<"guest">>, <<"/">>, <<"topic1">>, "^a"
    ),
    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"topic2">>, "^a"
    ),

    4 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    3 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    4 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"other-vhost">>),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    3 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"/">>, <<"topic1">>),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"/">>),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),


    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"non-existing-user">>, <<"other-vhost">>, <<"amq.topic">>, ".*"
    )),

    {error, {no_such_vhost, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"non-existing-vhost">>, <<"amq.topic">>, ".*"
    )),

    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"non-existing-user">>, <<"non-existing-vhost">>, <<"amq.topic">>, ".*"
    )),

    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:list_user_topic_permissions(
        "non-existing-user"
    )),

    {error, {no_such_vhost, _}} = (catch rabbit_auth_backend_internal:list_vhost_topic_permissions(
        "non-existing-vhost"
    )),

    {error, {invalid_regexp, _, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "["
    )),
    ok.

topic_permission_checks(_Config) ->
    0 = length(ets:tab2list(rabbit_topic_permission)),
    rabbit_misc:execute_mnesia_transaction(fun() ->
        ok = mnesia:write(rabbit_vhost,
            #vhost{virtual_host = <<"/">>},
            write),
        ok = mnesia:write(rabbit_vhost,
            #vhost{virtual_host = <<"other-vhost">>},
            write)
                                           end),
    rabbit_auth_backend_internal:add_user(<<"guest">>, <<"guest">>),
    rabbit_auth_backend_internal:add_user(<<"dummy">>, <<"dummy">>),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "^a"
    ),
    1 = length(ets:tab2list(rabbit_topic_permission)),
    1 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>, ".*"
    ),
    2 = length(ets:tab2list(rabbit_topic_permission)),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),

    User = #auth_user{username = <<"guest">>},
    Topic = #resource{name = <<"amq.topic">>, virtual_host = <<"/">>,
        options = #{routing_key => <<"a.b.c">>},
        kind = topic},
    %% user has access to exchange, routing key matches
    true = rabbit_auth_backend_internal:check_resource_access(
        User,
        Topic,
        write
    ),
    %% user has access to exchange, routing key does not match
    false = rabbit_auth_backend_internal:check_resource_access(
        User,
        Topic#resource{options = #{routing_key => <<"x.y.z">>}},
        write
    ),
    %% user has access to exchange but not on this vhost
    %% let pass when there's no match
    true = rabbit_auth_backend_internal:check_resource_access(
        User,
        Topic#resource{virtual_host = <<"fancyvhost">>},
        write
    ),
    %% user does not have access to exchange
    %% let pass when there's no match
    true = rabbit_auth_backend_internal:check_resource_access(
        #auth_user{username = <<"dummy">>},
        Topic,
        write
    ),
    ok.