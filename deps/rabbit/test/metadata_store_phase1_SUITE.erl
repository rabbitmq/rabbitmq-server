%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(metadata_store_phase1_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("khepri/include/khepri.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2,

         write_non_existing_vhost/1,
         write_existing_vhost/1,
         check_vhost_exists/1,
         list_vhost_names/1,
         list_vhost_objects/1,
         update_non_existing_vhost/1,
         update_existing_vhost/1,
         update_non_existing_vhost_desc_and_tags/1,
         update_existing_vhost_desc_and_tags/1,
         delete_non_existing_vhost/1,
         delete_existing_vhost/1,

         write_non_existing_user/1,
         write_existing_user/1,
         list_users/1,
         update_non_existing_user/1,
         update_existing_user/1,
         delete_non_existing_user/1,
         delete_existing_user/1,

         write_user_permission_for_non_existing_vhost/1,
         write_user_permission_for_non_existing_user/1,
         write_user_permission_for_existing_user/1,
         check_resource_access/1,
         list_user_permissions_on_non_existing_vhost/1,
         list_user_permissions_for_non_existing_user/1,
         list_user_permissions/1,
         clear_user_permission_for_non_existing_vhost/1,
         clear_user_permission_for_non_existing_user/1,
         clear_user_permission/1,
         delete_user_and_check_resource_access/1,
         delete_vhost_and_check_resource_access/1,

         write_topic_permission_for_non_existing_vhost/1,
         write_topic_permission_for_non_existing_user/1,
         write_topic_permission_for_existing_user/1,
         list_topic_permissions_on_non_existing_vhost/1,
         list_topic_permissions_for_non_existing_user/1,
         list_topic_permissions/1,
         clear_specific_topic_permission_for_non_existing_vhost/1,
         clear_specific_topic_permission_for_non_existing_user/1,
         clear_specific_topic_permission/1,
         clear_all_topic_permission_for_non_existing_vhost/1,
         clear_all_topic_permission_for_non_existing_user/1,
         clear_all_topic_permissions/1,
         delete_user_and_check_topic_access/1,
         delete_vhost_and_check_topic_access/1
        ]).

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
     {group, vhosts},
     {group, internal_users}
    ].

groups() ->
    [
     {vhosts, [],
      [
       write_non_existing_vhost,
       write_existing_vhost,
       check_vhost_exists,
       list_vhost_names,
       list_vhost_objects,
       update_non_existing_vhost,
       update_existing_vhost,
       update_non_existing_vhost_desc_and_tags,
       update_existing_vhost_desc_and_tags,
       delete_non_existing_vhost,
       delete_existing_vhost
      ]
     },
     {internal_users, [],
      [
       {users, [],
        [
         write_non_existing_user,
         write_existing_user,
         list_users,
         update_non_existing_user,
         update_existing_user,
         delete_non_existing_user,
         delete_existing_user
        ]
       },
       {user_permissions, [],
        [
         write_user_permission_for_non_existing_vhost,
         write_user_permission_for_non_existing_user,
         write_user_permission_for_existing_user,
         check_resource_access,
         list_user_permissions_on_non_existing_vhost,
         list_user_permissions_for_non_existing_user,
         list_user_permissions,
         clear_user_permission_for_non_existing_vhost,
         clear_user_permission_for_non_existing_user,
         clear_user_permission,
         delete_user_and_check_resource_access,
         delete_vhost_and_check_resource_access
        ]
       },
       {topic_permissions, [],
        [
         write_topic_permission_for_non_existing_vhost,
         write_topic_permission_for_non_existing_user,
         write_topic_permission_for_existing_user,
         list_topic_permissions_on_non_existing_vhost,
         list_topic_permissions_for_non_existing_user,
         list_topic_permissions,
         clear_specific_topic_permission_for_non_existing_vhost,
         clear_specific_topic_permission_for_non_existing_user,
         clear_specific_topic_permission,
         clear_all_topic_permission_for_non_existing_vhost,
         clear_all_topic_permission_for_non_existing_user,
         clear_all_topic_permissions,
         delete_user_and_check_topic_access,
         delete_vhost_and_check_topic_access
        ]
       }
      ]
     }
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [
       fun init_feature_flags/1,
       fun setup_code_mocking/1,
       fun setup_mnesia/1,
       fun setup_khepri/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      [
       fun remove_code_mocking/1
      ]).

setup_mnesia(Config) ->
    %% Configure Mnesia directory in the common_test priv_dir and start it.
    MnesiaDir = filename:join(
                  ?config(priv_dir, Config),
                  "mnesia"),
    ct:pal("Mnesia directory: ~ts", [MnesiaDir]),
    ok = file:make_dir(MnesiaDir),
    ok = application:load(mnesia),
    ok = application:set_env(mnesia, dir, MnesiaDir),
    ok = application:set_env(rabbit, data_dir, MnesiaDir),
    ok = mnesia:create_schema([node()]),
    {ok, _} = application:ensure_all_started(mnesia),

    ct:pal("Mnesia info below:"),
    mnesia:info(),
    Config.

setup_khepri(Config) ->
    %% Start Khepri.
    {ok, _} = application:ensure_all_started(khepri),

    %% Configure Khepri. It takes care of configuring Ra system & cluster. It
    %% uses the Mnesia directory to store files.
    ok = rabbit_khepri:setup(undefined),

    ct:pal("Khepri info below:"),
    rabbit_khepri:info(),
    Config.

setup_code_mocking(Config) ->
    %% Bypass rabbit_mnesia:execute_mnesia_transaction/1 (no worker_pool
    %% configured in particular) but keep the behavior of throwing the error.
    meck:new(rabbit_mnesia, [passthrough, no_link]),
    meck:expect(
      rabbit_mnesia, execute_mnesia_transaction,
      fun(Fun) ->
              case mnesia:sync_transaction(Fun) of
                  {atomic, Result}  -> Result;
                  {aborted, Reason} -> throw({error, Reason})
              end
      end),
    ?assert(meck:validate(rabbit_mnesia)),

    %% Bypass calls inside rabbit_vhost:vhost_cluster_state/1 because these
    %% are unit testcases without any sort of clustering.
    meck:new(rabbit_nodes, [passthrough, no_link]),
    meck:expect(
      rabbit_nodes, all_running,
      fun() -> [node()] end),

    meck:new(rabbit_vhost_sup_sup, [passthrough, no_link]),
    meck:expect(
      rabbit_vhost_sup_sup, is_vhost_alive,
      fun(_) -> true end),

    %% We ensure that we use the `vhost_v2` #vhost{} record so we can play
    %% with the description and tags.
    meck:new(rabbit_feature_flags, [passthrough, no_link]),
    meck:expect(
      rabbit_feature_flags, is_enabled,
      fun
          (virtual_host_metadata) -> true;
          (FeatureNames)           -> meck:passthrough([FeatureNames])
      end),

    ct:pal("Mocked: ~p", [meck:mocked()]),
    Config.

remove_code_mocking(Config) ->
    lists:foreach(
      fun(Mod) -> meck:unload(Mod) end,
      meck:mocked()),
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),

    rabbit_khepri:clear_forced_metadata_store(),

    %% Create Mnesia tables.
    TableDefs = rabbit_table:pre_khepri_definitions(),
    lists:foreach(
      fun ({Table, Def}) -> ok = rabbit_table:create(Table, Def) end,
      TableDefs),

    Config.

end_per_testcase(Testcase, Config) ->
    rabbit_khepri:clear_forced_metadata_store(),

    %% Delete Mnesia tables to clear any data.
    TableDefs = rabbit_table:pre_khepri_definitions(),
    lists:foreach(
      fun ({Table, _}) -> {atomic, ok} = mnesia:delete_table(Table) end,
      TableDefs),

    %% Clear all data in Khepri.
    ok = rabbit_khepri:clear_store(),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

init_feature_flags(Config) ->
    FFFile = filename:join(
                  ?config(priv_dir, Config),
                  "feature_flags"),
    ct:pal("Feature flags file: ~ts", [FFFile]),
    ok = application:load(rabbit),
    ok = application:set_env(rabbit, feature_flags_file, FFFile),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% We use `_With' (with the leading underscore) on purpose: we don't know if
%% the code in `T' will use it. That code can still use `_With' of course.
%% This simply avoids compiler warnings.
-define(with(T), fun(_With) -> T end).

-define(vhost_path(V),
        [rabbit_db_vhost, V]).
-define(user_path(U),
        [rabbit_db_user, users, U]).
-define(user_perm_path(U, V),
        [rabbit_db_user, users, U, user_permissions, V]).
-define(topic_perm_path(U, V, E),
        [rabbit_db_user, users, U, topic_permissions, V, E]).

%%
%% Virtual hosts.
%%

write_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

write_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {existing, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

check_vhost_exists(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assert(
              vhost_exists(_With, VHostName))),
     ?with(?assertNot(
              vhost_exists(_With, <<"non-existing-vhost">>))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_vhost_names(_) ->
    VHostNameA = <<"vhost-a">>,
    VHostDescA = <<>>,
    VHostTagsA = [],
    VHostA = vhost:new(
               VHostNameA,
               VHostTagsA,
               #{description => VHostDescA,
                 tags => VHostTagsA}),
    VHostNameB = <<"vhost-b">>,
    VHostDescB = <<>>,
    VHostTagsB = [],
    VHostB = vhost:new(
               VHostNameB,
               VHostTagsB,
               #{description => VHostDescB,
                 tags => VHostTagsB}),

    Tests =
    [
     ?with(?assertEqual(
              {new, VHostA},
              add_vhost(_With, VHostNameA, VHostA))),
     ?with(?assertEqual(
              {new, VHostB},
              add_vhost(_With, VHostNameB, VHostB))),
     ?with(?assertEqual(
              [VHostNameA, VHostNameB],
              list_vhosts(_With))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostNameA) => VHostA,
                 ?vhost_path(VHostNameB) => VHostB}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_vhost_objects(_) ->
    VHostNameA = <<"vhost-a">>,
    VHostDescA = <<>>,
    VHostTagsA = [],
    VHostA = vhost:new(
               VHostNameA,
               VHostTagsA,
               #{description => VHostDescA,
                 tags => VHostTagsA}),
    VHostNameB = <<"vhost-b">>,
    VHostDescB = <<>>,
    VHostTagsB = [],
    VHostB = vhost:new(
               VHostNameB,
               VHostTagsB,
               #{description => VHostDescB,
                 tags => VHostTagsB}),

    Tests =
    [
     ?with(?assertEqual(
              {new, VHostA},
              add_vhost(_With, VHostNameA, VHostA))),
     ?with(?assertEqual(
              {new, VHostB},
              add_vhost(_With, VHostNameB, VHostB))),
     ?with(?assertEqual(
              [VHostA, VHostB],
              list_vhost_records(_With))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostNameA) => VHostA,
                 ?vhost_path(VHostNameB) => VHostB}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

update_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    UpdatedVHost = vhost:set_limits(VHost, [limits]),
    Fun = fun(_) -> UpdatedVHost end,
    ?assertNotEqual(VHost, UpdatedVHost),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              update_vhost(_With, VHostName, Fun))),
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_db_vhost],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

update_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    UpdatedVHost = vhost:set_limits(VHost, [limits]),
    Fun = fun(_) -> UpdatedVHost end,
    ?assertNotEqual(VHost, UpdatedVHost),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              UpdatedVHost,
              update_vhost(_With, VHostName, Fun))),
     ?with(?assertEqual(
              UpdatedVHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [UpdatedVHost]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => UpdatedVHost}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

update_non_existing_vhost_desc_and_tags(_) ->
    VHostName = <<"vhost">>,
    NewVHostDesc = <<"New desc">>,
    NewVHostTags = [new_tag],

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {error, {no_such_vhost, VHostName}},
              update_vhost(_With, VHostName, NewVHostDesc, NewVHostTags))),
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_db_vhost],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

update_existing_vhost_desc_and_tags(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    NewVHostDesc = <<"New desc">>,
    NewVHostTags = [new_tag],
    UpdatedVHost = vhost:set_metadata(
                     VHost,
                     #{description => NewVHostDesc,
                       tags => NewVHostTags}),
    ct:pal("VHost: ~p~nUpdatedVHost: ~p", [VHost, UpdatedVHost]),
    ?assertNotEqual(VHost, UpdatedVHost),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              {ok, UpdatedVHost},
              update_vhost(_With, VHostName, NewVHostDesc, NewVHostTags))),
     ?with(?assertEqual(
              UpdatedVHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [UpdatedVHost]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => UpdatedVHost}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              false,
              delete_vhost(_With, VHostName))),
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_db_vhost],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              true,
              delete_vhost(_With, VHostName))),
     ?with(?assertEqual(
              undefined,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_db_vhost],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

%%
%% Users.
%%

write_non_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              User,
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [User]},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

write_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              User,
              lookup_user(_With, Username))),
     ?with(?assertThrow(
              {error, {user_already_exists, Username}},
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              User,
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [User]},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_users(_) ->
    UsernameA = <<"alice">>,
    UserA = internal_user:create_user(UsernameA, <<"password">>, undefined),
    UsernameB = <<"bob">>,
    UserB = internal_user:create_user(UsernameB, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameA, UserA))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameB, UserB))),
     ?with(?assertEqual(
              [UserA, UserB],
              list_user_records(_With))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [UserA, UserB]},
              {khepri, [rabbit_db_user],
               #{?user_path(UsernameA) => UserA,
                 ?user_path(UsernameB) => UserB}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

update_non_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UpdatedUser = internal_user:set_password_hash(
                    User, <<"updated-pw">>, undefined),
    Fun = fun(_) -> UpdatedUser end,
    ?assertNotEqual(User, UpdatedUser),

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              update_user(_With, Username, Fun))),
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, []},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

update_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UpdatedUser = internal_user:set_password_hash(
                    User, <<"updated-pw">>, undefined),
    Fun = fun(_) -> UpdatedUser end,
    ?assertNotEqual(User, UpdatedUser),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              User,
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              update_user(_With, Username, Fun))),
     ?with(?assertEqual(
              UpdatedUser,
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [UpdatedUser]},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => UpdatedUser}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_non_existing_user(_) ->
    Username = <<"alice">>,

    Tests =
    [
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              false,
              delete_user(_With, Username))),
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, []},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              User,
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              true,
              delete_user(_With, Username))),
     ?with(?assertEqual(
              undefined,
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, []},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

%%
%% User permissions.
%%

write_user_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<>>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

write_user_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<>>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

write_user_permission_for_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<>>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertEqual(
              ok,
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, [UserPermission]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User,
                 ?user_perm_path(Username, VHostName) => UserPermission}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

check_resource_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(?assertNot(
              check_resource_access(
                _With, Username, VHostName, "my-resource", write))),
     ?with(?assertNot(
              check_resource_access(
                _With, Username, VHostName, "other-resource", configure)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_user_permissions_on_non_existing_vhost(_) ->
    VHostName = <<"non-existing-vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              list_user_vhost_permissions(_With, '_', VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              list_user_vhost_permissions(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost], #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_user_permissions_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"non-existing-user">>,

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              [],
              list_user_vhost_permissions(_With, '_', VHostName))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              list_user_vhost_permissions(_With, Username, '_'))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              list_user_vhost_permissions(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost], #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user], #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_user_permissions(_) ->
    VHostNameA = <<"vhost-a">>,
    VHostDescA = <<>>,
    VHostTagsA = [],
    VHostA = vhost:new(
               VHostNameA,
               VHostTagsA,
               #{description => VHostDescA,
                 tags => VHostTagsA}),
    VHostNameB = <<"vhost-b">>,
    VHostDescB = <<>>,
    VHostTagsB = [],
    VHostB = vhost:new(
               VHostNameB,
               VHostTagsB,
               #{description => VHostDescB,
                 tags => VHostTagsB}),
    UsernameA = <<"alice">>,
    UserA = internal_user:create_user(UsernameA, <<"password">>, undefined),
    UsernameB = <<"bob">>,
    UserB = internal_user:create_user(UsernameB, <<"password">>, undefined),

    UserPermissionA1 = #user_permission{
                          user_vhost = #user_vhost{
                                          username     = UsernameA,
                                          virtual_host = VHostNameA},
                          permission = #permission{
                                          configure  = <<"my-resource">>,
                                          write      = <<>>,
                                          read       = <<>>}},
    UserPermissionA2 = #user_permission{
                          user_vhost = #user_vhost{
                                          username     = UsernameA,
                                          virtual_host = VHostNameB},
                          permission = #permission{
                                          configure  = <<"my-resource">>,
                                          write      = <<>>,
                                          read       = <<>>}},
    UserPermissionB1 = #user_permission{
                          user_vhost = #user_vhost{
                                          username     = UsernameB,
                                          virtual_host = VHostNameA},
                          permission = #permission{
                                          configure  = <<"my-resource">>,
                                          write      = <<>>,
                                          read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHostA},
              add_vhost(_With, VHostNameA, VHostA))),
     ?with(?assertEqual(
              {new, VHostB},
              add_vhost(_With, VHostNameB, VHostB))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameA, UserA))),
     ?with(?assertEqual(
              ok,
              set_permissions(
                _With, UsernameA, VHostNameA, UserPermissionA1))),
     ?with(?assertEqual(
              ok,
              set_permissions(
                _With, UsernameA, VHostNameB, UserPermissionA2))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameB, UserB))),
     ?with(?assertEqual(
              ok,
              set_permissions(
                _With, UsernameB, VHostNameA, UserPermissionB1))),
     ?with(?assertEqual(
              [UserPermissionA1, UserPermissionA2, UserPermissionB1],
              list_user_vhost_permissions(_With, '_', '_'))),
     ?with(?assertEqual(
              [UserPermissionA1, UserPermissionB1],
              list_user_vhost_permissions(_With, '_', VHostNameA))),
     ?with(?assertEqual(
              [UserPermissionA1, UserPermissionA2],
              list_user_vhost_permissions(_With, UsernameA, '_'))),
     ?with(?assertEqual(
              [UserPermissionA1],
              list_user_vhost_permissions(_With, UsernameA, VHostNameA))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {mnesia, rabbit_user, [UserA, UserB]},
              {mnesia, rabbit_user_permission, [UserPermissionA1,
                                                UserPermissionA2,
                                                UserPermissionB1]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostNameA) => VHostA,
                 ?vhost_path(VHostNameB) => VHostB}},
              {khepri, [rabbit_db_user],
               #{?user_path(UsernameA) => UserA,
                 ?user_path(UsernameB) => UserB,
                 ?user_perm_path(UsernameA, VHostNameA) =>
                 UserPermissionA1,
                 ?user_perm_path(UsernameA, VHostNameB) =>
                 UserPermissionA2,
                 ?user_perm_path(UsernameB, VHostNameA) =>
                 UserPermissionB1}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_user_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertEqual(
              ok,
              clear_permissions(_With, Username, VHostName))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_user_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertEqual(
              ok,
              clear_permissions(_With, Username, VHostName))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_user_permission(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(?assertEqual(
              ok,
              clear_permissions(_With, Username, VHostName))),
     ?with(?assertNot(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_user_and_check_resource_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assert(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(?assertEqual(
              true,
              delete_user(_With, Username))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertNot(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_vhost_and_check_resource_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(_With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assert(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(?assertEqual(
              true,
              delete_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, [UserPermission]},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    %% In mnesia the permissions have to be deleted explicitly
    %% Khepri permissions have a condition to automatically delete them
    %% when the vhost is deleted
    MnesiaTests =
        [?with(?assert(
                  check_vhost_access(_With, Username, VHostName))),
         ?with(?assert(
                  check_resource_access(
                    _With, Username, VHostName, "my-resource", configure)))],

    KhepriTests =
        [?with(?assertNot(
                  check_vhost_access(_With, Username, VHostName))),
         ?with(?assertNot(
                  check_resource_access(
                    _With, Username, VHostName, "my-resource", configure)))],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests ++ MnesiaTests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests ++ KhepriTests}]}],
         [verbose])).

%%
%% Topic permissions.
%%

write_topic_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<>>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              set_topic_permissions(
                _With, Username, VHostName, TopicPermission))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

write_topic_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<>>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              set_topic_permissions(
                _With, Username, VHostName, TopicPermission))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

write_topic_permission_for_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, [TopicPermission]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User,
                 ?topic_perm_path(Username, VHostName, Exchange) =>
                 TopicPermission}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_topic_permissions_on_non_existing_vhost(_) ->
    VHostName = <<"non-existing-vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              list_topic_permissions(_With, '_', VHostName, '_'))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              list_topic_permissions(
                _With, Username, VHostName, '_'))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_topic_permissions_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"non-existing-user">>,

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              [],
              list_topic_permissions(_With, '_', VHostName, '_'))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              list_topic_permissions(_With, Username, '_', '_'))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              list_topic_permissions(_With, Username, VHostName, '_'))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_db_vhost], #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user], #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

list_topic_permissions(_) ->
    VHostNameA = <<"vhost-a">>,
    VHostDescA = <<>>,
    VHostTagsA = [],
    VHostA = vhost:new(
               VHostNameA,
               VHostTagsA,
               #{description => VHostDescA,
                 tags => VHostTagsA}),
    VHostNameB = <<"vhost-b">>,
    VHostDescB = <<>>,
    VHostTagsB = [],
    VHostB = vhost:new(
               VHostNameB,
               VHostTagsB,
               #{description => VHostDescB,
                 tags => VHostTagsB}),
    UsernameA = <<"alice">>,
    UserA = internal_user:create_user(UsernameA, <<"password">>, undefined),
    UsernameB = <<"bob">>,
    UserB = internal_user:create_user(UsernameB, <<"password">>, undefined),

    ExchangeA = <<"exchange-a">>,
    ExchangeB = <<"exchange-b">>,
    TopicPermissionA1 = #topic_permission{
                           topic_permission_key =
                           #topic_permission_key{
                              user_vhost = #user_vhost{
                                              username = UsernameA,
                                              virtual_host = VHostNameA},
                              exchange = ExchangeA},
                           permission = #permission{
                                           write = <<>>,
                                           read = <<"^key$">>}
                          },
    TopicPermissionA2 = #topic_permission{
                           topic_permission_key =
                           #topic_permission_key{
                              user_vhost = #user_vhost{
                                              username = UsernameA,
                                              virtual_host = VHostNameB},
                              exchange = ExchangeB},
                           permission = #permission{
                                           write = <<>>,
                                           read = <<"^key$">>}
                          },
    TopicPermissionB1 = #topic_permission{
                           topic_permission_key =
                           #topic_permission_key{
                              user_vhost = #user_vhost{
                                              username = UsernameB,
                                              virtual_host = VHostNameA},
                              exchange = ExchangeA},
                           permission = #permission{
                                           write = <<>>,
                                           read = <<"^key$">>}
                          },

    Tests =
    [
     ?with(?assertEqual(
              {new, VHostA},
              add_vhost(_With, VHostNameA, VHostA))),
     ?with(?assertEqual(
              {new, VHostB},
              add_vhost(_With, VHostNameB, VHostB))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameA, UserA))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, UsernameA, VHostNameA, TopicPermissionA1))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, UsernameA, VHostNameB, TopicPermissionA2))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameB, UserB))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, UsernameB, VHostNameA, TopicPermissionB1))),
     ?with(?assertEqual(
              [TopicPermissionA1, TopicPermissionA2, TopicPermissionB1],
              list_topic_permissions(_With, '_', '_', '_'))),
     ?with(?assertEqual(
              [TopicPermissionA1, TopicPermissionB1],
              list_topic_permissions(_With, '_', VHostNameA, '_'))),
     ?with(?assertEqual(
              [TopicPermissionA1, TopicPermissionA2],
              list_topic_permissions(_With, UsernameA, '_', '_'))),
     ?with(?assertEqual(
              [TopicPermissionA1],
              list_topic_permissions(_With, UsernameA, VHostNameA, '_'))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {mnesia, rabbit_user, [UserA, UserB]},
              {mnesia, rabbit_topic_permission, [TopicPermissionA1,
                                                 TopicPermissionA2,
                                                 TopicPermissionB1]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostNameA) => VHostA,
                 ?vhost_path(VHostNameB) => VHostB}},
              {khepri, [rabbit_db_user],
               #{?user_path(UsernameA) => UserA,
                 ?user_path(UsernameB) => UserB,
                 ?topic_perm_path(UsernameA, VHostNameA, ExchangeA) =>
                 TopicPermissionA1,
                 ?topic_perm_path(UsernameA, VHostNameB, ExchangeB) =>
                 TopicPermissionA2,
                 ?topic_perm_path(UsernameB, VHostNameA, ExchangeA) =>
                 TopicPermissionB1}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_specific_topic_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(_With, Username, VHostName, Exchange))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_specific_topic_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    Exchange = <<"exchange">>,
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(_With, Username, VHostName, Exchange))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_specific_topic_permission(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    ExchangeA = <<"exchange-a">>,
    ExchangeB = <<"exchange-b">>,
    TopicPermissionA = #topic_permission{
                          topic_permission_key =
                          #topic_permission_key{
                             user_vhost = #user_vhost{
                                             username = Username,
                                             virtual_host = VHostName},
                             exchange = ExchangeA},
                          permission = #permission{
                                          write = <<>>,
                                          read = <<"^key$">>}
                         },
    TopicPermissionB = #topic_permission{
                          topic_permission_key =
                          #topic_permission_key{
                             user_vhost = #user_vhost{
                                             username = Username,
                                             virtual_host = VHostName},
                             exchange = ExchangeB},
                          permission = #permission{
                                          write = <<>>,
                                          read = <<"^key$">>}
                         },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermissionA))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermissionB))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(_With, Username, VHostName, ExchangeA))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read, Context))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, [TopicPermissionB]},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User,
                 ?topic_perm_path(Username, VHostName, ExchangeB) =>
                 TopicPermissionB}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_all_topic_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(_With, Username, VHostName, '_'))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_all_topic_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    Exchange = <<"exchange">>,
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(_With, Username, VHostName, '_'))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

clear_all_topic_permissions(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    ExchangeA = <<"exchange-a">>,
    ExchangeB = <<"exchange-b">>,
    TopicPermissionA = #topic_permission{
                          topic_permission_key =
                          #topic_permission_key{
                             user_vhost = #user_vhost{
                                             username = Username,
                                             virtual_host = VHostName},
                             exchange = ExchangeA},
                          permission = #permission{
                                          write = <<>>,
                                          read = <<"^key$">>}
                         },
    TopicPermissionB = #topic_permission{
                          topic_permission_key =
                          #topic_permission_key{
                             user_vhost = #user_vhost{
                                             username = Username,
                                             virtual_host = VHostName},
                             exchange = ExchangeB},
                          permission = #permission{
                                          write = <<>>,
                                          read = <<"^key$">>}
                         },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermissionA))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermissionB))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(_With, Username, VHostName, '_'))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read, Context))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read, Context))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_user_and_check_topic_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              true,
              delete_user(_With, Username))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              undefined,
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_db_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_db_user],
               #{}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

delete_vhost_and_check_topic_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost:new(
              VHostName,
              VHostTags,
              #{description => VHostDesc,
                tags => VHostTags}),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              {new, VHost},
              add_vhost(_With, VHostName, VHost))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assert(
              delete_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, [TopicPermission]},
              {khepri, [rabbit_db_vhost],
               #{}},
              {khepri, [rabbit_db_user],
               #{?user_path(Username) => User}}]))
    ],

    %% In mnesia the permissions have to be deleted explicitly
    %% Khepri permissions have a condition to automatically delete them
    %% when the vhost is deleted
    MnesiaTests =
        [?with(?assert(
                  check_topic_access(
                    _With, Username, VHostName, Exchange, read, Context))),
         ?with(?assertNot(
                  check_topic_access(
                    _With, Username, VHostName, Exchange, read,
                    Context#{routing_key => <<"something-else">>})))],

    KhepriTests =
        [?with(?assertEqual(
                  undefined,
                  check_topic_access(
                    _With, Username, VHostName, Exchange, read, Context))),
         ?with(?assertEqual(
                  undefined,
                  check_topic_access(
                    _With, Username, VHostName, Exchange, read,
                    Context#{routing_key => <<"something-else">>})))],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests ++ MnesiaTests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests ++ KhepriTests}]}],
         [verbose])).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

force_mnesia_use() ->
    ct:pal(?LOW_IMPORTANCE, "Using Mnesia (disabling feature flag)", []),
    rabbit_khepri:force_metadata_store(mnesia).

force_khepri_use() ->
    ct:pal(?LOW_IMPORTANCE, "Using Khepri (enabling feature flag)", []),
    rabbit_khepri:force_metadata_store(khepri).

add_vhost(mnesia, VHostName, VHost) ->
    rabbit_db_vhost:create_or_get_in_mnesia(VHostName, VHost);
add_vhost(khepri, VHostName, VHost) ->
    rabbit_db_vhost:create_or_get_in_khepri(VHostName, VHost).

lookup_vhost(mnesia, VHostName) ->
    rabbit_db_vhost:get_in_mnesia(VHostName);
lookup_vhost(khepri, VHostName) ->
    rabbit_db_vhost:get_in_khepri(VHostName).

vhost_exists(mnesia, VHostName) ->
    rabbit_db_vhost:exists_in_mnesia(VHostName);
vhost_exists(khepri, VHostName) ->
    rabbit_db_vhost:exists_in_khepri(VHostName).

list_vhosts(mnesia) ->
    lists:sort(rabbit_db_vhost:list_in_mnesia());
list_vhosts(khepri) ->
    lists:sort(rabbit_db_vhost:list_in_khepri()).

list_vhost_records(mnesia) ->
    lists:sort(rabbit_db_vhost:get_all_in_mnesia());
list_vhost_records(khepri) ->
    lists:sort(rabbit_db_vhost:get_all_in_khepri()).

update_vhost(mnesia, VHostName, Fun) ->
    rabbit_db_vhost:update_in_mnesia(VHostName, Fun);
update_vhost(khepri, VHostName, Fun) ->
    rabbit_db_vhost:update_in_khepri(VHostName, Fun).

update_vhost(mnesia, VHostName, Description, Tags) ->
    rabbit_db_vhost:merge_metadata_in_mnesia(VHostName,
                                             #{description => Description,
                                               tags => Tags});
update_vhost(khepri, VHostName, Description, Tags) ->
    rabbit_db_vhost:merge_metadata_in_khepri(VHostName,
                                             #{description => Description,
                                               tags => Tags}).

delete_vhost(mnesia, VHostName) ->
    rabbit_db_vhost:delete_in_mnesia(VHostName);
delete_vhost(khepri, VHostName) ->
    rabbit_db_vhost:delete_in_khepri(VHostName).

add_user(mnesia, Username, User) ->
    rabbit_db_user:create_in_mnesia(Username, User);
add_user(khepri, Username, User) ->
    rabbit_db_user:create_in_khepri(Username, User).

lookup_user(mnesia, Username) ->
    rabbit_db_user:get_in_mnesia(Username);
lookup_user(khepri, Username) ->
    rabbit_db_user:get_in_khepri(Username).

list_user_records(mnesia) ->
    lists:sort(rabbit_db_user:get_all_in_mnesia());
list_user_records(khepri) ->
    lists:sort(rabbit_db_user:get_all_in_khepri()).

update_user(mnesia, Username, Fun) ->
    rabbit_db_user:update_in_mnesia(Username, Fun);
update_user(khepri, Username, Fun) ->
    rabbit_db_user:update_in_khepri(Username, Fun).

delete_user(mnesia, Username) ->
    rabbit_db_user:delete_in_mnesia(Username);
delete_user(khepri, Username) ->
    rabbit_db_user:delete_in_khepri(Username).

set_permissions(mnesia, Username, VHostName, UserPermission) ->
    rabbit_db_user:set_user_permissions_in_mnesia(
      Username, VHostName, UserPermission);
set_permissions(khepri, Username, VHostName, UserPermission) ->
    rabbit_db_user:set_user_permissions_in_khepri(
      Username, VHostName, UserPermission).

list_user_vhost_permissions(mnesia, Username, VHostName) ->
    lists:sort(
      rabbit_db_user:match_user_permissions_in_mnesia(Username, VHostName));
list_user_vhost_permissions(khepri, Username, VHostName) ->
    lists:sort(
      rabbit_db_user:match_user_permissions_in_khepri(Username, VHostName)).

list_topic_permissions(mnesia, Username, VHostName, ExchangeName) ->
    lists:sort(
      rabbit_db_user:match_topic_permissions_in_mnesia(Username, VHostName, ExchangeName));
list_topic_permissions(khepri, Username, VHostName, ExchangeName) ->
    lists:sort(
      rabbit_db_user:match_topic_permissions_in_khepri(Username, VHostName, ExchangeName)).

check_vhost_access(mnesia, Username, VHostName) ->
    rabbit_db_user:get_user_permissions_in_mnesia(
      Username, VHostName) =/= undefined;
check_vhost_access(khepri, Username, VHostName) ->
    rabbit_db_user:get_user_permissions_in_khepri(
      Username, VHostName) =/= undefined.

set_topic_permissions(
  mnesia, Username, VHostName, TopicPermission) ->
    rabbit_db_user:set_topic_permissions_in_mnesia(
      Username, VHostName, TopicPermission);
set_topic_permissions(
  khepri, Username, VHostName, TopicPermission) ->
    rabbit_db_user:set_topic_permissions_in_khepri(
      Username, VHostName, TopicPermission).

check_topic_access(mnesia, Username, VHostName, Exchange, Perm, Context) ->
    case rabbit_db_user:get_topic_permissions_in_mnesia(
           Username, VHostName, Exchange) of
        undefined -> undefined;
        #topic_permission{permission = P} ->
            PermRegexp = case element(permission_index(Perm), P) of
                             <<"">> -> <<$^, $$>>;
                             RE     -> RE
                         end,
            case re:run(maps:get(routing_key, Context), PermRegexp, [{capture, none}]) of
                match    -> true;
                nomatch  -> false
            end
    end;
check_topic_access(khepri, Username, VHostName, Exchange, Perm, Context) ->
    case rabbit_db_user:get_topic_permissions_in_khepri(
           Username, VHostName, Exchange) of
        undefined -> undefined;
        #topic_permission{permission = P} ->
            PermRegexp = case element(permission_index(Perm), P) of
                             <<"">> -> <<$^, $$>>;
                             RE     -> RE
                         end,
            case re:run(maps:get(routing_key, Context), PermRegexp, [{capture, none}]) of
                match    -> true;
                nomatch  -> false
            end
    end.

clear_permissions(mnesia, Username, VHostName) ->
    rabbit_db_user:clear_user_permissions_in_mnesia(
      Username, VHostName);
clear_permissions(khepri, Username, VHostName) ->
    rabbit_db_user:clear_user_permissions_in_khepri(
      Username, VHostName).

check_resource_access(mnesia, Username, VHostName, Resource, Perm) ->
    case rabbit_db_user:get_user_permissions_in_mnesia(Username, VHostName) of
        undefined -> false;
        #user_permission{permission = P} ->
            PermRegexp = case element(permission_index(Perm), P) of
                             <<"">> -> <<$^, $$>>;
                             RE     -> RE
                         end,
            case re:run(Resource, PermRegexp, [{capture, none}]) of
                match    -> true;
                nomatch  -> false
            end
    end;
check_resource_access(khepri, Username, VHostName, Resource, Perm) ->
    case rabbit_db_user:get_user_permissions_in_khepri(Username, VHostName) of
        undefined -> false;
        #user_permission{permission = P} ->
            PermRegexp = case element(permission_index(Perm), P) of
                             <<"">> -> <<$^, $$>>;
                             RE     -> RE
                         end,
            case re:run(Resource, PermRegexp, [{capture, none}]) of
                match    -> true;
                nomatch  -> false
            end
    end.

permission_index(configure) -> #permission.configure;
permission_index(write)     -> #permission.write;
permission_index(read)      -> #permission.read.

clear_topic_permissions(mnesia, Username, VHostName, Exchange) ->
    rabbit_db_user:clear_topic_permissions_in_mnesia(
      Username, VHostName, Exchange);
clear_topic_permissions(khepri, Username, VHostName, Exchange) ->
    rabbit_db_user:clear_topic_permissions_in_khepri(
      Username, VHostName, Exchange).

check_storage(With, [{With, Source, Content} | Rest]) ->
    check_storage(With, Source, Content),
    check_storage(With, Rest);
check_storage(With, [_ | Rest]) ->
    check_storage(With, Rest);
check_storage(_, []) ->
    ok.

check_storage(mnesia, Table, Content) ->
    ?assertEqual(Content, lists:sort(ets:tab2list(Table)));
check_storage(khepri, Path, Content) ->
    rabbit_khepri:info(),
    Path1 = Path ++ [#if_all{conditions = [?KHEPRI_WILDCARD_STAR_STAR,
                                           #if_has_data{has_data = true}]}],
    ?assertEqual({ok, Content}, rabbit_khepri:match(Path1)).
