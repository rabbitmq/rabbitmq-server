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
         get_existing_vhost_info/1,
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
       get_existing_vhost_info,
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
    %% Bypass rabbit_misc:execute_mnesia_transaction/1 (no worker_pool
    %% configured in particular) but keep the behavior of throwing the error.
    meck:new(rabbit_misc, [passthrough, no_link]),
    meck:expect(
      rabbit_misc, execute_mnesia_transaction,
      fun(Fun) ->
              case mnesia:sync_transaction(Fun) of
                  {atomic, Result}  -> Result;
                  {aborted, Reason} -> throw({error, Reason})
              end
      end),
    ?assert(meck:validate(rabbit_misc)),

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
        [rabbit_vhost, V]).
-define(user_path(U),
        [rabbit_auth_backend_internal, users, U]).
-define(user_perm_path(U, V),
        [rabbit_auth_backend_internal, users, U, user_permissions, V]).
-define(topic_perm_path(U, V, E),
        [rabbit_auth_backend_internal, users, U, topic_permissions, V, E]).

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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {khepri, [rabbit_vhost],
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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {khepri, [rabbit_vhost],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assert(
              vhost_exists(_With, VHostName))),
     ?with(?assertNot(
              vhost_exists(_With, <<"non-existing-vhost">>))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

get_existing_vhost_info(_) ->
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              [{name, VHostName},
               {description, VHostDesc},
               {tags, VHostTags},
               {metadata, #{description => VHostDesc,
                            tags => VHostTags}},
               {tracing, false},
               {cluster_state, [{node(), running}]}],
              vhost_info(_With, VHostName)))
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
              VHostA,
              add_vhost(_With, VHostNameA, VHostDescA, VHostTagsA))),
     ?with(?assertEqual(
              VHostB,
              add_vhost(_With, VHostNameB, VHostDescB, VHostTagsB))),
     ?with(?assertEqual(
              [VHostNameA, VHostNameB],
              list_vhosts(_With))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {khepri, [rabbit_vhost],
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
              VHostA,
              add_vhost(_With, VHostNameA, VHostDescA, VHostTagsA))),
     ?with(?assertEqual(
              VHostB,
              add_vhost(_With, VHostNameB, VHostDescB, VHostTagsB))),
     ?with(?assertEqual(
              [VHostA, VHostB],
              list_vhost_records(_With))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {khepri, [rabbit_vhost],
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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              update_vhost(_With, VHostName, Fun))),
     ?with(?assertEqual(
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_vhost],
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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              UpdatedVHost,
              update_vhost(_With, VHostName, Fun))),
     ?with(?assertEqual(
              UpdatedVHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [UpdatedVHost]},
              {khepri, [rabbit_vhost],
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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {error, {no_such_vhost, VHostName}},
              update_vhost(_With, VHostName, NewVHostDesc, NewVHostTags))),
     ?with(?assertEqual(
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_vhost],
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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              {ok, UpdatedVHost},
              update_vhost(_With, VHostName, NewVHostDesc, NewVHostTags))),
     ?with(?assertEqual(
              UpdatedVHost,
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [UpdatedVHost]},
              {khepri, [rabbit_vhost],
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
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(case _With of
               %% There is a difference of behavior in this case. In the case
               %% of Mnesia, rabbit_vhost takes care of deleting user/topic
               %% permissions associated with the about-to-be-removed vhost.
               %% This will throw an exception if the vhost doesn't exist
               %% because rabbit_auth_backend_internal uses
               %% rabbit_vhost:with() to verify the vhost exists again.
               %%
               %% In the case of Khepri, that association is handled by a
               %% keep_while condition. Therefore the existence of the vhost
               %% is not checked.
               %%
               %% In practice, that's ok: rabbit_vhost:internal_delete() is
               %% only used in rabbit_vhost:delete() and the latter already
               %% verifies the existence of the vhost.
               %%
               %% We could even consider it's a bug in the case of Mnesia:
               %% rabbit_vhost:internal_delete() should probably not fail if
               %% the vhost doesn't exist.
               mnesia ->
                   %% The inner throw is emitted by
                   %% rabbit_auth_backend_internal:list_vhost_permissions().
                   %% This exception is thrown again in the delete_vhost()
                   %% helper in this testsuite.
                   ?assertThrow(
                      {error, {throw, {error, {no_such_vhost, VHostName}}}},
                      delete_vhost(_With, VHostName));
               khepri ->
                   ?assertEqual(
                      ok,
                      delete_vhost(_With, VHostName))
           end),
     ?with(?assertEqual(
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_vhost],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              VHost,
              lookup_vhost(_With, VHostName))),
     ?with(?assertEqual(
              ok,
              delete_vhost(_With, VHostName))),
     ?with(?assertEqual(
              {error, {no_such_vhost, VHostName}},
              lookup_vhost(_With, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {khepri, [rabbit_vhost],
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
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [User]},
              {khepri, [rabbit_auth_backend_internal],
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
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(_With, Username))),
     ?with(?assertThrow(
              {error, {user_already_exists, Username}},
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [User]},
              {khepri, [rabbit_auth_backend_internal],
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
              {khepri, [rabbit_auth_backend_internal],
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
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              update_user(_With, Username, Fun))),
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, []},
              {khepri, [rabbit_auth_backend_internal],
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
              {ok, User},
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              update_user(_With, Username, Fun))),
     ?with(?assertEqual(
              {ok, UpdatedUser},
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, [UpdatedUser]},
              {khepri, [rabbit_auth_backend_internal],
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
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              delete_user(_With, Username))),
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, []},
              {khepri, [rabbit_auth_backend_internal],
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
              {ok, User},
              lookup_user(_With, Username))),
     ?with(?assertEqual(
              ok,
              delete_user(_With, Username))),
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(_With, Username))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_user, []},
              {khepri, [rabbit_auth_backend_internal],
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
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
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
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
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
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
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
              rabbit_auth_backend_internal:list_vhost_permissions(
                VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              rabbit_auth_backend_internal:list_user_vhost_permissions(
                Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost], #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              [],
              rabbit_auth_backend_internal:list_vhost_permissions(
                VHostName))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              rabbit_auth_backend_internal:list_user_permissions(
                Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              rabbit_auth_backend_internal:list_user_vhost_permissions(
                Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost], #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal], #{}}]))
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
              VHostA,
              add_vhost(_With, VHostNameA, VHostDescA, VHostTagsA))),
     ?with(?assertEqual(
              VHostB,
              add_vhost(_With, VHostNameB, VHostDescB, VHostTagsB))),
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
              list_permissions(
                _With,
                rabbit_auth_backend_internal:match_user_vhost('_', '_'),
                rabbit_auth_backend_internal:match_path_in_khepri(
                  ?user_perm_path(?STAR, ?STAR))))),
     ?with(?assertEqual(
              [rabbit_auth_backend_internal:extract_user_permission_params(
                 rabbit_auth_backend_internal:vhost_perms_info_keys(),
                 UserPermissionA1),
               rabbit_auth_backend_internal:extract_user_permission_params(
                 rabbit_auth_backend_internal:vhost_perms_info_keys(),
                 UserPermissionB1)],
              lists:sort(
                rabbit_auth_backend_internal:list_vhost_permissions(
                  VHostNameA)))),
     ?with(?assertEqual(
              [rabbit_auth_backend_internal:extract_user_permission_params(
                 rabbit_auth_backend_internal:user_perms_info_keys(),
                 UserPermissionA1),
               rabbit_auth_backend_internal:extract_user_permission_params(
                 rabbit_auth_backend_internal:user_perms_info_keys(),
                 UserPermissionA2)],
              lists:sort(
                rabbit_auth_backend_internal:list_user_permissions(
                  UsernameA)))),
     ?with(?assertEqual(
              [rabbit_auth_backend_internal:extract_user_permission_params(
                 rabbit_auth_backend_internal:user_vhost_perms_info_keys(),
                 UserPermissionA1)],
              lists:sort(
                rabbit_auth_backend_internal:list_user_vhost_permissions(
                  UsernameA, VHostNameA)))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {mnesia, rabbit_user, [UserA, UserB]},
              {mnesia, rabbit_user_permission, [UserPermissionA1,
                                                UserPermissionA2,
                                                UserPermissionB1]},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostNameA) => VHostA,
                 ?vhost_path(VHostNameB) => VHostB}},
              {khepri, [rabbit_auth_backend_internal],
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
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              clear_permissions(_With, Username, VHostName))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              clear_permissions(_With, Username, VHostName))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
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
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
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
              ok,
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
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
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
              ok,
              delete_vhost(_With, VHostName))),
     ?with(?assertNot(
              check_vhost_access(_With, Username, VHostName))),
     ?with(?assertNot(
              check_resource_access(
                _With, Username, VHostName, "my-resource", configure))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
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

    %% Unset permissions equals to permissions granted.
    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              set_topic_permissions(
                _With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              set_topic_permissions(
                _With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, Exchange, TopicPermission))),
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
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              rabbit_auth_backend_internal:list_vhost_topic_permissions(
                VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              rabbit_auth_backend_internal:list_user_vhost_topic_permissions(
                Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              [],
              rabbit_auth_backend_internal:list_vhost_topic_permissions(
                VHostName))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              rabbit_auth_backend_internal:list_user_topic_permissions(
                Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              rabbit_auth_backend_internal:list_user_vhost_topic_permissions(
                Username, VHostName))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost], #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal], #{}}]))
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
              VHostA,
              add_vhost(_With, VHostNameA, VHostDescA, VHostTagsA))),
     ?with(?assertEqual(
              VHostB,
              add_vhost(_With, VHostNameB, VHostDescB, VHostTagsB))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameA, UserA))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, UsernameA, VHostNameA, ExchangeA, TopicPermissionA1))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, UsernameA, VHostNameB, ExchangeB, TopicPermissionA2))),
     ?with(?assertEqual(
              ok,
              add_user(_With, UsernameB, UserB))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, UsernameB, VHostNameA, ExchangeA, TopicPermissionB1))),
     ?with(?assertEqual(
              [TopicPermissionA1, TopicPermissionA2, TopicPermissionB1],
              list_topic_permissions(
                _With,
                rabbit_auth_backend_internal:
                match_user_vhost_topic_permission('_', '_', '_'),
                rabbit_auth_backend_internal:
                match_path_in_khepri(?topic_perm_path(?STAR, ?STAR, ?STAR))))),
     ?with(?assertEqual(
              [rabbit_auth_backend_internal:extract_topic_permission_params(
                 rabbit_auth_backend_internal:vhost_topic_perms_info_keys(),
                 TopicPermissionA1),
               rabbit_auth_backend_internal:extract_topic_permission_params(
                 rabbit_auth_backend_internal:vhost_topic_perms_info_keys(),
                 TopicPermissionB1)],
              lists:sort(
                rabbit_auth_backend_internal:list_vhost_topic_permissions(
                  VHostNameA)))),
     ?with(?assertEqual(
              [rabbit_auth_backend_internal:extract_topic_permission_params(
                 rabbit_auth_backend_internal:user_topic_perms_info_keys(),
                 TopicPermissionA1),
               rabbit_auth_backend_internal:extract_topic_permission_params(
                 rabbit_auth_backend_internal:user_topic_perms_info_keys(),
                 TopicPermissionA2)],
              lists:sort(
                rabbit_auth_backend_internal:list_user_topic_permissions(
                  UsernameA)))),
     ?with(?assertEqual(
              [rabbit_auth_backend_internal:extract_topic_permission_params(
                 rabbit_auth_backend_internal:
                 user_vhost_topic_perms_info_keys(),
                 TopicPermissionA1)],
              lists:sort(
                rabbit_auth_backend_internal:list_user_vhost_topic_permissions(
                  UsernameA, VHostNameA)))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHostA, VHostB]},
              {mnesia, rabbit_user, [UserA, UserB]},
              {mnesia, rabbit_topic_permission, [TopicPermissionA1,
                                                 TopicPermissionA2,
                                                 TopicPermissionB1]},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostNameA) => VHostA,
                 ?vhost_path(VHostNameB) => VHostB}},
              {khepri, [rabbit_auth_backend_internal],
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
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              clear_topic_permissions(_With, Username, VHostName, Exchange))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              clear_topic_permissions(_With, Username, VHostName, Exchange))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, ExchangeA, TopicPermissionA))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, ExchangeB, TopicPermissionB))),
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
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read, Context))),
     ?with(?assert(
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
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              clear_topic_permissions(_With, Username, VHostName))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              clear_topic_permissions(_With, Username, VHostName))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_user_permission, []},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, ExchangeA, TopicPermissionA))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, ExchangeB, TopicPermissionB))),
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
              clear_topic_permissions(_With, Username, VHostName))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read, Context))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeA, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read, Context))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, ExchangeB, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              delete_user(_With, Username))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, [VHost]},
              {mnesia, rabbit_user, []},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost],
               #{?vhost_path(VHostName) => VHost}},
              {khepri, [rabbit_auth_backend_internal],
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
              VHost,
              add_vhost(_With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(_With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                _With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              delete_vhost(_With, VHostName))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read, Context))),
     ?with(?assert(
              check_topic_access(
                _With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(check_storage(
             _With,
             [{mnesia, rabbit_vhost, []},
              {mnesia, rabbit_user, [User]},
              {mnesia, rabbit_topic_permission, []},
              {khepri, [rabbit_vhost],
               #{}},
              {khepri, [rabbit_auth_backend_internal],
               #{?user_path(Username) => User}}]))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun force_mnesia_use/0, [{with, mnesia, Tests}]},
          {setup, fun force_khepri_use/0, [{with, khepri, Tests}]}],
         [verbose])).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

force_mnesia_use() ->
    ct:pal(?LOW_IMPORTANCE, "Using Mnesia (disabling feature flag)", []),
    rabbit_khepri:force_metadata_store(mnesia),
    mock_feature_flag_state(false).

force_khepri_use() ->
    ct:pal(?LOW_IMPORTANCE, "Using Khepri (enabling feature flag)", []),
    rabbit_khepri:force_metadata_store(khepri),
    mock_feature_flag_state(true).

mock_feature_flag_state(State) ->
    _ = (catch meck:unload(rabbit_khepri)),
    meck:new(rabbit_khepri, [passthrough, no_link]),
    meck:expect(rabbit_khepri, is_enabled, fun(_) -> State end).

add_vhost(mnesia, VHostName, VHostDesc, VHostTags) ->
    rabbit_vhost:do_add_to_mnesia(VHostName, VHostDesc, VHostTags);
add_vhost(khepri, VHostName, VHostDesc, VHostTags) ->
    rabbit_vhost:do_add_to_khepri(VHostName, VHostDesc, VHostTags).

lookup_vhost(mnesia, VHostName) ->
    rabbit_vhost:lookup_in_mnesia(VHostName);
lookup_vhost(khepri, VHostName) ->
    rabbit_vhost:lookup_in_khepri(VHostName).

vhost_exists(mnesia, VHostName) ->
    rabbit_vhost:exists_in_mnesia(VHostName);
vhost_exists(khepri, VHostName) ->
    rabbit_vhost:exists_in_khepri(VHostName).

list_vhosts(mnesia) ->
    lists:sort(rabbit_vhost:list_names_in_mnesia());
list_vhosts(khepri) ->
    lists:sort(rabbit_vhost:list_names_in_khepri()).

list_vhost_records(mnesia) ->
    lists:sort(rabbit_vhost:all_in_mnesia());
list_vhost_records(khepri) ->
    lists:sort(rabbit_vhost:all_in_khepri()).

update_vhost(mnesia, VHostName, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              rabbit_vhost:update_in_mnesia(VHostName, Fun)
      end);
update_vhost(khepri, VHostName, Fun) ->
    rabbit_vhost:update_in_khepri(VHostName, Fun).

update_vhost(mnesia, VHostName, Description, Tags) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              rabbit_vhost:update_in_mnesia(VHostName, Description, Tags)
      end);
update_vhost(khepri, VHostName, Description, Tags) ->
    rabbit_vhost:update_in_khepri(VHostName, Description, Tags).

vhost_info(mnesia, VHostName) ->
    rabbit_vhost:info_in_mnesia(VHostName);
vhost_info(khepri, VHostName) ->
    rabbit_vhost:info_in_khepri(VHostName).

delete_vhost(mnesia, VHostName) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              rabbit_vhost:internal_delete_in_mnesia_part1(VHostName, <<>>),
              rabbit_vhost:internal_delete_in_mnesia_part2(VHostName)
      end);
delete_vhost(khepri, VHostName) ->
    rabbit_vhost:internal_delete_in_khepri(VHostName).

add_user(mnesia, Username, User) ->
    rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
      Username, User);
add_user(khepri, Username, User) ->
    rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
      Username, User).

lookup_user(mnesia, Username) ->
    rabbit_auth_backend_internal:lookup_user_in_mnesia(Username);
lookup_user(khepri, Username) ->
    rabbit_auth_backend_internal:lookup_user_in_khepri(Username).

list_user_records(mnesia) ->
    lists:sort(rabbit_auth_backend_internal:all_users_in_mnesia());
list_user_records(khepri) ->
    lists:sort(rabbit_auth_backend_internal:all_users_in_khepri()).

update_user(mnesia, Username, Fun) ->
    rabbit_auth_backend_internal:update_user_in_mnesia(Username, Fun);
update_user(khepri, Username, Fun) ->
    rabbit_auth_backend_internal:update_user_in_khepri(Username, Fun).

delete_user(mnesia, Username) ->
    rabbit_auth_backend_internal:delete_user_in_mnesia(Username);
delete_user(khepri, Username) ->
    rabbit_auth_backend_internal:delete_user_in_khepri(Username).

set_permissions(mnesia, Username, VHostName, UserPermission) ->
    rabbit_auth_backend_internal:set_permissions_in_mnesia(
      Username, VHostName, UserPermission);
set_permissions(khepri, Username, VHostName, UserPermission) ->
    rabbit_auth_backend_internal:set_permissions_in_khepri(
      Username, VHostName, UserPermission).

list_permissions(mnesia, MnesiaThunk, _) ->
    lists:sort(
      rabbit_auth_backend_internal:list_permissions_in_mnesia(MnesiaThunk));
list_permissions(khepri, _, KhepriThunk) ->
    lists:sort(
      rabbit_auth_backend_internal:list_permissions_in_khepri(KhepriThunk)).

check_vhost_access(mnesia, Username, VHostName) ->
    rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
      Username, VHostName);
check_vhost_access(khepri, Username, VHostName) ->
    rabbit_auth_backend_internal:check_vhost_access_in_khepri(
      Username, VHostName).

set_topic_permissions(
  mnesia, Username, VHostName, Exchange, TopicPermission) ->
    rabbit_auth_backend_internal:set_topic_permissions_in_mnesia(
      Username, VHostName, Exchange, TopicPermission);
set_topic_permissions(
  khepri, Username, VHostName, Exchange, TopicPermission) ->
    rabbit_auth_backend_internal:set_topic_permissions_in_khepri(
      Username, VHostName, Exchange, TopicPermission).

check_topic_access(mnesia, Username, VHostName, Exchange, Perm, Context) ->
    rabbit_auth_backend_internal:check_topic_access_in_mnesia(
      Username, VHostName, Exchange, Perm, Context);
check_topic_access(khepri, Username, VHostName, Exchange, Perm, Context) ->
    rabbit_auth_backend_internal:check_topic_access_in_khepri(
      Username, VHostName, Exchange, Perm, Context).

list_topic_permissions(mnesia, QueryThunk, _) ->
    lists:sort(
      rabbit_auth_backend_internal:list_topic_permissions_in_mnesia(
        QueryThunk));
list_topic_permissions(khepri, _, Path) ->
    lists:sort(
      rabbit_auth_backend_internal:list_topic_permissions_in_khepri(
        Path)).

clear_permissions(mnesia, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_permissions_in_mnesia(
      Username, VHostName);
clear_permissions(khepri, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_permissions_in_khepri(
      Username, VHostName).

check_resource_access(mnesia, Username, VHostName, Resource, Perm) ->
    rabbit_auth_backend_internal:check_resource_access_in_mnesia(
      Username, VHostName, Resource, Perm);
check_resource_access(khepri, Username, VHostName, Resource, Perm) ->
    rabbit_auth_backend_internal:check_resource_access_in_khepri(
      Username, VHostName, Resource, Perm).

clear_topic_permissions(mnesia, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_mnesia(
      Username, VHostName);
clear_topic_permissions(khepri, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_khepri(
      Username, VHostName).

clear_topic_permissions(mnesia, Username, VHostName, Exchange) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_mnesia(
      Username, VHostName, Exchange);
clear_topic_permissions(khepri, Username, VHostName, Exchange) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_khepri(
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
    Path1 = Path ++ [?STAR_STAR],
    ?assertEqual({ok, Content}, rabbit_khepri:match_and_get_data(Path1)).
