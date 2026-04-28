%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(user_tags_count_limit_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%% Must match the cap defined in rabbit_auth_backend_internal.
-define(MAX_USER_TAGS, 32).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          set_tags_within_limit_succeeds,
          set_tags_at_limit_succeeds,
          set_tags_above_limit_rejected,
          add_user_within_limit_succeeds,
          add_user_above_limit_rejected,
          add_user_with_hash_above_limit_rejected,
          put_user_csv_within_limit_succeeds,
          put_user_csv_above_limit_rejected,
          put_user_list_above_limit_rejected,
          prop_set_tags_count_bound
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

set_tags_within_limit_succeeds(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_tags_within_limit_succeeds1, []).

set_tags_within_limit_succeeds1() ->
    Username = <<"set_tags_within_limit">>,
    ensure_user(Username),
    Tags = generate_tag_atoms(8),
    ok = rabbit_auth_backend_internal:set_tags(Username, Tags, <<"acting-user">>),
    {ok, User} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual(Tags, internal_user:get_tags(User)),
    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),
    passed.

set_tags_at_limit_succeeds(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_tags_at_limit_succeeds1, []).

set_tags_at_limit_succeeds1() ->
    Username = <<"set_tags_at_limit">>,
    ensure_user(Username),
    Tags = generate_tag_atoms(?MAX_USER_TAGS),
    ok = rabbit_auth_backend_internal:set_tags(Username, Tags, <<"acting-user">>),
    {ok, User} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual(?MAX_USER_TAGS, length(internal_user:get_tags(User))),
    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),
    passed.

set_tags_above_limit_rejected(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_tags_above_limit_rejected1, []).

set_tags_above_limit_rejected1() ->
    Username = <<"set_tags_above_limit">>,
    ensure_user(Username),
    Tags = generate_tag_atoms(?MAX_USER_TAGS + 1),
    ?assertThrow(
       {error, {too_many_tags, ?MAX_USER_TAGS}},
       rabbit_auth_backend_internal:set_tags(Username, Tags, <<"acting-user">>)),
    {ok, User} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual([], internal_user:get_tags(User)),
    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),
    passed.

add_user_within_limit_succeeds(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, add_user_within_limit_succeeds1, []).

add_user_within_limit_succeeds1() ->
    Username = <<"add_user_within_limit">>,
    Password = <<"hunter2-1234567890">>,
    delete_if_exists(Username),
    Tags = generate_tag_atoms(4),
    ok = rabbit_auth_backend_internal:add_user(
           Username, Password, <<"acting-user">>, undefined, Tags),
    {ok, User} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual(Tags, internal_user:get_tags(User)),
    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),
    passed.

add_user_above_limit_rejected(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, add_user_above_limit_rejected1, []).

add_user_above_limit_rejected1() ->
    Username = <<"add_user_above_limit">>,
    Password = <<"hunter2-1234567890">>,
    delete_if_exists(Username),
    Tags = generate_tag_atoms(?MAX_USER_TAGS + 1),
    ?assertThrow(
       {error, {too_many_tags, ?MAX_USER_TAGS}},
       rabbit_auth_backend_internal:add_user(
         Username, Password, <<"acting-user">>, undefined, Tags)),
    ?assertEqual({error, not_found},
                 rabbit_auth_backend_internal:lookup_user(Username)),
    passed.

add_user_with_hash_above_limit_rejected(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, add_user_with_hash_above_limit_rejected1, []).

add_user_with_hash_above_limit_rejected1() ->
    Username = <<"add_user_with_hash_above_limit">>,
    delete_if_exists(Username),
    HashingMod = rabbit_password:hashing_mod(),
    Hash = rabbit_password:hash(HashingMod, <<"hunter2-1234567890">>),
    Tags = generate_tag_atoms(?MAX_USER_TAGS + 1),
    ?assertThrow(
       {error, {too_many_tags, ?MAX_USER_TAGS}},
       rabbit_auth_backend_internal:add_user_sans_validation(
         Username, Hash, HashingMod, Tags, undefined, <<"acting-user">>)),
    ?assertEqual({error, not_found},
                 rabbit_auth_backend_internal:lookup_user(Username)),
    passed.

put_user_csv_within_limit_succeeds(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, put_user_csv_within_limit_succeeds1, []).

put_user_csv_within_limit_succeeds1() ->
    Username = <<"put_user_csv_within_limit">>,
    delete_if_exists(Username),
    TagsCSV = csv_tag_string(8),
    UserDoc = #{name => Username,
                password => <<"hunter2-1234567890">>,
                tags => TagsCSV},
    ok = rabbit_auth_backend_internal:put_user(UserDoc, <<"acting-user">>),
    {ok, User} = rabbit_auth_backend_internal:lookup_user(Username),
    ?assertEqual(8, length(internal_user:get_tags(User))),
    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),
    passed.

put_user_csv_above_limit_rejected(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, put_user_csv_above_limit_rejected1, []).

put_user_csv_above_limit_rejected1() ->
    Username = <<"put_user_csv_above_limit">>,
    delete_if_exists(Username),
    TagsCSV = csv_tag_string(?MAX_USER_TAGS + 1),
    UserDoc = #{name => Username,
                password => <<"hunter2-1234567890">>,
                tags => TagsCSV},
    ?assertThrow(
       {error, {too_many_tags, ?MAX_USER_TAGS}},
       rabbit_auth_backend_internal:put_user(UserDoc, <<"acting-user">>)),
    ?assertEqual({error, not_found},
                 rabbit_auth_backend_internal:lookup_user(Username)),
    passed.

put_user_list_above_limit_rejected(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, put_user_list_above_limit_rejected1, []).

put_user_list_above_limit_rejected1() ->
    Username = <<"put_user_list_above_limit">>,
    delete_if_exists(Username),
    %% `put_user/3` also accepts the tag list as a plain Erlang list,
    %% which goes through `tag_list_from/1`'s list-shaped clause.
    Tags = [list_to_binary("user_tags_count_limit_tag_" ++ integer_to_list(I))
            || I <- lists:seq(1, ?MAX_USER_TAGS + 1)],
    UserDoc = #{name => Username,
                password => <<"hunter2-1234567890">>,
                tags => Tags},
    ?assertThrow(
       {error, {too_many_tags, ?MAX_USER_TAGS}},
       rabbit_auth_backend_internal:put_user(UserDoc, <<"acting-user">>)),
    ?assertEqual({error, not_found},
                 rabbit_auth_backend_internal:lookup_user(Username)),
    passed.

%% Property: any input bounded by the cap is accepted; any input above
%% the cap is rejected, regardless of contents.
prop_set_tags_count_bound(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, prop_set_tags_count_bound1, []).

prop_set_tags_count_bound1() ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_tag_count_bound/0, [], 50).

prop_tag_count_bound() ->
    ?FORALL(N, range(0, ?MAX_USER_TAGS * 2),
        begin
            Username = <<"proper_user">>,
            ensure_user(Username),
            Tags = generate_tag_atoms(N),
            Result =
                try
                    ok = rabbit_auth_backend_internal:set_tags(
                           Username, Tags, <<"acting-user">>),
                    accepted
                catch
                    throw:{error, {too_many_tags, ?MAX_USER_TAGS}} ->
                        rejected
                end,
            _ = rabbit_auth_backend_internal:delete_user(
                  Username, <<"acting-user">>),
            case N =< ?MAX_USER_TAGS of
                true  -> Result =:= accepted;
                false -> Result =:= rejected
            end
        end).

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

ensure_user(Username) ->
    delete_if_exists(Username),
    ok = rabbit_auth_backend_internal:add_user(
           Username, <<"hunter2-1234567890">>, <<"acting-user">>).

delete_if_exists(Username) ->
    case rabbit_auth_backend_internal:lookup_user(Username) of
        {ok, _} ->
            rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>);
        _ ->
            ok
    end.

%% Use stable, deterministic atom names. They are never converted via
%% `binary_to_atom/2` by the system under test (the suite calls APIs that
%% forward atoms straight through), so reusing them across cases is safe.
generate_tag_atoms(N) ->
    [list_to_atom("user_tags_count_limit_tag_" ++ integer_to_list(I))
     || I <- lists:seq(1, N)].

csv_tag_string(N) ->
    Names = ["user_tags_count_limit_tag_" ++ integer_to_list(I)
             || I <- lists:seq(1, N)],
    list_to_binary(string:join(Names, ",")).
