%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_khepri_migrations_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_proper_helpers, [run_proper/3]).

all() ->
    [{group, unit_tests},
     {group, property_tests}].

groups() ->
    [{unit_tests, [parallel], [
        expand_table_with_converter,
        expand_multiple_tables_sharing_converter,
        expand_bare_table,
        expand_mfa_with_converter,
        expand_mfa_with_converter_and_args,
        expand_bare_mfa,
        expand_mixed_like_real_migrations,
        expand_empty_migrations,
        expand_mfa_returning_empty_list,
        mnesia_tables_from_converter_pairs,
        mnesia_tables_from_bare_tables,
        mnesia_tables_from_mixed
    ]},
     {property_tests, [parallel], [
        prop_expand_preserves_static_entries,
        prop_expand_mfa_preserves_converter_association,
        prop_mnesia_tables_are_always_bare_atoms,
        prop_mnesia_table_count_matches_migration_count,
        prop_expanded_migrations_form_valid_proplist
    ]}].

dedup_tables() ->
    [rabbit_dedup_exchange_1, rabbit_dedup_exchange_2].

tracking_tables_for_node(Node) ->
    Suffix = atom_to_list(Node),
    [list_to_atom("rabbit_tracked_connection_" ++ Suffix),
     list_to_atom("rabbit_tracked_channel_" ++ Suffix)].

no_dynamic_tables() ->
    [].

expand_table_with_converter(_Config) ->
    Input = [{rabbit_vhost, rabbit_db_vhost_m2k_converter}],
    [{rabbit_vhost, rabbit_db_vhost_m2k_converter}] =
        rabbit_khepri:expand_mnesia_migrations(Input),
    ok.

expand_multiple_tables_sharing_converter(_Config) ->
    Input = [{rabbit_user, rabbit_db_user_m2k_converter},
             {rabbit_user_permission, rabbit_db_user_m2k_converter},
             {rabbit_topic_permission, rabbit_db_user_m2k_converter}],
    Input = rabbit_khepri:expand_mnesia_migrations(Input),
    ok.

expand_bare_table(_Config) ->
    Input = [rabbit_semi_durable_route],
    [rabbit_semi_durable_route] =
        rabbit_khepri:expand_mnesia_migrations(Input),
    ok.

expand_mfa_with_converter(_Config) ->
    Input = [{{mfa, ?MODULE, dedup_tables, []},
              rabbit_db_dedup_m2k_converter}],
    Result = rabbit_khepri:expand_mnesia_migrations(Input),
    ?assertEqual(
       [{rabbit_dedup_exchange_1, rabbit_db_dedup_m2k_converter},
        {rabbit_dedup_exchange_2, rabbit_db_dedup_m2k_converter}],
       Result),
    ok.

expand_mfa_with_converter_and_args(_Config) ->
    Input = [{{mfa, ?MODULE, tracking_tables_for_node, ['rabbit@host']},
              rabbit_db_tracking_m2k_converter}],
    Result = rabbit_khepri:expand_mnesia_migrations(Input),
    ?assertEqual(
       [{rabbit_tracked_connection_rabbit@host,
         rabbit_db_tracking_m2k_converter},
        {rabbit_tracked_channel_rabbit@host,
         rabbit_db_tracking_m2k_converter}],
       Result),
    ok.

expand_bare_mfa(_Config) ->
    Input = [{mfa, ?MODULE, dedup_tables, []}],
    Result = rabbit_khepri:expand_mnesia_migrations(Input),
    ?assertEqual(
       [rabbit_dedup_exchange_1, rabbit_dedup_exchange_2],
       Result),
    ok.

expand_mixed_like_real_migrations(_Config) ->
    Input = [{rabbit_vhost, rabbit_db_vhost_m2k_converter},
             {rabbit_user, rabbit_db_user_m2k_converter},
             {rabbit_user_permission, rabbit_db_user_m2k_converter},
             rabbit_semi_durable_route,
             rabbit_reverse_route,
             {{mfa, ?MODULE, dedup_tables, []},
              rabbit_db_dedup_m2k_converter},
             {mfa, ?MODULE, tracking_tables_for_node, ['rabbit@n1']}],
    Result = rabbit_khepri:expand_mnesia_migrations(Input),
    ?assertEqual(
       [{rabbit_vhost, rabbit_db_vhost_m2k_converter},
        {rabbit_user, rabbit_db_user_m2k_converter},
        {rabbit_user_permission, rabbit_db_user_m2k_converter},
        rabbit_semi_durable_route,
        rabbit_reverse_route,
        {rabbit_dedup_exchange_1, rabbit_db_dedup_m2k_converter},
        {rabbit_dedup_exchange_2, rabbit_db_dedup_m2k_converter},
        rabbit_tracked_connection_rabbit@n1,
        rabbit_tracked_channel_rabbit@n1],
       Result),
    ok.

expand_empty_migrations(_Config) ->
    [] = rabbit_khepri:expand_mnesia_migrations([]),
    ok.

expand_mfa_returning_empty_list(_Config) ->
    Input = [{{mfa, ?MODULE, no_dynamic_tables, []},
              rabbit_db_dedup_m2k_converter},
             {mfa, ?MODULE, no_dynamic_tables, []}],
    [] = rabbit_khepri:expand_mnesia_migrations(Input),
    ok.

mnesia_tables_from_converter_pairs(_Config) ->
    Input = [{rabbit_vhost, rabbit_db_vhost_m2k_converter},
             {rabbit_user, rabbit_db_user_m2k_converter}],
    ?assertEqual(
       [rabbit_vhost, rabbit_user],
       rabbit_khepri:mnesia_tables_from_migrations(Input)),
    ok.

mnesia_tables_from_bare_tables(_Config) ->
    Input = [rabbit_semi_durable_route,
             rabbit_reverse_route,
             rabbit_index_route],
    ?assertEqual(
       [rabbit_semi_durable_route,
        rabbit_reverse_route,
        rabbit_index_route],
       rabbit_khepri:mnesia_tables_from_migrations(Input)),
    ok.

mnesia_tables_from_mixed(_Config) ->
    Input = [{rabbit_vhost, rabbit_db_vhost_m2k_converter},
             rabbit_semi_durable_route,
             {rabbit_route, rabbit_db_binding_m2k_converter}],
    ?assertEqual(
       [rabbit_vhost, rabbit_semi_durable_route, rabbit_route],
       rabbit_khepri:mnesia_tables_from_migrations(Input)),
    ok.

prop_expand_preserves_static_entries(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 Entries, static_migrations_gen(),
                 begin
                     Result =
                         rabbit_khepri:expand_mnesia_migrations(
                           Entries),
                     Result =:= Entries
                 end)
      end, [], 500).

prop_expand_mfa_preserves_converter_association(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 {StaticEntries, ConverterMod},
                 {static_migrations_gen(), converter_mod_gen()},
                 begin
                     MfaEntry =
                         {{mfa, ?MODULE, dedup_tables, []},
                          ConverterMod},
                     Input = StaticEntries ++ [MfaEntry],
                     Result =
                         rabbit_khepri:expand_mnesia_migrations(
                           Input),
                     MfaSuffix =
                         lists:nthtail(
                           length(StaticEntries), Result),
                     lists:all(
                       fun({_T, Mod}) ->
                               Mod =:= ConverterMod;
                          (_) ->
                               false
                       end,
                       MfaSuffix)
                 end)
      end, [], 500).

prop_mnesia_tables_are_always_bare_atoms(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 Expanded, expanded_migrations_gen(),
                 begin
                     Tables =
                         rabbit_khepri:mnesia_tables_from_migrations(
                           Expanded),
                     lists:all(fun is_atom/1, Tables)
                 end)
      end, [], 500).

prop_mnesia_table_count_matches_migration_count(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 Expanded, expanded_migrations_gen(),
                 begin
                     Tables =
                         rabbit_khepri:mnesia_tables_from_migrations(
                           Expanded),
                     length(Tables) =:= length(Expanded)
                 end)
      end, [], 500).

prop_expanded_migrations_form_valid_proplist(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 Entries, unique_static_migrations_gen(),
                 begin
                     Result =
                         rabbit_khepri:expand_mnesia_migrations(
                           Entries),
                     lists:all(
                       fun({Table, Mod}) ->
                               proplists:get_value(
                                 Table, Result) =:= Mod;
                          (Table) when is_atom(Table) ->
                               proplists:get_value(
                                 Table, Result) =:= true
                       end,
                       Result)
                 end)
      end, [], 500).

converter_mod_gen() ->
    oneof([rabbit_db_vhost_m2k_converter,
           rabbit_db_user_m2k_converter,
           rabbit_db_queue_m2k_converter,
           rabbit_db_exchange_m2k_converter,
           rabbit_db_binding_m2k_converter]).

table_name_gen() ->
    oneof([rabbit_vhost,
           rabbit_user,
           rabbit_user_permission,
           rabbit_topic_permission,
           rabbit_queue,
           rabbit_durable_queue,
           rabbit_exchange,
           rabbit_route]).

bare_table_gen() ->
    oneof([rabbit_semi_durable_route,
           rabbit_reverse_route,
           rabbit_index_route]).

static_entry_gen() ->
    oneof([
        ?LET({T, M}, {table_name_gen(), converter_mod_gen()},
             {T, M}),
        bare_table_gen()
    ]).

static_migrations_gen() ->
    list(static_entry_gen()).

unique_static_migrations_gen() ->
    ?LET(Entries, static_migrations_gen(),
         lists:uniq(
           fun({T, _}) -> T; (T) -> T end, Entries)).

expanded_migrations_gen() ->
    static_migrations_gen().
