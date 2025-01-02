%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%


%% This module is responsible for definition import. Definition import makes
%% it possible to seed a cluster with virtual hosts, users, permissions, policies,
%% a messaging topology, and so on.
%%
%% These resources can be loaded from a local filesystem (a JSON file or a conf.d-style
%% directory of files), an HTTPS source or any other source using a user-provided module.
%%
%% Definition import can be performed on node boot or at any time via CLI tools
%% or the HTTP API. On node boot, every node performs definition import independently.
%% However, some resource types (queues and bindings) are imported only when a certain
%% number of nodes join the cluster. This is so that queues and their dependent
%% objects (bindings) have enough nodes to place their replicas on.
%%
%% It is possible for the user to opt into skipping definition import if
%% file/source content has not changed.
%%
%% See also
%%
%%  * rabbit.schema (core Cuttlefish schema mapping file)
%%  * rabbit_definitions_import_local_filesystem
%%  * rabbit_definitions_import_http
%%  * rabbit_definitions_hashing
-module(rabbit_definitions).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([boot/0]).
%% automatic import on boot
-export([
    maybe_load_definitions/0,
    maybe_load_definitions/2,
    maybe_load_definitions_from/2,

    has_configured_definitions_to_load/0
]).
%% import
-export([import_raw/1, import_raw/2, import_parsed/1, import_parsed/2,
         import_parsed_with_hashing/1, import_parsed_with_hashing/2,
         apply_defs/2, apply_defs/3,
         should_skip_if_unchanged/0
        ]).

-export([all_definitions/0]).
-export([
  list_users/0, list_vhosts/0, list_permissions/0, list_topic_permissions/0,
  list_runtime_parameters/0, list_global_runtime_parameters/0, list_policies/0,
  list_exchanges/0, list_queues/0, list_bindings/0,
  is_internal_parameter/1
]).
-export([decode/1, decode/2, args/1, validate_definitions/1]).

%% for tests
-export([
    maybe_load_definitions_from_local_filesystem_if_unchanged/3,
    maybe_load_definitions_from_pluggable_source_if_unchanged/2
]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_data_coercion, [to_binary/1]).

%%
%% API
%%

-type definition_category() :: 'users' |
                               'vhosts' |
                               'permissions' |
                               'topic_permissions' |
                               'parameters' |
                               'global_parameters' |
                               'policies' |
                               'queues' |
                               'bindings' |
                               'exchanges'.

-type definition_key() :: binary() | atom().
-type definition_object() :: #{definition_key() => any()}.
-type definition_list() :: [definition_object()].

-type definitions() :: #{
    definition_category() => definition_list()
}.

-export_type([definition_object/0, definition_list/0, definition_category/0, definitions/0]).

-define(IMPORT_WORK_POOL, definition_import_pool).

boot() ->
    PoolSize = application:get_env(rabbit, definition_import_work_pool_size, rabbit_runtime:guess_number_of_cpu_cores()),
    rabbit_sup:start_supervisor_child(definition_import_pool_sup, worker_pool_sup, [PoolSize, ?IMPORT_WORK_POOL]).

maybe_load_definitions() ->
    %% Classic source: local file or data directory
    case maybe_load_definitions_from_local_filesystem(rabbit, load_definitions) of
        ok ->
            % Extensible sources
            maybe_load_definitions_from_pluggable_source(rabbit, definitions);
        {error, E} -> {error, E}
    end.

-spec validate_parsing_of_doc(any()) -> boolean().
validate_parsing_of_doc(Body) when is_binary(Body) ->
    case decode(Body) of
        {ok, _Map}    -> true;
        {error, _Err} -> false
    end.

-spec validate_parsing_of_doc_collection(list(any())) -> boolean().
validate_parsing_of_doc_collection(Defs) when is_list(Defs) ->
    lists:foldl(fun(_Body, false) ->
        false;
        (Body, true) ->
            case decode(Body) of
                {ok, _Map}    -> true;
                {error, _Err} -> false
            end
    end, true, Defs).

-spec filter_orphaned_objects(definition_list()) -> definition_list().
filter_orphaned_objects(Maps) ->
    lists:filter(fun(M) -> maps:get(<<"vhost">>, M, undefined) =:= undefined end, Maps).

-spec any_orphaned_objects(definition_list()) -> boolean().
any_orphaned_objects(Maps) ->
    length(filter_orphaned_objects(Maps)) > 0.

-spec any_orphaned_in_doc(definitions()) -> boolean().
any_orphaned_in_doc(DefsMap) ->
    any_orphaned_in_category(DefsMap, <<"queues">>)
    orelse any_orphaned_in_category(DefsMap, <<"exchanges">>)
    orelse any_orphaned_in_category(DefsMap, <<"bindings">>).

-spec any_orphaned_in_category(definitions(), definition_category() | binary()) -> boolean().
any_orphaned_in_category(DefsMap, Category) ->
    %% try both binary and atom keys
    any_orphaned_objects(maps:get(Category, DefsMap,
                            maps:get(rabbit_data_coercion:to_atom(Category), DefsMap, []))).

-spec validate_orphaned_objects_in_doc_collection(list() | binary()) -> boolean().
validate_orphaned_objects_in_doc_collection(Defs) when is_list(Defs) ->
    lists:foldl(fun(_Body, false) ->
        false;
        (Body, true) ->
            validate_parsing_of_doc(Body)
    end, true, Defs).

-spec validate_orphaned_objects_in_doc(binary()) -> boolean().
validate_orphaned_objects_in_doc(Body) when is_binary(Body) ->
    case decode(Body) of
        {ok, DefsMap}    ->
            AnyOrphaned = any_orphaned_in_doc(DefsMap),
            case AnyOrphaned of
                true  ->
                    log_an_error_about_orphaned_objects();
                false -> ok
            end,
            AnyOrphaned;
        {error, _Err} -> false
    end.

-spec validate_definitions(list(any()) | binary()) -> boolean().
validate_definitions(Defs) when is_list(Defs) ->
    validate_parsing_of_doc_collection(Defs) andalso
    validate_orphaned_objects_in_doc_collection(Defs);
validate_definitions(Body) when is_binary(Body) ->
    case decode(Body) of
        {ok, Defs}    -> validate_orphaned_objects_in_doc(Defs);
        {error, _Err} -> false
    end.

-spec import_raw(Body :: binary() | iolist()) -> ok | {error, term()}.
import_raw(Body) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~ts", [?INTERNAL_USER]),
    case decode([], Body) of
        {error, E}   -> {error, E};
        {ok, _, Map} -> apply_defs(Map, ?INTERNAL_USER)
    end.

-spec import_raw(Body :: binary() | iolist(), VHost :: vhost:name()) -> ok | {error, term()}.
import_raw(Body, VHost) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~ts", [?INTERNAL_USER]),
    case decode([], Body) of
        {error, E}   -> {error, E};
        {ok, _, Map} -> apply_defs(Map, ?INTERNAL_USER, fun() -> ok end, VHost)
    end.

-spec import_parsed(Defs :: #{any() => any()} | list()) -> ok | {error, term()}.
import_parsed(Body0) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0));
import_parsed(Body0) when is_map(Body0) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~ts", [?INTERNAL_USER]),
    Body = atomise_map_keys(Body0),
    apply_defs(Body, ?INTERNAL_USER).

-spec import_parsed(Defs :: #{any() => any() | list()}, VHost :: vhost:name()) -> ok | {error, term()}.
import_parsed(Body0, VHost) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0), VHost);
import_parsed(Body0, VHost) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~ts", [?INTERNAL_USER]),
    Body = atomise_map_keys(Body0),
    apply_defs(Body, ?INTERNAL_USER, fun() -> ok end, VHost).


-spec import_parsed_with_hashing(Defs :: #{any() => any()} | list()) -> ok | {error, term()}.
import_parsed_with_hashing(Body0) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0));
import_parsed_with_hashing(Body0) when is_map(Body0) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~ts", [?INTERNAL_USER]),
    case should_skip_if_unchanged() of
        false ->
            import_parsed(Body0);
        true  ->
            Body         = atomise_map_keys(Body0),
            PreviousHash = rabbit_definitions_hashing:stored_global_hash(),
            Algo         = rabbit_definitions_hashing:hashing_algorithm(),
            case rabbit_definitions_hashing:hash(Algo, Body) of
                PreviousHash ->
                    rabbit_log:info("Submitted definition content hash matches the stored one: ~ts", [binary:part(rabbit_misc:hexify(PreviousHash), 0, 12)]),
                    ok;
                Other        ->
                    rabbit_log:debug("Submitted definition content hash: ~ts, stored one: ~ts", [
                        binary:part(rabbit_misc:hexify(PreviousHash), 0, 10),
                        binary:part(rabbit_misc:hexify(Other), 0, 10)
                    ]),
                    Result = apply_defs(Body, ?INTERNAL_USER),
                    rabbit_definitions_hashing:store_global_hash(Other),
                    Result
            end
    end.

-spec import_parsed_with_hashing(Defs :: #{any() => any() | list()}, VHost :: vhost:name()) -> ok | {error, term()}.
import_parsed_with_hashing(Body0, VHost) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0), VHost);
import_parsed_with_hashing(Body0, VHost) ->
    rabbit_log:info("Asked to import definitions for virtual host '~ts'. Acting user: ~ts", [?INTERNAL_USER, VHost]),

    case should_skip_if_unchanged() of
        false ->
            import_parsed(Body0, VHost);
        true  ->
            Body = atomise_map_keys(Body0),
            PreviousHash = rabbit_definitions_hashing:stored_vhost_specific_hash(VHost),
            Algo         = rabbit_definitions_hashing:hashing_algorithm(),
            case rabbit_definitions_hashing:hash(Algo, Body) of
                PreviousHash ->
                    rabbit_log:info("Submitted definition content hash matches the stored one: ~ts", [binary:part(rabbit_misc:hexify(PreviousHash), 0, 12)]),
                    ok;
                Other        ->
                    rabbit_log:debug("Submitted definition content hash: ~ts, stored one: ~ts", [
                        binary:part(rabbit_misc:hexify(PreviousHash), 0, 10),
                        binary:part(rabbit_misc:hexify(Other), 0, 10)
                    ]),
                    Result = apply_defs(Body, ?INTERNAL_USER, fun() -> ok end, VHost),
                    rabbit_definitions_hashing:store_vhost_specific_hash(VHost, Other, ?INTERNAL_USER),
                    Result
            end
    end.


-spec all_definitions() -> map().
all_definitions() ->
    Xs = list_exchanges(),
    Qs = list_queues(),
    Bs = list_bindings(),

    Users  = list_users(),
    VHosts = list_vhosts(),
    Params  = list_runtime_parameters(),
    GParams = list_global_runtime_parameters(),
    Pols    = list_policies(),

    Perms   = list_permissions(),
    TPerms  = list_topic_permissions(),

    {ok, Vsn} = application:get_key(rabbit, vsn),
    #{
        rabbit_version    => rabbit_data_coercion:to_binary(Vsn),
        rabbitmq_version  => rabbit_data_coercion:to_binary(Vsn),
        users             => Users,
        vhosts            => VHosts,
        permissions       => Perms,
        topic_permissions => TPerms,
        parameters        => Params,
        global_parameters => GParams,
        policies          => Pols,
        queues            => Qs,
        bindings          => Bs,
        exchanges         => Xs
    }.

-spec has_configured_definitions_to_load() -> boolean().
has_configured_definitions_to_load() ->
    has_configured_definitions_to_load_via_classic_option() or
        has_configured_definitions_to_load_via_modern_option() or
        has_configured_definitions_to_load_via_management_option().

%% Retained for backwards compatibility, implicitly assumes the local filesystem source
maybe_load_definitions(App, Key) ->
    maybe_load_definitions_from_local_filesystem(App, Key).

maybe_load_definitions_from(IsDir, Path) ->
    rabbit_definitions_import_local_filesystem:load(IsDir, Path).

%%
%% Implementation
%%

-spec has_configured_definitions_to_load_via_modern_option() -> boolean().
has_configured_definitions_to_load_via_modern_option() ->
    case application:get_env(rabbit, definitions) of
        undefined  -> false;
        {ok, none} -> false;
        {ok, []}   -> false;
        {ok, _Options} -> true
    end.

has_configured_definitions_to_load_via_classic_option() ->
    case application:get_env(rabbit, load_definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, _Path} -> true
    end.

has_configured_definitions_to_load_via_management_option() ->
    case application:get_env(rabbitmq_management, load_definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, _Path} -> true
    end.

maybe_load_definitions_from_local_filesystem(App, Key) ->
    case application:get_env(App, Key) of
        undefined  -> ok;
        {ok, none} -> ok;
        {ok, Path} ->
            rabbit_log:debug("~ts.~ts is set to '~ts', will discover definition file(s) to import", [App, Key, Path]),
            IsDir = filelib:is_dir(Path),
            Mod = rabbit_definitions_import_local_filesystem,
            rabbit_log:debug("Will use module ~ts to import definitions", [Mod]),

            case should_skip_if_unchanged() of
                false ->
                    rabbit_log:debug("Will re-import definitions even if they have not changed"),
                    Mod:load(IsDir, Path);
                true ->
                    maybe_load_definitions_from_local_filesystem_if_unchanged(Mod, IsDir, Path)
            end
    end.

maybe_load_definitions_from_local_filesystem_if_unchanged(Mod, IsDir, Path) ->
    Algo = rabbit_definitions_hashing:hashing_algorithm(),
    rabbit_log:debug("Will import definitions only if definition file/directory has changed, hashing algo: ~ts", [Algo]),
    CurrentHash = rabbit_definitions_hashing:stored_global_hash(),
    rabbit_log:debug("Previously stored hash value of imported definitions: ~ts...", [binary:part(rabbit_misc:hexify(CurrentHash), 0, 12)]),
    case Mod:load_with_hashing(IsDir, Path, CurrentHash, Algo) of
        {error, Err} ->
            {error, Err};
        CurrentHash ->
            rabbit_log:info("Hash value of imported definitions matches current contents");
        UpdatedHash ->
            rabbit_log:debug("Hash value of imported definitions has changed to ~ts", [binary:part(rabbit_misc:hexify(UpdatedHash), 0, 12)]),
            rabbit_definitions_hashing:store_global_hash(UpdatedHash)
    end.

maybe_load_definitions_from_pluggable_source(App, Key) ->
    case application:get_env(App, Key) of
        undefined  -> ok;
        {ok, none} -> ok;
        {ok, []}   -> ok;
        {ok, Proplist} ->
            case pget(import_backend, Proplist, undefined) of
                undefined  ->
                    {error, "definition import source is configured but definitions.import_backend is not set"};
                ModOrAlias ->
                    Mod = normalize_backend_module(ModOrAlias),
                    maybe_load_definitions_from_pluggable_source_if_unchanged(Mod, Proplist)
            end
    end.

maybe_load_definitions_from_pluggable_source_if_unchanged(Mod, Proplist) ->
    case should_skip_if_unchanged() of
        false ->
            rabbit_log:debug("Will use module ~ts to import definitions", [Mod]),
            Mod:load(Proplist);
        true ->
            rabbit_log:debug("Will use module ~ts to import definitions (if definition file/directory/source has changed)", [Mod]),
            CurrentHash = rabbit_definitions_hashing:stored_global_hash(),
            rabbit_log:debug("Previously stored hash value of imported definitions: ~ts...", [binary:part(rabbit_misc:hexify(CurrentHash), 0, 12)]),
            Algo = rabbit_definitions_hashing:hashing_algorithm(),
            case Mod:load_with_hashing(Proplist, CurrentHash, Algo) of
                {error, Err} ->
                    {error, Err};
                CurrentHash ->
                    rabbit_log:info("Hash value of imported definitions matches current contents");
                UpdatedHash ->
                    rabbit_log:debug("Hash value of imported definitions has changed to ~ts...", [binary:part(rabbit_misc:hexify(CurrentHash), 0, 12)]),
                    rabbit_definitions_hashing:store_global_hash(UpdatedHash)
            end
    end.



normalize_backend_module(local_filesystem) ->
    rabbit_definitions_import_local_filesystem;
normalize_backend_module(local) ->
    rabbit_definitions_import_local_filesystem;
normalize_backend_module(https) ->
    rabbit_definitions_import_https;
normalize_backend_module(http)  ->
    rabbit_definitions_import_https;
normalize_backend_module(rabbitmq_definitions_import_local_filesystem) ->
    rabbit_definitions_import_local_filesystem;
normalize_backend_module(rabbitmq_definitions_import_https) ->
    rabbit_definitions_import_https;
normalize_backend_module(Other) ->
    Other.

decode(Keys, Body) ->
    case decode(Body) of
        {ok, J0} ->
            J = maps:fold(fun(K, V, Acc) ->
                  Acc#{rabbit_data_coercion:to_atom(K, utf8) => V}
                end, J0, J0),
            Results = [get_or_missing(K, J) || K <- Keys],
            case [E || E = {key_missing, _} <- Results] of
                []      -> {ok, Results, J};
                Errors  -> {error, Errors}
            end;
        Else     -> Else
    end.

decode(<<"">>) ->
    {ok, #{}};
decode(Body) ->
    try
      Decoded = rabbit_json:decode(Body),
      Normalised = atomise_map_keys(Decoded),
      {ok, Normalised}
    catch error:_ -> {error, not_json}
    end.

atomise_map_keys(Decoded) ->
    maps:fold(fun(K, V, Acc) ->
        Acc#{rabbit_data_coercion:to_atom(K, utf8) => V}
              end, Decoded, Decoded).

-spec should_skip_if_unchanged() -> boolean().
should_skip_if_unchanged() ->
    OptedIn = case application:get_env(rabbit, definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, []}    -> false;
        {ok, Proplist} ->
            pget(skip_if_unchanged, Proplist, false)
    end,
    %% if we do not take this into consideration, delayed queue import will be delayed
    %% on nodes that join before the target cluster size is reached, and skipped
    %% once it is
    ReachedTargetClusterSize = rabbit_nodes:reached_target_cluster_size(),
    OptedIn andalso ReachedTargetClusterSize.

log_an_error_about_orphaned_objects() ->
    rabbit_log:error("Definitions import: some queues, exchanges or bindings in the definition file "
        "are missing the virtual host field. Such files are produced when definitions of "
        "a single virtual host are exported. They cannot be used to import definitions at boot time").

-spec apply_defs(Map :: #{atom() => any()}, ActingUser :: rabbit_types:username()) -> 'ok' | {error, term()}.

apply_defs(Map, ActingUser) ->
    apply_defs(Map, ActingUser, fun () -> ok end).

-type vhost_or_success_fun() :: vhost:name() | fun(() -> 'ok').
-spec apply_defs(Map :: #{atom() => any()},
                 ActingUser :: rabbit_types:username(),
                 VHostOrSuccessFun :: vhost_or_success_fun()) -> 'ok' | {error, term()}.

apply_defs(Map, ActingUser, VHost) when is_binary(VHost) ->
    apply_defs(Map, ActingUser, fun () -> ok end, VHost);
apply_defs(Map, ActingUser, SuccessFun) when is_function(SuccessFun) ->
    Version = maps:get(rabbitmq_version, Map, maps:get(rabbit_version, Map, undefined)),

    %% If any of the queues or exchanges do not have virtual hosts set,
    %% this definition file was a virtual-host specific import. They cannot be applied
    %% as "complete" definition imports, most notably, imported on boot.
    AnyOrphaned = any_orphaned_in_doc(Map),

    case AnyOrphaned of
        true ->
            log_an_error_about_orphaned_objects(),
            throw({error, invalid_definitions_file});
        false ->
            ok
    end,

    try
        concurrent_for_all(users, ActingUser, Map,
                fun(User, _Username) ->
                    rabbit_auth_backend_internal:put_user(User, Version, ActingUser)
                end),
        concurrent_for_all(vhosts,             ActingUser, Map, fun add_vhost/2),
        validate_limits(Map),
        concurrent_for_all(permissions,        ActingUser, Map, fun add_permission/2),
        concurrent_for_all(topic_permissions,  ActingUser, Map, fun add_topic_permission/2),

        concurrent_for_all(exchanges,          ActingUser, Map, fun add_exchange/2),

        sequential_for_all(global_parameters,  ActingUser, Map, fun add_global_parameter/2),
        %% importing policies concurrently can be unsafe as queues will be getting
        %% potentially out of order notifications of applicable policy changes
        sequential_for_all(policies,           ActingUser, Map, fun add_policy/2),
        sequential_for_all(parameters,         ActingUser, Map, fun add_parameter/2),

        rabbit_nodes:if_reached_target_cluster_size(
            fun() ->
                concurrent_for_all(queues,   ActingUser, Map, fun add_queue/2),
                concurrent_for_all(bindings, ActingUser, Map, fun add_binding/2)
            end,

            fun() ->
                rabbit_log:info("There are fewer than target cluster size (~b) nodes online,"
                                " skipping queue and binding import from definitions",
                                [rabbit_nodes:target_cluster_size_hint()])
            end
        ),

        SuccessFun(),
        ok
    catch {error, E} -> {error, format(E)};
          exit:E     -> {error, format(E)}
    after
        rabbit_runtime:gc_all_processes()
    end.

-spec apply_defs(Map :: #{atom() => any()},
                ActingUser :: rabbit_types:username(),
                SuccessFun :: fun(() -> 'ok'),
                VHost :: vhost:name()) -> 'ok' | {error, term()}.

apply_defs(Map, ActingUser, SuccessFun, VHost) when is_function(SuccessFun); is_binary(VHost) ->
    rabbit_log:info("Asked to import definitions for a virtual host. Virtual host: ~tp, acting user: ~tp",
                    [VHost, ActingUser]),
    try
        validate_limits(Map, VHost),

        concurrent_for_all(exchanges,  ActingUser, Map, VHost, fun add_exchange/3),
        sequential_for_all(parameters, ActingUser, Map, VHost, fun add_parameter/3),
        %% importing policies concurrently can be unsafe as queues will be getting
        %% potentially out of order notifications of applicable policy changes
        sequential_for_all(policies,   ActingUser, Map, VHost, fun add_policy/3),

        rabbit_nodes:if_reached_target_cluster_size(
            fun() ->
                concurrent_for_all(queues,     ActingUser, Map, VHost, fun add_queue/3),
                concurrent_for_all(bindings,   ActingUser, Map, VHost, fun add_binding/3)
            end,

            fun() ->
                rabbit_log:info("There are fewer than target cluster size (~b) nodes online,"
                                " skipping queue and binding import from definitions",
                                [rabbit_nodes:target_cluster_size_hint()])
            end
        ),

        SuccessFun()
    catch {error, E} -> {error, format(E)};
          exit:E     -> {error, format(E)}
    after
        rabbit_runtime:gc_all_processes()
    end.

sequential_for_all(Category, ActingUser, Definitions, Fun) ->
    try
        sequential_for_all0(Category, ActingUser, Definitions, Fun),
        ok
    after
        rabbit_runtime:gc_all_processes()
    end.

sequential_for_all0(Category, ActingUser, Definitions, Fun) ->
    _ = case maps:get(rabbit_data_coercion:to_atom(Category), Definitions, undefined) of
        undefined -> ok;
        List      ->
            case length(List) of
                0 -> ok;
                N -> rabbit_log:info("Importing sequentially ~tp ~ts...", [N, human_readable_category_name(Category)])
            end,
            [begin
                 %% keys are expected to be atoms
                 Fun(atomize_keys(M), ActingUser)
             end || M <- List, is_map(M)]
    end,
    ok.

sequential_for_all(Name, ActingUser, Definitions, VHost, Fun) ->
    try
        sequential_for_all0(Name, ActingUser, Definitions, VHost, Fun),
        ok
    after
        rabbit_runtime:gc_all_processes()
    end.

sequential_for_all0(Name, ActingUser, Definitions, VHost, Fun) ->
    _ = case maps:get(rabbit_data_coercion:to_atom(Name), Definitions, undefined) of
        undefined -> ok;
        List      -> _ = [Fun(VHost, atomize_keys(M), ActingUser) || M <- List, is_map(M)]
    end,
    ok.

concurrent_for_all(Category, ActingUser, Definitions, Fun) ->
    try
        concurrent_for_all0(Category, ActingUser, Definitions, Fun)
    after
        rabbit_runtime:gc_all_processes()
    end.

concurrent_for_all0(Category, ActingUser, Definitions, Fun) ->
    case maps:get(rabbit_data_coercion:to_atom(Category), Definitions, undefined) of
        undefined -> ok;
        List      ->
            case length(List) of
                0 -> ok;
                N -> rabbit_log:info("Importing concurrently ~tp ~ts...", [N, human_readable_category_name(Category)])
            end,
            WorkPoolFun = fun(M) ->
                                  Fun(atomize_keys(M), ActingUser)
                          end,
            do_concurrent_for_all(List, WorkPoolFun),
            ok
    end.

concurrent_for_all(Name, ActingUser, Definitions, VHost, Fun) ->
    try
        concurrent_for_all0(Name, ActingUser, Definitions, VHost, Fun)
    after
        rabbit_runtime:gc_all_processes()
    end.

concurrent_for_all0(Name, ActingUser, Definitions, VHost, Fun) ->
    case maps:get(rabbit_data_coercion:to_atom(Name), Definitions, undefined) of
        undefined -> ok;
        List      ->
            WorkPoolFun = fun(M) ->
                                  Fun(VHost, atomize_keys(M), ActingUser)
                          end,
            do_concurrent_for_all(List, WorkPoolFun)
    end.

do_concurrent_for_all(List, WorkPoolFun) ->
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         %% keys are expected to be atoms
         ok = gatherer:fork(Gatherer),
         worker_pool:submit_async(
           ?IMPORT_WORK_POOL,
           fun() ->
                   _ = try
                       WorkPoolFun(M)
                   catch {error, E}     -> gatherer:in(Gatherer, {error, E});
                         _:E:Stacktrace ->
                             rabbit_log:debug("Definition import: a work pool operation has thrown an exception ~st, stacktrace: ~p",
                                              [E, Stacktrace]),
                             gatherer:in(Gatherer, {error, E})
                   end,
                   gatherer:finish(Gatherer)
           end)
     end || M <- List, is_map(M)],
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer);
        {value, {error, E}} ->
            ok = gatherer:stop(Gatherer),
            throw({error, E})
    end.

-spec atomize_keys(#{any() => any()}) -> #{atom() => any()}.

atomize_keys(M) ->
    maps:fold(fun(K, V, Acc) ->
                maps:put(rabbit_data_coercion:to_atom(K), V, Acc)
              end, #{}, M).

-spec human_readable_category_name(definition_category()) -> string().

human_readable_category_name(topic_permissions) -> "topic permissions";
human_readable_category_name(parameters) -> "runtime parameters";
human_readable_category_name(global_parameters) -> "global runtime parameters";
human_readable_category_name(Other) -> rabbit_data_coercion:to_list(Other).


format(#amqp_error{name = Name, explanation = Explanation}) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~ts: ~ts", [Name, Explanation]));
format({no_such_vhost, undefined}) ->
    rabbit_data_coercion:to_binary(
      "Virtual host does not exist and is not specified in definitions file.");
format({no_such_vhost, VHost}) ->
    rabbit_data_coercion:to_binary(
      rabbit_misc:format("Please create virtual host \"~ts\" prior to importing definitions.",
                         [VHost]));
format({vhost_limit_exceeded, ErrMsg}) ->
    rabbit_data_coercion:to_binary(ErrMsg);
format({shutdown, _} = Error) ->
    rabbit_log:debug("Metadata store is unavailable: ~p", [Error]),
    rabbit_data_coercion:to_binary(
      rabbit_misc:format("Metadata store is unavailable. Please try again.", []));
format(E) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~tp", [E])).

add_parameter(Param, Username) ->
    VHost = maps:get(vhost,     Param, undefined),
    add_parameter(VHost, Param, Username).

add_parameter(VHost, Param, Username) ->
    Comp  = maps:get(component, Param, undefined),
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    Result = case is_map(Term) of
        true ->
            %% coerce maps to proplists for backwards compatibility.
            %% See rabbitmq-management#528.
            TermProplist = rabbit_data_coercion:to_proplist(Term),
            rabbit_runtime_parameters:set(VHost, Comp, Key, TermProplist, Username);
        _ ->
            rabbit_runtime_parameters:set(VHost, Comp, Key, Term, Username)
    end,
    case Result of
        ok                -> ok;
        {error_string, E} ->
            S = rabbit_misc:format(" (~ts/~ts/~ts)", [VHost, Comp, Key]),
            exit(rabbit_data_coercion:to_binary(rabbit_misc:escape_html_tags(E ++ S)))
    end.

add_global_parameter(Param, Username) ->
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    case is_map(Term) of
        true ->
            %% coerce maps to proplists for backwards compatibility.
            %% See rabbitmq-management#528.
            TermProplist = rabbit_data_coercion:to_proplist(Term),
            rabbit_runtime_parameters:set_global(Key, TermProplist, Username);
        _ ->
            rabbit_runtime_parameters:set_global(Key, Term, Username)
    end.

add_policy(Param, Username) ->
    VHost = maps:get(vhost, Param, undefined),
    add_policy(VHost, Param, Username).

add_policy(VHost, Param, Username) ->
    Key   = maps:get(name,  Param, undefined),
    case Key of
      undefined -> exit(rabbit_misc:format("policy in virtual host '~ts' has undefined name", [VHost]));
      _ -> ok
    end,
    case rabbit_policy:set(
           VHost, Key, maps:get(pattern, Param, undefined),
           case maps:get(definition, Param, undefined) of
               undefined -> undefined;
               Def -> rabbit_data_coercion:to_proplist(Def)
           end,
           maps:get(priority, Param, undefined),
           maps:get('apply-to', Param, <<"all">>),
           Username) of
        ok                -> ok;
        {error_string, E} -> S = rabbit_misc:format(" (~ts/~ts)", [VHost, Key]),
                             exit(rabbit_data_coercion:to_binary(rabbit_misc:escape_html_tags(E ++ S)))
    end.

-spec add_vhost(map(), rabbit_types:username()) -> ok | no_return().

add_vhost(VHost, ActingUser) ->
    Name             = maps:get(name, VHost, undefined),
    IsTracingEnabled = maps:get(tracing, VHost, undefined),
    Metadata         = rabbit_data_coercion:atomize_keys(maps:get(metadata, VHost, #{})),
    Description      = maps:get(description, VHost, maps:get(description, Metadata, <<"">>)),
    Tags             = maps:get(tags, VHost, maps:get(tags, Metadata, [])),
    DefaultQueueType = maps:get(default_queue_type, Metadata, undefined),

    case rabbit_vhost:put_vhost(Name, Description, Tags, DefaultQueueType, IsTracingEnabled, ActingUser) of
        ok ->
            ok;
        {error, _} = Err ->
            throw(Err)
    end.

add_permission(Permission, ActingUser) ->
    rabbit_auth_backend_internal:set_permissions(maps:get(user,      Permission, undefined),
                                                 maps:get(vhost,     Permission, undefined),
                                                 maps:get(configure, Permission, undefined),
                                                 maps:get(write,     Permission, undefined),
                                                 maps:get(read,      Permission, undefined),
                                                 ActingUser).

add_topic_permission(TopicPermission, ActingUser) ->
    rabbit_auth_backend_internal:set_topic_permissions(
        maps:get(user,      TopicPermission, undefined),
        maps:get(vhost,     TopicPermission, undefined),
        maps:get(exchange,  TopicPermission, undefined),
        maps:get(write,     TopicPermission, undefined),
        maps:get(read,      TopicPermission, undefined),
      ActingUser).

add_queue(Queue, ActingUser) ->
    add_queue_int(Queue, r(queue, Queue), ActingUser).

add_queue(VHost, Queue, ActingUser) ->
    add_queue_int(Queue, rv(VHost, queue, Queue), ActingUser).

add_queue_int(_Queue, R = #resource{kind = queue,
                                    name = <<"amq.", _/binary>>}, ActingUser) ->
    Name = R#resource.name,
    rabbit_log:warning("Skipping import of a queue whose name begins with 'amq.', "
                       "name: ~ts, acting user: ~ts", [Name, ActingUser]);
add_queue_int(_Queue, R = #resource{kind = queue, virtual_host = undefined}, ActingUser) ->
    Name = R#resource.name,
    rabbit_log:warning("Skipping import of a queue with an unset virtual host field, "
    "name: ~ts, acting user: ~ts", [Name, ActingUser]);
add_queue_int(Queue, Name = #resource{virtual_host = VHostName}, ActingUser) ->
    case rabbit_amqqueue:exists(Name) of
        true ->
            ok;
        false ->
            AutoDelete = maps:get(auto_delete, Queue, false),
            DurableDeclare = maps:get(durable, Queue, true),
            ExclusiveDeclare = maps:get(exclusive, Queue, false),

            Args0 = args(maps:get(arguments, Queue, #{})),
            Args1 = rabbit_amqqueue:augment_declare_args(VHostName,
                                                         DurableDeclare,
                                                         ExclusiveDeclare,
                                                         AutoDelete,
                                                         Args0),

            rabbit_amqqueue:declare(Name,
                                    DurableDeclare,
                                    AutoDelete,
                                    Args1,
                                    none,
                                    ActingUser)
    end.

add_exchange(Exchange, ActingUser) ->
    add_exchange_int(Exchange, r(exchange, Exchange), ActingUser).

add_exchange(VHost, Exchange, ActingUser) ->
    add_exchange_int(Exchange, rv(VHost, exchange, Exchange), ActingUser).

add_exchange_int(_Exchange, #resource{kind = exchange, name = <<"">>}, ActingUser) ->
    rabbit_log:warning("Not importing the default exchange, acting user: ~ts", [ActingUser]);
add_exchange_int(_Exchange, R = #resource{kind = exchange,
                                          name = <<"amq.", _/binary>>}, ActingUser) ->
    Name = R#resource.name,
    rabbit_log:warning("Skipping import of an exchange whose name begins with 'amq.', "
                       "name: ~ts, acting user: ~ts", [Name, ActingUser]);
add_exchange_int(Exchange, Name, ActingUser) ->
    case rabbit_exchange:exists(Name) of
        true ->
            ok;
        false ->
            Internal = case maps:get(internal, Exchange, undefined) of
                           undefined -> false; %% =< 2.2.0
                           I         -> I
                       end,
            case rabbit_exchange:declare(Name,
                                         rabbit_exchange:check_type(maps:get(type, Exchange, undefined)),
                                         maps:get(durable,                         Exchange, undefined),
                                         maps:get(auto_delete,                     Exchange, undefined),
                                         Internal,
                                         args(maps:get(arguments, Exchange, undefined)),
                                         ActingUser) of
                {ok, _Exchange} ->
                    ok;
                {error, timeout} = Err ->
                    throw(Err)
            end
    end.

add_binding(Binding, ActingUser) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, r(exchange, source, Binding),
                    r(DestType, destination, Binding), ActingUser).

add_binding(VHost, Binding, ActingUser) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, rv(VHost, exchange, source, Binding),
                    rv(VHost, DestType, destination, Binding), ActingUser).

add_binding_int(Binding, Source, Destination, ActingUser) ->
    case rabbit_binding:add(
           #binding{source       = Source,
                    destination  = Destination,
                    key          = maps:get(routing_key, Binding, undefined),
                    args         = args(maps:get(arguments, Binding, undefined))},
           ActingUser) of
        ok ->
            ok;
        {error, _} = Err ->
            throw(Err)
    end.

dest_type(Binding) ->
    rabbit_data_coercion:to_atom(maps:get(destination_type, Binding, undefined)).

r(Type, Props) -> r(Type, name, Props).

r(Type, Name, Props) ->
    rabbit_misc:r(maps:get(vhost, Props, undefined), Type, maps:get(Name, Props, undefined)).

rv(VHost, Type, Props) -> rv(VHost, Type, name, Props).

rv(VHost, Type, Name, Props) ->
    rabbit_misc:r(VHost, Type, maps:get(Name, Props, undefined)).

%%--------------------------------------------------------------------

validate_limits(All) ->
    case maps:get(queues, All, undefined) of
        undefined -> ok;
        Queues0 ->
            {ok, VHostMap} = filter_out_existing_queues(Queues0),
            _ = rabbit_log:debug("Definition import. Virtual host map for validation: ~p", [VHostMap]),
            maps:fold(fun validate_vhost_limit/3, ok, VHostMap)
    end.

validate_limits(All, VHost) ->
    case maps:get(queues, All, undefined) of
        undefined -> ok;
        Queues0 ->
            Queues1 = filter_out_existing_queues(VHost, Queues0),
            AddCount = length(Queues1),
            validate_vhost_limit(VHost, AddCount, ok)
    end.

filter_out_existing_queues(Queues) ->
    build_filtered_map(Queues, maps:new()).

filter_out_existing_queues(VHost, Queues) ->
    Pred = fun(Queue) ->
               Rec = rv(VHost, queue, <<"name">>, Queue),
               not rabbit_amqqueue:exists(Rec)
           end,
    lists:filter(Pred, Queues).

build_queue_data(Queue) ->
    VHost = maps:get(<<"vhost">>, Queue, undefined),
    case VHost of
        undefined -> undefined;
        Value ->
            Rec = rv(Value, queue, <<"name">>, Queue),
            {Rec, VHost}
    end.

build_filtered_map([], AccMap) ->
    {ok, AccMap};
build_filtered_map([Queue|Rest], AccMap0) ->
    %% If virtual host is not specified in a queue,
    %% this definition file is likely virtual host-specific.
    %%
    %% Skip such queues.
    case build_queue_data(Queue) of
        undefined -> build_filtered_map(Rest, AccMap0);
        {Rec, VHost} when VHost =/= undefined ->
            case rabbit_amqqueue:exists(Rec) of
                false ->
                    AccMap1 = maps:update_with(VHost, fun(V) -> V + 1 end, 1, AccMap0),
                    build_filtered_map(Rest, AccMap1);
                true ->
                    build_filtered_map(Rest, AccMap0)
            end
    end.

validate_vhost_limit(VHost, AddCount, ok) ->
    WouldExceed = rabbit_vhost_limit:would_exceed_queue_limit(AddCount, VHost),
    validate_vhost_queue_limit(VHost, AddCount, WouldExceed).

validate_vhost_queue_limit(_VHost, 0, _) ->
    % Note: not adding any new queues so the upload
    % must be update-only
    ok;
validate_vhost_queue_limit(_VHost, _AddCount, false) ->
    % Note: would not exceed queue limit
    ok;
validate_vhost_queue_limit(VHost, AddCount, {true, Limit, QueueCount}) ->
    ErrFmt = "Adding ~B queue(s) to virtual host \"~ts\" would exceed the limit of ~B queue(s).~n~nThis virtual host currently has ~B queue(s) defined.~n~nImport aborted!",
    ErrInfo = [AddCount, VHost, Limit, QueueCount],
    ErrMsg = rabbit_misc:format(ErrFmt, ErrInfo),
    exit({vhost_limit_exceeded, ErrMsg}).

get_or_missing(K, L) ->
    case maps:get(K, L, undefined) of
        undefined -> {key_missing, K};
        V         -> V
    end.

args(undefined) -> args(#{});
args([]) -> args(#{});
args(L)  -> rabbit_misc:to_amqp_table(L).

%%
%% Export
%%

list_exchanges() ->
    %% exclude internal exchanges, they are not meant to be declared or used by
    %% applications
    [exchange_definition(X) || X <- lists:filter(fun(#exchange{internal = true}) -> false;
                                                    (#exchange{name = #resource{name = <<>>}}) -> false;
                                                    (#exchange{name = #resource{name = <<"amq.", _Rest/binary>>}}) -> false;
                                                    (_) -> true
                                                 end,
                                                 rabbit_exchange:list())].

exchange_definition(#exchange{name = #resource{virtual_host = VHost, name = Name},
                              type = Type,
                              durable = Durable, auto_delete = AD, arguments = Args}) ->
    #{<<"vhost">> => VHost,
      <<"name">> => Name,
      <<"type">> => Type,
      <<"durable">> => Durable,
      <<"auto_delete">> => AD,
      <<"arguments">> => rabbit_misc:amqp_table(Args)}.

list_queues() ->
    %% exclude exclusive queues, they cannot be restored
    [queue_definition(Q) || Q <- lists:filter(fun(Q0) ->
                                                amqqueue:get_exclusive_owner(Q0) =:= none
                                              end,
                                              rabbit_amqqueue:list())].

queue_definition(Q) ->
    #resource{virtual_host = VHost, name = Name} = amqqueue:get_name(Q),
    Type = case amqqueue:get_type(Q) of
               rabbit_classic_queue -> classic;
               rabbit_quorum_queue -> quorum;
               rabbit_stream_queue -> stream;
               T -> T
           end,
    #{
        <<"vhost">> => VHost,
        <<"name">> => Name,
        <<"type">> => Type,
        <<"durable">> => amqqueue:is_durable(Q),
        <<"auto_delete">> => amqqueue:is_auto_delete(Q),
        <<"arguments">> => rabbit_misc:amqp_table(amqqueue:get_arguments(Q))
    }.

list_bindings() ->
    [binding_definition(B) || B <- rabbit_binding:list_explicit()].

binding_definition(#binding{source      = S,
                            key         = RoutingKey,
                            destination = D,
                            args        = Args}) ->
    #{
        <<"source">> => S#resource.name,
        <<"vhost">> => S#resource.virtual_host,
        <<"destination">> => D#resource.name,
        <<"destination_type">> => D#resource.kind,
        <<"routing_key">> => RoutingKey,
        <<"arguments">> => rabbit_misc:amqp_table(Args)
    }.

list_vhosts() ->
    [vhost_definition(V) || V <- rabbit_vhost:all()].

vhost_definition(VHost) ->
    Name = vhost:get_name(VHost),
    DQT = rabbit_queue_type:short_alias_of(rabbit_vhost:default_queue_type(Name)),
    #{
        <<"name">> => Name,
        <<"limits">> => vhost:get_limits(VHost),
        <<"metadata">> => vhost:get_metadata(VHost),
        <<"default_queue_type">> => DQT
    }.

list_users() ->
    [user_definition(U) || U <- rabbit_auth_backend_internal:all_users()].

user_definition(User) ->
    #{<<"name">>              => internal_user:get_username(User),
      <<"password_hash">>     => base64:encode(internal_user:get_password_hash(User)),
      <<"hashing_algorithm">> => rabbit_auth_backend_internal:hashing_module_for_user(User),
      <<"tags">>              => tags_as_binaries(internal_user:get_tags(User)),
      <<"limits">>            => internal_user:get_limits(User)
    }.

list_runtime_parameters() ->
    [runtime_parameter_definition(P) || P <- rabbit_runtime_parameters:list(), is_list(P)].

runtime_parameter_definition(Param) ->
    #{
        <<"vhost">> => pget(vhost, Param),
        <<"component">> => pget(component, Param),
        <<"name">> => pget(name, Param),
        <<"value">> => maybe_map(pget(value, Param))
    }.

maybe_map(Value) ->
    %% Not all definitions are maps. `federation-upstream-set` is
    %% a list of maps, and it should be exported as it has been
    %% imported
    try
        rabbit_data_coercion:to_map(Value)
    catch
        error:badarg ->
            Value
    end.

list_global_runtime_parameters() ->
    [global_runtime_parameter_definition(P) || P <- rabbit_runtime_parameters:list_global(), not is_internal_parameter(P)].

global_runtime_parameter_definition(P0) ->
    P = [{rabbit_data_coercion:to_binary(K), V} || {K, V} <- P0],
    maps:from_list(P).

-define(INTERNAL_GLOBAL_PARAM_PREFIX, "internal").

is_internal_parameter(Param) ->
    Name = rabbit_data_coercion:to_list(pget(name, Param)),
    %% if global parameter name starts with an "internal", consider it to be internal
    %% and exclude it from definition export
    string:left(Name, length(?INTERNAL_GLOBAL_PARAM_PREFIX)) =:= ?INTERNAL_GLOBAL_PARAM_PREFIX.

list_policies() ->
    [policy_definition(P) || P <- rabbit_policy:list()].

policy_definition(Policy) ->
    #{
        <<"vhost">> => pget(vhost, Policy),
        <<"name">> => pget(name, Policy),
        <<"pattern">> => pget(pattern, Policy),
        <<"apply-to">> => pget('apply-to', Policy),
        <<"priority">> => pget(priority, Policy),
        <<"definition">> => maps:from_list(pget(definition, Policy))
    }.

list_permissions() ->
    [permission_definition(P) || P <- rabbit_auth_backend_internal:list_permissions()].

permission_definition(P0) ->
    P = [{rabbit_data_coercion:to_binary(K), V} || {K, V} <- P0],
    maps:from_list(P).

list_topic_permissions() ->
    [topic_permission_definition(P) || P <- rabbit_auth_backend_internal:list_topic_permissions()].

topic_permission_definition(P0) ->
    P = [{rabbit_data_coercion:to_binary(K), V} || {K, V} <- P0],
    maps:from_list(P).

tags_as_binaries(Tags) ->
    [to_binary(T) || T <- Tags].
