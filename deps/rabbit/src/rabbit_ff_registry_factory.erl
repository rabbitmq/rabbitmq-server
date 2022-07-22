%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ff_registry_factory).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([initialize_registry/0,
         initialize_registry/1,
         initialize_registry/3,
         acquire_state_change_lock/0,
         release_state_change_lock/0]).

-ifdef(TEST).
-export([registry_loading_lock/0]).
-endif.

-define(FF_STATE_CHANGE_LOCK, {feature_flags_state_change, self()}).
-define(FF_REGISTRY_LOADING_LOCK, {feature_flags_registry_loading, self()}).

-type registry_vsn() :: term().

-spec acquire_state_change_lock() -> boolean().
acquire_state_change_lock() ->
    rabbit_log_feature_flags:debug(
      "Feature flags: acquiring lock ~p",
      [?FF_STATE_CHANGE_LOCK]),
    Ret = global:set_lock(?FF_STATE_CHANGE_LOCK),
    rabbit_log_feature_flags:debug(
      "Feature flags: acquired lock ~p",
      [?FF_STATE_CHANGE_LOCK]),
    Ret.

-spec release_state_change_lock() -> true.
release_state_change_lock() ->
    rabbit_log_feature_flags:debug(
      "Feature flags: releasing lock ~p",
      [?FF_STATE_CHANGE_LOCK]),
    global:del_lock(?FF_STATE_CHANGE_LOCK).

-spec initialize_registry() -> ok | {error, any()} | no_return().
%% @private
%% @doc
%% Initializes or reinitializes the registry.
%%
%% The registry is an Erlang module recompiled at runtime to hold the
%% state of all supported feature flags.
%%
%% That Erlang module is called {@link rabbit_ff_registry}. The initial
%% source code of this module simply calls this function so it is
%% replaced by a proper registry.
%%
%% Once replaced, the registry contains the map of all supported feature
%% flags and their state. This makes it very efficient to query a
%% feature flag state or property.
%%
%% The registry is local to all RabbitMQ nodes.

initialize_registry() ->
    initialize_registry(#{}).

-spec initialize_registry(rabbit_feature_flags:feature_flags()) ->
    ok | {error, any()} | no_return().
%% @private
%% @doc
%% Initializes or reinitializes the registry.
%%
%% See {@link initialize_registry/0} for a description of the registry.
%%
%% This function takes a map of new supported feature flags (so their
%% name and extended properties) to add to the existing known feature
%% flags.

initialize_registry(NewSupportedFeatureFlags) ->
    %% The first step is to get the feature flag states: if this is the
    %% first time we initialize it, we read the list from disk (the
    %% `feature_flags` file). Otherwise we query the existing registry
    %% before it is replaced.
    RegistryInitialized = rabbit_ff_registry:is_registry_initialized(),
    FeatureStates =
    case RegistryInitialized of
        true ->
            rabbit_ff_registry:states();
        false ->
            EnabledFeatureNames =
            rabbit_feature_flags:read_enabled_feature_flags_list(),
            rabbit_feature_flags:enabled_feature_flags_to_feature_states(
              EnabledFeatureNames)
    end,

    %% We also record if the feature flags state was correctly written
    %% to disk. Currently we don't use this information, but in the
    %% future, we might want to retry the write if it failed so far.
    %%
    %% TODO: Retry to write the feature flags state if the first try
    %% failed.
    WrittenToDisk = case RegistryInitialized of
                        true ->
                            rabbit_ff_registry:is_registry_written_to_disk();
                        false ->
                            true
                    end,
    initialize_registry(NewSupportedFeatureFlags,
                        FeatureStates,
                        WrittenToDisk).

-spec initialize_registry(rabbit_feature_flags:feature_flags(),
                          rabbit_feature_flags:feature_states(),
                          boolean()) ->
    ok | {error, any()} | no_return().
%% @private
%% @doc
%% Initializes or reinitializes the registry.
%%
%% See {@link initialize_registry/0} for a description of the registry.
%%
%% This function takes a map of new supported feature flags (so their
%% name and extended properties) to add to the existing known feature
%% flags, a map of the new feature flag states (whether they are
%% enabled, disabled or `state_changing'), and a flag to indicate if the
%% feature flag states was recorded to disk.
%%
%% The latter is used to block callers asking if a feature flag is
%% enabled or disabled while its state is changing.

initialize_registry(NewSupportedFeatureFlags,
                    NewFeatureStates,
                    WrittenToDisk) ->
    try
        Ret = maybe_initialize_registry(NewSupportedFeatureFlags,
                                        NewFeatureStates,
                                        WrittenToDisk),
        case Ret of
            ok      -> ok;
            restart -> initialize_registry(NewSupportedFeatureFlags,
                                           NewFeatureStates,
                                           WrittenToDisk);
            Error1  -> Error1
        end
    catch
        throw:{error, _} = Error2 ->
            Error2
    end.

-spec maybe_initialize_registry(rabbit_feature_flags:feature_flags(),
                                rabbit_feature_flags:feature_states(),
                                boolean()) ->
    ok | restart | {error, any()} | no_return().

maybe_initialize_registry(NewSupportedFeatureFlags,
                          NewFeatureStates,
                          WrittenToDisk) ->
    %% We save the version of the current registry before computing
    %% the new one. This is used when we do the actual reload: if the
    %% current registry was reloaded in the meantime, we need to restart
    %% the computation to make sure we don't loose data.
    RegistryVsn = registry_vsn(),

    %% We take the feature flags already registered.
    RegistryInitialized = rabbit_ff_registry:is_registry_initialized(),
    KnownFeatureFlags1 = case RegistryInitialized of
                             true  -> rabbit_ff_registry:list(all);
                             false -> #{}
                         end,

    %% Query the list (it's a map to be exact) of known
    %% supported feature flags. That list comes from the
    %% `-rabbitmq_feature_flag().` module attributes exposed by all
    %% currently loaded Erlang modules.
    {ScannedApps, KnownFeatureFlags2} =
    rabbit_feature_flags:query_supported_feature_flags(),

    %% We merge the feature flags we already knew about
    %% (KnownFeatureFlags1), those found in the loaded applications
    %% (KnownFeatureFlags2) and those specified in arguments
    %% (NewSupportedFeatureFlags). The latter come from remote nodes
    %% usually: for example, they can come from plugins loaded on remote
    %% node but the plugins are missing locally. In this case, we
    %% consider those feature flags supported because there is no code
    %% locally which would cause issues.
    %%
    %% It means that the list of feature flags only grows. we don't try
    %% to clean it at some point because we want to remember about the
    %% feature flags we saw (and their state). It should be fine because
    %% that list should remain small.
    KnownFeatureFlags = maps:merge(KnownFeatureFlags1,
                                   KnownFeatureFlags2),
    AllFeatureFlags = maps:merge(KnownFeatureFlags,
                                 NewSupportedFeatureFlags),

    %% Next we want to update the feature states, based on the new
    %% states passed as arguments.
    %%
    %% At the same time, we pay attention to required feature flags. Those
    %% are feature flags which must be enabled. The compatibility and
    %% migration code behind them is gone at that point. We distinguish two
    %% situations:
    %%   1. The node starts for the very first time (the
    %%      `enabled_feature_flags' file does not exist). In this case, the
    %%      required feature flags are marked as enabled right away.
    %%   2. This is a node restart (the file exists), and thus possibly an
    %%      upgrade. This time, if required feature flags are not enabled, we
    %%      return an error (and RabbitMQ start will abort). RabbitMQ won't be
    %%      able to work, especially if the feature flag needed some
    %%      migration, because the corresponding code was removed.
    NewNode =
    not rabbit_feature_flags:does_enabled_feature_flags_list_file_exist(),
    FeatureStates0 = case RegistryInitialized of
                         true ->
                             maps:merge(rabbit_ff_registry:states(),
                                        NewFeatureStates);
                         false ->
                             NewFeatureStates
                     end,
    FeatureStates = maps:map(
                      fun(FeatureName, FeatureProps) ->
                              Stability = maps:get(
                                            stability, FeatureProps, stable),
                              State = case FeatureStates0 of
                                          #{FeatureName := FeatureState} ->
                                              FeatureState;
                                          _ ->
                                              false
                                      end,
                              case Stability of
                                  required when State =:= true ->
                                      %% The required feature flag is already
                                      %% enabled, we keep it this way.
                                      State;
                                  required when NewNode ->
                                      %% This is the very first time the node
                                      %% starts, we already mark the required
                                      %% feature flag as enabled.
                                      ?assertNotEqual(state_changing, State),
                                      true;
                                  required ->
                                      %% This is not a new node and the
                                      %% required feature flag is disabled.
                                      %% This is an error and RabbitMQ must be
                                      %% downgraded to enable the feature
                                      %% flag.
                                      ?assertNotEqual(state_changing, State),
                                      rabbit_log_feature_flags:error(
                                        "Feature flags: `~s`: required "
                                        "feature flag not enabled! It must "
                                        "be enabled before upgrading "
                                        "RabbitMQ.",
                                        [FeatureName]),
                                      throw({error,
                                             {disabled_required_feature_flag,
                                              FeatureName}});
                                  _ ->
                                      State
                              end
                      end, AllFeatureFlags),

    %% The feature flags inventory is used by rabbit_ff_controller to query
    %% feature flags atomically. The inventory also contains the list of
    %% scanned applications: this is used to determine if an application is
    %% known by this node or not, and decide if a missing feature flag is
    %% unknown or unsupported.
    Inventory = #{applications => ScannedApps,
                  feature_flags => KnownFeatureFlags2,
                  states => FeatureStates},

    Proceed = does_registry_need_refresh(AllFeatureFlags,
                                         FeatureStates,
                                         WrittenToDisk),

    case Proceed of
        true ->
            rabbit_log_feature_flags:debug(
              "Feature flags: (re)initialize registry (~p)",
              [self()]),
            T0 = erlang:timestamp(),
            Ret = do_initialize_registry(RegistryVsn,
                                         AllFeatureFlags,
                                         FeatureStates,
                                         Inventory,
                                         WrittenToDisk),
            T1 = erlang:timestamp(),
            rabbit_log_feature_flags:debug(
              "Feature flags: time to regen registry: ~p us",
              [timer:now_diff(T1, T0)]),
            Ret;
        false ->
            rabbit_log_feature_flags:debug(
              "Feature flags: registry already up-to-date, skipping init"),
            ok
    end.

-spec does_registry_need_refresh(rabbit_feature_flags:feature_flags(),
                                 rabbit_feature_flags:feature_states(),
                                 boolean()) ->
    boolean().

does_registry_need_refresh(AllFeatureFlags,
                           FeatureStates,
                           WrittenToDisk) ->
    case rabbit_ff_registry:is_registry_initialized() of
        true ->
            %% Before proceeding with the actual
            %% (re)initialization, let's see if there are any
            %% changes.
            CurrentAllFeatureFlags = rabbit_ff_registry:list(all),
            CurrentFeatureStates = rabbit_ff_registry:states(),
            CurrentWrittenToDisk =
            rabbit_ff_registry:is_registry_written_to_disk(),

            if
                AllFeatureFlags =/= CurrentAllFeatureFlags ->
                    rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: "
                      "yes, list of feature flags differs"),
                    true;
                FeatureStates =/= CurrentFeatureStates ->
                    rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: "
                      "yes, feature flag states differ"),
                    true;
                WrittenToDisk =/= CurrentWrittenToDisk ->
                    rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: "
                      "yes, \"written to disk\" state changed"),
                    true;
                true ->
                    rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: no"),
                    false
            end;
        false ->
            rabbit_log_feature_flags:debug(
              "Feature flags: registry refresh needed: "
              "yes, first-time initialization"),
            true
    end.

-spec do_initialize_registry(registry_vsn(),
                             rabbit_feature_flags:feature_flags(),
                             rabbit_feature_flags:feature_states(),
                             rabbit_feature_flags:inventory(),
                             boolean()) ->
    ok | restart | {error, any()} | no_return().
%% @private

do_initialize_registry(RegistryVsn,
                       AllFeatureFlags,
                       FeatureStates,
                       #{applications := ScannedApps} = Inventory,
                       WrittenToDisk) ->
    %% We log the state of those feature flags.
    rabbit_log_feature_flags:debug(
      "Feature flags: list of feature flags found:~n" ++
      lists:flatten(
        [rabbit_misc:format(
           "Feature flags:   [~s] ~s~n",
           [case maps:get(FeatureName, FeatureStates, false) of
                true           -> "x";
                state_changing -> "~~";
                false          -> " "
            end,
            FeatureName])
         || FeatureName <- lists:sort(maps:keys(AllFeatureFlags))] ++
        [rabbit_misc:format(
           "Feature flags: scanned applications: ~p~n"
           "Feature flags: feature flag states written to disk: ~s",
           [ScannedApps,
            case WrittenToDisk of
                true  -> "yes";
                false -> "no"
            end])])
     ),

    %% We request the registry to be regenerated and reloaded with the
    %% new state.
    regen_registry_mod(RegistryVsn,
                       AllFeatureFlags,
                       FeatureStates,
                       Inventory,
                       WrittenToDisk).

-spec regen_registry_mod(
        RegistryVsn, AllFeatureFlags, FeatureStates, Inventory,
        WrittenToDisk) -> Ret when
      RegistryVsn :: registry_vsn(),
      AllFeatureFlags :: rabbit_feature_flags:feature_flags(),
      FeatureStates :: rabbit_feature_flags:feature_states(),
      Inventory :: rabbit_feature_flags:inventory(),
      WrittenToDisk :: boolean(),
      Ret :: ok | restart | {error, any()} | no_return().
%% @private

regen_registry_mod(RegistryVsn,
                   AllFeatureFlags,
                   FeatureStates,
                   Inventory,
                   WrittenToDisk) ->
    %% Here, we recreate the source code of the `rabbit_ff_registry`
    %% module from scratch.
    %%
    %% IMPORTANT: We want both modules to have the exact same public
    %% API in order to simplify the life of developers and their tools
    %% (Dialyzer, completion, and so on).

    %% -module(rabbit_ff_registry).
    ModuleAttr = erl_syntax:attribute(
                   erl_syntax:atom(module),
                   [erl_syntax:atom(rabbit_ff_registry)]),
    ModuleForm = erl_syntax:revert(ModuleAttr),
    %% -export([...]).
    ExportAttr = erl_syntax:attribute(
                   erl_syntax:atom(export),
                   [erl_syntax:list(
                      [erl_syntax:arity_qualifier(
                         erl_syntax:atom(F),
                         erl_syntax:integer(A))
                       || {F, A} <- [{get, 1},
                                     {list, 1},
                                     {states, 0},
                                     {is_supported, 1},
                                     {is_enabled, 1},
                                     {is_registry_initialized, 0},
                                     {is_registry_written_to_disk, 0},
                                     {inventory, 0}]]
                     )
                   ]
                  ),
    ExportForm = erl_syntax:revert(ExportAttr),
    %% get(_) -> ...
    GetClauses = [erl_syntax:clause(
                    [erl_syntax:atom(FeatureName)],
                    [],
                    [erl_syntax:abstract(maps:get(FeatureName,
                                                  AllFeatureFlags))])
                     || FeatureName <- maps:keys(AllFeatureFlags)
                    ],
    GetUnknownClause = erl_syntax:clause(
                         [erl_syntax:variable("_")],
                         [],
                         [erl_syntax:atom(undefined)]),
    GetFun = erl_syntax:function(
               erl_syntax:atom(get),
               GetClauses ++ [GetUnknownClause]),
    GetFunForm = erl_syntax:revert(GetFun),
    %% list(_) -> ...
    ListAllBody = erl_syntax:abstract(AllFeatureFlags),
    ListAllClause = erl_syntax:clause([erl_syntax:atom(all)],
                                      [],
                                      [ListAllBody]),
    EnabledFeatureFlags = maps:filter(
                            fun(FeatureName, _) ->
                                    maps:is_key(FeatureName,
                                                FeatureStates)
                                    andalso
                                    maps:get(FeatureName, FeatureStates)
                                    =:=
                                    true
                            end, AllFeatureFlags),
    ListEnabledBody = erl_syntax:abstract(EnabledFeatureFlags),
    ListEnabledClause = erl_syntax:clause(
                          [erl_syntax:atom(enabled)],
                          [],
                          [ListEnabledBody]),
    DisabledFeatureFlags = maps:filter(
                            fun(FeatureName, _) ->
                                    not maps:is_key(FeatureName,
                                                    FeatureStates)
                                    orelse
                                    maps:get(FeatureName, FeatureStates)
                                    =:=
                                    false
                            end, AllFeatureFlags),
    ListDisabledBody = erl_syntax:abstract(DisabledFeatureFlags),
    ListDisabledClause = erl_syntax:clause(
                          [erl_syntax:atom(disabled)],
                          [],
                          [ListDisabledBody]),
    StateChangingFeatureFlags = maps:filter(
                                  fun(FeatureName, _) ->
                                          maps:is_key(FeatureName,
                                                      FeatureStates)
                                          andalso
                                          maps:get(FeatureName, FeatureStates)
                                          =:=
                                          state_changing
                                  end, AllFeatureFlags),
    ListStateChangingBody = erl_syntax:abstract(StateChangingFeatureFlags),
    ListStateChangingClause = erl_syntax:clause(
                                [erl_syntax:atom(state_changing)],
                                [],
                                [ListStateChangingBody]),
    ListFun = erl_syntax:function(
                erl_syntax:atom(list),
                [ListAllClause,
                 ListEnabledClause,
                 ListDisabledClause,
                 ListStateChangingClause]),
    ListFunForm = erl_syntax:revert(ListFun),
    %% states() -> ...
    StatesBody = erl_syntax:abstract(FeatureStates),
    StatesClause = erl_syntax:clause([], [], [StatesBody]),
    StatesFun = erl_syntax:function(
                  erl_syntax:atom(states),
                  [StatesClause]),
    StatesFunForm = erl_syntax:revert(StatesFun),
    %% is_supported(_) -> ...
    IsSupportedClauses = [erl_syntax:clause(
                            [erl_syntax:atom(FeatureName)],
                            [],
                            [erl_syntax:atom(true)])
                          || FeatureName <- maps:keys(AllFeatureFlags)
                         ],
    NotSupportedClause = erl_syntax:clause(
                           [erl_syntax:variable("_")],
                           [],
                           [erl_syntax:atom(false)]),
    IsSupportedFun = erl_syntax:function(
                       erl_syntax:atom(is_supported),
                       IsSupportedClauses ++ [NotSupportedClause]),
    IsSupportedFunForm = erl_syntax:revert(IsSupportedFun),
    %% is_enabled(_) -> ...
    IsEnabledClauses = [erl_syntax:clause(
                          [erl_syntax:atom(FeatureName)],
                          [],
                          [case maps:is_key(FeatureName, FeatureStates) of
                               true ->
                                   erl_syntax:atom(
                                     maps:get(FeatureName, FeatureStates));
                               false ->
                                   erl_syntax:atom(false)
                           end])
                        || FeatureName <- maps:keys(AllFeatureFlags)
                       ],
    NotEnabledClause = erl_syntax:clause(
                         [erl_syntax:variable("_")],
                         [],
                         [erl_syntax:atom(false)]),
    IsEnabledFun = erl_syntax:function(
                     erl_syntax:atom(is_enabled),
                     IsEnabledClauses ++ [NotEnabledClause]),
    IsEnabledFunForm = erl_syntax:revert(IsEnabledFun),
    %% is_registry_initialized() -> ...
    IsInitializedClauses = [erl_syntax:clause(
                              [],
                              [],
                              [erl_syntax:atom(true)])
                           ],
    IsInitializedFun = erl_syntax:function(
                         erl_syntax:atom(is_registry_initialized),
                         IsInitializedClauses),
    IsInitializedFunForm = erl_syntax:revert(IsInitializedFun),
    %% is_registry_written_to_disk() -> ...
    IsWrittenToDiskClauses = [erl_syntax:clause(
                                [],
                                [],
                                [erl_syntax:atom(WrittenToDisk)])
                             ],
    IsWrittenToDiskFun = erl_syntax:function(
                           erl_syntax:atom(is_registry_written_to_disk),
                           IsWrittenToDiskClauses),
    IsWrittenToDiskFunForm = erl_syntax:revert(IsWrittenToDiskFun),
    %% inventory() -> ...
    InventoryBody = erl_syntax:abstract(Inventory),
    InventoryClause = erl_syntax:clause([], [], [InventoryBody]),
    InventoryFun = erl_syntax:function(
                     erl_syntax:atom(inventory),
                     [InventoryClause]),
    InventoryFunForm = erl_syntax:revert(InventoryFun),
    %% Compilation!
    Forms = [ModuleForm,
             ExportForm,
             GetFunForm,
             ListFunForm,
             StatesFunForm,
             IsSupportedFunForm,
             IsEnabledFunForm,
             IsInitializedFunForm,
             IsWrittenToDiskFunForm,
             InventoryFunForm],
    maybe_log_registry_source_code(Forms),
    CompileOpts = [return_errors,
                   return_warnings],
    case compile:forms(Forms, CompileOpts) of
        {ok, Mod, Bin, _} ->
            load_registry_mod(RegistryVsn, Mod, Bin);
        {error, Errors, Warnings} ->
            rabbit_log_feature_flags:error(
              "Feature flags: registry compilation failure:~n"
              "Errors: ~p~n"
              "Warnings: ~p",
              [Errors, Warnings]),
            {error, {compilation_failure, Errors, Warnings}};
        error ->
            rabbit_log_feature_flags:error(
              "Feature flags: registry compilation failure",
              []),
            {error, {compilation_failure, [], []}}
    end.

maybe_log_registry_source_code(Forms) ->
    case rabbit_prelaunch:get_context() of
        #{log_feature_flags_registry := true} ->
            rabbit_log_feature_flags:debug(
              "== FEATURE FLAGS REGISTRY ==~n"
              "~s~n"
              "== END ==~n",
              [erl_prettypr:format(erl_syntax:form_list(Forms))]),
            ok;
        _ ->
            ok
    end.

-ifdef(TEST).
registry_loading_lock() -> ?FF_REGISTRY_LOADING_LOCK.
-endif.

-spec load_registry_mod(registry_vsn(), module(), binary()) ->
    ok | restart | no_return().
%% @private

load_registry_mod(RegistryVsn, Mod, Bin) ->
    rabbit_log_feature_flags:debug(
      "Feature flags: registry module ready, loading it (~p)...",
      [self()]),
    FakeFilename = "Compiled and loaded by " ?MODULE_STRING,
    %% Time to load the new registry, replacing the old one. We use a
    %% lock here to synchronize concurrent reloads.
    global:set_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
    rabbit_log_feature_flags:debug(
      "Feature flags: acquired lock before reloading registry module (~p)",
     [self()]),
    %% We want to make sure that the old registry (not the one being
    %% currently in use) is purged by the code server. It means no
    %% process lingers on that old code.
    %%
    %% We use code:soft_purge() for that (meaning no process is killed)
    %% and we wait in an infinite loop for that to succeed.
    ok = purge_old_registry(Mod),
    %% Now we can replace the currently loaded registry by the new one.
    %% The code server takes care of marking the current registry as old
    %% and load the new module in an atomic operation.
    %%
    %% Therefore there is no chance of a window where there is no
    %% registry module available, causing the one on disk to be
    %% reloaded.
    Ret = case registry_vsn() of
              RegistryVsn -> code:load_binary(Mod, FakeFilename, Bin);
              OtherVsn    -> {error, {restart, RegistryVsn, OtherVsn}}
          end,
    rabbit_log_feature_flags:debug(
      "Feature flags: releasing lock after reloading registry module (~p)",
     [self()]),
    global:del_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
    case Ret of
        {module, _} ->
            rabbit_log_feature_flags:debug(
              "Feature flags: registry module loaded (vsn: ~p -> ~p)",
              [RegistryVsn, registry_vsn()]),
            ok;
        {error, {restart, Expected, Current}} ->
            rabbit_log_feature_flags:error(
              "Feature flags: another registry module was loaded in the "
              "meantime (expected old vsn: ~p, current vsn: ~p); "
              "restarting the regen",
              [Expected, Current]),
            restart;
        {error, Reason} ->
            rabbit_log_feature_flags:error(
              "Feature flags: failed to load registry module: ~p",
              [Reason]),
            throw({feature_flag_registry_reload_failure, Reason})
    end.

-spec registry_vsn() -> registry_vsn().
%% @private

registry_vsn() ->
    Attrs = rabbit_ff_registry:module_info(attributes),
    proplists:get_value(vsn, Attrs, undefined).

purge_old_registry(Mod) ->
    case code:is_loaded(Mod) of
        {file, _} -> do_purge_old_registry(Mod);
        false     -> ok
    end.

do_purge_old_registry(Mod) ->
    case code:soft_purge(Mod) of
        true  -> ok;
        false -> do_purge_old_registry(Mod)
    end.
