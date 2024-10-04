%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2024 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ff_registry_factory).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-include("src/rabbit_feature_flags.hrl").
-include("src/rabbit_ff_registry.hrl").

-export([initialize_registry/0,
         initialize_registry/1,
         initialize_registry/3,
         acquire_state_change_lock/0,
         release_state_change_lock/0,
         reset_registry/0]).

-ifdef(TEST).
-export([registry_loading_lock/0]).
-endif.

-define(FF_STATE_CHANGE_LOCK, {feature_flags_state_change, self()}).
-define(FF_REGISTRY_LOADING_LOCK, {feature_flags_registry_loading, self()}).

-spec acquire_state_change_lock() -> ok.

acquire_state_change_lock() ->
    ?LOG_DEBUG(
      "Feature flags: acquiring lock ~tp",
      [?FF_STATE_CHANGE_LOCK],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    true = global:set_lock(?FF_STATE_CHANGE_LOCK),
    ?LOG_DEBUG(
      "Feature flags: acquired lock ~tp",
      [?FF_STATE_CHANGE_LOCK],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    ok.

-spec release_state_change_lock() -> ok.

release_state_change_lock() ->
    ?LOG_DEBUG(
      "Feature flags: releasing lock ~tp",
      [?FF_STATE_CHANGE_LOCK],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    true = global:del_lock(?FF_STATE_CHANGE_LOCK),
    ok.

-spec initialize_registry() -> Ret when
      Ret :: ok | {error, any()} | no_return().
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

-spec initialize_registry(FeatureFlags) -> Ret when
      FeatureFlags :: rabbit_feature_flags:feature_flags(),
      Ret :: ok | {error, any()} | no_return().
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
    %% The first step is to get the feature flag states. We start from the
    %% recorded states on disk (the `feature_flags' file).
    %%
    %% If this is the first time we initialize the registry, we use the
    %% recorded states. If the registry is refreshed, we merged the current
    %% in-memory states (which should have been recorded on disk already) on
    %% top of the on-disk states.
    %%
    %% This takes care of plugins initialized during node startup. A plugin's
    %% feature flags might have been enabled in a previous instance of the
    %% RabbitMQ node. Their state will be recorded on disk, but the in-memory
    %% registry (loaded earlier during startup⁾ doesn't have their state
    %% because the feature flags were not known at that time. That's why the
    %% on-disk states are read every time.

    AlreadyEnabledFeatureNames =
    rabbit_feature_flags:read_enabled_feature_flags_list(),
    FeatureStates0 = enabled_feature_flags_to_feature_states(
                       AlreadyEnabledFeatureNames),

    RegistryInitialized = rabbit_ff_registry:is_registry_initialized(),
    FeatureStates = case RegistryInitialized of
                        true ->
                            maps:merge(
                              FeatureStates0,
                              rabbit_ff_registry_wrapper:states());
                        false ->
                            FeatureStates0
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

-spec enabled_feature_flags_to_feature_states(FeatureNames) ->
    FeatureStates when
      FeatureNames :: [rabbit_feature_flags:feature_name()],
      FeatureStates :: rabbit_feature_flags:feature_states().

enabled_feature_flags_to_feature_states(FeatureNames) ->
    maps:from_list([{FeatureName, true} || FeatureName <- FeatureNames]).

-spec initialize_registry(FeatureFlags,
                          FeatureStates,
                          WrittenToDisk) -> Ret when
      FeatureFlags :: rabbit_feature_flags:feature_flags(),
      FeatureStates :: rabbit_feature_flags:feature_states(),
      WrittenToDisk :: boolean(),
      Ret :: ok | {error, any()} | no_return().
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
        true = global:set_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
        ?LOG_DEBUG(
           "Feature flags: acquired lock before initializing registry (~tp)",
           [self()],
           #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
        ok = maybe_initialize_registry(NewSupportedFeatureFlags,
                                       NewFeatureStates,
                                       WrittenToDisk)
    catch
        throw:{error, _} = Error2 ->
            Error2
    after
        ?LOG_DEBUG(
           "Feature flags: releasing lock after initializing registry (~tp)",
           [self()],
           #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
        true = global:del_lock(?FF_REGISTRY_LOADING_LOCK, [node()])
    end.

-spec maybe_initialize_registry(FeatureFlags,
                                FeatureStates,
                                WrittenToDisk) -> Ret when
      FeatureFlags :: rabbit_feature_flags:feature_flags(),
      FeatureStates :: rabbit_feature_flags:feature_states(),
      WrittenToDisk :: boolean(),
      Ret :: ok | no_return().

maybe_initialize_registry(NewSupportedFeatureFlags,
                          NewFeatureStates,
                          WrittenToDisk) ->
    %% We take the feature flags already registered.
    RegistryInitialized = rabbit_ff_registry:is_registry_initialized(),
    KnownFeatureFlags1 = case RegistryInitialized of
                             true  -> rabbit_ff_registry_wrapper:list(all);
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
                             maps:merge(
                               rabbit_ff_registry_wrapper:states(),
                               NewFeatureStates);
                         false ->
                             NewFeatureStates
                     end,
    FeatureStates1 =
    maps:map(
      fun
          (FeatureName, FeatureProps) when ?IS_FEATURE_FLAG(FeatureProps) ->
              Stability = rabbit_feature_flags:get_stability(FeatureProps),
              ProvidedBy = maps:get(provided_by, FeatureProps),
              State = case FeatureStates0 of
                          #{FeatureName := FeatureState} -> FeatureState;
                          _                              -> false
                      end,
              case Stability of
                  required when State =:= true ->
                      %% The required feature flag is already enabled, we keep
                      %% it this way.
                      State;
                  required when NewNode ->
                      %% This is the very first time the node starts, we
                      %% already mark the required feature flag as enabled.
                      ?assertNotEqual(state_changing, State),
                      true;
                  required when ProvidedBy =/= rabbit ->
                      ?assertNotEqual(state_changing, State),
                      true;
                  required ->
                      %% This is not a new node and the required feature flag
                      %% is disabled. This is an error and RabbitMQ must be
                      %% downgraded to enable the feature flag.
                      ?assertNotEqual(state_changing, State),
                      ?LOG_ERROR(
                         "Feature flags: `~ts`: required feature flag not "
                         "enabled! It must be enabled before upgrading "
                         "RabbitMQ.",
                         [FeatureName],
                         #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                      throw({error,
                             {disabled_required_feature_flag,
                              FeatureName}});
                  _ ->
                      State
              end;
          (FeatureName, FeatureProps) when ?IS_DEPRECATION(FeatureProps) ->
              case FeatureStates0 of
                  #{FeatureName := FeatureState} ->
                      FeatureState;
                  _ ->
                      not rabbit_deprecated_features:should_be_permitted(
                            FeatureName, FeatureProps)
              end
      end, AllFeatureFlags),

    %% We don't record the state of deprecated features because it is
    %% controlled from configuration and they can be disabled (the deprecated
    %% feature can be turned back on) if the deprecated feature allows it.
    %%
    %% However, some feature flags may depend on deprecated features. If those
    %% feature flags are enabled, we need to enable the deprecated features
    %% (turn off the deprecated features) they depend on regardless of the
    %% configuration.
    FeatureStates =
    enable_deprecated_features_required_by_enabled_feature_flags(
      AllFeatureFlags, FeatureStates1),

    %% The feature flags inventory is used by rabbit_ff_controller to query
    %% feature flags atomically. The inventory also contains the list of
    %% scanned applications: this is used to determine if an application is
    %% known by this node or not, and decide if a missing feature flag is
    %% unknown or unsupported.
    Inventory = #{applications => ScannedApps,
                  feature_flags => AllFeatureFlags,
                  states => FeatureStates,
                  written_to_disk => WrittenToDisk},

    Proceed = does_registry_need_refresh(Inventory),

    case Proceed of
        true ->
            ?LOG_DEBUG(
              "Feature flags: (re)initialize registry (~tp)",
              [self()],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            T0 = erlang:monotonic_time(),
            Ret = do_initialize_registry(Inventory),
            T1 = erlang:monotonic_time(),
            ?LOG_DEBUG(
              "Feature flags: time to regen registry: ~tp us",
              [erlang:convert_time_unit(T1 - T0, native, microsecond)],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Ret;
        false ->
            ?LOG_DEBUG(
              "Feature flags: registry already up-to-date, skipping init",
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ok
    end.

-spec does_registry_need_refresh(Inventory) -> Ret when
      Inventory :: rabbit_feature_flags:inventory(),
      Ret :: boolean().

does_registry_need_refresh(#{feature_flags := AllFeatureFlags,
                             states := FeatureStates,
                             written_to_disk := WrittenToDisk}) ->
    case rabbit_ff_registry:inventory() of
        #{feature_flags := CurrentAllFeatureFlags,
          states := CurrentFeatureStates,
          written_to_disk := CurrentWrittenToDisk} ->
            %% Before proceeding with the actual
            %% (re)initialization, let's see if there are any
            %% changes.
            if
                AllFeatureFlags =/= CurrentAllFeatureFlags ->
                    ?LOG_DEBUG(
                      "Feature flags: registry refresh needed: "
                      "yes, list of feature flags differs",
                      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    true;
                FeatureStates =/= CurrentFeatureStates ->
                    ?LOG_DEBUG(
                      "Feature flags: registry refresh needed: "
                      "yes, feature flag states differ",
                      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    true;
                WrittenToDisk =/= CurrentWrittenToDisk ->
                    ?LOG_DEBUG(
                      "Feature flags: registry refresh needed: "
                      "yes, \"written to disk\" state changed",
                      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    true;
                true ->
                    ?LOG_DEBUG(
                      "Feature flags: registry refresh needed: no",
                      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    false
            end;
        init_required ->
            ?LOG_DEBUG(
              "Feature flags: registry refresh needed: "
              "yes, first-time initialization",
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            true
    end.

-spec enable_deprecated_features_required_by_enabled_feature_flags(
        FeatureFlags, FeatureStates) -> NewFeatureStates when
      FeatureFlags :: rabbit_feature_flags:feature_flags(),
      FeatureStates :: rabbit_feature_flags:feature_states(),
      NewFeatureStates :: rabbit_feature_flags:feature_states().

enable_deprecated_features_required_by_enabled_feature_flags(
  FeatureFlags, FeatureStates) ->
    FeatureStates1 =
    maps:map(
      fun
          (DependencyName, false) ->
              RequiredBy =
              maps:filter(
                fun
                    (FeatureName, #{depends_on := DependsOn}) ->
                        lists:member(DependencyName, DependsOn) andalso
                        maps:get(FeatureName, FeatureStates) =:= true;
                    (_FeatureName, _FeatureProps) ->
                        false
                end, FeatureFlags),
              maps:size(RequiredBy) > 0;
          (_DependencyName, State) ->
              State
      end, FeatureStates),
    case FeatureStates1 of
        FeatureStates ->
            FeatureStates;
        _ ->
            enable_deprecated_features_required_by_enabled_feature_flags(
              FeatureFlags, FeatureStates1)
    end.

-spec do_initialize_registry(Inventory) -> Ret when
      Inventory :: rabbit_feature_flags:inventory(),
      Ret :: ok.
%% @private

do_initialize_registry(#{feature_flags := AllFeatureFlags,
                         states := FeatureStates,
                         applications := ScannedApps,
                         written_to_disk := WrittenToDisk} = Inventory) ->
    %% We log the state of those feature flags.
    ?LOG_DEBUG(
       begin
           AllFeatureNames = lists:sort(maps:keys(AllFeatureFlags)),
           {FeatureNames,
            DeprFeatureNames} = lists:partition(
                                  fun(FeatureName) ->
                                          FeatureProps = maps:get(
                                                           FeatureName,
                                                           AllFeatureFlags),
                                          ?IS_FEATURE_FLAG(FeatureProps)
                                  end, AllFeatureNames),

           IsRequired = fun(FeatureName) ->
                                FeatureProps = maps:get(
                                                 FeatureName,
                                                 AllFeatureFlags),
                                required =:=
                                rabbit_feature_flags:get_stability(
                                  FeatureProps)
                        end,
           {ReqFeatureNames,
            NonReqFeatureNames} = lists:partition(IsRequired, FeatureNames),
           {ReqDeprFeatureNames,
            NonReqDeprFeatureNames} = lists:partition(
                                        IsRequired, DeprFeatureNames),

           lists:flatten(
             "Feature flags: list of feature flags found:\n" ++
             [io_lib:format(
                "Feature flags:   [~ts] ~ts~n",
                [case maps:get(FeatureName, FeatureStates, false) of
                     true           -> "x";
                     state_changing -> "~";
                     false          -> " "
                 end,
                 FeatureName])
              || FeatureName <- NonReqFeatureNames] ++
             "Feature flags: list of deprecated features found:\n" ++
             [io_lib:format(
                "Feature flags:   [~ts] ~ts~n",
                [case maps:get(FeatureName, FeatureStates, false) of
                     true           -> "x";
                     state_changing -> "~";
                     false          -> " "
                 end,
                 FeatureName])
              || FeatureName <- NonReqDeprFeatureNames] ++
             [io_lib:format(
                "Feature flags: required feature flags not listed above: ~b~n"
                "Feature flags: removed deprecated features not listed "
                "above: ~b~n"
                "Feature flags: scanned applications: ~0tp~n"
                "Feature flags: feature flag states written to disk: ~ts",
                [length(ReqFeatureNames),
                 length(ReqDeprFeatureNames),
                 ScannedApps,
                 case WrittenToDisk of
                     true  -> "yes";
                     false -> "no"
                 end])])
       end,
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}
     ),

    persistent_term:put(?PT_INVENTORY_KEY, Inventory).

-ifdef(TEST).
registry_loading_lock() -> ?FF_REGISTRY_LOADING_LOCK.
-endif.

-spec reset_registry() -> ok.

reset_registry() ->
    ?LOG_DEBUG(
       "Feature flags: resetting loaded registry",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    persistent_term:erase(?PT_INVENTORY_KEY),
    ?assertNot(rabbit_ff_registry:is_registry_initialized()),
    ok.
