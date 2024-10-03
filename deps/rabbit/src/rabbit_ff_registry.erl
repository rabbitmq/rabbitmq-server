%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2024 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2019-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc.
%% and/or its subsidiaries. All rights reserved.
%%
%% @doc
%% This module exposes the API of the {@link rabbit_feature_flags}
%% registry. The feature flags registry is an Erlang module, compiled at
%% runtime, storing all the informations about feature flags: which are
%% supported, which are enabled, etc.
%%
%% Because it is compiled at runtime, the initial source code is mostly
%% an API reference. What the initial module does is merely ask {@link
%% rabbit_feature_flags} to generate the real registry.

-module(rabbit_ff_registry).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-include("src/rabbit_feature_flags.hrl").
-include("src/rabbit_ff_registry.hrl").

-export([get/1,
         list/1,
         states/0,
         is_supported/1,
         is_enabled/1,
         is_registry_initialized/0,
         is_registry_written_to_disk/0,
         inventory/0]).

-spec get(FeatureName) -> Ret when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: FeatureProps | init_required,
      FeatureProps :: rabbit_feature_flags:feature_props_extended() |
                      rabbit_deprecated_features:feature_props_extended() |
                      undefined.
%% @doc
%% Returns the properties of a feature flag.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @param FeatureName The name of the feature flag.
%% @returns the properties of the specified feature flag.

get(FeatureName) ->
    case inventory() of
        init_required ->
            init_required;
        #{feature_flags := FeatureFlags} ->
            maps:get(FeatureName, FeatureFlags, undefined)
    end.

-spec list(Which) -> Ret when
      Which :: all | enabled | disabled | state_changing,
      Ret :: FeatureFlags | init_required,
      FeatureFlags :: rabbit_feature_flags:feature_flags().
%% @doc
%% Lists all, enabled or disabled feature flags, depending on the argument.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @param Which The group of feature flags to return: `all', `enabled' or
%% `disabled'.
%% @returns A map of selected feature flags.

list(all) ->
    case inventory() of
        init_required ->
            init_required;
        #{feature_flags := AllFeatureFlags} ->
            AllFeatureFlags
    end;
list(enabled) ->
    case inventory() of
        init_required ->
            init_required;
        #{feature_flags := AllFeatureFlags, states := FeatureStates} ->
            maps:filter(
              fun(FeatureName, _FeatureProps) ->
                      maps:is_key(FeatureName, FeatureStates)
                      andalso
                      maps:get(FeatureName, FeatureStates) =:= true
              end, AllFeatureFlags)
    end;
list(disabled) ->
    case inventory() of
        init_required ->
            init_required;
        #{feature_flags := AllFeatureFlags, states := FeatureStates} ->
            maps:filter(
              fun(FeatureName, _FeatureProps) ->
                      not maps:is_key(FeatureName, FeatureStates)
                      orelse
                      maps:get(FeatureName, FeatureStates) =:= false
              end, AllFeatureFlags)
    end;
list(state_changing) ->
    case inventory() of
        init_required ->
            init_required;
        #{feature_flags := AllFeatureFlags, states := FeatureStates} ->
            maps:filter(
              fun(FeatureName, _FeatureProps) ->
                      maps:is_key(FeatureName, FeatureStates)
                      andalso
                      maps:get(FeatureName, FeatureStates) =:= state_changing
              end, AllFeatureFlags)
    end.

-spec states() -> Ret when
      Ret :: FeatureStates | init_required,
      FeatureStates :: rabbit_feature_flags:feature_states().
%% @doc
%% Returns the states of supported feature flags.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @returns A map of feature flag states.

states() ->
    case inventory() of
        init_required ->
            init_required;
        #{states := FeatureStates} ->
            FeatureStates
    end.

-spec is_supported(FeatureName) -> Ret when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: Supported | init_required,
      Supported :: boolean().
%% @doc
%% Returns if a feature flag is supported.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @param FeatureName The name of the feature flag to be checked.
%% @returns `true' if the feature flag is supported, or `false'
%%   otherwise.

is_supported(FeatureName) ->
    case inventory() of
        init_required ->
            init_required;
        #{feature_flags := FeatureFlags} ->
            maps:is_key(FeatureName, FeatureFlags)
    end.

-spec is_enabled(FeatureName) -> Ret when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: Enabled | init_required,
      Enabled :: boolean() | state_changing.
%% @doc
%% Returns if a feature flag is enabled or if its state is changing.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @param FeatureName The name of the feature flag to be checked.
%% @returns `true' if the feature flag is enabled, `state_changing' if
%%   its state is transient, or `false' otherwise.

is_enabled(FeatureName) ->
    case inventory() of
        init_required ->
            init_required;
        #{states := FeatureStates} ->
            maps:get(FeatureName, FeatureStates, false)
    end.

-spec is_registry_initialized() -> IsInitialized when
      IsInitialized :: boolean().
%% @doc
%% Indicates if the registry is initialized.
%%
%% The registry is considered initialized once the initial Erlang module
%% was replaced by the copy compiled at runtime.
%%
%% @returns `true' when the module is the one compiled at runtime,
%%   `false' when the module is the initial one compiled from RabbitMQ
%%   source code.

is_registry_initialized() ->
    inventory() =/= init_required.

-spec is_registry_written_to_disk() -> WrittenToDisk when
      WrittenToDisk :: boolean().
%% @doc
%% Indicates if the feature flags state was successfully persisted to disk.
%%
%% Note that on startup, {@link rabbit_feature_flags} tries to determine
%% the state of each supported feature flag, regardless of the
%% information on disk, to ensure maximum consistency. However, this can
%% be done for feature flags supporting it only.
%%
%% @returns `true' if the state was successfully written to disk and
%%   the registry can be initialized from that during the next RabbitMQ
%%   startup, `false' if the write failed and the node might loose feature
%%   flags state on restart.

is_registry_written_to_disk() ->
    case inventory() of
        init_required ->
            false;
        #{written_to_disk := IsWrittenToDisk} ->
            IsWrittenToDisk
    end.

-spec inventory() -> Ret when
      Ret :: Inventory | init_required,
      Inventory :: rabbit_feature_flags:inventory().

inventory() ->
    persistent_term:get(?PT_INVENTORY_KEY, init_required).
