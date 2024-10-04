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
%% This module sits in front of {@link rabbit_ff_registry}.
%%
%% If {@link rabbit_ff_registry} is the registry stub, calls to its functions
%% will return `init_required'. In this case, this module is responsible for
%% running the registry initialization and call {@link rabbit_ff_registry}
%% again.
%%
%% It was introduced to fix a deadlock with the Code server when a process
%% called the registry stub and that triggered the initialization of the
%% registry. If the registry stub was already marked as deleted in the Code
%% server, purging it while a process lingered on the deleted copy would cause
%% that deadlock: the Code server would never return because that process
%% would never make progress and thus would never stop using the deleted
%% registry stub.

-module(rabbit_ff_registry_wrapper).

-compile({no_auto_import, [get/1]}).

-export([get/1,
         list/1,
         states/0,
         is_supported/1,
         is_enabled/1,
         inventory/0]).

-spec get(FeatureName) -> FeatureProps when
      FeatureName :: rabbit_feature_flags:feature_name(),
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
    case rabbit_ff_registry:get(FeatureName) of
        init_required ->
            initialize_registry(),
            get(FeatureName);
        Ret ->
            Ret
    end.

-spec list(Which) -> FeatureFlags when
      Which :: all | enabled | disabled,
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

list(Which) ->
    case rabbit_ff_registry:list(Which) of
        init_required ->
            initialize_registry(),
            list(Which);
        Ret ->
            Ret
    end.

-spec states() -> FeatureStates when
      FeatureStates :: rabbit_feature_flags:feature_states().
%% @doc
%% Returns the states of supported feature flags.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @returns A map of feature flag states.

states() ->
    case rabbit_ff_registry:states() of
        init_required ->
            initialize_registry(),
            states();
        Ret ->
            Ret
    end.

-spec is_supported(FeatureName) -> Supported when
      FeatureName :: rabbit_feature_flags:feature_name(),
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
    case rabbit_ff_registry:is_supported(FeatureName) of
        init_required ->
            initialize_registry(),
            is_supported(FeatureName);
        Ret ->
            Ret
    end.

-spec is_enabled(FeatureName) -> Enabled when
      FeatureName :: rabbit_feature_flags:feature_name(),
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
    case rabbit_ff_registry:is_enabled(FeatureName) of
        init_required ->
            initialize_registry(),
            is_enabled(FeatureName);
        Ret ->
            Ret
    end.

-spec inventory() -> Inventory when
      Inventory :: rabbit_feature_flags:inventory().
%% @doc Unused for now (FIXME)

inventory() ->
    case rabbit_ff_registry:inventory() of
        init_required ->
            initialize_registry(),
            inventory();
        Ret ->
            Ret
    end.

initialize_registry() ->
    %% We acquire the feature flags registry reload lock here to make sure we
    %% don't reload the registry in the middle of a cluster join. Indeed, the
    %% registry is reset and feature flags states are copied from a remote
    %% node. Therefore, there is a small window where the registry is not
    %% loaded and the states on disk do not reflect the intent.
    rabbit_ff_registry_factory:acquire_state_change_lock(),
    try
        _ = rabbit_ff_registry_factory:initialize_registry(),
        ok
    after
        rabbit_ff_registry_factory:release_state_change_lock()
    end.
