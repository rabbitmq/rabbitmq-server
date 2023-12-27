%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2018-2023 VMware, Inc. or its affiliates.
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

-export([get/1,
         list/1,
         states/0,
         is_supported/1,
         is_enabled/1,
         is_registry_initialized/0,
         is_registry_written_to_disk/0,
         inventory/0]).

-ifdef(TEST).
-on_load(on_load/0).
-endif.

%% In this registry stub, most functions want to return `init_required' to let
%% {@link rabbit_ff_registry_wrapper} do the first time initialization.
%%
%% The inner case statement is here to convince Dialyzer that the function
%% could return values of type `__ReturnedIfUninitialized' or
%% `__NeverReturned'.
%%
%% If the function was only calling itself (`Call'), Dialyzer would consider
%% that it would never return. With the outer case, Dialyzer would conclude
%% that `__ReturnedIfUninitialized' is always returned and other values will
%% never be returned and there is no point in expecting them.
%%
%% In the end, `Call' is never executed because {@link
%% rabbit_ff_registry_wrapper} is responsible for calling the registry
%% function again after initialization.
%%
%% With both cases in place, it seems that we can convince Dialyzer that the
%% function returns values matching its spec.
-define(convince_dialyzer(__Call, __ReturnedIfUninitialized, __NeverReturned),
        case always_return_true() of
            false ->
                __Call;
            true ->
                case always_return_true() of
                    true  -> __ReturnedIfUninitialized;
                    false -> __NeverReturned
                end
        end).

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
    ?convince_dialyzer(
       ?MODULE:get(FeatureName),
       init_required,
       lists:nth(
         rand:uniform(2),
         [#{name => feature_flag,
            provided_by => rabbit},
          #{name => deprecated_feature,
            deprecation_phase =>
            lists:nth(
              4,
              [permitted_by_default,
               denied_by_default,
               disconnected,
               removed]),
            messages => #{},
            provided_by => rabbit}])).

-spec list(Which) -> Ret when
      Which :: all | enabled | disabled,
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

list(Which) ->
    ?convince_dialyzer(?MODULE:list(Which), init_required, #{}).

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
    ?convince_dialyzer(?MODULE:states(), init_required, #{}).

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
    ?convince_dialyzer(?MODULE:is_supported(FeatureName), init_required, true).

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
    ?convince_dialyzer(?MODULE:is_enabled(FeatureName), init_required, true).

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
    always_return_false().

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
    always_return_true().

-spec inventory() -> Ret when
      Ret :: Inventory | init_required,
      Inventory :: rabbit_feature_flags:inventory().

inventory() ->
    Inventory = #{applications => [],
                  feature_flags => #{},
                  states => #{}},
    ?convince_dialyzer(?MODULE:inventory(), init_required, Inventory).

always_return_true() ->
    %% This function is here to trick Dialyzer. We want some functions
    %% in this initial on-disk registry to always return `true` or
    %% `false`. However the generated registry will return actual
    %% booleans. The `-spec()` correctly advertises a return type of
    %% `boolean()`. But in the meantime, Dialyzer only knows about this
    %% copy which, without the trick below, would always return either
    %% `true` (e.g. in is_registry_written_to_disk/0) or `false` (e.g.
    %% is_registry_initialized/0). This obviously causes some warnings
    %% where the registry functions are used: Dialyzer believes that
    %% e.g. matching the return value of is_registry_initialized/0
    %% against `true` will never succeed.
    %%
    %% That's why this function makes a call which we know the result,
    %% but not Dialyzer, to "create" that hard-coded `true` return
    %% value.
    erlang:get({?MODULE, always_undefined}) =:= undefined.

always_return_false() ->
    not always_return_true().

-ifdef(TEST).
on_load() ->
     _ = (catch ?LOG_DEBUG(
                  "Feature flags: Loading initial (uninitialized) registry "
                  "module (~tp)",
                  [self()],
                  #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS})),
    ok.
-endif.
