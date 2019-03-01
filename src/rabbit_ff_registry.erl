%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2018-2019 Pivotal Software, Inc.
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

-export([get/1,
         list/1,
         states/0,
         is_supported/1,
         is_enabled/1,
         is_registry_initialized/0,
         is_registry_written_to_disk/0]).

-spec get(rabbit_feature_flags:feature_name()) ->
    rabbit_feature_flags:feature_props() | undefined.
%% @doc
%% Returns the properties of a feature flag.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @param FeatureName The name of the feature flag.
%% @returns the properties of the specified feature flag.

get(FeatureName) ->
    rabbit_feature_flags:initialize_registry(),
    %% Initially, is_registry_initialized/0 always returns `false`
    %% and this ?MODULE:get(FeatureName) is always called. The case
    %% statement is here to please Dialyzer.
    case is_registry_initialized() of
        false -> ?MODULE:get(FeatureName);
        true  -> undefined
    end.

-spec list(all | enabled | disabled) -> rabbit_feature_flags:feature_flags().
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
    rabbit_feature_flags:initialize_registry(),
    %% See get/1 for an explanation of the case statement below.
    case is_registry_initialized() of
        false -> ?MODULE:list(Which);
        true  -> #{}
    end.

-spec states() -> rabbit_feature_flags:feature_states().
%% @doc
%% Returns the states of supported feature flags.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @returns A map of feature flag states.

states() ->
    rabbit_feature_flags:initialize_registry(),
    %% See get/1 for an explanation of the case statement below.
    case is_registry_initialized() of
        false -> ?MODULE:states();
        true  -> #{}
    end.

-spec is_supported(rabbit_feature_flags:feature_name()) -> boolean().
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
    rabbit_feature_flags:initialize_registry(),
    %% See get/1 for an explanation of the case statement below.
    case is_registry_initialized() of
        false -> ?MODULE:is_supported(FeatureName);
        true  -> false
    end.

-spec is_enabled(rabbit_feature_flags:feature_name()) -> boolean() | state_changing.
%% @doc
%% Returns if a feature flag is supported or if its state is changing.
%%
%% Only the informations stored in the local registry is used to answer
%% this call.
%%
%% @param FeatureName The name of the feature flag to be checked.
%% @returns `true' if the feature flag is supported, `state_changing' if
%%   its state is transient, or `false' otherwise.

is_enabled(FeatureName) ->
    rabbit_feature_flags:initialize_registry(),
    %% See get/1 for an explanation of the case statement below.
    case is_registry_initialized() of
        false -> ?MODULE:is_enabled(FeatureName);
        true  -> false
    end.

-spec is_registry_initialized() -> boolean().
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

-spec is_registry_written_to_disk() -> boolean().
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

always_return_true() ->
    %% This function is here to trick Dialyzer. We want some functions
    %% in this initial on-disk registry to always return `true` or
    %% `false`. However the generated regsitry will return actual
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
    rand:uniform(1) > 0.

always_return_false() ->
    not always_return_true().
