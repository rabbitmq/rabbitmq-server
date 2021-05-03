%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2018-2021 VMware, Inc. or its affiliates.
%%
%% @doc
%% This module offers a framework to declare capabilities a RabbitMQ node
%% supports and therefore a way to determine if multiple RabbitMQ nodes in
%% a cluster are compatible and can work together.
%%
%% == What a feature flag is ==
%%
%% A <strong>feature flag</strong> is a name and several properties given
%% to a change in RabbitMQ which impacts its communication with other
%% RabbitMQ nodes. This kind of change can be:
%% <ul>
%% <li>an update to an Erlang record</li>
%% <li>a modification to a replicated Mnesia table schema</li>
%% <li>a modification to Erlang messages exchanged between Erlang processes
%%   which might run on remote nodes</li>
%% </ul>
%%
%% A feature flag is qualified by:
%% <ul>
%% <li>a <strong>name</strong></li>
%% <li>a <strong>description</strong> (optional)</li>
%% <li>a list of other <strong>feature flags this feature flag depends on
%%   </strong> (optional). This can be useful when the change builds up on
%%   top of a previous change. For instance, it expands a record which was
%%   already modified by a previous feature flag.</li>
%% <li>a <strong>migration function</strong> (optional). If provided, this
%%   function is called when the feature flag is enabled. It is responsible
%%   for doing all the data conversion, if any, and confirming the feature
%%   flag can be enabled.</li>
%% <li>a level of stability (stable or experimental). For now, this is only
%%   informational. But it might be used for specific purposes in the
%%   future.</li>
%% </ul>
%%
%% == How to declare a feature flag ==
%%
%% To define a new feature flag, you need to use the
%% `rabbit_feature_flag()' module attribute:
%%
%% ```
%% -rabbit_feature_flag(FeatureFlag).
%% '''
%%
%% `FeatureFlag' is a {@type feature_flag_modattr()}.
%%
%% == How to enable a feature flag ==
%%
%% To enable a supported feature flag, you have the following solutions:
%%
%% <ul>
%% <li>Using this module API:
%% ```
%% rabbit_feature_flags:enable(FeatureFlagName).
%% '''
%% </li>
%% <li>Using the `rabbitmqctl' CLI:
%% ```
%% rabbitmqctl enable_feature_flag "$feature_flag_name"
%% '''
%% </li>
%% </ul>
%%
%% == How to disable a feature flag ==
%%
%% Once enabled, there is <strong>currently no way to disable</strong> a
%% feature flag.

-module(rabbit_feature_flags).

-export([list/0,
         list/1,
         list/2,
         enable/1,
         enable_all/0,
         disable/1,
         disable_all/0,
         is_supported/1,
         is_supported/2,
         is_supported_locally/1,
         is_supported_remotely/1,
         is_supported_remotely/2,
         is_supported_remotely/3,
         is_enabled/1,
         is_enabled/2,
         is_disabled/1,
         is_disabled/2,
         info/0,
         info/1,
         init/0,
         get_state/1,
         get_stability/1,
         check_node_compatibility/1,
         check_node_compatibility/2,
         is_node_compatible/1,
         is_node_compatible/2,
         sync_feature_flags_with_cluster/2,
         sync_feature_flags_with_cluster/3,
         refresh_feature_flags_after_app_load/1,
         enabled_feature_flags_list_file/0
        ]).

%% RabbitMQ internal use only.
-export([initialize_registry/0,
         initialize_registry/1,
         mark_as_enabled_locally/2,
         remote_nodes/0,
         running_remote_nodes/0,
         does_node_support/3,
         merge_feature_flags_from_unknown_apps/1,
         do_sync_feature_flags_with_node/1]).

-ifdef(TEST).
-export([inject_test_feature_flags/1,
         initialize_registry/3,
         query_supported_feature_flags/0,
         mark_as_enabled_remotely/2,
         mark_as_enabled_remotely/4,
         registry_loading_lock/0]).
-endif.

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

-define(FF_REGISTRY_LOADING_LOCK, {feature_flags_registry_loading, self()}).
-define(FF_STATE_CHANGE_LOCK,     {feature_flags_state_change, self()}).

-type feature_flag_modattr() :: {feature_name(),
                                 feature_props()}.
%% The value of a `-rabbitmq_feature_flag()' module attribute used to
%% declare a new feature flag.

-type feature_name() :: atom().
%% The feature flag's name. It is used in many places to identify a
%% specific feature flag. In particular, this is how an end-user (or
%% the CLI) can enable a feature flag. This is also the only bit which
%% is persisted so a node remember which feature flags are enabled.

-type feature_props() :: #{desc => string(),
                           doc_url => string(),
                           stability => stability(),
                           depends_on => [feature_name()],
                           migration_fun => migration_fun_name()}.
%% The feature flag properties.
%%
%% All properties are optional.
%%
%% The properties are:
%% <ul>
%% <li>`desc': a description of the feature flag</li>
%% <li>`doc_url': a URL pointing to more documentation about the feature
%%   flag</li>
%% <li>`stability': the level of stability</li>
%% <li>`depends_on': a list of feature flags name which must be enabled
%%   before this one</li>
%% <li>`migration_fun': a migration function specified by its module and
%%   function names</li>
%% </ul>
%%
%% Note that the `migration_fun' is a {@type migration_fun_name()},
%% not a {@type migration_fun()}. However, the function signature
%% must conform to the {@type migration_fun()} signature. The reason
%% is that we must be able to represent it as an Erlang term when
%% we regenerate the registry module source code (using {@link
%% erl_syntax:abstract/1}).

-type feature_flags() :: #{feature_name() => feature_props_extended()}.
%% The feature flags map as returned or accepted by several functions in
%% this module. In particular, this what the {@link list/0} function
%% returns.

-type feature_props_extended() :: #{desc => string(),
                                    doc_url => string(),
                                    stability => stability(),
                                    migration_fun => migration_fun_name(),
                                    depends_on => [feature_name()],
                                    provided_by => atom()}.
%% The feature flag properties, once expanded by this module when feature
%% flags are discovered.
%%
%% The new properties compared to {@type feature_props()} are:
%% <ul>
%% <li>`provided_by': the name of the application providing the feature flag</li>
%% </ul>

-type feature_state() :: boolean() | state_changing.
%% The state of the feature flag: enabled if `true', disabled if `false'
%% or `state_changing'.

-type feature_states() :: #{feature_name() => feature_state()}.

-type stability() :: stable | experimental.
%% The level of stability of a feature flag. Currently, only informational.

-type migration_fun_name() :: {Module :: atom(), Function :: atom()}.
%% The name of the module and function to call when changing the state of
%% the feature flag.

-type migration_fun() :: fun((feature_name(),
                              feature_props_extended(),
                              migration_fun_context())
                             -> ok | {error, any()} |   % context = enable
                                boolean() | undefined). % context = is_enabled
%% The migration function signature.
%%
%% It is called with context `enable' when a feature flag is being enabled.
%% The function is responsible for this feature-flag-specific verification
%% and data conversion. It returns `ok' if RabbitMQ can mark the feature
%% flag as enabled an continue with the next one, if any. Otherwise, it
%% returns `{error, any()}' if there is an error and the feature flag should
%% remain disabled. The function must be idempotent: if the feature flag is
%% already enabled on another node and the local node is running this function
%% again because it is syncing its feature flags state, it should succeed.
%%
%% It is called with the context `is_enabled' to check if a feature flag
%% is actually enabled. It is useful on RabbitMQ startup, just in case
%% the previous instance failed to write the feature flags list file.

-type migration_fun_context() :: enable | is_enabled.

-type registry_vsn() :: term().

-export_type([feature_flag_modattr/0,
              feature_props/0,
              feature_name/0,
              feature_flags/0,
              feature_props_extended/0,
              feature_state/0,
              feature_states/0,
              stability/0,
              migration_fun_name/0,
              migration_fun/0,
              migration_fun_context/0]).

-on_load(on_load/0).

-spec list() -> feature_flags().
%% @doc
%% Lists all supported feature flags.
%%
%% @returns A map of all supported feature flags.

list() -> list(all).

-spec list(Which :: all | enabled | disabled) -> feature_flags().
%% @doc
%% Lists all, enabled or disabled feature flags, depending on the argument.
%%
%% @param Which The group of feature flags to return: `all', `enabled' or
%% `disabled'.
%% @returns A map of selected feature flags.

list(all)      -> rabbit_ff_registry:list(all);
list(enabled)  -> rabbit_ff_registry:list(enabled);
list(disabled) -> maps:filter(
                    fun(FeatureName, _) -> is_disabled(FeatureName) end,
                    list(all)).

-spec list(all | enabled | disabled, stability()) -> feature_flags().
%% @doc
%% Lists all, enabled or disabled feature flags, depending on the first
%% argument, only keeping those having the specified stability.
%%
%% @param Which The group of feature flags to return: `all', `enabled' or
%% `disabled'.
%% @param Stability The level of stability used to filter the map of feature
%% flags.
%% @returns A map of selected feature flags.

list(Which, Stability)
  when Stability =:= stable orelse Stability =:= experimental ->
    maps:filter(fun(_, FeatureProps) ->
                        Stability =:= get_stability(FeatureProps)
                end, list(Which)).

-spec enable(feature_name() | [feature_name()]) -> ok |
                                                   {error, Reason :: any()}.
%% @doc
%% Enables the specified feature flag or set of feature flags.
%%
%% @param FeatureName The name or the list of names of feature flags to
%%   enable.
%% @returns `ok' if the feature flags (and all the feature flags they
%%   depend on) were successfully enabled, or `{error, Reason}' if one
%%   feature flag could not be enabled (subsequent feature flags in the
%%   dependency tree are left unchanged).

enable(FeatureName) when is_atom(FeatureName) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flag `~s`: REQUEST TO ENABLE",
      [FeatureName]),
    case is_enabled(FeatureName) of
        true ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flag `~s`: already enabled",
              [FeatureName]),
            ok;
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flag `~s`: not enabled, check if supported by cluster",
              [FeatureName]),
            %% The feature flag must be supported locally and remotely
            %% (i.e. by all members of the cluster).
            case is_supported(FeatureName) of
                true ->
                    _ = rabbit_log_feature_flags:info(
                      "Feature flag `~s`: supported, attempt to enable...",
                      [FeatureName]),
                    do_enable(FeatureName);
                false ->
                    _ = rabbit_log_feature_flags:error(
                      "Feature flag `~s`: not supported",
                      [FeatureName]),
                    {error, unsupported}
            end
    end;
enable(FeatureNames) when is_list(FeatureNames) ->
    with_feature_flags(FeatureNames, fun enable/1).

-spec enable_all() -> ok | {error, any()}.
%% @doc
%% Enables all supported feature flags.
%%
%% @returns `ok' if the feature flags were successfully enabled,
%%   or `{error, Reason}' if one feature flag could not be enabled
%%   (subsequent feature flags in the dependency tree are left
%%   unchanged).

enable_all() ->
    with_feature_flags(maps:keys(list(all)), fun enable/1).

-spec disable(feature_name() | [feature_name()]) -> ok | {error, any()}.
%% @doc
%% Disables the specified feature flag or set of feature flags.
%%
%% @param FeatureName The name or the list of names of feature flags to
%%   disable.
%% @returns `ok' if the feature flags (and all the feature flags they
%%   depend on) were successfully disabled, or `{error, Reason}' if one
%%   feature flag could not be disabled (subsequent feature flags in the
%%   dependency tree are left unchanged).

disable(FeatureName) when is_atom(FeatureName) ->
    {error, unsupported};
disable(FeatureNames) when is_list(FeatureNames) ->
    with_feature_flags(FeatureNames, fun disable/1).

-spec disable_all() -> ok | {error, any()}.
%% @doc
%% Disables all supported feature flags.
%%
%% @returns `ok' if the feature flags were successfully disabled,
%%   or `{error, Reason}' if one feature flag could not be disabled
%%   (subsequent feature flags in the dependency tree are left
%%   unchanged).

disable_all() ->
    with_feature_flags(maps:keys(list(all)), fun disable/1).

-spec with_feature_flags([feature_name()],
                         fun((feature_name()) -> ok | {error, any()})) ->
    ok | {error, any()}.
%% @private

with_feature_flags([FeatureName | Rest], Fun) ->
    case Fun(FeatureName) of
        ok    -> with_feature_flags(Rest, Fun);
        Error -> Error
    end;
with_feature_flags([], _) ->
    ok.

-spec is_supported(feature_name() | [feature_name()]) -> boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by the entire cluster.
%%
%% This is the same as calling both {@link is_supported_locally/1} and
%% {@link is_supported_remotely/1} with a logical AND.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if the set of feature flags is entirely supported, or
%%   `false' if one of them is not or the RPC timed out.

is_supported(FeatureNames) ->
    is_supported_locally(FeatureNames) andalso
    is_supported_remotely(FeatureNames).

-spec is_supported(feature_name() | [feature_name()], timeout()) ->
    boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by the entire cluster.
%%
%% This is the same as calling both {@link is_supported_locally/1} and
%% {@link is_supported_remotely/2} with a logical AND.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @param Timeout Time in milliseconds after which the RPC gives up.
%% @returns `true' if the set of feature flags is entirely supported, or
%%   `false' if one of them is not or the RPC timed out.

is_supported(FeatureNames, Timeout) ->
    is_supported_locally(FeatureNames) andalso
    is_supported_remotely(FeatureNames, Timeout).

-spec is_supported_locally(feature_name() | [feature_name()]) -> boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by the local node.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if the set of feature flags is entirely supported, or
%%   `false' if one of them is not.

is_supported_locally(FeatureName) when is_atom(FeatureName) ->
    rabbit_ff_registry:is_supported(FeatureName);
is_supported_locally(FeatureNames) when is_list(FeatureNames) ->
    lists:all(fun(F) -> rabbit_ff_registry:is_supported(F) end, FeatureNames).

-spec is_supported_remotely(feature_name() | [feature_name()]) -> boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by all remote nodes.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if the set of feature flags is entirely supported, or
%%   `false' if one of them is not or the RPC timed out.

is_supported_remotely(FeatureNames) ->
    is_supported_remotely(FeatureNames, ?TIMEOUT).

-spec is_supported_remotely(feature_name() | [feature_name()], timeout()) -> boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by all remote nodes.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @param Timeout Time in milliseconds after which the RPC gives up.
%% @returns `true' if the set of feature flags is entirely supported, or
%%   `false' if one of them is not or the RPC timed out.

is_supported_remotely(FeatureName, Timeout) when is_atom(FeatureName) ->
    is_supported_remotely([FeatureName], Timeout);
is_supported_remotely([], _) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: skipping query for feature flags support as the "
      "given list is empty"),
    true;
is_supported_remotely(FeatureNames, Timeout) when is_list(FeatureNames) ->
    case running_remote_nodes() of
        [] ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: isolated node; skipping remote node query "
              "=> consider `~p` supported",
              [FeatureNames]),
            true;
        RemoteNodes ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: about to query these remote nodes about "
              "support for `~p`: ~p",
              [FeatureNames, RemoteNodes]),
            is_supported_remotely(RemoteNodes, FeatureNames, Timeout)
    end.

-spec is_supported_remotely([node()],
                            feature_name() | [feature_name()],
                            timeout()) -> boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by specified remote nodes.
%%
%% @param RemoteNodes The list of remote nodes to query.
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @param Timeout Time in milliseconds after which the RPC gives up.
%% @returns `true' if the set of feature flags is entirely supported by
%%   all nodes, or `false' if one of them is not or the RPC timed out.

is_supported_remotely(_, [], _) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: skipping query for feature flags support as the "
      "given list is empty"),
    true;
is_supported_remotely([Node | Rest], FeatureNames, Timeout) ->
    case does_node_support(Node, FeatureNames, Timeout) of
        true ->
            is_supported_remotely(Rest, FeatureNames, Timeout);
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: stopping query for support for `~p` here",
              [FeatureNames]),
            false
    end;
is_supported_remotely([], FeatureNames, _) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: all running remote nodes support `~p`",
      [FeatureNames]),
    true.

-spec is_enabled(feature_name() | [feature_name()]) -> boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% enabled.
%%
%% This is the same as calling {@link is_enabled/2} as a `blocking'
%% call.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if the set of feature flags is enabled, or
%%   `false' if one of them is not.

is_enabled(FeatureNames) ->
    is_enabled(FeatureNames, blocking).

-spec is_enabled
(feature_name() | [feature_name()], blocking) ->
    boolean();
(feature_name() | [feature_name()], non_blocking) ->
    feature_state().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% enabled.
%%
%% When `blocking' is passed, the function waits (blocks) for the
%% state of a feature flag being disabled or enabled stabilizes before
%% returning its final state.
%%
%% When `non_blocking' is passed, the function returns immediately with
%% the state of the feature flag (`true' if enabled, `false' otherwise)
%% or `state_changing' is the state is being changed at the time of the
%% call.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if the set of feature flags is enabled,
%%   `false' if one of them is not, or `state_changing' if one of them
%%   is being worked on. Note that `state_changing' has precedence over
%%   `false', so if one is `false' and another one is `state_changing',
%%   `state_changing' is returned.

is_enabled(FeatureNames, non_blocking) ->
    is_enabled_nb(FeatureNames);
is_enabled(FeatureNames, blocking) ->
    case is_enabled_nb(FeatureNames) of
        state_changing ->
            global:set_lock(?FF_STATE_CHANGE_LOCK),
            global:del_lock(?FF_STATE_CHANGE_LOCK),
            is_enabled(FeatureNames, blocking);
        IsEnabled ->
            IsEnabled
    end.

is_enabled_nb(FeatureName) when is_atom(FeatureName) ->
    rabbit_ff_registry:is_enabled(FeatureName);
is_enabled_nb(FeatureNames) when is_list(FeatureNames) ->
    lists:foldl(
      fun
          (_F, state_changing = Acc) ->
              Acc;
          (F, false = Acc) ->
              case rabbit_ff_registry:is_enabled(F) of
                  state_changing -> state_changing;
                  _              -> Acc
              end;
          (F, _) ->
              rabbit_ff_registry:is_enabled(F)
      end,
      true, FeatureNames).

-spec is_disabled(feature_name() | [feature_name()]) -> boolean().
%% @doc
%% Returns if a single feature flag or one feature flag in a set of
%% feature flags is disabled.
%%
%% This is the same as negating the result of {@link is_enabled/1}.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if one of the feature flags is disabled, or
%%   `false' if they are all enabled.

is_disabled(FeatureNames) ->
    is_disabled(FeatureNames, blocking).

-spec is_disabled
(feature_name() | [feature_name()], blocking) ->
    boolean();
(feature_name() | [feature_name()], non_blocking) ->
    feature_state().
%% @doc
%% Returns if a single feature flag or one feature flag in a set of
%% feature flags is disabled.
%%
%% This is the same as negating the result of {@link is_enabled/2},
%% except that `state_changing' is returned as is.
%%
%% See {@link is_enabled/2} for a description of the `blocking' and
%% `non_blocking' modes.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if one feature flag in the set of feature flags is
%%   disabled, `false' if they are all enabled, or `state_changing' if
%%   one of them is being worked on. Note that `state_changing' has
%%   precedence over `true', so if one is `true' (i.e. disabled) and
%%   another one is `state_changing', `state_changing' is returned.
%%
%% @see is_enabled/2

is_disabled(FeatureName, Blocking) ->
    case is_enabled(FeatureName, Blocking) of
        state_changing -> state_changing;
        IsEnabled      -> not IsEnabled
    end.

-spec info() -> ok.
%% @doc
%% Displays a table on stdout summing up the supported feature flags,
%% their state and various informations about them.

info() ->
    info(#{}).

-spec info(#{color => boolean(),
             lines => boolean(),
             verbose => non_neg_integer()}) -> ok.
%% @doc
%% Displays a table on stdout summing up the supported feature flags,
%% their state and various informations about them.
%%
%% Supported options are:
%% <ul>
%% <li>`color': a boolean to indicate if colors should be used to
%%   highlight some elements.</li>
%% <li>`lines': a boolean to indicate if table borders should be drawn
%%   using ASCII lines instead of regular characters.</li>
%% <li>`verbose': a non-negative integer to specify the level of
%%   verbosity.</li>
%% </ul>
%%
%% @param Options A map of various options to tune the displayed table.

info(Options) when is_map(Options) ->
    rabbit_ff_extra:info(Options).

-spec get_state(feature_name()) -> enabled | disabled | unavailable.
%% @doc
%% Returns the state of a feature flag.
%%
%% The possible states are:
%% <ul>
%% <li>`enabled': the feature flag is enabled.</li>
%% <li>`disabled': the feature flag is supported by all nodes in the
%%   cluster but currently disabled.</li>
%% <li>`unavailable': the feature flag is unsupported by at least one
%%   node in the cluster and can not be enabled for now.</li>
%% </ul>
%%
%% @param FeatureName The name of the feature flag to check.
%% @returns `enabled', `disabled' or `unavailable'.

get_state(FeatureName) when is_atom(FeatureName) ->
    IsEnabled = is_enabled(FeatureName),
    IsSupported = is_supported(FeatureName),
    case IsEnabled of
        true  -> enabled;
        false -> case IsSupported of
                     true  -> disabled;
                     false -> unavailable
                 end
    end.

-spec get_stability(feature_name() | feature_props_extended()) -> stability().
%% @doc
%% Returns the stability of a feature flag.
%%
%% The possible stability levels are:
%% <ul>
%% <li>`stable': the feature flag is stable and will not change in future
%%   releases: it can be enabled in production.</li>
%% <li>`experimental': the feature flag is experimental and may change in
%%   the future (without a guaranteed upgrade path): enabling it in
%%   production is not recommended.</li>
%% <li>`unavailable': the feature flag is unsupported by at least one
%%   node in the cluster and can not be enabled for now.</li>
%% </ul>
%%
%% @param FeatureName The name of the feature flag to check.
%% @returns `stable' or `experimental'.

get_stability(FeatureName) when is_atom(FeatureName) ->
    case rabbit_ff_registry:get(FeatureName) of
        undefined    -> undefined;
        FeatureProps -> get_stability(FeatureProps)
    end;
get_stability(FeatureProps) when is_map(FeatureProps) ->
    maps:get(stability, FeatureProps, stable).

%% -------------------------------------------------------------------
%% Feature flags registry.
%% -------------------------------------------------------------------

-spec init() -> ok | no_return().
%% @private

init() ->
    %% We want to make sure the `feature_flags` file exists once
    %% RabbitMQ was started at least once. This is not required by
    %% this module (it works fine if the file is missing) but it helps
    %% external tools.
    _ = ensure_enabled_feature_flags_list_file_exists(),

    %% We also "list" supported feature flags. We are not interested in
    %% that list, however, it triggers the first initialization of the
    %% registry.
    _ = list(all),
    ok.

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
%% flags and their state. This is makes it very efficient to query a
%% feature flag state or property.
%%
%% The registry is local to all RabbitMQ nodes.

initialize_registry() ->
    initialize_registry(#{}).

-spec initialize_registry(feature_flags()) ->
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
    FeatureStates = case RegistryInitialized of
                        true ->
                            rabbit_ff_registry:states();
                        false ->
                            EnabledFeatureNames =
                            read_enabled_feature_flags_list(),
                            list_of_enabled_feature_flags_to_feature_states(
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

-spec list_of_enabled_feature_flags_to_feature_states([feature_name()]) ->
    feature_states().

list_of_enabled_feature_flags_to_feature_states(FeatureNames) ->
    maps:from_list([{FeatureName, true} || FeatureName <- FeatureNames]).

-spec initialize_registry(feature_flags(),
                          feature_states(),
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
    Ret = maybe_initialize_registry(NewSupportedFeatureFlags,
                                    NewFeatureStates,
                                    WrittenToDisk),
    case Ret of
        ok      -> ok;
        restart -> initialize_registry(NewSupportedFeatureFlags,
                                       NewFeatureStates,
                                       WrittenToDisk);
        Error   -> Error
    end.

-spec maybe_initialize_registry(feature_flags(),
                                feature_states(),
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
    KnownFeatureFlags2 = query_supported_feature_flags(),

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
    FeatureStates0 = case RegistryInitialized of
                         true ->
                             maps:merge(rabbit_ff_registry:states(),
                                        NewFeatureStates);
                         false ->
                             NewFeatureStates
                     end,
    FeatureStates = maps:filter(
                      fun(_, true) -> true;
                         (_, state_changing) -> true;
                         (_, false) -> false
                      end, FeatureStates0),

    Proceed = does_registry_need_refresh(AllFeatureFlags,
                                         FeatureStates,
                                         WrittenToDisk),

    case Proceed of
        true ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: (re)initialize registry (~p)",
              [self()]),
            T0 = erlang:timestamp(),
            Ret = do_initialize_registry(RegistryVsn,
                                         AllFeatureFlags,
                                         FeatureStates,
                                         WrittenToDisk),
            T1 = erlang:timestamp(),
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: time to regen registry: ~p µs",
              [timer:now_diff(T1, T0)]),
            Ret;
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: registry already up-to-date, skipping init"),
            ok
    end.

-spec does_registry_need_refresh(feature_flags(),
                                 feature_states(),
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
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: "
                      "yes, list of feature flags differs"),
                    true;
                FeatureStates =/= CurrentFeatureStates ->
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: "
                      "yes, feature flag states differ"),
                    true;
                WrittenToDisk =/= CurrentWrittenToDisk ->
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: "
                      "yes, \"written to disk\" state changed"),
                    true;
                true ->
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags: registry refresh needed: no"),
                    false
            end;
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: registry refresh needed: "
              "yes, first-time initialization"),
            true
    end.

-spec do_initialize_registry(registry_vsn(),
                             feature_flags(),
                             feature_states(),
                             boolean()) ->
    ok | restart | {error, any()} | no_return().
%% @private

do_initialize_registry(RegistryVsn,
                       AllFeatureFlags,
                       FeatureStates,
                       WrittenToDisk) ->
    %% We log the state of those feature flags.
    _ = rabbit_log_feature_flags:info(
      "Feature flags: list of feature flags found:"),
    lists:foreach(
      fun(FeatureName) ->
              _ = rabbit_log_feature_flags:info(
                "Feature flags:   [~s] ~s",
                [case maps:is_key(FeatureName, FeatureStates) of
                     true ->
                         case maps:get(FeatureName, FeatureStates) of
                             true           -> "x";
                             state_changing -> "~"
                         end;
                     false ->
                         " "
                 end,
                 FeatureName])
      end, lists:sort(maps:keys(AllFeatureFlags))),
    _ = rabbit_log_feature_flags:info(
      "Feature flags: feature flag states written to disk: ~s",
      [case WrittenToDisk of
           true  -> "yes";
           false -> "no"
       end]),

    %% We request the registry to be regenerated and reloaded with the
    %% new state.
    regen_registry_mod(RegistryVsn,
                       AllFeatureFlags,
                       FeatureStates,
                       WrittenToDisk).

-spec query_supported_feature_flags() -> feature_flags().
%% @private

-ifdef(TEST).
-define(PT_TESTSUITE_ATTRS, {?MODULE, testsuite_feature_flags_attrs}).

inject_test_feature_flags(AttributesFromTestsuite) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: injecting feature flags from testsuite: ~p",
      [AttributesFromTestsuite]),
    ok = persistent_term:put(?PT_TESTSUITE_ATTRS, AttributesFromTestsuite),
    initialize_registry().

module_attributes_from_testsuite() ->
    persistent_term:get(?PT_TESTSUITE_ATTRS, []).

query_supported_feature_flags() ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: query feature flags in loaded applications "
      "+ testsuite"),
    T0 = erlang:timestamp(),
    AttributesPerApp = rabbit_misc:rabbitmq_related_module_attributes(
                         rabbit_feature_flag),
    AttributesFromTestsuite = module_attributes_from_testsuite(),
    T1 = erlang:timestamp(),
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: time to find supported feature flags: ~p µs",
      [timer:now_diff(T1, T0)]),
    AllAttributes = AttributesPerApp ++ AttributesFromTestsuite,
    prepare_queried_feature_flags(AllAttributes, #{}).
-else.
query_supported_feature_flags() ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: query feature flags in loaded applications"),
    T0 = erlang:timestamp(),
    AttributesPerApp = rabbit_misc:rabbitmq_related_module_attributes(
                         rabbit_feature_flag),
    T1 = erlang:timestamp(),
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: time to find supported feature flags: ~p µs",
      [timer:now_diff(T1, T0)]),
    prepare_queried_feature_flags(AttributesPerApp, #{}).
-endif.

prepare_queried_feature_flags([{App, _Module, Attributes} | Rest],
                              AllFeatureFlags) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: application `~s` has ~b feature flags",
      [App, length(Attributes)]),
    AllFeatureFlags1 = lists:foldl(
                         fun({FeatureName, FeatureProps}, AllFF) ->
                                 merge_new_feature_flags(AllFF,
                                                         App,
                                                         FeatureName,
                                                         FeatureProps)
                         end, AllFeatureFlags, Attributes),
    prepare_queried_feature_flags(Rest, AllFeatureFlags1);
prepare_queried_feature_flags([], AllFeatureFlags) ->
    AllFeatureFlags.

-spec merge_new_feature_flags(feature_flags(),
                              atom(),
                              feature_name(),
                              feature_props()) -> feature_flags().
%% @private

merge_new_feature_flags(AllFeatureFlags, App, FeatureName, FeatureProps)
  when is_atom(FeatureName) andalso is_map(FeatureProps) ->
    %% We expand the feature flag properties map with:
    %%   - the name of the application providing it: only informational
    %%     for now, but can be handy to understand that a feature flag
    %%     comes from a plugin.
    FeatureProps1 = maps:put(provided_by, App, FeatureProps),
    maps:merge(AllFeatureFlags,
               #{FeatureName => FeatureProps1}).

-spec regen_registry_mod(registry_vsn(),
                         feature_flags(),
                         feature_states(),
                         boolean()) ->
    ok | restart | {error, any()} | no_return().
%% @private

regen_registry_mod(RegistryVsn,
                   AllFeatureFlags,
                   FeatureStates,
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
                                     {is_registry_written_to_disk, 0}]]
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
    %% Compilation!
    Forms = [ModuleForm,
             ExportForm,
             GetFunForm,
             ListFunForm,
             StatesFunForm,
             IsSupportedFunForm,
             IsEnabledFunForm,
             IsInitializedFunForm,
             IsWrittenToDiskFunForm],
    maybe_log_registry_source_code(Forms),
    CompileOpts = [return_errors,
                   return_warnings],
    case compile:forms(Forms, CompileOpts) of
        {ok, Mod, Bin, _} ->
            load_registry_mod(RegistryVsn, Mod, Bin);
        {error, Errors, Warnings} ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: registry compilation:~n"
              "Errors: ~p~n"
              "Warnings: ~p",
              [Errors, Warnings]),
            {error, {compilation_failure, Errors, Warnings}}
    end.

maybe_log_registry_source_code(Forms) ->
    case rabbit_prelaunch:get_context() of
        #{log_feature_flags_registry := true} ->
            _ = rabbit_log_feature_flags:debug(
              "== FEATURE FLAGS REGISTRY ==~n"
              "~s~n"
              "== END ==~n",
              [erl_prettypr:format(erl_syntax:form_list(Forms))]);
        _ ->
            ok
    end.

-ifdef(TEST).
registry_loading_lock() -> ?FF_REGISTRY_LOADING_LOCK.
-endif.

-spec load_registry_mod(registry_vsn(), atom(), binary()) ->
    ok | restart | no_return().
%% @private

load_registry_mod(RegistryVsn, Mod, Bin) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: registry module ready, loading it (~p)...",
      [self()]),
    FakeFilename = "Compiled and loaded by " ?MODULE_STRING,
    %% Time to load the new registry, replacing the old one. We use a
    %% lock here to synchronize concurrent reloads.
    global:set_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
    _ = rabbit_log_feature_flags:debug(
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
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: releasing lock after reloading registry module (~p)",
     [self()]),
    global:del_lock(?FF_REGISTRY_LOADING_LOCK, [node()]),
    case Ret of
        {module, _} ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: registry module loaded (vsn: ~p -> ~p)",
              [RegistryVsn, registry_vsn()]),
            ok;
        {error, {restart, Expected, Current}} ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: another registry module was loaded in the "
              "meantime (expected old vsn: ~p, current vsn: ~p); "
              "restarting the regen",
              [Expected, Current]),
            restart;
        {error, Reason} ->
            _ = rabbit_log_feature_flags:error(
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

%% -------------------------------------------------------------------
%% Feature flags state storage.
%% -------------------------------------------------------------------

-spec ensure_enabled_feature_flags_list_file_exists() -> ok | {error, any()}.
%% @private

ensure_enabled_feature_flags_list_file_exists() ->
    File = enabled_feature_flags_list_file(),
    case filelib:is_regular(File) of
        true  -> ok;
        false -> write_enabled_feature_flags_list([])
    end.

-spec read_enabled_feature_flags_list() ->
    [feature_name()] | no_return().
%% @private

read_enabled_feature_flags_list() ->
    case try_to_read_enabled_feature_flags_list() of
        {error, Reason} ->
            File = enabled_feature_flags_list_file(),
            throw({feature_flags_file_read_error, File, Reason});
        Ret ->
            Ret
    end.

-spec try_to_read_enabled_feature_flags_list() ->
    [feature_name()] | {error, any()}.
%% @private

try_to_read_enabled_feature_flags_list() ->
    File = enabled_feature_flags_list_file(),
    case file:consult(File) of
        {ok, [List]} ->
            List;
        {error, enoent} ->
            %% If the file is missing, we consider the list of enabled
            %% feature flags to be empty.
            [];
        {error, Reason} = Error ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: failed to read the `feature_flags` "
              "file at `~s`: ~s",
              [File, file:format_error(Reason)]),
            Error
    end.

-spec write_enabled_feature_flags_list([feature_name()]) ->
    ok | no_return().
%% @private

write_enabled_feature_flags_list(FeatureNames) ->
    case try_to_write_enabled_feature_flags_list(FeatureNames) of
        {error, Reason} ->
            File = enabled_feature_flags_list_file(),
            throw({feature_flags_file_write_error, File, Reason});
        Ret ->
            Ret
    end.

-spec try_to_write_enabled_feature_flags_list([feature_name()]) ->
    ok | {error, any()}.
%% @private

try_to_write_enabled_feature_flags_list(FeatureNames) ->
    %% Before writing the new file, we read the existing one. If there
    %% are unknown feature flags in that file, we want to keep their
    %% state, even though they are unsupported at this time. It could be
    %% that a plugin was disabled in the meantime.
    %%
    %% FIXME: Lock this code to fix concurrent read/modify/write.
    PreviouslyEnabled = case try_to_read_enabled_feature_flags_list() of
                            {error, _} -> [];
                            List       -> List
                        end,
    FeatureNames1 = lists:foldl(
                      fun(Name, Acc) ->
                              case is_supported_locally(Name) of
                                  true  -> Acc;
                                  false -> [Name | Acc]
                              end
                      end, FeatureNames, PreviouslyEnabled),
    FeatureNames2 = lists:sort(FeatureNames1),

    File = enabled_feature_flags_list_file(),
    Content = io_lib:format("~p.~n", [FeatureNames2]),
    %% TODO: If we fail to write the the file, we should spawn a process
    %% to retry the operation.
    case file:write_file(File, Content) of
        ok ->
            ok;
        {error, Reason} = Error ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: failed to write the `feature_flags` "
              "file at `~s`: ~s",
              [File, file:format_error(Reason)]),
            Error
    end.

-spec enabled_feature_flags_list_file() -> file:filename().
%% @doc
%% Returns the path to the file where the state of feature flags is stored.
%%
%% @returns the path to the file.

enabled_feature_flags_list_file() ->
    case application:get_env(rabbit, feature_flags_file) of
        {ok, Val} -> Val;
        undefined -> throw(feature_flags_file_not_set)
    end.

%% -------------------------------------------------------------------
%% Feature flags management: enabling.
%% -------------------------------------------------------------------

-spec do_enable(feature_name()) -> ok | {error, any()} | no_return().
%% @private

do_enable(FeatureName) ->
    %% We mark this feature flag as "state changing" before doing the
    %% actual state change. We also take a global lock: this permits
    %% to block callers asking about a feature flag changing state.
    global:set_lock(?FF_STATE_CHANGE_LOCK),
    Ret = case mark_as_enabled(FeatureName, state_changing) of
              ok ->
                  case enable_dependencies(FeatureName, true) of
                      ok ->
                          case run_migration_fun(FeatureName, enable) of
                              ok ->
                                  mark_as_enabled(FeatureName, true);
                              {error, no_migration_fun} ->
                                  mark_as_enabled(FeatureName, true);
                              Error ->
                                  Error
                          end;
                      Error ->
                          Error
                  end;
              Error ->
                  Error
          end,
    case Ret of
        ok -> ok;
        _  -> mark_as_enabled(FeatureName, false)
    end,
    global:del_lock(?FF_STATE_CHANGE_LOCK),
    Ret.

-spec enable_locally(feature_name()) -> ok | {error, any()} | no_return().
%% @private

enable_locally(FeatureName) when is_atom(FeatureName) ->
    case is_enabled(FeatureName) of
        true ->
            ok;
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flag `~s`: enable locally (as part of feature "
              "flag states synchronization)",
              [FeatureName]),
            do_enable_locally(FeatureName)
    end.

-spec do_enable_locally(feature_name()) -> ok | {error, any()} | no_return().
%% @private

do_enable_locally(FeatureName) ->
    case enable_dependencies(FeatureName, false) of
        ok ->
            case run_migration_fun(FeatureName, enable) of
                ok ->
                    mark_as_enabled_locally(FeatureName, true);
                {error, no_migration_fun} ->
                    mark_as_enabled_locally(FeatureName, true);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec enable_dependencies(feature_name(), boolean()) ->
    ok | {error, any()} | no_return().
%% @private

enable_dependencies(FeatureName, Everywhere) ->
    FeatureProps = rabbit_ff_registry:get(FeatureName),
    DependsOn = maps:get(depends_on, FeatureProps, []),
    _ = rabbit_log_feature_flags:debug(
      "Feature flag `~s`: enable dependencies: ~p",
      [FeatureName, DependsOn]),
    enable_dependencies(FeatureName, DependsOn, Everywhere).

-spec enable_dependencies(feature_name(), [feature_name()], boolean()) ->
    ok | {error, any()} | no_return().
%% @private

enable_dependencies(TopLevelFeatureName, [FeatureName | Rest], Everywhere) ->
    Ret = case Everywhere of
              true  -> enable(FeatureName);
              false -> enable_locally(FeatureName)
          end,
    case Ret of
        ok    -> enable_dependencies(TopLevelFeatureName, Rest, Everywhere);
        Error -> Error
    end;
enable_dependencies(_, [], _) ->
    ok.

-spec run_migration_fun(feature_name(), any()) ->
    any() | {error, any()}.
%% @private

run_migration_fun(FeatureName, Arg) ->
    FeatureProps = rabbit_ff_registry:get(FeatureName),
    run_migration_fun(FeatureName, FeatureProps, Arg).

run_migration_fun(FeatureName, FeatureProps, Arg) ->
    case maps:get(migration_fun, FeatureProps, none) of
        {MigrationMod, MigrationFun}
          when is_atom(MigrationMod) andalso is_atom(MigrationFun) ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flag `~s`: run migration function ~p with arg: ~p",
              [FeatureName, MigrationFun, Arg]),
            try
                erlang:apply(MigrationMod,
                             MigrationFun,
                             [FeatureName, FeatureProps, Arg])
            catch
                _:Reason:Stacktrace ->
                    _ = rabbit_log_feature_flags:error(
                      "Feature flag `~s`: migration function crashed: ~p~n~p",
                      [FeatureName, Reason, Stacktrace]),
                    {error, {migration_fun_crash, Reason, Stacktrace}}
            end;
        none ->
            {error, no_migration_fun};
        Invalid ->
            _ = rabbit_log_feature_flags:error(
              "Feature flag `~s`: invalid migration function: ~p",
              [FeatureName, Invalid]),
            {error, {invalid_migration_fun, Invalid}}
    end.

-spec mark_as_enabled(feature_name(), feature_state()) ->
    any() | {error, any()} | no_return().
%% @private

mark_as_enabled(FeatureName, IsEnabled) ->
    case mark_as_enabled_locally(FeatureName, IsEnabled) of
        ok ->
            mark_as_enabled_remotely(FeatureName, IsEnabled);
        Error ->
            Error
    end.

-spec mark_as_enabled_locally(feature_name(), feature_state()) ->
    any() | {error, any()} | no_return().
%% @private

mark_as_enabled_locally(FeatureName, IsEnabled) ->
    _ = rabbit_log_feature_flags:info(
      "Feature flag `~s`: mark as enabled=~p",
      [FeatureName, IsEnabled]),
    EnabledFeatureNames = maps:keys(list(enabled)),
    NewEnabledFeatureNames = case IsEnabled of
                                 true ->
                                     [FeatureName | EnabledFeatureNames];
                                 false ->
                                     EnabledFeatureNames -- [FeatureName];
                                 state_changing ->
                                     EnabledFeatureNames
                             end,
    WrittenToDisk = case NewEnabledFeatureNames of
                        EnabledFeatureNames ->
                            rabbit_ff_registry:is_registry_written_to_disk();
                        _ ->
                            ok =:= try_to_write_enabled_feature_flags_list(
                                     NewEnabledFeatureNames)
                    end,
    initialize_registry(#{},
                        #{FeatureName => IsEnabled},
                        WrittenToDisk).

-spec mark_as_enabled_remotely(feature_name(), feature_state()) ->
    any() | {error, any()} | no_return().
%% @private

mark_as_enabled_remotely(FeatureName, IsEnabled) ->
    Nodes = running_remote_nodes(),
    mark_as_enabled_remotely(Nodes, FeatureName, IsEnabled, ?TIMEOUT).

-spec mark_as_enabled_remotely([node()],
                               feature_name(),
                               feature_state(),
                               timeout()) ->
    any() | {error, any()} | no_return().
%% @private

mark_as_enabled_remotely([], _FeatureName, _IsEnabled, _Timeout) ->
    ok;
mark_as_enabled_remotely(Nodes, FeatureName, IsEnabled, Timeout) ->
    T0 = erlang:timestamp(),
    Rets = [{Node, rpc:call(Node,
                            ?MODULE,
                            mark_as_enabled_locally,
                            [FeatureName, IsEnabled],
                            Timeout)}
            || Node <- Nodes],
    FailedNodes = [Node || {Node, Ret} <- Rets, Ret =/= ok],
    case FailedNodes of
        [] ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: `~s` successfully marked as enabled=~p on all "
              "nodes", [FeatureName, IsEnabled]),
            ok;
        _ ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: failed to mark feature flag `~s` as enabled=~p "
              "on the following nodes:", [FeatureName, IsEnabled]),
            [_ = rabbit_log_feature_flags:error(
               "Feature flags:   - ~s: ~p",
               [Node, Ret])
             || {Node, Ret} <- Rets,
                Ret =/= ok],
            Sleep = 1000,
            T1 = erlang:timestamp(),
            Duration = timer:now_diff(T1, T0),
            NewTimeout = (Timeout * 1000 - Duration) div 1000 - Sleep,
            if
                NewTimeout > 0 ->
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags:   retrying with a timeout of ~b "
                      "ms after sleeping for ~b ms",
                      [NewTimeout, Sleep]),
                    timer:sleep(Sleep),
                    mark_as_enabled_remotely(FailedNodes,
                                             FeatureName,
                                             IsEnabled,
                                             NewTimeout);
                true ->
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags:   not retrying; RPC went over the "
                      "~b milliseconds timeout", [Timeout]),
                    %% FIXME: Is crashing the process the best solution here?
                    throw(
                      {failed_to_mark_feature_flag_as_enabled_on_remote_nodes,
                       FeatureName, IsEnabled, FailedNodes})
            end
    end.

%% -------------------------------------------------------------------
%% Coordination with remote nodes.
%% -------------------------------------------------------------------

-spec remote_nodes() -> [node()].
%% @private

remote_nodes() ->
    mnesia:system_info(db_nodes) -- [node()].

-spec running_remote_nodes() -> [node()].
%% @private

running_remote_nodes() ->
    mnesia:system_info(running_db_nodes) -- [node()].

query_running_remote_nodes(Node, Timeout) ->
    case rpc:call(Node, mnesia, system_info, [running_db_nodes], Timeout) of
        {badrpc, _} = Error -> Error;
        Nodes               -> Nodes -- [node()]
    end.

-spec does_node_support(node(), [feature_name()], timeout()) -> boolean().
%% @private

does_node_support(Node, FeatureNames, Timeout) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: querying `~p` support on node ~s...",
      [FeatureNames, Node]),
    Ret = case node() of
              Node ->
                  is_supported_locally(FeatureNames);
              _ ->
                  run_feature_flags_mod_on_remote_node(
                    Node, is_supported_locally, [FeatureNames], Timeout)
          end,
    case Ret of
        {error, pre_feature_flags_rabbitmq} ->
            %% See run_feature_flags_mod_on_remote_node/4 for
            %% an explanation why we consider this node a 3.7.x
            %% pre-feature-flags node.
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: no feature flags support on node `~s`, "
              "consider the feature flags unsupported: ~p",
              [Node, FeatureNames]),
            false;
        {error, Reason} ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: error while querying `~p` support on "
              "node ~s: ~p",
              [FeatureNames, Node, Reason]),
            false;
        true ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: node `~s` supports `~p`",
              [Node, FeatureNames]),
            true;
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: node `~s` does not support `~p`; "
              "stopping query here",
              [Node, FeatureNames]),
            false
    end.

-spec check_node_compatibility(node()) -> ok | {error, any()}.
%% @doc
%% Checks if a node is compatible with the local node.
%%
%% To be compatible, the following two conditions must be met:
%% <ol>
%% <li>feature flags enabled on the local node must be supported by the
%%   remote node</li>
%% <li>feature flags enabled on the remote node must be supported by the
%%   local node</li>
%% </ol>
%%
%% @param Node the name of the remote node to test.
%% @returns `ok' if they are compatible, `{error, Reason}' if they are not.

check_node_compatibility(Node) ->
    check_node_compatibility(Node, ?TIMEOUT).

-spec check_node_compatibility(node(), timeout()) -> ok | {error, any()}.
%% @doc
%% Checks if a node is compatible with the local node.
%%
%% See {@link check_node_compatibility/1} for the conditions required to
%% consider two nodes compatible.
%%
%% @param Node the name of the remote node to test.
%% @param Timeout Time in milliseconds after which the RPC gives up.
%% @returns `ok' if they are compatible, `{error, Reason}' if they are not.
%%
%% @see check_node_compatibility/1

check_node_compatibility(Node, Timeout) ->
    %% Before checking compatibility, we exchange feature flags from
    %% unknown Erlang applications. So we fetch remote feature flags
    %% from applications which are not loaded locally, and the opposite.
    %%
    %% The goal is that such feature flags are not blocking the
    %% communication between nodes because the code (which would
    %% break) is missing on those nodes. Therefore they should not be
    %% considered when determining compatibility.
    exchange_feature_flags_from_unknown_apps(Node, Timeout),

    %% FIXME:
    %% When we try to cluster two nodes, we get:
    %%   Feature flags: starting an unclustered node: all feature flags
    %%   will be enabled by default
    %% It should probably not be the case...

    %% We can now proceed with the actual compatibility check.
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: node `~s` compatibility check, part 1/2",
      [Node]),
    Part1 = local_enabled_feature_flags_is_supported_remotely(Node, Timeout),
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: node `~s` compatibility check, part 2/2",
      [Node]),
    Part2 = remote_enabled_feature_flags_is_supported_locally(Node, Timeout),
    case {Part1, Part2} of
        {true, true} ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: node `~s` is compatible",
              [Node]),
            ok;
        {false, _} ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: node `~s` is INCOMPATIBLE: "
              "feature flags enabled locally are not supported remotely",
              [Node]),
            {error, incompatible_feature_flags};
        {_, false} ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: node `~s` is INCOMPATIBLE: "
              "feature flags enabled remotely are not supported locally",
              [Node]),
            {error, incompatible_feature_flags}
    end.

-spec is_node_compatible(node()) -> boolean().
%% @doc
%% Returns if a node is compatible with the local node.
%%
%% This function calls {@link check_node_compatibility/2} and returns
%% `true' the latter returns `ok'. Therefore this is the same code,
%% except that this function returns a boolean, but not the reason of
%% the incompatibility if any.
%%
%% @param Node the name of the remote node to test.
%% @returns `true' if they are compatible, `false' otherwise.

is_node_compatible(Node) ->
    is_node_compatible(Node, ?TIMEOUT).

-spec is_node_compatible(node(), timeout()) -> boolean().
%% @doc
%% Returns if a node is compatible with the local node.
%%
%% This function calls {@link check_node_compatibility/2} and returns
%% `true' the latter returns `ok'. Therefore this is the same code,
%% except that this function returns a boolean, but not the reason
%% of the incompatibility if any. If the RPC times out, nodes are
%% considered incompatible.
%%
%% @param Node the name of the remote node to test.
%% @param Timeout Time in milliseconds after which the RPC gives up.
%% @returns `true' if they are compatible, `false' otherwise.

is_node_compatible(Node, Timeout) ->
    check_node_compatibility(Node, Timeout) =:= ok.

-spec local_enabled_feature_flags_is_supported_remotely(node(),
                                                        timeout()) ->
    boolean().
%% @private

local_enabled_feature_flags_is_supported_remotely(Node, Timeout) ->
    LocalEnabledFeatureNames = maps:keys(list(enabled)),
    is_supported_remotely([Node], LocalEnabledFeatureNames, Timeout).

-spec remote_enabled_feature_flags_is_supported_locally(node(),
                                                        timeout()) ->
    boolean().
%% @private

remote_enabled_feature_flags_is_supported_locally(Node, Timeout) ->
    case query_remote_feature_flags(Node, enabled, Timeout) of
        {error, _} ->
            false;
        RemoteEnabledFeatureFlags when is_map(RemoteEnabledFeatureFlags) ->
            RemoteEnabledFeatureNames = maps:keys(RemoteEnabledFeatureFlags),
            is_supported_locally(RemoteEnabledFeatureNames)
    end.

-spec run_feature_flags_mod_on_remote_node(node(),
                                           atom(),
                                           [term()],
                                           timeout()) ->
    term() | {error, term()}.
%% @private

run_feature_flags_mod_on_remote_node(Node, Function, Args, Timeout) ->
    case rpc:call(Node, ?MODULE, Function, Args, Timeout) of
        {badrpc, {'EXIT',
                  {undef,
                   [{?MODULE, Function, Args, []}
                    | _]}}} ->
            %% If rabbit_feature_flags:Function() is undefined
            %% on the remote node, we consider it to be a 3.7.x
            %% pre-feature-flags node.
            %%
            %% Theoretically, it could be an older version (3.6.x and
            %% older). But the RabbitMQ version consistency check
            %% (rabbit_misc:version_minor_equivalent/2) called from
            %% rabbit_mnesia:check_rabbit_consistency/2 already blocked
            %% this situation from happening before we reach this point.
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: ~s:~s~p unavailable on node `~s`: "
              "assuming it is a RabbitMQ 3.7.x pre-feature-flags node",
              [?MODULE, Function, Args, Node]),
            {error, pre_feature_flags_rabbitmq};
        {badrpc, Reason} = Error ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: error while running ~s:~s~p "
              "on node `~s`: ~p",
              [?MODULE, Function, Args, Node, Reason]),
            {error, Error};
        Ret ->
            Ret
    end.

-spec query_remote_feature_flags(node(),
                                 Which :: all | enabled | disabled,
                                 timeout()) ->
    feature_flags() | {error, any()}.
%% @private

query_remote_feature_flags(Node, Which, Timeout) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: querying ~s feature flags on node `~s`...",
      [Which, Node]),
    case run_feature_flags_mod_on_remote_node(Node, list, [Which], Timeout) of
        {error, pre_feature_flags_rabbitmq} ->
            %% See run_feature_flags_mod_on_remote_node/4 for
            %% an explanation why we consider this node a 3.7.x
            %% pre-feature-flags node.
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: no feature flags support on node `~s`, "
              "consider the list of feature flags empty", [Node]),
            #{};
        {error, Reason} = Error ->
            _ = rabbit_log_feature_flags:error(
              "Feature flags: error while querying ~s feature flags "
              "on node `~s`: ~p",
              [Which, Node, Reason]),
            Error;
        RemoteFeatureFlags when is_map(RemoteFeatureFlags) ->
            RemoteFeatureNames = maps:keys(RemoteFeatureFlags),
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: querying ~s feature flags on node `~s` "
              "done; ~s features: ~p",
              [Which, Node, Which, RemoteFeatureNames]),
            RemoteFeatureFlags
    end.

-spec merge_feature_flags_from_unknown_apps(feature_flags()) ->
    ok | {error, any()}.
%% @private

merge_feature_flags_from_unknown_apps(FeatureFlags)
  when is_map(FeatureFlags) ->
    LoadedApps = [App || {App, _, _} <- application:loaded_applications()],
    FeatureFlagsFromUnknownApps =
    maps:fold(
      fun(FeatureName, FeatureProps, UnknownFF) ->
              case is_supported_locally(FeatureName) of
                  true ->
                      UnknownFF;
                  false ->
                      FeatureProvider = maps:get(provided_by, FeatureProps),
                      case lists:member(FeatureProvider, LoadedApps) of
                          true  -> UnknownFF;
                          false -> maps:put(FeatureName, FeatureProps,
                                            UnknownFF)
                      end
              end
      end,
      #{},
      FeatureFlags),
    case maps:keys(FeatureFlagsFromUnknownApps) of
        [] ->
            ok;
        _ ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: register feature flags provided by applications "
              "unknown locally: ~p",
              [maps:keys(FeatureFlagsFromUnknownApps)]),
            initialize_registry(FeatureFlagsFromUnknownApps)
    end.

exchange_feature_flags_from_unknown_apps(Node, Timeout) ->
    %% The first step is to fetch feature flags from Erlang applications
    %% we don't know locally (they are loaded remotely, but not
    %% locally).
    fetch_remote_feature_flags_from_apps_unknown_locally(Node, Timeout),

    %% The next step is to do the opposite: push feature flags to remote
    %% nodes so they can register those from applications they don't
    %% know.
    push_local_feature_flags_from_apps_unknown_remotely(Node, Timeout).

fetch_remote_feature_flags_from_apps_unknown_locally(Node, Timeout) ->
    RemoteFeatureFlags = query_remote_feature_flags(Node, all, Timeout),
    merge_feature_flags_from_unknown_apps(RemoteFeatureFlags).

push_local_feature_flags_from_apps_unknown_remotely(Node, Timeout) ->
    LocalFeatureFlags = list(all),
    push_local_feature_flags_from_apps_unknown_remotely(
      Node, LocalFeatureFlags, Timeout).

push_local_feature_flags_from_apps_unknown_remotely(
  Node, FeatureFlags, Timeout)
  when map_size(FeatureFlags) > 0 ->
    case query_running_remote_nodes(Node, Timeout) of
        {badrpc, Reason} ->
            {error, Reason};
        Nodes ->
            lists:foreach(
              fun(N) ->
                      run_feature_flags_mod_on_remote_node(
                        N,
                        merge_feature_flags_from_unknown_apps,
                        [FeatureFlags],
                        Timeout)
              end, Nodes)
    end;
push_local_feature_flags_from_apps_unknown_remotely(_, _, _) ->
    ok.

-spec sync_feature_flags_with_cluster([node()], boolean()) ->
    ok | {error, any()} | no_return().
%% @private

sync_feature_flags_with_cluster(Nodes, NodeIsVirgin) ->
    sync_feature_flags_with_cluster(Nodes, NodeIsVirgin, ?TIMEOUT).

-spec sync_feature_flags_with_cluster([node()], boolean(), timeout()) ->
    ok | {error, any()} | no_return().
%% @private

sync_feature_flags_with_cluster([], NodeIsVirgin, _) ->
    verify_which_feature_flags_are_actually_enabled(),
    case NodeIsVirgin of
        true ->
            FeatureNames = get_forced_feature_flag_names(),
            case remote_nodes() of
                [] when FeatureNames =:= undefined ->
                    _ = rabbit_log_feature_flags:debug(
                      "Feature flags: starting an unclustered node "
                      "for the first time: all feature flags will be "
                      "enabled by default"),
                    enable_all();
                [] ->
                    case FeatureNames of
                        [] ->
                            _ = rabbit_log_feature_flags:debug(
                              "Feature flags: starting an unclustered "
                              "node for the first time: all feature "
                              "flags are forcibly left disabled from "
                              "the $RABBITMQ_FEATURE_FLAGS environment "
                              "variable"),
                            ok;
                        _ ->
                            _ = rabbit_log_feature_flags:debug(
                              "Feature flags: starting an unclustered "
                              "node for the first time: only the "
                              "following feature flags specified in "
                              "the $RABBITMQ_FEATURE_FLAGS environment "
                              "variable will be enabled: ~p",
                              [FeatureNames]),
                            enable(FeatureNames)
                    end;
                _ ->
                    ok
            end;
        false ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: starting an unclustered node which is "
              "already initialized: all feature flags left in their "
              "current state"),
            ok
    end;
sync_feature_flags_with_cluster(Nodes, _, Timeout) ->
    verify_which_feature_flags_are_actually_enabled(),
    RemoteNodes = Nodes -- [node()],
    sync_feature_flags_with_cluster1(RemoteNodes, Timeout).

sync_feature_flags_with_cluster1([], _) ->
    ok;
sync_feature_flags_with_cluster1(RemoteNodes, Timeout) ->
    RandomRemoteNode = pick_one_node(RemoteNodes),
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: SYNCING FEATURE FLAGS with node `~s`...",
      [RandomRemoteNode]),
    case query_remote_feature_flags(RandomRemoteNode, enabled, Timeout) of
        {error, _} = Error ->
            Error;
        RemoteFeatureFlags ->
            RemoteFeatureNames = maps:keys(RemoteFeatureFlags),
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: enabling locally feature flags already "
              "enabled on node `~s`...",
              [RandomRemoteNode]),
            case do_sync_feature_flags_with_node(RemoteFeatureNames) of
                ok ->
                    sync_feature_flags_with_cluster2(
                      RandomRemoteNode, Timeout);
                Error ->
                    Error
            end
    end.

sync_feature_flags_with_cluster2(RandomRemoteNode, Timeout) ->
    LocalFeatureNames = maps:keys(list(enabled)),
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: enabling on node `~s` feature flags already "
      "enabled locally...",
      [RandomRemoteNode]),
    Ret = run_feature_flags_mod_on_remote_node(
            RandomRemoteNode,
            do_sync_feature_flags_with_node,
            [LocalFeatureNames],
            Timeout),
    case Ret of
        {error, pre_feature_flags_rabbitmq} -> ok;
        _                                   -> Ret
    end.

pick_one_node(Nodes) ->
    RandomIndex = rand:uniform(length(Nodes)),
    lists:nth(RandomIndex, Nodes).

do_sync_feature_flags_with_node([FeatureFlag | Rest]) ->
    case enable_locally(FeatureFlag) of
        ok    -> do_sync_feature_flags_with_node(Rest);
        Error -> Error
    end;
do_sync_feature_flags_with_node([]) ->
    ok.

-spec get_forced_feature_flag_names() -> [feature_name()] | undefined.
%% @private
%% @doc
%% Returns the (possibly empty) list of feature flags the user want
%% to enable out-of-the-box when starting a node for the first time.
%%
%% Without this, the default is to enable all the supported feature
%% flags.
%%
%% There are two ways to specify that list:
%% <ol>
%% <li>Using the `$RABBITMQ_FEATURE_FLAGS' environment variable; for
%%   instance `RABBITMQ_FEATURE_FLAGS=quorum_queue,mnevis'.</li>
%% <li>Using the `forced_feature_flags_on_init' configuration parameter;
%%   for instance
%%   `{rabbit, [{forced_feature_flags_on_init, [quorum_queue, mnevis]}]}'.</li>
%% </ol>
%%
%% The environment variable has precedence over the configuration
%% parameter.

get_forced_feature_flag_names() ->
    Ret = case get_forced_feature_flag_names_from_env() of
              undefined -> get_forced_feature_flag_names_from_config();
              List      -> List
          end,
    case Ret of
        undefined -> ok;
        []        -> _ = rabbit_log_feature_flags:info(
                       "Feature flags: automatic enablement of feature "
                       "flags disabled (i.e. none will be enabled "
                       "automatically)");
        _         -> _ = rabbit_log_feature_flags:info(
                       "Feature flags: automatic enablement of feature "
                       "flags limited to the following list: ~p", [Ret])
    end,
    Ret.

-spec get_forced_feature_flag_names_from_env() -> [feature_name()] | undefined.
%% @private

get_forced_feature_flag_names_from_env() ->
    case rabbit_prelaunch:get_context() of
        #{forced_feature_flags_on_init := ForcedFFs}
          when is_list(ForcedFFs) ->
            ForcedFFs;
        _ ->
            undefined
    end.

-spec get_forced_feature_flag_names_from_config() -> [feature_name()] | undefined.
%% @private

get_forced_feature_flag_names_from_config() ->
    Value = application:get_env(rabbit,
                                forced_feature_flags_on_init,
                                undefined),
    case Value of
        undefined ->
            Value;
        _ when is_list(Value) ->
            case lists:all(fun is_atom/1, Value) of
                true  -> Value;
                false -> undefined
            end;
        _ ->
            undefined
    end.

-spec verify_which_feature_flags_are_actually_enabled() ->
    ok | {error, any()} | no_return().
%% @private

verify_which_feature_flags_are_actually_enabled() ->
    AllFeatureFlags = list(all),
    EnabledFeatureNames = read_enabled_feature_flags_list(),
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: double-checking feature flag states..."),
    %% In case the previous instance of the node failed to write the
    %% feature flags list file, we want to double-check the list of
    %% enabled feature flags read from disk. For each feature flag,
    %% we call the migration function to query if the feature flag is
    %% actually enabled.
    %%
    %% If a feature flag doesn't provide a migration function (or if the
    %% function fails), we keep the current state of the feature flag.
    List1 = maps:fold(
              fun(Name, Props, Acc) ->
                      Ret = run_migration_fun(Name, Props, is_enabled),
                      case Ret of
                          true ->
                              [Name | Acc];
                          false ->
                              Acc;
                          _ ->
                              MarkedAsEnabled = is_enabled(Name),
                              case MarkedAsEnabled of
                                  true  -> [Name | Acc];
                                  false -> Acc
                              end
                      end
              end,
              [], AllFeatureFlags),
    RepairedEnabledFeatureNames = lists:sort(List1),
    %% We log the list of feature flags for which the state changes
    %% after the check above.
    WereEnabled = RepairedEnabledFeatureNames -- EnabledFeatureNames,
    WereDisabled = EnabledFeatureNames -- RepairedEnabledFeatureNames,
    case {WereEnabled, WereDisabled} of
        {[], []} -> ok;
        _        -> _ = rabbit_log_feature_flags:warning(
                      "Feature flags: the previous instance of this node "
                      "must have failed to write the `feature_flags` "
                      "file at `~s`:",
                      [enabled_feature_flags_list_file()])
    end,
    case WereEnabled of
        [] -> ok;
        _  -> _ = rabbit_log_feature_flags:warning(
                "Feature flags:   - list of previously enabled "
                "feature flags now marked as such: ~p", [WereEnabled])
    end,
    case WereDisabled of
        [] -> ok;
        _  -> _ = rabbit_log_feature_flags:warning(
                "Feature flags:   - list of previously disabled "
                "feature flags now marked as such: ~p", [WereDisabled])
    end,
    %% Finally, if the new list of enabled feature flags is different
    %% than the one on disk, we write the new list and re-initialize the
    %% registry.
    case RepairedEnabledFeatureNames of
        EnabledFeatureNames ->
            ok;
        _ ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: write the repaired list of enabled feature "
              "flags"),
            WrittenToDisk = ok =:= try_to_write_enabled_feature_flags_list(
                                     RepairedEnabledFeatureNames),
            initialize_registry(
              #{},
              list_of_enabled_feature_flags_to_feature_states(
                RepairedEnabledFeatureNames),
              WrittenToDisk)
    end.

-spec refresh_feature_flags_after_app_load([atom()]) ->
    ok | {error, any()} | no_return().

refresh_feature_flags_after_app_load([]) ->
    ok;
refresh_feature_flags_after_app_load(Apps) ->
    _ = rabbit_log_feature_flags:debug(
      "Feature flags: new apps loaded: ~p -> refreshing feature flags",
      [Apps]),

    FeatureFlags0 = list(all),
    FeatureFlags1 = query_supported_feature_flags(),

    %% The following list contains all the feature flags this node
    %% learned about only because remote nodes have them. Now, the
    %% applications providing them are loaded locally as well.
    %% Therefore, we may run their migration function in case the state
    %% of this node needs it.
    AlreadySupportedFeatureNames = maps:keys(
                                     maps:filter(
                                       fun(_, #{provided_by := App}) ->
                                               lists:member(App, Apps)
                                       end, FeatureFlags0)),
    case AlreadySupportedFeatureNames of
        [] ->
            ok;
        _ ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: new apps loaded: feature flags already "
              "supported: ~p",
              [lists:sort(AlreadySupportedFeatureNames)])
    end,

    %% The following list contains all the feature flags no nodes in the
    %% cluster knew about before: this is the first time we see them in
    %% this instance of the cluster. We need to register them on all
    %% nodes.
    NewSupportedFeatureFlags = maps:filter(
                                 fun(FeatureName, _) ->
                                         not maps:is_key(FeatureName,
                                                         FeatureFlags0)
                                 end, FeatureFlags1),
    case maps:keys(NewSupportedFeatureFlags) of
        [] ->
            ok;
        NewSupportedFeatureNames ->
            _ = rabbit_log_feature_flags:debug(
              "Feature flags: new apps loaded: new feature flags (unseen so "
              "far): ~p ",
              [lists:sort(NewSupportedFeatureNames)])
    end,

    case initialize_registry() of
        ok ->
            Ret = maybe_enable_locally_after_app_load(
                    AlreadySupportedFeatureNames),
            case Ret of
                ok ->
                    share_new_feature_flags_after_app_load(
                      NewSupportedFeatureFlags, ?TIMEOUT);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

maybe_enable_locally_after_app_load([]) ->
    ok;
maybe_enable_locally_after_app_load([FeatureName | Rest]) ->
    case is_enabled(FeatureName) of
        true ->
            case do_enable_locally(FeatureName) of
                ok    -> maybe_enable_locally_after_app_load(Rest);
                Error -> Error
            end;
        false ->
            maybe_enable_locally_after_app_load(Rest)
    end.

share_new_feature_flags_after_app_load(FeatureFlags, Timeout) ->
    push_local_feature_flags_from_apps_unknown_remotely(
      node(), FeatureFlags, Timeout).

on_load() ->
    %% The goal of this `on_load()` code server hook is to prevent this
    %% module from being loaded in an already running RabbitMQ node if
    %% the running version does not have the feature flags subsystem.
    %%
    %% This situation happens when an upgrade overwrites RabbitMQ files
    %% with the node still running. This is the case with many packages:
    %% files are updated on disk, then a post-install step takes care of
    %% restarting the service.
    %%
    %% The problem is that if many nodes in a cluster are updated at the
    %% same time, one node running the newer version might query feature
    %% flags on an old node where this module is already available
    %% (because files were already overwritten). This causes the query
    %% to report an unexpected answer and the newer node to refuse to
    %% start.
    %%
    %% However, when the module is executed outside of RabbitMQ (for
    %% debugging purpose or in the context of EUnit for instance), we
    %% want to allow the load. That's why we first check if RabbitMQ is
    %% actually running.
    case rabbit:is_running() of
        true ->
            %% RabbitMQ is running.
            %%
            %% Now we want to differentiate a pre-feature-flags node
            %% from one having the subsystem.
            %%
            %% To do that, we verify if the `feature_flags_file`
            %% application environment variable is defined. With a
            %% feature-flags-enabled node, this application environment
            %% variable is defined by rabbitmq-server(8).
            case application:get_env(rabbit, feature_flags_file) of
                {ok, _} ->
                    %% This is a feature-flags-enabled version. Loading
                    %% the module is permitted.
                    ok;
                _ ->
                    %% This is a pre-feature-flags version. We deny the
                    %% load and report why, possibly specifying the
                    %% version of RabbitMQ.
                    Vsn = case application:get_key(rabbit, vsn) of
                              {ok, V}   -> V;
                              undefined -> "unknown version"
                          end,
                    "Refusing to load '" ?MODULE_STRING "' on this "
                    "node. It appears to be running a pre-feature-flags "
                    "version of RabbitMQ (" ++ Vsn ++ "). This is fine: "
                    "a newer version of RabbitMQ was deployed on this "
                    "node, but it was not restarted yet. This warning "
                    "is probably caused by a remote node querying this "
                    "node for its feature flags."
            end;
        false ->
            %% RabbitMQ is not running. Loading the module is permitted
            %% because this Erlang node will never be queried for its
            %% feature flags.
            ok
    end.
