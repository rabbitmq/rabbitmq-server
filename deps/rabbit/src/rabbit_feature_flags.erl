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
%% <li>a level of stability (stable or experimental). Experimental feature
%%   flags are not enabled by default on a brand new RabbitMQ nodes. They are
%%   also skipped by {@link enable_all/0}.</li>
%% </ul>
%%
%% == How to declare a feature flag ==
%%
%% To define a new feature flag, you need to use the
%% `-rabbit_feature_flag()' module attribute:
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

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([list/0,
         list/1,
         list/2,
         enable/1,
         enable_all/0,
         enable_all/1,
         disable/1,
         disable_all/0,
         is_supported/1,
         is_supported/2,
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
         sync_feature_flags_with_cluster/2,
         refresh_feature_flags_after_app_load/0,
         enabled_feature_flags_list_file/0
        ]).

%% RabbitMQ internal use only.
-export([is_supported_locally/1, %% Called remotely.
         mark_as_enabled_locally/2, %% Called remotely.
         remote_nodes/0,
         running_remote_nodes/0,
         does_node_support/3,
         inject_test_feature_flags/1,
         query_supported_feature_flags/0,
         does_enabled_feature_flags_list_file_exist/0,
         read_enabled_feature_flags_list/0,
         uses_callbacks/1]).

-ifdef(TEST).
-export([override_nodes/1,
         override_running_nodes/1,
         get_overriden_nodes/0, %% Called remotely.
         get_overriden_running_nodes/0]).
-endif.

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

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
                           callbacks =>
                           #{callback_name() => callback_fun_name()}}.
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
%% <li>`callbacks': a map of callback names</li>
%% </ul>
%%
%% Note that each `callbacks' is a {@type callback_fun_name()}, not a {@type
%% callbacks()}. However, the function signature must conform to the right
%% {@type callbacks()} signature. The reason is that we must be able to
%% represent it as an Erlang term when we regenerate the registry module
%% source code (using {@link erl_syntax:abstract/1}).

-type feature_flags() :: #{feature_name() => feature_props_extended()}.
%% The feature flags map as returned or accepted by several functions in
%% this module. In particular, this what the {@link list/0} function
%% returns.

-type feature_props_extended() :: #{desc => string(),
                                    doc_url => string(),
                                    stability => stability(),
                                    depends_on => [feature_name()],
                                    callbacks =>
                                    #{callback_name() => callback_fun_name()},
                                    provided_by := atom()}.
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

-type stability() :: required | stable | experimental.
%% The level of stability of a feature flag.
%%
%% Experimental feature flags are not enabled by default on a fresh RabbitMQ
%% node. They must be enabled by the user.

-type callback_fun_name() :: {Module :: module(), Function :: atom()}.
%% The name of the module and function to call when changing the state of
%% the feature flag.

-type callbacks() :: enable_callback() | post_enable_callback().
%% All possible callbacks.

-type callbacks_args() :: enable_callback_args() | post_enable_callback_args().
%% All possible callbacks arguments.

-type callbacks_rets() :: enable_callback_ret() | post_enable_callback_ret().
%% All possible callbacks return values.

-type callback_name() :: enable | post_enable.
%% Name of the callback.

-type enable_callback() :: fun((enable_callback_args())
                               -> enable_callback_ret()).
%% The callback called while enabling a feature flag.
%%
%% It is called when a feature flag is being enabled. The function is
%% responsible for this feature-flag-specific verification and data
%% conversion. It returns `ok' if RabbitMQ can mark the feature flag as
%% enabled an continue with the next one, if any. `{error, Reason}' and
%% exceptions are an error and the feature flag will remain disabled.
%%
%% The migration function is called on all nodes which fulfill the following
%% conditions:
%% <ol>
%% <li>The node knows the feature flag.</li>
%% <li>The feature flag is disabled on that node before calling the migration
%% function.</li>
%% </ol>
%%
%% All executions of the callback on these nodes will run in parallel
%% (concurrently). The callback is responsible for its own locking and
%% synchronization.
%%
%% When a node is joining a cluster where one side has a feature flag enabled,
%% that feature flag will be enabled on the other side. It means the callback
%% will run on the nodes where it is disabled. Therefore the callback can run
%% in clusters where some nodes previously executed it and the feature flag
%% was already enabled.
%%
%% The callback should also be idempotent. For instance, if the feature flag
%% couldn't be marked as enabled everywhere after the callback was called, it
%% may be called again.

-type enable_callback_args() :: #{feature_name := feature_name(),
                                  feature_props := feature_props_extended(),
                                  command := enable,
                                  nodes := [node()]}.
%% A map passed to {@type enable_callback()}.

-type enable_callback_ret() :: ok | {error, term()}.
%% Return value of the `enable' callback.

-type post_enable_callback() :: fun((post_enable_callback_args())
                                    -> post_enable_callback_ret()).
%% The callback called after enabling a feature flag.
%%
%% It is called after a feature flag has been marked as enabled. The return
%% value or any exceptions are ignored and the feature flag will remain
%% enabled even if there is a failure.
%%
%% All executions of the callback on nodes will run in parallel
%% (concurrently). The callback is responsible for its own locking and
%% synchronization.
%%
%% When a node is joining a cluster where one side has a feature flag enabled,
%% that feature flag will be enabled on the other side. It means the callback
%% will run on the nodes where it is disabled. Therefore the callback can run
%% in clusters where some nodes previously executed it and the feature flag
%% was already enabled.
%%
%% The callback should also be idempotent. For instance, if the feature flag
%% couldn't be marked as enabled everywhere after the callback was called, it
%% may be called again.

-type post_enable_callback_args() :: #{feature_name := feature_name(),
                                       feature_props :=
                                       feature_props_extended(),
                                       command := post_enable,
                                       nodes := [node()]}.
%% A map passed to {@type post_enable_callback()}.

-type post_enable_callback_ret() :: ok.
%% Return value of the `post_enable' callback.

-type inventory() :: #{applications := [atom()],
                       feature_flags := feature_flags(),
                       states := feature_states()}.

-type cluster_inventory() :: #{feature_flags := feature_flags(),
                               applications_per_node :=
                               #{node() => [atom()]},
                               states_per_node :=
                               #{node() => feature_states()}}.

-export_type([feature_flag_modattr/0,
              feature_props/0,
              feature_name/0,
              feature_flags/0,
              feature_props_extended/0,
              feature_state/0,
              feature_states/0,
              stability/0,
              callback_fun_name/0,
              callbacks/0,
              callback_name/0,
              callbacks_args/0,
              callbacks_rets/0,
              enable_callback/0,
              enable_callback_args/0,
              enable_callback_ret/0,
              post_enable_callback/0,
              post_enable_callback_args/0,
              post_enable_callback_ret/0,
              inventory/0,
              cluster_inventory/0]).

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

list(Which, stable) ->
    maps:filter(fun(_, FeatureProps) ->
                        Stability = get_stability(FeatureProps),
                        stable =:= Stability orelse required =:= Stability
                end, list(Which));
list(Which, experimental) ->
    maps:filter(fun(_, FeatureProps) ->
                        experimental =:= get_stability(FeatureProps)
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
    rabbit_ff_controller:enable(FeatureName);
enable(FeatureNames) when is_list(FeatureNames) ->
    with_feature_flags(FeatureNames, fun enable/1).

uses_callbacks(FeatureName) when is_atom(FeatureName) ->
    case rabbit_ff_registry:get(FeatureName) of
        undefined    -> false;
        FeatureProps -> uses_callbacks(FeatureProps)
    end;
uses_callbacks(FeatureProps) when is_map(FeatureProps) ->
    maps:is_key(callbacks, FeatureProps).

-spec enable_all() -> ok | {error, any()}.
%% @doc
%% Enables all stable feature flags.
%%
%% Experimental feature flags are not enabled with this function. Use {@link
%% enable/1} to enable them.
%%
%% @returns `ok' if the feature flags were successfully enabled,
%%   or `{error, Reason}' if one feature flag could not be enabled
%%   (subsequent feature flags in the dependency tree are left
%%   unchanged).

enable_all() ->
    enable_all(stable).

-spec enable_all(stability()) -> ok | {error, any()}.
%% @doc
%% Enables all supported feature flags matching the given stability.
%%
%% @param Stability The level of stability used to filter the feature flags to
%%   enable.
%% @returns `ok' if the feature flags were successfully enabled,
%%   or `{error, Reason}' if one feature flag could not be enabled
%%   (subsequent feature flags in the dependency tree are left
%%   unchanged).

enable_all(Stability)
  when Stability =:= stable orelse Stability =:= experimental ->
    enable(maps:keys(list(all, Stability))).

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
    rabbit_ff_controller:is_supported(FeatureNames).

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
    rabbit_ff_controller:is_supported(FeatureNames, Timeout).

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
            rabbit_ff_registry_factory:acquire_state_change_lock(),
            rabbit_ff_registry_factory:release_state_change_lock(),
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

-spec get_stability
(FeatureName) -> Stability | undefined when
      FeatureName :: feature_name(),
      Stability :: stability();
(FeatureProps) -> Stability when
      FeatureProps :: feature_props_extended(),
      Stability :: stability().
%% @doc
%% Returns the stability of a feature flag.
%%
%% The possible stability levels are:
%% <ul>
%% <li>`required': the feature flag is required and always enabled. It
%%   means the behavior prior to the introduction of the feature flags is no
%%   longer supported.</li>
%% <li>`stable': the feature flag is stable and will not change in future
%%   releases: it can be enabled in production.</li>
%% <li>`experimental': the feature flag is experimental and may change in
%%   the future (without a guaranteed upgrade path): enabling it in
%%   production is not recommended.</li>
%% </ul>
%%
%% @param FeatureName The name of the feature flag to check.
%% @param FeatureProps A feature flag properties map.
%% @returns `required', `stable' or `experimental', or `undefined' if the
%% given feature flag name doesn't correspond to a known feature flag.

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
    %% We list enabled feature flags. This has two purposes:
    %%   1. We initialize the registry (the generated module storing the list
    %%      and states of feature flags).
    %%   2. We use the returned list to initialize the `enabled_feature_flags'
    %%      file on disk if it doesn't exist. Some external tools rely on that
    %%      file too.
    EnabledFeatureFlags = list(enabled),
    ok = ensure_enabled_feature_flags_list_file_exists(EnabledFeatureFlags),
    ok.

-define(PT_TESTSUITE_ATTRS, {?MODULE, testsuite_feature_flags_attrs}).

inject_test_feature_flags(FeatureFlags) ->
    ExistingAppAttrs = module_attributes_from_testsuite(),
    FeatureFlagsPerApp0 = lists:foldl(
                            fun({Origin, Origin, FFlags}, Acc) ->
                                    Acc#{Origin => maps:from_list(FFlags)}
                            end, #{}, ExistingAppAttrs),
    FeatureFlagsPerApp1 = maps:fold(
                            fun(FeatureName, FeatureProps, Acc) ->
                                    Origin = case FeatureProps of
                                                 #{provided_by := App} ->
                                                     App;
                                                 _ ->
                                                     '$injected'
                                             end,
                                    FFlags0 = maps:get(Origin, Acc, #{}),
                                    FFlags1 = FFlags0#{
                                                FeatureName => FeatureProps},
                                    Acc#{Origin => FFlags1}
                            end, FeatureFlagsPerApp0, FeatureFlags),
    AttributesFromTestsuite = maps:fold(
                                fun(Origin, FFlags, Acc) ->
                                        [{Origin, % Application
                                          Origin, % Module
                                          maps:to_list(FFlags)} | Acc]
                                end, [], FeatureFlagsPerApp1),
    ?LOG_DEBUG(
      "Feature flags: injecting feature flags from testsuite: ~tp~n"
      "Feature flags: all injected feature flags: ~tp",
      [FeatureFlags, AttributesFromTestsuite],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    ok = persistent_term:put(?PT_TESTSUITE_ATTRS, AttributesFromTestsuite),
    rabbit_ff_registry_factory:initialize_registry().

module_attributes_from_testsuite() ->
    persistent_term:get(?PT_TESTSUITE_ATTRS, []).

-spec query_supported_feature_flags() -> {ScannedApps, FeatureFlags} when
      ScannedApps :: [atom()],
      FeatureFlags :: feature_flags().
%% @private

query_supported_feature_flags() ->
    ?LOG_DEBUG(
      "Feature flags: query feature flags in loaded applications",
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    T0 = erlang:timestamp(),
    %% We need to know the list of applications we scanned for feature flags.
    %% We can't derive that list of the returned feature flags because an
    %% application might be loaded/present and not have a specific feature
    %% flag. In this case, the feature flag should be considered unsupported.
    ScannedApps = rabbit_misc:rabbitmq_related_apps(),
    AttributesPerApp = rabbit_misc:module_attributes_from_apps(
                         rabbit_feature_flag, ScannedApps),
    AttributesFromTestsuite = module_attributes_from_testsuite(),
    TestsuiteProviders = [App || {App, _, _} <- AttributesFromTestsuite],
    T1 = erlang:timestamp(),
    ?LOG_DEBUG(
      "Feature flags: time to find supported feature flags: ~tp us",
      [timer:now_diff(T1, T0)],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    AllAttributes = AttributesPerApp ++ AttributesFromTestsuite,
    AllApps = lists:usort(ScannedApps ++ TestsuiteProviders),
    {AllApps, prepare_queried_feature_flags(AllAttributes, #{})}.

prepare_queried_feature_flags([{App, _Module, Attributes} | Rest],
                              AllFeatureFlags) ->
    ?LOG_DEBUG(
      "Feature flags: application `~ts` has ~b feature flags",
      [App, length(Attributes)],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    AllFeatureFlags1 = lists:foldl(
                         fun({FeatureName, FeatureProps}, AllFF) ->
                                 assert_feature_flag_is_valid(
                                   FeatureName, FeatureProps),
                                 merge_new_feature_flags(AllFF,
                                                         App,
                                                         FeatureName,
                                                         FeatureProps)
                         end, AllFeatureFlags, Attributes),
    prepare_queried_feature_flags(Rest, AllFeatureFlags1);
prepare_queried_feature_flags([], AllFeatureFlags) ->
    AllFeatureFlags.

assert_feature_flag_is_valid(FeatureName, FeatureProps) ->
    try
        ?assert(is_atom(FeatureName)),
        ?assert(is_map(FeatureProps)),
        Stability = get_stability(FeatureProps),
        ?assert(Stability =:= stable orelse
                Stability =:= experimental orelse
                Stability =:= required),
        ?assertNot(maps:is_key(migration_fun, FeatureProps)),
        case FeatureProps of
            #{callbacks := Callbacks} ->
                Known = [enable,
                         post_enable],
                ?assert(is_map(Callbacks)),
                ?assertEqual([], maps:keys(Callbacks) -- Known),
                lists:foreach(
                  fun(CallbackMF) ->
                          ?assertMatch({_, _}, CallbackMF),
                          {CallbackMod, CallbackFun} = CallbackMF,
                          ?assert(is_atom(CallbackMod)),
                          ?assert(is_atom(CallbackFun)),
                          ?assert(erlang:function_exported(
                                    CallbackMod, CallbackFun, 1))
                  end, maps:values(Callbacks));
            _ ->
                ok
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(
              "Feature flags: `~ts`: invalid properties:~n"
              "Feature flags: `~ts`:   Properties: ~tp~n"
              "Feature flags: `~ts`:   Error: ~tp",
              [FeatureName,
               FeatureName, FeatureProps,
               FeatureName, Reason],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            erlang:raise(Class, Reason, Stacktrace)
    end.

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

%% -------------------------------------------------------------------
%% Feature flags state storage.
%% -------------------------------------------------------------------

-spec does_enabled_feature_flags_list_file_exist() -> boolean().
%% @private

does_enabled_feature_flags_list_file_exist() ->
    try
        File = enabled_feature_flags_list_file(),
        filelib:is_regular(File)
    catch
        throw:feature_flags_file_not_set ->
            false
    end.

-spec ensure_enabled_feature_flags_list_file_exists(EnabledFeatureFlags) ->
    Ret when
      EnabledFeatureFlags :: feature_flags(),
      Ret :: ok | {error, any()}.
%% @private

ensure_enabled_feature_flags_list_file_exists(EnabledFeatureFlags) ->
    case does_enabled_feature_flags_list_file_exist() of
        true ->
            ok;
        false ->
            EnabledFeatureNames = maps:keys(EnabledFeatureFlags),
            write_enabled_feature_flags_list(EnabledFeatureNames)
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
            ?LOG_ERROR(
              "Feature flags: failed to read the `feature_flags` "
              "file at `~ts`: ~ts",
              [File, file:format_error(Reason)],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
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
    Content = io_lib:format("~tp.~n", [FeatureNames2]),
    %% TODO: If we fail to write the the file, we should spawn a process
    %% to retry the operation.
    case file:write_file(File, Content) of
        ok ->
            ok;
        {error, Reason} = Error ->
            ?LOG_ERROR(
              "Feature flags: failed to write the `feature_flags` "
              "file at `~ts`: ~ts",
              [File, file:format_error(Reason)],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
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

-spec mark_as_enabled_locally(feature_name(), feature_state()) ->
    any() | {error, any()} | no_return().
%% @private

mark_as_enabled_locally(FeatureName, IsEnabled) ->
    ?LOG_DEBUG(
      "Feature flags: `~ts`: mark as enabled=~tp",
      [FeatureName, IsEnabled],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
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
    rabbit_ff_registry_factory:initialize_registry(
      #{},
      #{FeatureName => IsEnabled},
      WrittenToDisk).

%% -------------------------------------------------------------------
%% Coordination with remote nodes.
%% -------------------------------------------------------------------

-spec remote_nodes() -> [node()].
%% @private

remote_nodes() ->
    rabbit_ff_controller:all_nodes() -- [node()].

-spec running_remote_nodes() -> [node()].
%% @private

running_remote_nodes() ->
    rabbit_ff_controller:running_nodes() -- [node()].

-ifdef(TEST).
-define(PT_OVERRIDDEN_NODES, {?MODULE, overridden_nodes}).
-define(PT_OVERRIDDEN_RUNNING_NODES, {?MODULE, overridden_running_nodes}).

override_nodes(Nodes) ->
    persistent_term:put(?PT_OVERRIDDEN_NODES, Nodes).

get_overriden_nodes() ->
    persistent_term:get(?PT_OVERRIDDEN_NODES, undefined).

override_running_nodes(Nodes) ->
    persistent_term:put(?PT_OVERRIDDEN_RUNNING_NODES, Nodes).

get_overriden_running_nodes() ->
    persistent_term:get(?PT_OVERRIDDEN_RUNNING_NODES, undefined).
-endif.

-spec does_node_support(node(), [feature_name()], timeout()) -> boolean().
%% @private

does_node_support(Node, FeatureNames, Timeout) ->
    ?LOG_DEBUG(
      "Feature flags: querying `~tp` support on node ~ts...",
      [FeatureNames, Node],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Ret = case node() of
              Node ->
                  is_supported_locally(FeatureNames);
              _ ->
                  run_feature_flags_mod_on_remote_node(
                    Node, is_supported_locally, [FeatureNames], Timeout)
          end,
    case Ret of
        {error, Reason} ->
            ?LOG_ERROR(
              "Feature flags: error while querying `~tp` support on "
              "node ~ts: ~tp",
              [FeatureNames, Node, Reason],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            false;
        true ->
            ?LOG_DEBUG(
              "Feature flags: node `~ts` supports `~tp`",
              [Node, FeatureNames],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            true;
        false ->
            ?LOG_DEBUG(
              "Feature flags: node `~ts` does not support `~tp`; "
              "stopping query here",
              [Node, FeatureNames],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
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
    rabbit_ff_controller:check_node_compatibility(Node).

run_feature_flags_mod_on_remote_node(Node, Function, Args, Timeout) ->
    rabbit_ff_controller:rpc_call(Node, ?MODULE, Function, Args, Timeout).

-spec sync_feature_flags_with_cluster([node()], boolean()) ->
    ok | {error, any()} | no_return().
%% @private

sync_feature_flags_with_cluster([] = _Nodes, true = _NodeIsVirgin) ->
    rabbit_ff_controller:enable_default();
sync_feature_flags_with_cluster([] = _Nodes, false = _NodeIsVirgin) ->
    ok;
sync_feature_flags_with_cluster(_Nodes, _NodeIsVirgin) ->
    rabbit_ff_controller:sync_cluster().

-spec refresh_feature_flags_after_app_load() ->
    ok | {error, any()} | no_return().

refresh_feature_flags_after_app_load() ->
    rabbit_ff_controller:refresh_after_app_load().
