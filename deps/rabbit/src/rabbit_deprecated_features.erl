%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2023 VMware, Inc. or its affiliates.
%%
%% @doc
%% This module provides an API to manage deprecated features in RabbitMQ. It
%% is built on top of the Feature flags subsystem.
%%
%% == What a deprecated feature is ==
%%
%% A <strong>deprecated feature</strong> is a name and several properties
%% given to a feature in RabbitMQ that will be removed in a future version. By
%% defining a deprecated feature, we can communicate to end users that what
%% they are using is going away and allow them to test how RabbitMQ behaves as
%% if the feature was already removed.
%%
%% Because it is based on feature flags, everything in {@link
%% rabbit_feature_flags} applies. However, the semantic is kind of reversed:
%% when the feature flag behind a deprecated feature is enabled, this means the
%% deprecated feature is removed or can be considered removed. Therefore
%% <strong>the use of a deprecated feature is permitted while the backing
%% feature flag is disabled and denied once the feature flag is
%% enabled</strong>.
%%
%% == How to declare a deprecated feature ==
%%
%% To define a deprecated feature, you need to use the
%% `-rabbit_deprecated_feature()' module attribute:
%%
%% ```
%% -rabbit_deprecated_feature(DeprecatedFeature).
%% '''
%%
%% `DeprecatedFeature' is a {@type deprecated_feature_modattr()}.
%%
%% == How to check that a deprecated feature is permitted ==
%%
%% To check in the code if a deprecated feature is permitted:
%%
%% ```
%% case rabbit_deprecated_features:is_permitted(DeprecatedFeatureName) of
%%     true ->
%%         %% The deprecated feature is still permitted.
%%         ok;
%%     false ->
%%         %% The deprecated feature is gone or should be considered
%%         %% unavailable.
%%         error
%% end.
%% '''
%%
%% == How to permit or not a deprecated feature from the configuration ==
%%
%% The following configuration snippet permits one deprecated feature and
%% denies another one:
%%
%% ```
%% deprecated_features.permit.my_deprecated_feature_1 = true
%% deprecated_features.permit.my_deprecated_feature_2 = false
%% '''
%%
%% == Differences with regular feature flags ==
%%
%% Despite the fact that a deprecated feature is implemented as a feature flag
%% behind the scene, there is a slight difference of behavior in the way a
%% deprecated feature's feature flag is enabled.
%%
%% A regular feature flag is disabled during RabbitMQ startup, except if it is
%% required or has been enabled in a previous run. If this is the first time
%% RabbitMQ starts, all stable feature flags are enabled.
%%
%% A deprecated feature's feature flag is enabled or disabled on startup
%% depending on its deprecation phase. During `permitted_by_default', it is
%% disabled out-of-the-box, except if configured otherwise. During
%% `denied_by_default', it is enabled out-of-the-box, except if configured
%% otherwise. When `disconnected' or `removed', it is always enabled like a
%% required regular feature flag. This logic is in the registry initialization
%% code in {@link rabbit_ff_registry_factory:maybe_initialize_registry/3}.
%%
%% Later during the boot process, after plugins are loaded and the Feature
%% flags subsystem refreshes known feature flags, we execute the {@link
%% is_feature_used_callback()} callback. If the feature is used and the
%% underlying feature flag was enabled, then the refresh fails and RabbitMQ
%% fails to start.
%%
%% This callback is also used when the underlying feature flag is enabled
%% later at runtime by calling {@link rabbit_feature_flags:enable/1} or during
%% a cluster-wide sync of the feature flags states. Again, the underlying
%% feature flag won't be enabled if the feature is used.
%%
%% Note that this callback is only used when in `permitted_by_default' or
%% `denied_by_default': remember that `disconnected' and `removed' are the
%% same as a required feature flag.
%%
%% Another difference is that the state of a deprecated feature's feature flag
%% is not recorded in the {@link
%% rabbit_feature_flags:enabled_feature_flags_list_file/0}. As said earlier,
%% the state is always computed when the registry is initialized.

-module(rabbit_deprecated_features).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-include("src/rabbit_feature_flags.hrl").

-export([is_permitted/1,
         get_phase/1,
         get_warning/1]).
-export([extend_properties/2,
         should_be_permitted/2,
         enable_underlying_feature_flag_cb/1]).

-type deprecated_feature_modattr() :: {rabbit_feature_flags:feature_name(),
                                       feature_props()}.
%% The value of a `-rabbitmq_deprecated_feature()' module attribute used to
%% declare a deprecated feature.
%%
%% Example:
%% ```
%% -rabbit_deprecated_feature(
%%    {my_deprecated_feature_1,
%%     #{deprecation_phase => permitted_by_default,
%%       msg_when_permitted => "Feature 1 will be removed from RabbitMQ X.0"
%%      }}).
%% '''

-type deprecation_phase() :: permitted_by_default |
                             denied_by_default |
                             disconnected |
                             removed.
%% The deprecation phase of a feature.
%%
%% Deprecation phases are used in the following order:
%% <ol>
%% <li>`permitted_by_default': the feature is enabled by default and the user
%% can use it like they did so far. They can turn it off in the configuration
%% to experiment with the absence of the feature.</li>
%% <li>`denied_by_default': the feature is disabled by default and the user
%% must enable it in the configuration to be able to use it.</li>
%% <li>`disconnected': the code of the feature is still there but it is
%% disabled and can't be re-enabled from the configuration. The user has to
%% recompile RabbitMQ to re-enable it.</li>
%% <li>`removed': the code of the feature is no longer in the product. There is
%% no way to re-enable it at this point. The deprecated feature must still be
%% defined because, like required feature flags, its presence is important to
%% determine if nodes are compatible and can be clustered together.</li>
%% </ol>

-type feature_props() :: #{desc => string(),
                           doc_url => string(),
                           deprecation_phase := deprecation_phase(),
                           messages => #{when_permitted => string(),
                                         when_denied => string(),
                                         when_removed => string()},
                           callbacks =>
                           #{callback_name() =>
                             rabbit_feature_flags:callback_fun_name()}}.
%% The deprecated feature properties.
%%
%% The properties are:
%% <ul>
%% <li>`deprecation_phase': where the deprecated feature is in its
%% lifecycle</li>
%% <li>`messages': a map of warning/error messages for each situation:
%% <ul>
%% <li>`when_permitted': what message to log and possibly display to the user
%% when the feature is being permitted and used. It is logged as a warning or
%% displayed to the user by the CLI or in the management UI for instance.</li>
%% <li>`when_denied': like `when_permitted', message used when an attempt to
%% use a denied deprecated feature is being made. It is logged as an error or
%% displayed to the user by the CLI or in the management UI for instance.</li>
%% </ul></li>
%% </ul>
%%
%% Other properties are the same as {@link
%% rabbit_feature_flags:feature_props()}.

-type feature_props_extended() ::
      #{name := rabbit_feature_flags:feature_name(),
        desc => string(),
        doc_url => string(),
        callbacks => #{callback_name() | enable =>
                       rabbit_feature_flags:callback_fun_name()},
        deprecation_phase := deprecation_phase(),
        messages := #{when_permitted => string(),
                      when_denied => string(),
                      when_removed => string()},
        provided_by := atom()}.
%% The deprecated feature properties, once expanded by this module when
%% feature flags are discovered.
%%
%% We make sure messages are set, possibly generating them automatically if
%% needed. Other added properties are the same as {@link
%% rabbit_feature_flags:feature_props_extended()}.

-type callbacks() :: is_feature_used_callback().
%% All possible callbacks.

-type callbacks_args() :: is_feature_used_callback_args().
%% All possible callbacks arguments.

-type callbacks_rets() :: is_feature_used_callback_ret().
%% All possible callbacks return values.

-type callback_name() :: is_feature_used.
%% Name of the callback.

-type is_feature_used_callback() :: fun((is_feature_used_callback_args())
                                        -> is_feature_used_callback_ret()).
%% The callback called when a deprecated feature is about to be denied.
%%
%% If this callback returns true (i.e. the feature is currently actively
%% used), the deprecated feature won't be marked as denied. This will also
%% prevent the RabbitMQ node from starting.

-type is_feature_used_callback_args() ::
      #{feature_name := rabbit_feature_flags:feature_name(),
        feature_props := feature_props_extended(),
        command := is_feature_used,
        nodes := [node()]}.
%% A map passed to {@type is_feature_used_callback()}.

-type is_feature_used_callback_ret() :: boolean().
%% Return value of the `is_feature_used' callback.

-export_type([deprecated_feature_modattr/0,
              deprecation_phase/0,
              feature_props/0,
              feature_props_extended/0,
              callbacks/0,
              callback_name/0,
              callbacks_args/0,
              callbacks_rets/0,
              is_feature_used_callback/0,
              is_feature_used_callback_args/0,
              is_feature_used_callback_ret/0]).

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

-spec is_permitted(FeatureName) -> IsPermitted when
      FeatureName :: rabbit_feature_flags:feature_name(),
      IsPermitted :: boolean().
%% @doc Indicates if the given deprecated feature is permitted or not.
%%
%% Calling this function automatically logs a warning or an error to let the
%% user know they are using something that is or will be removed. For a given
%% deprecated feature, automatic warning is limited to one occurence per day.
%%
%% @param FeatureName the name of the deprecated feature.
%%
%% @returns true if the deprecated feature can be used, false otherwise.

is_permitted(FeatureName) ->
    Permitted = is_permitted_nolog(FeatureName),
    maybe_log_warning(FeatureName, Permitted),
    Permitted.

is_permitted_nolog(FeatureName) ->
    not rabbit_feature_flags:is_enabled(FeatureName).

-spec get_phase
(FeatureName) -> Phase | undefined when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Phase :: deprecation_phase();
(FeatureProps) -> Phase when
      FeatureProps :: feature_props() | feature_props_extended(),
      Phase :: deprecation_phase().
%% @doc Returns the deprecation phase of the given deprecated feature.
%%
%% @param FeatureName the name of the deprecated feature.
%% @param FeatureProps the properties of the deprecated feature.
%%
%% @returns the deprecation phase, or `undefined' if the deprecated feature
%% was given by its name and this name corresponds to no known deprecated
%% features.

get_phase(FeatureName) when is_atom(FeatureName) ->
    case rabbit_ff_registry_wrapper:get(FeatureName) of
        undefined    -> undefined;
        FeatureProps -> get_phase(FeatureProps)
    end;
get_phase(FeatureProps) when is_map(FeatureProps) ->
    ?assert(?IS_DEPRECATION(FeatureProps)),
    maps:get(deprecation_phase, FeatureProps).

-spec get_warning
(FeatureName) -> Warning | undefined when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Warning :: string() | undefined;
(FeatureProps) -> Warning when
      FeatureProps :: feature_props_extended(),
      Warning :: string().
%% @doc Returns the message associated with the given deprecated feature.
%%
%% Messages are set in the `msg_when_permitted' and `msg_when_denied'
%% properties.
%%
%% If the deprecated feature defines no warning in its declaration, a warning
%% message is generated automatically.
%%
%% @param FeatureName the name of the deprecated feature.
%% @param FeatureProps the properties of the deprecated feature.
%%
%% @returns the warning message, or `undefined' if the deprecated feature was
%% given by its name and this name corresponds to no known deprecated
%% features.

get_warning(FeatureName) when is_atom(FeatureName) ->
    case rabbit_ff_registry_wrapper:get(FeatureName) of
        undefined    -> undefined;
        FeatureProps -> get_warning(FeatureProps)
    end;
get_warning(FeatureProps) when is_map(FeatureProps) ->
    ?assert(?IS_DEPRECATION(FeatureProps)),
    #{name := FeatureName} = FeatureProps,
    Permitted = is_permitted_nolog(FeatureName),
    get_warning(FeatureProps, Permitted).

get_warning(FeatureName, Permitted) when is_atom(FeatureName) ->
    case rabbit_ff_registry_wrapper:get(FeatureName) of
        undefined    -> undefined;
        FeatureProps -> get_warning(FeatureProps, Permitted)
    end;
get_warning(FeatureProps, Permitted) when is_map(FeatureProps) ->
    ?assert(?IS_DEPRECATION(FeatureProps)),
    Phase = get_phase(FeatureProps),
    Msgs = maps:get(messages, FeatureProps),
    if
        Phase =:= permitted_by_default orelse Phase =:= denied_by_default ->
            case Permitted of
                true  -> maps:get(when_permitted, Msgs);
                false -> maps:get(when_denied, Msgs)
            end;
        true ->
            maps:get(when_removed, Msgs)
    end.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec extend_properties(FeatureName, FeatureProps) -> ExtFeatureProps when
      FeatureName :: rabbit_feature_flags:feature_name(),
      FeatureProps :: feature_props() |
                      feature_props_extended(),
      ExtFeatureProps :: feature_props_extended().
%% @doc Extend the deprecated feature properties.
%%
%% <ol>
%% <li>It generates warning/error messages automatically if the properties
%% don't have them set.</li>
%% <li>It wraps the `is_feature_used' callback.</li>
%% </ol>
%%
%% @private

extend_properties(FeatureName, FeatureProps)
  when ?IS_DEPRECATION(FeatureProps) ->
    FeatureProps1 = generate_warnings(FeatureName, FeatureProps),
    FeatureProps2 = wrap_callback(FeatureName, FeatureProps1),
    FeatureProps2.

generate_warnings(FeatureName, FeatureProps) ->
    Msgs0 = maps:get(messages, FeatureProps, #{}),
    Msgs1 = generate_warnings1(FeatureName, FeatureProps, Msgs0),
    FeatureProps#{messages => Msgs1}.

generate_warnings1(FeatureName, FeatureProps, Msgs) ->
    Phase = get_phase(FeatureProps),
    DefaultMsgs =
    if
        Phase =:= permitted_by_default ->
            #{when_permitted =>
              rabbit_misc:format(
                "Feature `~ts` is deprecated.~n"
                "By default, this feature can still be used for now.~n"
                "Its use will not be permitted by default in a future minor "
                "RabbitMQ version and the feature will be removed from a"
                "future major RabbitMQ version; actual versions to be"
                "determined.~n"
                "To continue using this feature when it is not permitted "
                "by default, set the following parameter in your "
                "configuration:~n"
                "    \"deprecated_features.permit.~ts = true\"~n"
                "To test RabbitMQ as if the feature was removed, set this "
                "in your configuration:~n"
                "    \"deprecated_features.permit.~ts = false\"",
                [FeatureName, FeatureName, FeatureName]),

              when_denied =>
              rabbit_misc:format(
                "Feature `~ts` is deprecated.~n"
                "Its use is not permitted per the configuration "
                "(overriding the default, which is permitted):~n"
                "    \"deprecated_features.permit.~ts = false\"~n"
                "Its use will not be permitted by default in a future minor "
                "RabbitMQ version and the feature will be removed from a "
                "future major RabbitMQ version; actual versions to be "
                "determined.~n"
                "To continue using this feature when it is not permitted "
                "by default, set the following parameter in your "
                "configuration:~n"
                "    \"deprecated_features.permit.~ts = true\"",
                [FeatureName, FeatureName, FeatureName])};

        Phase =:= denied_by_default ->
            #{when_permitted =>
              rabbit_misc:format(
                "Feature `~ts` is deprecated.~n"
                "Its use is permitted per the configuration (overriding "
                "the default, which is not permitted):~n"
                "    \"deprecated_features.permit.~ts = true\"~n"
                "The feature will be removed from a future major RabbitMQ "
                "version, regardless of the configuration.",
                [FeatureName, FeatureName]),

              when_denied =>
              rabbit_misc:format(
                "Feature `~ts` is deprecated.~n"
                "By default, this feature is not permitted anymore.~n"
                "The feature will be removed from a future major RabbitMQ "
                "version, regardless of the configuration; actual version "
                "to be determined.~n"
                "To continue using this feature when it is not permitted "
                "by default, set the following parameter in your "
                "configuration:~n"
                "    \"deprecated_features.permit.~ts = true\"",
                [FeatureName, FeatureName])};

        Phase =:= disconnected orelse Phase =:= removed ->
            #{when_removed =>
              rabbit_misc:format(
                "Feature `~ts` is removed; "
                "its use is not possible anymore.~n"
                "If RabbitMQ refuses to start because of this, you need to "
                "downgrade RabbitMQ and make sure the feature is not used "
                "at all before upgrading again.",
                [FeatureName])}
    end,
    maps:merge(DefaultMsgs, Msgs).

wrap_callback(_FeatureName, #{callbacks := Callbacks} = FeatureProps) ->
    Callbacks1 = Callbacks#{
                   enable => {?MODULE, enable_underlying_feature_flag_cb}},
    FeatureProps#{callbacks => Callbacks1};
wrap_callback(_FeatureName, FeatureProps) ->
    FeatureProps.

-spec should_be_permitted(FeatureName, FeatureProps) -> IsPermitted when
      FeatureName :: rabbit_feature_flags:feature_name(),
      FeatureProps :: feature_props_extended(),
      IsPermitted :: boolean().
%% @doc Indicates if the deprecated feature should be permitted.
%%
%% The decision is based on the deprecation phase and the configuration.
%%
%% @private

should_be_permitted(FeatureName, FeatureProps) ->
    case get_phase(FeatureProps) of
        permitted_by_default ->
            is_permitted_in_configuration(FeatureName, true);
        denied_by_default ->
            is_permitted_in_configuration(FeatureName, false);
        Phase ->
            case is_permitted_in_configuration(FeatureName, false) of
                true ->
                    ?LOG_WARNING(
                       "Deprecated features: `~ts`: ~ts feature, it "
                       "cannot be permitted from configuration",
                       [FeatureName, Phase],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS});
                false ->
                    ok
            end,
            false
    end.

-spec is_permitted_in_configuration(FeatureName, Default) -> IsPermitted when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Default :: boolean(),
      IsPermitted :: boolean().
%% @private

is_permitted_in_configuration(FeatureName, Default) ->
    Settings = application:get_env(rabbit, permit_deprecated_features, #{}),
    case maps:get(FeatureName, Settings, undefined) of
        undefined ->
            ?LOG_DEBUG(
               "Deprecated features: `~ts`: `permit_deprecated_features` "
               "map unset in configuration, using default",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Default;
        Default ->
            PermittedStr = case Default of
                               true  -> "permitted";
                               false -> "not permitted"
                           end,
            ?LOG_DEBUG(
               "Deprecated features: `~ts`: ~ts in configuration, same as "
               "default",
               [FeatureName, PermittedStr],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Default;
        Permitted ->
            PermittedStr = case Permitted of
                               true  -> "permitted";
                               false -> "not permitted"
                           end,
            ?LOG_DEBUG(
               "Deprecated features: `~ts`: ~ts in configuration, overrides "
               "default",
               [FeatureName, PermittedStr],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ?assert(is_boolean(Permitted)),
            Permitted
    end.

-spec maybe_log_warning(FeatureName, Permitted) -> ok when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Permitted :: boolean().
%% @private

maybe_log_warning(FeatureName, Permitted) ->
    case should_log_warning(FeatureName) of
        false ->
            ok;
        true ->
            Warning = get_warning(FeatureName, Permitted),
            FormatStr = "Deprecated features: `~ts`: ~ts",
            FormatArgs = [FeatureName, Warning],
            case Permitted of
                true ->
                    ?LOG_WARNING(
                       FormatStr, FormatArgs,
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS});
                false ->
                    ?LOG_ERROR(
                       FormatStr, FormatArgs,
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS})
            end
    end.

-define(PT_DEPRECATION_WARNING_TS(FeatureName), {?MODULE, FeatureName}).

-spec should_log_warning(FeatureName) -> ShouldLog when
      FeatureName :: rabbit_feature_flags:feature_name(),
      ShouldLog :: boolean().
%% @private

should_log_warning(FeatureName) ->
    Key = ?PT_DEPRECATION_WARNING_TS(FeatureName),
    Now = erlang:timestamp(),
    try
        Last = persistent_term:get(Key),
        Diff = timer:now_diff(Now, Last),
        if
            Diff >= 24 * 60 * 60 * 1000 * 1000 ->
                persistent_term:put(Key, Now),
                true;
            true ->
                false
        end
    catch
        error:badarg ->
            persistent_term:put(Key, Now),
            true
    end.

enable_underlying_feature_flag_cb(
  #{command := enable,
    feature_name := FeatureName,
    feature_props := #{callbacks := Callbacks}} = Args) ->
    case Callbacks of
        #{is_feature_used := {CallbackMod, CallbackFun}} ->
            Args1 = Args#{command => is_feature_used},
            IsUsed = erlang:apply(CallbackMod, CallbackFun, [Args1]),
            case IsUsed of
                false ->
                    ok;
                true ->
                    ?LOG_ERROR(
                       "Deprecated features: `~ts`: can't deny deprecated "
                       "feature because it is actively used",
                       [FeatureName],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    {error,
                     {failed_to_deny_deprecated_features, [FeatureName]}}
            end;
        _ ->
            ok
    end.
