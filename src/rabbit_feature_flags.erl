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
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_feature_flags).

-export([
         list/0,
         list/1,
         enable/1,
         disable/1,
         is_supported/1,
         is_supported_locally/1,
         is_supported_remotely/1,
         is_enabled/1,
         initialize_registry/0
        ]).

list() -> list(all).

list(all)      -> rabbit_ff_registry:list(all);
list(enabled)  -> rabbit_ff_registry:list(enabled);
list(disabled) -> maps:filter(
                    fun(FeatureName, _) -> not is_enabled(FeatureName) end,
                    list(all)).

enable(FeatureName) ->
    rabbit_log:info("Feature flag `~s`: request to enable",
                    [FeatureName]),
    case is_enabled(FeatureName) of
        true ->
            rabbit_log:info("Feature flag `~s`: already enabled",
                            [FeatureName]),
            ok;
        false ->
            rabbit_log:info("Feature flag `~s`: not enabled, "
                            "check if supported by cluster",
                            [FeatureName]),
            %% The feature flag must be supported locally and remotely
            %% (i.e. by all members of the cluster).
            case is_supported(FeatureName) of
                true ->
                    rabbit_log:info("Feature flag `~s`: supported, "
                                    "attempt to enable...",
                                    [FeatureName]),
                    do_enable(FeatureName);
                false ->
                    rabbit_log:info("Feature flag `~s`: not supported",
                                    [FeatureName]),
                    {error, unsupported}
            end
    end.

disable(_FeatureName) ->
    {error, unsupported}.

is_supported(FeatureName) when is_atom(FeatureName) ->
    is_supported_locally(FeatureName) andalso
    is_supported_remotely(FeatureName).

is_supported_locally(FeatureName) ->
    rabbit_ff_registry:is_supported(FeatureName).

is_supported_remotely(_FeatureName) ->
    % TODO.
    true.

is_enabled(FeatureName) when is_atom(FeatureName) ->
    rabbit_ff_registry:is_enabled(FeatureName).

%% -------------------------------------------------------------------

initialize_registry() ->
    rabbit_log:info("Feature flags: (re)initialize registry", []),
    AllFeatureFlags = query_supported_feature_flags(),
    EnabledFeatureNames = read_enabled_feature_flags_list(),
    EnabledFeatureFlags = maps:filter(
                            fun(FeatureName, _) ->
                                    lists:member(FeatureName,
                                                 EnabledFeatureNames)
                            end, AllFeatureFlags),
    List = [
            io_lib:format("~n  [~s] ~s",
                          [case maps:is_key(FeatureName, EnabledFeatureFlags) of
                               true  -> "x";
                               false -> " "
                           end,
                           FeatureName])
            || FeatureName <- lists:sort(maps:keys(AllFeatureFlags))],
    rabbit_log:info("Feature flags:~s", [List]),
    regen_registry_mod(AllFeatureFlags, EnabledFeatureFlags).

query_supported_feature_flags() ->
    LoadedApps = [App
                  || {App, _, _} <- application:loaded_applications()],
    rabbit_log:info("Feature flags: query feature flags in applications: ~p",
                    [LoadedApps]),
    query_supported_feature_flags(LoadedApps, #{}).

query_supported_feature_flags([App | Rest], AllFeatureFlags) ->
    case application:get_env(App, feature_flags_list) of
        {ok, FeatureFlags} when is_map(FeatureFlags) ->
            rabbit_log:info("Feature flags: application `~s` "
                            "has ~b feature flags",
                            [App, maps:size(FeatureFlags)]),
            AllFeatureFlags1 = maps:merge(AllFeatureFlags, FeatureFlags),
            query_supported_feature_flags(Rest, AllFeatureFlags1);
        {ok, {FFMod, FFFun}} when is_atom(FFMod) andalso is_atom(FFFun) ->
            rabbit_log:info("Feature flags: application `~s` "
                            "provides a feature_flags_list function: ~s:~s()",
                            [App, FFMod, FFFun]),
            try
                case erlang:apply(FFMod, FFFun, []) of
                    FeatureFlags when is_map(FeatureFlags) ->
                        rabbit_log:info("Feature flags: application `~s` "
                                        "has ~b feature flags",
                                        [App, maps:size(FeatureFlags)]),
                        AllFeatureFlags1 = maps:merge(AllFeatureFlags,
                                                      FeatureFlags),
                        query_supported_feature_flags(Rest, AllFeatureFlags1);
                    Invalid ->
                        rabbit_log:error(
                          "Feature flags: invalid feature flags "
                          "in application `~s`: ~p",
                          [App, Invalid]),
                        query_supported_feature_flags(Rest, AllFeatureFlags)
                end
            catch
                _:Reason:Stacktrace ->
                    rabbit_log:error(
                      "Feature flags: failed to query feature flags "
                      "of application `~s` "
                      "using `~s:~s()`: ~p~n~p",
                      [App, FFMod, FFFun, Reason, Stacktrace]),
                    query_supported_feature_flags(Rest, AllFeatureFlags)
            end;
        undefined ->
            query_supported_feature_flags(Rest, AllFeatureFlags);
        {ok, Invalid} ->
            rabbit_log:error("Feature flags: invalid feature flags "
                             "in application `~s`: ~p",
                             [App, Invalid]),
            query_supported_feature_flags(Rest, AllFeatureFlags)
    end;
query_supported_feature_flags([], AllFeatureFlags) ->
    AllFeatureFlags.

read_enabled_feature_flags_list() ->
    File = enabled_feature_flags_list_file(),
    case file:consult(File) of
        {ok, [List]}    -> List;
        {error, enoent} -> [];
        {error, Reason} -> {error, Reason}
    end.

write_enabled_feature_flags_list(FeatureNames) ->
    File = enabled_feature_flags_list_file(),
    Content = io_lib:format("~p.~n", [FeatureNames]),
    file:write_file(File, Content).

enabled_feature_flags_list_file() ->
    "/tmp/feature-flags.erl".

do_enable(FeatureName) ->
    #{FeatureName := FeatureProps} = rabbit_ff_registry:list(all),
    DependsOn = maps:get(depends_on, FeatureProps, []),
    rabbit_log:info("Feature flag `~s`: enable dependencies: ~p",
                    [FeatureName, DependsOn]),
    case enable_dependencies(FeatureName, DependsOn) of
        ok    -> run_migration_fun(FeatureName, FeatureProps, enable);
        Error -> Error
    end.

enable_dependencies(TopLevelFeatureName, [FeatureName | Rest]) ->
    case enable(FeatureName) of
        ok    -> enable_dependencies(TopLevelFeatureName, Rest);
        Error -> Error
    end;
enable_dependencies(_, []) ->
    ok.

run_migration_fun(FeatureName, FeatureProps, Arg) ->
    case maps:get(migration_fun, FeatureProps, none) of
        {MigrationMod, MigrationFun}
          when is_atom(MigrationMod) andalso is_atom(MigrationFun) ->
            rabbit_log:info("Feature flag `~s`: run migration function ~p "
                            "with arg: ~p",
                            [FeatureName, MigrationFun, Arg]),
            try
                case erlang:apply(MigrationMod, MigrationFun, [Arg]) of
                    ok    -> mark_as_enabled(FeatureName);
                    Error -> Error
                end
            catch
                _:Reason:Stacktrace ->
                    rabbit_log:error("Feature flag `~s`: migration function "
                                     "crashed: ~p~n~p",
                                     [FeatureName, Reason, Stacktrace]),
                    {error, {migration_fun_crash, Reason, Stacktrace}}
            end;
        none ->
            mark_as_enabled(FeatureName);
        Invalid ->
            rabbit_log:error("Feature flag `~s`: invalid migration "
                             "function: ~p",
                            [FeatureName, Invalid]),
            {error, {invalid_migration_fun, Invalid}}
    end.

mark_as_enabled(FeatureName) ->
    rabbit_log:info("Feature flag `~s`: mark as enabled",
                    [FeatureName]),
    EnabledFeatureNames = read_enabled_feature_flags_list(),
    EnabledFeatureNames1 = [FeatureName | EnabledFeatureNames],
    write_enabled_feature_flags_list(EnabledFeatureNames1),
    initialize_registry().

regen_registry_mod(AllFeatureFlags, EnabledFeatureFlags) ->
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
                       || {F, A} <- [{list, 1},
                                     {is_supported, 1},
                                     {is_enabled, 1}]]
                     )
                   ]
                  ),
    ExportForm = erl_syntax:revert(ExportAttr),
    %% list(_) -> ...
    ListAllBody = erl_syntax:abstract(AllFeatureFlags),
    ListAllClause = erl_syntax:clause([erl_syntax:atom(all)],
                                      [],
                                      [ListAllBody]),
    ListEnabledBody = erl_syntax:abstract(EnabledFeatureFlags),
    ListEnabledClause = erl_syntax:clause([erl_syntax:atom(enabled)],
                                          [],
                                          [ListEnabledBody]),
    ListFun = erl_syntax:function(
                erl_syntax:atom(list),
                [ListAllClause, ListEnabledClause]),
    ListFunForm = erl_syntax:revert(ListFun),
    %% is_supported(_) -> ...
    IsSupportedClauses = [
                          erl_syntax:clause(
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
    IsEnabledClauses = [
                        erl_syntax:clause(
                          [erl_syntax:atom(FeatureName)],
                          [],
                          [erl_syntax:atom(
                             maps:is_key(FeatureName, EnabledFeatureFlags))])
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
    %% Compilation!
    Forms = [ModuleForm,
             ExportForm,
             ListFunForm,
             IsSupportedFunForm,
             IsEnabledFunForm],
    CompileOpts = [return_errors,
                   return_warnings],
    case compile:forms(Forms, CompileOpts) of
        {ok, Mod, Bin, _} ->
            load_registry_mod(Mod, Bin);
        {error, Errors, Warnings} ->
            rabbit_log:error("Feature flags: registry compilation:~n"
                             "Errors: ~p~n"
                             "Warnings: ~p",
                             [Errors, Warnings]),
            {error, compilation_failure}
    end.

load_registry_mod(Mod, Bin) ->
    rabbit_log:info("Feature flags: registry module ready, loading it..."),
    LockId = {?MODULE, self()},
    FakeFilename = "Compiled and loaded by " ++ ?MODULE_STRING,
    global:set_lock(LockId, [node()]),
    _ = code:soft_purge(Mod),
    _ = code:delete(Mod),
    Ret = code:load_binary(Mod, FakeFilename, Bin),
    global:del_lock(LockId, [node()]),
    case Ret of
        {module, _} ->
            rabbit_log:info("Feature flags: registry module loaded"),
            ok;
        {error, Reason} ->
            rabbit_log:info("Feature flags: failed to load registry "
                            "module: ~p",
                            [Reason]),
            throw({feature_flag_registry_reload_failure, Reason})
    end.
