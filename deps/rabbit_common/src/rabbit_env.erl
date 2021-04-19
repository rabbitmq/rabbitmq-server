%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_env).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").

-include("logging.hrl").

-export([get_context/0,
         get_context/1,
         get_context_before_logging_init/0,
         get_context_before_logging_init/1,
         get_context_after_logging_init/1,
         get_context_after_reloading_env/1,
         dbg_config/0,
         env_vars/0,
         has_var_been_overridden/1,
         has_var_been_overridden/2,
         get_used_env_vars/0,
         log_process_env/0,
         log_context/1,
         context_to_app_env_vars/1,
         context_to_app_env_vars_no_logging/1,
         context_to_code_path/1]).

-ifdef(TEST).
-export([parse_conf_env_file_output2/2,
         value_is_yes/1]).
-endif.

%% Vary from OTP version to version.
-ignore_xref([
    {os, env, 0},
    {os, list_env_vars, 0}
]).
%% Relies on functions only available in certain OTP versions.
-dialyzer({nowarn_function, [env_vars/0]}).

-define(USED_ENV_VARS,
        [
         "RABBITMQ_ALLOW_INPUT",
         "RABBITMQ_ADVANCED_CONFIG_FILE",
         "RABBITMQ_BASE",
         "RABBITMQ_CONF_ENV_FILE",
         "RABBITMQ_CONFIG_FILE",
         "RABBITMQ_CONFIG_FILES",
         "RABBITMQ_DBG",
         "RABBITMQ_DIST_PORT",
         "RABBITMQ_ENABLED_PLUGINS",
         "RABBITMQ_ENABLED_PLUGINS_FILE",
         "RABBITMQ_FEATURE_FLAGS",
         "RABBITMQ_FEATURE_FLAGS_FILE",
         "RABBITMQ_HOME",
         "RABBITMQ_KEEP_PID_FILE_ON_EXIT",
         "RABBITMQ_LOG",
         "RABBITMQ_LOG_BASE",
         "RABBITMQ_LOG_FF_REGISTRY",
         "RABBITMQ_LOGS",
         "RABBITMQ_MNESIA_BASE",
         "RABBITMQ_MNESIA_DIR",
         "RABBITMQ_MOTD_FILE",
         "RABBITMQ_NODE_IP_ADDRESS",
         "RABBITMQ_NODE_PORT",
         "RABBITMQ_NODENAME",
         "RABBITMQ_PID_FILE",
         "RABBITMQ_PLUGINS_DIR",
         "RABBITMQ_PLUGINS_EXPAND_DIR",
         "RABBITMQ_PRODUCT_NAME",
         "RABBITMQ_PRODUCT_VERSION",
         "RABBITMQ_QUORUM_DIR",
         "RABBITMQ_STREAM_DIR",
         "RABBITMQ_UPGRADE_LOG",
         "RABBITMQ_USE_LONGNAME",
         "SYS_PREFIX"
        ]).

-export_type([context/0]).

-type context() :: map().

get_context() ->
    Context0 = get_context_before_logging_init(),
    Context1 = get_context_after_logging_init(Context0),
    get_context_after_reloading_env(Context1).

get_context(TakeFromRemoteNode) ->
    Context0 = get_context_before_logging_init(TakeFromRemoteNode),
    Context1 = get_context_after_logging_init(Context0),
    get_context_after_reloading_env(Context1).

get_context_before_logging_init() ->
    get_context_before_logging_init(false).

get_context_before_logging_init(TakeFromRemoteNode) ->
    %% The order of steps below is important because some of them
    %% depends on previous steps.
    Steps = [
             fun os_type/1,
             fun log_levels/1,
             fun interactive_shell/1,
             fun output_supports_colors/1
            ],

    run_context_steps(context_base(TakeFromRemoteNode), Steps).

get_context_after_logging_init(Context) ->
    %% The order of steps below is important because some of them
    %% depends on previous steps.
    Steps = [
             fun sys_prefix/1,
             fun rabbitmq_base/1,
             fun data_dir/1,
             fun rabbitmq_home/1,
             fun config_base_dir/1,
             fun load_conf_env_file/1,
             fun log_levels/1
            ],

    run_context_steps(Context, Steps).

get_context_after_reloading_env(Context) ->
    %% The order of steps below is important because some of them
    %% depends on previous steps.
    Steps = [
             fun nodename_type/1,
             fun nodename/1,
             fun split_nodename/1,
             fun maybe_setup_dist_for_remote_query/1,
             fun dbg_config/1,
             fun main_config_file/1,
             fun additional_config_files/1,
             fun advanced_config_file/1,
             fun log_base_dir/1,
             fun main_log_file/1,
             fun upgrade_log_file/1,
             fun mnesia_base_dir/1,
             fun mnesia_dir/1,
             fun quorum_queue_dir/1,
             fun stream_queue_dir/1,
             fun pid_file/1,
             fun keep_pid_file_on_exit/1,
             fun feature_flags_file/1,
             fun forced_feature_flags_on_init/1,
             fun log_feature_flags_registry/1,
             fun plugins_path/1,
             fun plugins_expand_dir/1,
             fun enabled_plugins_file/1,
             fun enabled_plugins/1,
             fun maybe_stop_dist_for_remote_query/1,
             fun amqp_ipaddr/1,
             fun amqp_tcp_port/1,
             fun erlang_dist_tcp_port/1,
             fun product_name/1,
             fun product_version/1,
             fun motd_file/1
            ],

    run_context_steps(Context, Steps).

context_base(TakeFromRemoteNode) ->
    Context = #{},
    case TakeFromRemoteNode of
        false ->
            Context;
        offline ->
            update_context(Context,
                           from_remote_node,
                           offline);
        _ when is_atom(TakeFromRemoteNode) ->
            update_context(Context,
                           from_remote_node,
                           {TakeFromRemoteNode, 10000});
        {RemoteNode, infinity}
          when is_atom(RemoteNode) ->
            update_context(Context,
                           from_remote_node,
                           TakeFromRemoteNode);
        {RemoteNode, Timeout}
          when is_atom(RemoteNode) andalso
               is_integer(Timeout) andalso
               Timeout >= 0 ->
            update_context(Context,
                           from_remote_node,
                           {TakeFromRemoteNode, Timeout})
    end.

-ifdef(TEST).
os_type(Context) ->
    {OSType, Origin} =
    try
        {persistent_term:get({?MODULE, os_type}), environment}
    catch
        _:badarg ->
            {os:type(), default}
    end,
    update_context(Context, os_type, OSType, Origin).
-else.
os_type(Context) ->
    update_context(Context, os_type, os:type(), default).
-endif.

run_context_steps(Context, Steps) ->
    lists:foldl(
      fun(Step, Context1) -> Step(Context1) end,
      Context,
      Steps).

update_context(Context, Key, Value) ->
    Context#{Key => Value}.

-define(origin_is_valid(O),
        O =:= default orelse
        O =:= environment orelse
        O =:= remote_node).

update_context(#{var_origins := Origins} = Context, Key, Value, Origin)
  when ?origin_is_valid(Origin) ->
    Context#{Key => Value,
             var_origins => Origins#{Key => Origin}};
update_context(Context, Key, Value, Origin)
  when ?origin_is_valid(Origin) ->
    Context#{Key => Value,
             var_origins => #{Key => Origin}}.

env_vars() ->
    case erlang:function_exported(os, list_env_vars, 0) of
        true  -> os:list_env_vars(); %% OTP < 24
        false -> os:env()            %% OTP >= 24
    end.

has_var_been_overridden(Var) ->
    has_var_been_overridden(get_context(), Var).

has_var_been_overridden(#{var_origins := Origins}, Var) ->
    case maps:get(Var, Origins, default) of
        default -> false;
        _       -> true
    end.

get_used_env_vars() ->
    lists:filter(
      fun({Var, _}) -> var_is_used(Var) end,
      lists:sort(env_vars())).

log_process_env() ->
    ?LOG_DEBUG("Process environment:"),
    lists:foreach(
      fun({Var, Value}) ->
              ?LOG_DEBUG("  - ~s = ~ts", [Var, Value])
      end, lists:sort(env_vars())).

log_context(Context) ->
    ?LOG_DEBUG("Context (based on environment variables):"),
    lists:foreach(
      fun(Key) ->
              Value = maps:get(Key, Context),
              ?LOG_DEBUG("  - ~s: ~p", [Key, Value])
      end,
      lists:sort(maps:keys(Context))).

context_to_app_env_vars(Context) ->
    ?LOG_DEBUG(
      "Setting default application environment variables:",
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    Fun = fun({App, Param, Value}) ->
                  ?LOG_DEBUG(
                     "  - ~s:~s = ~p", [App, Param, Value],
                     #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                  ok = application:set_env(
                         App, Param, Value, [{persistent, true}])
          end,
    context_to_app_env_vars1(Context, Fun).

context_to_app_env_vars_no_logging(Context) ->
    Fun = fun({App, Param, Value}) ->
                  ok = application:set_env(
                         App, Param, Value, [{persistent, true}])
          end,
    context_to_app_env_vars1(Context, Fun).

context_to_app_env_vars1(
  #{mnesia_dir := MnesiaDir,
    feature_flags_file := FFFile,
    quorum_queue_dir := QuorumQueueDir,
    stream_queue_dir := StreamQueueDir,
    plugins_path := PluginsPath,
    plugins_expand_dir := PluginsExpandDir,
    enabled_plugins_file := EnabledPluginsFile} = Context,
  Fun) ->
    lists:foreach(
      Fun,
      %% Those are all the application environment variables which
      %% were historically set on the erl(1) command line in
      %% rabbitmq-server(8).
      [{kernel, inet_default_connect_options, [{nodelay, true}]},
       {sasl, errlog_type, error},
       {os_mon, start_cpu_sup, false},
       {os_mon, start_disksup, false},
       {os_mon, start_memsup, false},
       {mnesia, dir, MnesiaDir},
       {ra, data_dir, QuorumQueueDir},
       {osiris, data_dir, StreamQueueDir},
       {rabbit, feature_flags_file, FFFile},
       {rabbit, plugins_dir, PluginsPath},
       {rabbit, plugins_expand_dir, PluginsExpandDir},
       {rabbit, enabled_plugins_file, EnabledPluginsFile}]),

    case Context of
        #{erlang_dist_tcp_port := DistTcpPort} ->
            lists:foreach(
              Fun,
              [{kernel, inet_dist_listen_min, DistTcpPort},
               {kernel, inet_dist_listen_max, DistTcpPort}]);
        _ ->
            ok
    end,
    case Context of
        #{amqp_ipaddr := IpAddr,
          amqp_tcp_port := TcpPort}
          when IpAddr /= undefined andalso TcpPort /= undefined ->
            Fun({rabbit, tcp_listeners, [{IpAddr, TcpPort}]});
        _ ->
            ok
    end,
    ok.

context_to_code_path(#{os_type := OSType, plugins_path := PluginsPath}) ->
    Dirs = get_user_lib_dirs(OSType, PluginsPath),
    code:add_pathsa(lists:reverse(Dirs)).

%% -------------------------------------------------------------------
%% Code copied from `kernel/src/code_server.erl`.
%%
%% The goal is to mimic the behavior of the `$ERL_LIBS` environment
%% variable.

get_user_lib_dirs(OSType, Path) ->
    Sep = case OSType of
              {win32, _} -> ";";
              _          -> ":"
          end,
    SplitPath = string:lexemes(Path, Sep),
    get_user_lib_dirs_1(SplitPath).

get_user_lib_dirs_1([Dir|DirList]) ->
    case erl_prim_loader:list_dir(Dir) of
        {ok, Dirs} ->
            Paths = make_path(Dir, Dirs),
            %% Only add paths trailing with ./ebin.
            [P || P <- Paths, filename:basename(P) =:= "ebin"] ++
                get_user_lib_dirs_1(DirList);
        error ->
            get_user_lib_dirs_1(DirList)
    end;
get_user_lib_dirs_1([]) -> [].

%%
%% Create the initial path.
%%
make_path(BundleDir, Bundles0) ->
    Bundles = choose_bundles(Bundles0),
    make_path(BundleDir, Bundles, []).

choose_bundles(Bundles) ->
    ArchiveExt = archive_extension(),
    Bs = lists:sort([create_bundle(B, ArchiveExt) || B <- Bundles]),
    [FullName || {_Name,_NumVsn,FullName} <-
                     choose(lists:reverse(Bs), [], ArchiveExt)].

create_bundle(FullName, ArchiveExt) ->
    BaseName = filename:basename(FullName, ArchiveExt),
    case split_base(BaseName) of
        {Name, VsnStr} ->
            case vsn_to_num(VsnStr) of
                {ok, VsnNum} ->
                    {Name,VsnNum,FullName};
                false ->
                    {FullName,[0],FullName}
            end;
        _ ->
            {FullName,[0],FullName}
    end.

%% Convert "X.Y.Z. ..." to [K, L, M| ...]
vsn_to_num(Vsn) ->
    case is_vsn(Vsn) of
        true ->
            {ok, [list_to_integer(S) || S <- string:lexemes(Vsn, ".")]};
        _  ->
            false
    end.

is_vsn(Str) when is_list(Str) ->
    Vsns = string:lexemes(Str, "."),
    lists:all(fun is_numstr/1, Vsns).

is_numstr(Cs) ->
    lists:all(fun (C) when $0 =< C, C =< $9 -> true;
                  (_)                       -> false
              end, Cs).

choose([{Name,NumVsn,NewFullName}=New|Bs], Acc, ArchiveExt) ->
    case lists:keyfind(Name, 1, Acc) of
        {_, NV, OldFullName} when NV =:= NumVsn ->
            case filename:extension(OldFullName) =:= ArchiveExt of
                false ->
                    choose(Bs,Acc, ArchiveExt);
                true ->
                    Acc2 = lists:keystore(Name, 1, Acc, New),
                    choose(Bs,Acc2, ArchiveExt)
            end;
        {_, _, _} ->
            choose(Bs,Acc, ArchiveExt);
        false ->
            choose(Bs,[{Name,NumVsn,NewFullName}|Acc], ArchiveExt)
    end;
choose([],Acc, _ArchiveExt) ->
    Acc.

make_path(_, [], Res) ->
    Res;
make_path(BundleDir, [Bundle|Tail], Res) ->
    Dir = filename:append(BundleDir, Bundle),
    Ebin = filename:append(Dir, "ebin"),
    %% First try with /ebin
    case is_dir(Ebin) of
        true ->
            make_path(BundleDir, Tail, [Ebin|Res]);
        false ->
            %% Second try with archive
            Ext = archive_extension(),
            Base = filename:basename(Bundle, Ext),
            Ebin2 = filename:join([BundleDir, Base ++ Ext, Base, "ebin"]),
            Ebins =
                case split_base(Base) of
                    {AppName,_} ->
                        Ebin3 = filename:join([BundleDir, Base ++ Ext,
                                               AppName, "ebin"]),
                        [Ebin3, Ebin2, Dir];
                    _ ->
                        [Ebin2, Dir]
                end,
            case try_ebin_dirs(Ebins) of
                {ok,FoundEbin} ->
                    make_path(BundleDir, Tail, [FoundEbin|Res]);
                error ->
                    make_path(BundleDir, Tail, Res)
            end
    end.

try_ebin_dirs([Ebin|Ebins]) ->
    case is_dir(Ebin) of
        true -> {ok,Ebin};
        false -> try_ebin_dirs(Ebins)
    end;
try_ebin_dirs([]) ->
    error.

split_base(BaseName) ->
    case string:lexemes(BaseName, "-") of
        [_, _|_] = Toks ->
            Vsn = lists:last(Toks),
            AllButLast = lists:droplast(Toks),
            {string:join(AllButLast, "-"),Vsn};
        [_|_] ->
            BaseName
    end.

is_dir(Path) ->
    case erl_prim_loader:read_file_info(Path) of
        {ok,#file_info{type=directory}} -> true;
        _ -> false
    end.

archive_extension() ->
    init:archive_extension().

%% -------------------------------------------------------------------
%%
%% RABBITMQ_NODENAME
%%   Erlang node name.
%%   Default: rabbit@<hostname>
%%
%% RABBITMQ_USE_LONGNAME
%%   Flag indicating if long Erlang node names should be used instead
%%   of short ones.
%%   Default: unset (use short names)

nodename_type(Context) ->
    case get_prefixed_env_var("RABBITMQ_USE_LONGNAME") of
        false ->
            update_context(Context, nodename_type, shortnames, default);
        Value ->
            NameType = case value_is_yes(Value) of
                           true  -> longnames;
                           false -> shortnames
                       end,
            update_context(Context, nodename_type, NameType, environment)
    end.

nodename(#{nodename_type := NameType} = Context) ->
    LongHostname = net_adm:localhost(),
    ShortHostname = re:replace(LongHostname, "\\..*$", "", [{return, list}]),
    case get_prefixed_env_var("RABBITMQ_NODENAME") of
        false when NameType =:= shortnames ->
            Nodename = rabbit_nodes_common:make({"rabbit", ShortHostname}),
            update_context(Context, nodename, Nodename, default);
        false when NameType =:= longnames ->
            Nodename = rabbit_nodes_common:make({"rabbit", LongHostname}),
            update_context(Context, nodename, Nodename, default);
        Value ->
            Nodename = case string:find(Value, "@") of
                           nomatch when NameType =:= shortnames ->
                               rabbit_nodes_common:make({Value, ShortHostname});
                           nomatch when NameType =:= longnames ->
                               rabbit_nodes_common:make({Value, LongHostname});
                           _ ->
                               rabbit_nodes_common:make(Value)
                       end,
            update_context(Context, nodename, Nodename, environment)
    end.

split_nodename(#{nodename := Nodename} = Context) ->
    update_context(Context,
                   split_nodename, rabbit_nodes_common:parts(Nodename)).

%% -------------------------------------------------------------------
%%
%% RABBITMQ_CONFIG_FILE
%%   Main configuration file.
%%   Extension is optional. `.config` for the old erlang-term-based
%%   format, `.conf` for the new Cuttlefish-based format.
%%   Default: (Unix) ${SYS_PREFIX}/etc/rabbitmq/rabbitmq
%%         (Windows) ${RABBITMQ_BASE}\rabbitmq
%%
%% RABBITMQ_CONFIG_FILES
%%   Additional configuration files.
%%   If a directory, all files directly inside it are loaded.
%%   If a glob pattern, all matching file are loaded.
%%   Only considered if the main configuration file is Cuttlefish-based.
%%   Default: (Unix) ${SYS_PREFIX}/etc/rabbitmq/conf.d/*.conf
%%         (Windows) ${RABBITMQ_BASE}\conf.d\*.conf
%%
%% RABBITMQ_ADVANCED_CONFIG_FILE
%%   Advanced configuration file.
%%   Erlang-term-based format with a `.config` extension.
%%   Default: (Unix) ${SYS_PREFIX}/etc/rabbitmq/advanced.config
%%         (Windows) ${RABBITMQ_BASE}\advanced.config

config_base_dir(#{os_type := {unix, _},
                  sys_prefix := SysPrefix} = Context) ->
    Dir = filename:join([SysPrefix, "etc", "rabbitmq"]),
    update_context(Context, config_base_dir, Dir);
config_base_dir(#{os_type := {win32, _},
                  rabbitmq_base := Dir} = Context) ->
    update_context(Context, config_base_dir, Dir).

main_config_file(Context) ->
    case get_prefixed_env_var("RABBITMQ_CONFIG_FILE") of
        false ->
            File = get_default_main_config_file(Context),
            update_context(Context, main_config_file, File, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, main_config_file, File, environment)
    end.

get_default_main_config_file(#{config_base_dir := ConfigBaseDir}) ->
    filename:join(ConfigBaseDir, "rabbitmq").

additional_config_files(Context) ->
    case get_prefixed_env_var("RABBITMQ_CONFIG_FILES") of
        false ->
            Pattern = get_default_additional_config_files(Context),
            update_context(
              Context, additional_config_files, Pattern, default);
        Value ->
            Pattern = normalize_path(Value),
            update_context(
              Context, additional_config_files, Pattern, environment)
    end.

get_default_additional_config_files(#{config_base_dir := ConfigBaseDir}) ->
    filename:join([ConfigBaseDir, "conf.d", "*.conf"]).

advanced_config_file(Context) ->
    case get_prefixed_env_var("RABBITMQ_ADVANCED_CONFIG_FILE") of
        false ->
            File = get_default_advanced_config_file(Context),
            update_context(Context, advanced_config_file, File, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, advanced_config_file, File, environment)
    end.

get_default_advanced_config_file(#{config_base_dir := ConfigBaseDir}) ->
    filename:join(ConfigBaseDir, "advanced.config").

%% -------------------------------------------------------------------
%%
%% RABBITMQ_LOG_BASE
%%   Directory to write log files
%%   Default: (Unix) ${SYS_PREFIX}/var/log/rabbitmq
%%         (Windows) ${RABBITMQ_BASE}\log
%%
%% RABBITMQ_LOGS
%%   Main log file
%%   Default: ${RABBITMQ_LOG_BASE}/${RABBITMQ_NODENAME}.log
%%
%% RABBITMQ_UPDATE_LOG
%%   Upgrade-procesure-specific log file
%%   Default: ${RABBITMQ_LOG_BASE}/${RABBITMQ_NODENAME}_upgrade.log
%%
%% RABBITMQ_LOG
%%   Log level; overrides the configuration file value
%%   Default: (undefined)
%%
%% RABBITMQ_DBG
%%   List of `module`, `module:function` or `module:function/arity`
%%   to watch with dbg.
%%   Default: (undefined)

log_levels(Context) ->
    case get_prefixed_env_var("RABBITMQ_LOG") of
        false ->
            update_context(Context, log_levels, undefined, default);
        Value ->
            LogLevels = parse_log_levels(string:lexemes(Value, ","), #{}),
            update_context(Context, log_levels, LogLevels, environment)
    end.

parse_log_levels([CategoryValue | Rest], Result) ->
    case string:lexemes(CategoryValue, "=") of
        ["+color"] ->
            Result1 = Result#{color => true},
            parse_log_levels(Rest, Result1);
        ["-color"] ->
            Result1 = Result#{color => false},
            parse_log_levels(Rest, Result1);
        ["+json"] ->
            Result1 = Result#{json => true},
            parse_log_levels(Rest, Result1);
        ["-json"] ->
            Result1 = Result#{json => false},
            parse_log_levels(Rest, Result1);
        [CategoryOrLevel] ->
            case parse_level(CategoryOrLevel) of
                undefined ->
                    Result1 = Result#{CategoryOrLevel => info},
                    parse_log_levels(Rest, Result1);
                Level ->
                    Result1 = Result#{global => Level},
                    parse_log_levels(Rest, Result1)
            end;
        [Category, Level0] ->
            case parse_level(Level0) of
                undefined ->
                    parse_log_levels(Rest, Result);
                Level ->
                    Result1 = Result#{Category => Level},
                    parse_log_levels(Rest, Result1)
            end
    end;
parse_log_levels([], Result) ->
    Result.

parse_level("debug")     -> debug;
parse_level("info")      -> info;
parse_level("notice")    -> notice;
parse_level("warning")   -> warning;
parse_level("error")     -> error;
parse_level("critical")  -> critical;
parse_level("alert")     -> alert;
parse_level("emergency") -> emergency;
parse_level("none")      -> none;
parse_level(_)           -> undefined.

log_base_dir(#{os_type := OSType} = Context) ->
    case {get_prefixed_env_var("RABBITMQ_LOG_BASE"), OSType} of
        {false, {unix, _}} ->
            #{sys_prefix := SysPrefix} = Context,
            Dir = filename:join([SysPrefix, "var", "log", "rabbitmq"]),
            update_context(Context, log_base_dir, Dir, default);
        {false, {win32, _}} ->
            #{rabbitmq_base := RabbitmqBase} = Context,
            Dir = filename:join([RabbitmqBase, "log"]),
            update_context(Context, log_base_dir, Dir, default);
        {Value, _} ->
            Dir = normalize_path(Value),
            update_context(Context, log_base_dir, Dir, environment)
    end.

main_log_file(#{nodename := Nodename,
                log_base_dir := LogBaseDir} = Context) ->
    case get_prefixed_env_var("RABBITMQ_LOGS") of
        false ->
            File= filename:join(LogBaseDir,
                                atom_to_list(Nodename) ++ ".log"),
            update_context(Context, main_log_file, File, default);
        "-"  = Value ->
            update_context(Context, main_log_file, Value, environment);
        "-stderr" = Value ->
            update_context(Context, main_log_file, Value, environment);
        "exchange:" ++ _ = Value ->
            update_context(Context, main_log_file, Value, environment);
        "syslog:" ++ _ = Value ->
            update_context(Context, main_log_file, Value, environment);
        Value ->
            File = normalize_path(Value),
            update_context(Context, main_log_file, File, environment)
    end.

upgrade_log_file(#{nodename := Nodename,
                   log_base_dir := LogBaseDir} = Context) ->
    case get_prefixed_env_var("RABBITMQ_UPGRADE_LOG") of
        false ->
            File = filename:join(LogBaseDir,
                                 atom_to_list(Nodename) ++ "_upgrade.log"),
            update_context(Context, upgrade_log_file, File, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, upgrade_log_file, File, environment)
    end.

dbg_config() ->
    {Mods, Output} = get_dbg_config(),
    #{dbg_output => Output,
      dbg_mods => Mods}.

dbg_config(Context) ->
    DbgContext = dbg_config(),
    maps:merge(Context, DbgContext).

get_dbg_config() ->
    Output = stdout,
    DbgValue = get_prefixed_env_var("RABBITMQ_DBG"),
    case DbgValue of
        false -> {[], Output};
        _     -> get_dbg_config1(string:lexemes(DbgValue, ","), [], Output)
    end.

get_dbg_config1(["=" ++ Filename | Rest], Mods, _) ->
    get_dbg_config1(Rest, Mods, Filename);
get_dbg_config1([SpecValue | Rest], Mods, Output) ->
    Pattern = "([^:]+)(?::([^/]+)(?:/([0-9]+))?)?",
    Options = [{capture, all_but_first, list}],
    Mods1 = case re:run(SpecValue, Pattern, Options) of
                {match, [M, F, A]} ->
                    Entry = {list_to_atom(M),
                             list_to_atom(F),
                             list_to_integer(A)},
                    [Entry | Mods];
                {match, [M, F]} ->
                    Entry = {list_to_atom(M),
                             list_to_atom(F),
                             '_'},
                    [Entry | Mods];
                {match, [M]} ->
                    Entry = {list_to_atom(M),
                             '_',
                             '_'},
                    [Entry | Mods];
                nomatch ->
                    Mods
            end,
    get_dbg_config1(Rest, Mods1, Output);
get_dbg_config1([], Mods, Output) ->
    {lists:reverse(Mods), Output}.

%% -------------------------------------------------------------------
%%
%% RABBITMQ_MNESIA_BASE
%%   Directory where to create Mnesia directory.
%%   Default: (Unix) ${SYS_PREFIX}/var/lib/rabbitmq/mnesia
%%         (Windows) ${RABBITMQ_BASE}/db
%%
%% RABBITMQ_MNESIA_DIR
%%   Directory where to put Mnesia data.
%%   Default: (Unix) ${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME}
%%         (Windows) ${RABBITMQ_MNESIA_BASE}\${RABBITMQ_NODENAME}-mnesia

mnesia_base_dir(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_MNESIA_BASE") of
        false when Remote =:= offline ->
            update_context(Context, mnesia_base_dir, undefined, default);
        false ->
            mnesia_base_dir_from_node(Context);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, mnesia_base_dir, Dir, environment)
    end;
mnesia_base_dir(Context) ->
    mnesia_base_dir_from_env(Context).

mnesia_base_dir_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_MNESIA_BASE") of
        false ->
            Dir = get_default_mnesia_base_dir(Context),
            update_context(Context, mnesia_base_dir, Dir, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, mnesia_base_dir, Dir, environment)
    end.

mnesia_base_dir_from_node(Context) ->
    %% This variable is used to compute other variables only, we
    %% don't need to know what a remote node used initially. Only the
    %% variables based on it are relevant.
    update_context(Context, mnesia_base_dir, undefined, default).

get_default_mnesia_base_dir(#{data_dir := DataDir} = Context) ->
    Basename = case Context of
                   #{os_type := {unix, _}}  -> "mnesia";
                   #{os_type := {win32, _}} -> "db"
               end,
    filename:join(DataDir, Basename).

mnesia_dir(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_MNESIA_DIR") of
        false when Remote =:= offline ->
            update_context(Context, mnesia_dir, undefined, default);
        false ->
            mnesia_dir_from_node(Context);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, mnesia_dir, Dir, environment)
    end;
mnesia_dir(Context) ->
    mnesia_dir_from_env(Context).

mnesia_dir_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_MNESIA_DIR") of
        false ->
            Dir = get_default_mnesia_dir(Context),
            update_context(Context, mnesia_dir, Dir, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, mnesia_dir, Dir, environment)
    end.

mnesia_dir_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = query_remote(Remote, application, get_env, [mnesia, dir]),
    case Ret of
        {ok, undefined} ->
            throw({query, Remote, {mnesia, dir, undefined}});
        {ok, {ok, Value}} ->
            Dir = normalize_path(Value),
            update_context(Context, mnesia_dir, Dir, remote_node);
        {badrpc, nodedown} ->
            update_context(Context, mnesia_dir, undefined, default)
    end.

get_default_mnesia_dir(#{os_type := {unix, _},
                         nodename := Nodename,
                         mnesia_base_dir := MnesiaBaseDir})
  when MnesiaBaseDir =/= undefined ->
    filename:join(MnesiaBaseDir, atom_to_list(Nodename));
get_default_mnesia_dir(#{os_type := {win32, _},
                         nodename := Nodename,
                         mnesia_base_dir := MnesiaBaseDir})
  when MnesiaBaseDir =/= undefined ->
    filename:join(MnesiaBaseDir, atom_to_list(Nodename) ++ "-mnesia").

%% -------------------------------------------------------------------
%%
%% RABBITMQ_QUORUM_DIR
%%   Directory where to store Ra state for quorum queues.
%%   Default: ${RABBITMQ_MNESIA_DIR}/quorum

quorum_queue_dir(#{mnesia_dir := MnesiaDir} = Context) ->
    case get_prefixed_env_var("RABBITMQ_QUORUM_DIR") of
        false when MnesiaDir =/= undefined ->
            Dir = filename:join(MnesiaDir, "quorum"),
            update_context(Context, quorum_queue_dir, Dir, default);
        false when MnesiaDir =:= undefined ->
            update_context(Context, quorum_queue_dir, undefined, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, quorum_queue_dir, Dir, environment)
    end.

%% -------------------------------------------------------------------
%%
%% RABBITMQ_STREAM_DIR
%%   Directory where to store Ra state for stream queues.
%%   Default: ${RABBITMQ_MNESIA_DIR}/stream

stream_queue_dir(#{mnesia_dir := MnesiaDir} = Context) ->
    case get_prefixed_env_var("RABBITMQ_STREAM_DIR") of
        false when MnesiaDir =/= undefined ->
            Dir = filename:join(MnesiaDir, "stream"),
            update_context(Context, stream_queue_dir, Dir, default);
        false when MnesiaDir =:= undefined ->
            update_context(Context, stream_queue_dir, undefined, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, stream_queue_dir, Dir, environment)
    end.

%% -------------------------------------------------------------------
%%
%% RABBITMQ_PID_FILE
%%   File used to write the Erlang VM OS PID.
%%   Default: ${RABBITMQ_MNESIA_DIR}.pid
%%
%% RABBITMQ_KEEP_PID_FILE_ON_EXIT
%%   Whether to keep or remove the PID file on Erlang VM exit.
%%   Default: true

pid_file(#{mnesia_base_dir := MnesiaBaseDir,
           nodename := Nodename} = Context) ->
    case get_prefixed_env_var("RABBITMQ_PID_FILE") of
        false when MnesiaBaseDir =/= undefined ->
            File = filename:join(MnesiaBaseDir,
                                 atom_to_list(Nodename) ++ ".pid"),
            update_context(Context, pid_file, File, default);
        false when MnesiaBaseDir =:= undefined ->
            update_context(Context, pid_file, undefined, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, pid_file, File, environment)
    end.

keep_pid_file_on_exit(Context) ->
    case get_prefixed_env_var("RABBITMQ_KEEP_PID_FILE_ON_EXIT") of
        false ->
            update_context(Context, keep_pid_file_on_exit, false, default);
        Value ->
            Keep = value_is_yes(Value),
            update_context(Context, keep_pid_file_on_exit, Keep, environment)
    end.

%% -------------------------------------------------------------------
%%
%% RABBITMQ_FEATURE_FLAGS_FILE
%%   File used to store enabled feature flags.
%%   Default: ${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME}-feature_flags

feature_flags_file(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_FEATURE_FLAGS_FILE") of
        false when Remote =:= offline ->
            update_context(Context, feature_flags_file, undefined, default);
        false ->
            feature_flags_file_from_node(Context);
        Value ->
            File = normalize_path(Value),
            update_context(Context, feature_flags_file, File, environment)
    end;
feature_flags_file(Context) ->
    feature_flags_file_from_env(Context).

feature_flags_file_from_env(#{mnesia_base_dir := MnesiaBaseDir,
                              nodename := Nodename} = Context) ->
    case get_env_var("RABBITMQ_FEATURE_FLAGS_FILE") of
        false ->
            File = filename:join(MnesiaBaseDir,
                                 atom_to_list(Nodename) ++ "-feature_flags"),
            update_context(Context, feature_flags_file, File, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, feature_flags_file, File, environment)
    end.

feature_flags_file_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = query_remote(Remote,
                        application, get_env, [rabbit, feature_flags_file]),
    case Ret of
        {ok, undefined} ->
            throw({query, Remote, {rabbit, feature_flags_file, undefined}});
        {ok, {ok, Value}} ->
            File = normalize_path(Value),
            update_context(Context, feature_flags_file, File, remote_node);
        {badrpc, nodedown} ->
            update_context(Context, feature_flags_file, undefined, default)
    end.

forced_feature_flags_on_init(Context) ->
    Value = get_prefixed_env_var("RABBITMQ_FEATURE_FLAGS",
                                 [keep_empty_string_as_is]),
    case Value of
        false ->
            %% get_prefixed_env_var() considers an empty string
            %% is the same as an undefined environment variable.
            update_context(Context,
                           forced_feature_flags_on_init, undefined, default);
        _ ->
            Flags = [list_to_atom(V) || V <- string:lexemes(Value, ",")],
            update_context(Context,
                           forced_feature_flags_on_init, Flags, environment)
    end.

log_feature_flags_registry(Context) ->
    case get_prefixed_env_var("RABBITMQ_LOG_FF_REGISTRY") of
        false ->
            update_context(Context,
                           log_feature_flags_registry, false, default);
        Value ->
            Log = value_is_yes(Value),
            update_context(Context,
                           log_feature_flags_registry, Log, environment)
    end.

%% -------------------------------------------------------------------
%%
%% RABBITMQ_PLUGINS_DIR
%%   List of directories where to look for plugins.
%%   Directories are separated by:
%%     ':' on Unix
%%     ';' on Windows
%%   Default: ${RABBITMQ_HOME}/plugins
%%
%% RABBITMQ_PLUGINS_EXPAND_DIR
%%   Directory where to expand plugin archives.
%%   Default: ${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME}-plugins-expand
%%
%% RABBITMQ_ENABLED_PLUGINS_FILE
%%   File where the list of enabled plugins is stored.
%%   Default: (Unix) ${SYS_PREFIX}/etc/rabbitmq/enabled_plugins
%%         (Windows) ${RABBITMQ_BASE}\enabled_plugins
%%
%% RABBITMQ_ENABLED_PLUGINS
%%   List of plugins to enable on startup.
%%   Values are:
%%     "ALL" to enable all plugins
%%     "" to enable no plugin
%%     a list of plugin names, separated by a coma (',')
%%   Default: Empty (i.e. use ${RABBITMQ_ENABLED_PLUGINS_FILE})

plugins_path(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_PLUGINS_DIR") of
        false when Remote =:= offline ->
            update_context(Context, plugins_path, undefined, default);
        false ->
            plugins_path_from_node(Context);
        Path ->
            update_context(Context, plugins_path, Path, environment)
    end;
plugins_path(Context) ->
    plugins_path_from_env(Context).

plugins_path_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_PLUGINS_DIR") of
        false ->
            Path = get_default_plugins_path_from_env(Context),
            update_context(Context, plugins_path, Path, default);
        Path ->
            update_context(Context, plugins_path, Path, environment)
    end.

plugins_path_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = query_remote(Remote, application, get_env, [rabbit, plugins_dir]),
    case Ret of
        {ok, undefined} ->
            throw({query, Remote, {rabbit, plugins_dir, undefined}});
        {ok, {ok, Path}} ->
            update_context(Context, plugins_path, Path, remote_node);
        {badrpc, nodedown} ->
            update_context(Context, plugins_path, undefined, default)
    end.

get_default_plugins_path(#{from_remote_node := offline}) ->
    undefined;
get_default_plugins_path(#{from_remote_node := Remote}) ->
    get_default_plugins_path_from_node(Remote);
get_default_plugins_path(Context) ->
    get_default_plugins_path_from_env(Context).

get_default_plugins_path_from_env(#{os_type := OSType}) ->
    ThisModDir = this_module_dir(),
    PluginsDir = rabbit_common_mod_location_to_plugins_dir(ThisModDir),
    case {OSType, PluginsDir} of
        {{unix, _}, "/usr/lib/rabbitmq/" ++ _} ->
            UserPluginsDir = filename:join(
                               ["/", "usr", "lib", "rabbitmq", "plugins"]),
            UserPluginsDir ++ ":" ++ PluginsDir;
        _ ->
            PluginsDir
    end.

get_default_plugins_path_from_node(Remote) ->
    Ret = query_remote(Remote, code, where_is_file, ["rabbit_common.app"]),
    case Ret of
        {ok, non_existing = Error} ->
            throw({query, Remote, {code, where_is_file, Error}});
        {ok, Path} ->
            rabbit_common_mod_location_to_plugins_dir(filename:dirname(Path));
        {badrpc, nodedown} ->
            undefined
    end.

rabbit_common_mod_location_to_plugins_dir(ModDir) ->
    case filename:basename(ModDir) of
        "ebin" ->
            case filelib:is_dir(ModDir) of
                false ->
                    %% rabbit_common in the plugin's .ez archive.
                    filename:dirname(
                      filename:dirname(
                        filename:dirname(ModDir)));
                true ->
                    %% rabbit_common in the plugin's directory.
                    filename:dirname(
                      filename:dirname(ModDir))
            end;
        _ ->
            %% rabbit_common in the CLI escript.
            filename:join(
              filename:dirname(
                filename:dirname(ModDir)),
              "plugins")
    end.

plugins_expand_dir(#{mnesia_base_dir := MnesiaBaseDir,
                     nodename := Nodename} = Context) ->
    case get_prefixed_env_var("RABBITMQ_PLUGINS_EXPAND_DIR") of
        false when MnesiaBaseDir =/= undefined ->
            Dir = filename:join(
                    MnesiaBaseDir,
                    atom_to_list(Nodename) ++ "-plugins-expand"),
            update_context(Context, plugins_expand_dir, Dir, default);
        false when MnesiaBaseDir =:= undefined ->
            update_context(Context, plugins_expand_dir, undefined, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, plugins_expand_dir, Dir, environment)
    end.

enabled_plugins_file(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_ENABLED_PLUGINS_FILE") of
        false when Remote =:= offline ->
            update_context(Context, enabled_plugins_file, undefined, default);
        false ->
            enabled_plugins_file_from_node(Context);
        Value ->
            File = normalize_path(Value),
            update_context(Context, enabled_plugins_file, File, environment)
    end;
enabled_plugins_file(Context) ->
    enabled_plugins_file_from_env(Context).

enabled_plugins_file_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_ENABLED_PLUGINS_FILE") of
        false ->
            File = get_default_enabled_plugins_file(Context),
            update_context(Context, enabled_plugins_file, File, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, enabled_plugins_file, File, environment)
    end.

get_default_enabled_plugins_file(#{config_base_dir := ConfigBaseDir}) ->
    filename:join(ConfigBaseDir, "enabled_plugins").

enabled_plugins_file_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = query_remote(Remote,
                       application, get_env, [rabbit, enabled_plugins_file]),
    case Ret of
        {ok, undefined} ->
            throw({query, Remote, {rabbit, enabled_plugins_file, undefined}});
        {ok, {ok, Value}} ->
            File = normalize_path(Value),
            update_context(Context, enabled_plugins_file, File, remote_node);
        {badrpc, nodedown} ->
            update_context(Context, enabled_plugins_file, undefined, default)
    end.

enabled_plugins(Context) ->
    Value = get_prefixed_env_var(
              "RABBITMQ_ENABLED_PLUGINS",
              [keep_empty_string_as_is]),
    case Value of
        false ->
            update_context(Context, enabled_plugins, undefined, default);
        "ALL" ->
            update_context(Context, enabled_plugins, all, environment);
        "" ->
            update_context(Context, enabled_plugins, [], environment);
        _ ->
            Plugins = [list_to_atom(P) || P <- string:lexemes(Value, ",")],
            update_context(Context, enabled_plugins, Plugins, environment)
    end.

%% -------------------------------------------------------------------
%%
%% RABBITMQ_NODE_IP_ADDRESS
%%   AMQP TCP IP address to listen on
%%   Default: unset (i.e. listen on all interfaces)
%%
%% RABBITMQ_NODE_PORT
%%   AMQP TCP port.
%%   Default: 5672
%%
%% RABBITMQ_DIST_PORT
%%   Erlang distribution TCP port.
%%   Default: ${RABBITMQ_NODE_PORT} + 20000

amqp_ipaddr(Context) ->
    case get_prefixed_env_var("RABBITMQ_NODE_IP_ADDRESS") of
        false ->
            update_context(Context, amqp_ipaddr, "auto", default);
        Value ->
            update_context(Context, amqp_ipaddr, Value, environment)
    end.

amqp_tcp_port(Context) ->
    case get_prefixed_env_var("RABBITMQ_NODE_PORT") of
        false ->
            update_context(Context, amqp_tcp_port, 5672, default);
        TcpPortStr ->
            try
                TcpPort = erlang:list_to_integer(TcpPortStr),
                update_context(Context, amqp_tcp_port, TcpPort, environment)
            catch
                _:badarg ->
                    ?LOG_ERROR(
                       "Invalid value for $RABBITMQ_NODE_PORT: ~p",
                       [TcpPortStr],
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    throw({exit, ex_config})
            end
    end.

erlang_dist_tcp_port(#{amqp_tcp_port := AmqpTcpPort} = Context) ->
    case get_prefixed_env_var("RABBITMQ_DIST_PORT") of
        false ->
            TcpPort = AmqpTcpPort + 20000,
            update_context(Context, erlang_dist_tcp_port, TcpPort, default);
        TcpPortStr ->
            try
                TcpPort = erlang:list_to_integer(TcpPortStr),
                update_context(Context,
                               erlang_dist_tcp_port, TcpPort, environment)
            catch
                _:badarg ->
                    ?LOG_ERROR(
                       "Invalid value for $RABBITMQ_DIST_PORT: ~p",
                       [TcpPortStr],
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    throw({exit, ex_config})
            end
    end.

%% -------------------------------------------------------------------
%%
%% SYS_PREFIX [Unix only]
%%   Default: ""
%%
%% RABBITMQ_BASE [Windows only]
%%   Directory where to put RabbitMQ data.
%%   Default: !APPDATA!\RabbitMQ

sys_prefix(#{os_type := {unix, _}} = Context) ->
    case get_env_var("SYS_PREFIX") of
        false ->
            update_context(Context, sys_prefix, "", default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, sys_prefix, Dir, environment)
    end;
sys_prefix(Context) ->
    Context.

rabbitmq_base(#{os_type := {win32, _}} = Context) ->
    case get_env_var("RABBITMQ_BASE") of
        false ->
            AppData = normalize_path(get_env_var("APPDATA")),
            Dir = filename:join(AppData, "RabbitMQ"),
            update_context(Context, rabbitmq_base, Dir, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, rabbitmq_base, Dir, environment)
    end;
rabbitmq_base(Context) ->
    Context.

data_dir(#{os_type := {unix, _},
           sys_prefix := SysPrefix} = Context) ->
    Dir = filename:join([SysPrefix, "var", "lib", "rabbitmq"]),
    update_context(Context, data_dir, Dir);
data_dir(#{os_type := {win32, _},
           rabbitmq_base := RabbitmqBase} = Context) ->
    update_context(Context, data_dir, RabbitmqBase).

rabbitmq_home(Context) ->
    case get_env_var("RABBITMQ_HOME") of
        false ->
            Dir = filename:dirname(get_default_plugins_path(Context)),
            update_context(Context, rabbitmq_home, Dir, default);
        Value ->
            Dir = normalize_path(Value),
            update_context(Context, rabbitmq_home, Dir, environment)
    end.

%% -------------------------------------------------------------------
%%
%%  RABBITMQ_ALLOW_INPUT
%%    Indicate if an Erlang shell is started or not.
%%    Default: false

interactive_shell(Context) ->
    case get_env_var("RABBITMQ_ALLOW_INPUT") of
        false ->
            update_context(Context,
                           interactive_shell, false, default);
        Value ->
            update_context(Context,
                           interactive_shell, value_is_yes(Value), environment)
    end.

%% FIXME: We would need a way to call isatty(3) to make sure the output
%% is a terminal.
output_supports_colors(#{os_type := {unix, _}} = Context) ->
    update_context(Context, output_supports_colors, true, default);
output_supports_colors(#{os_type := {win32, _}} = Context) ->
    update_context(Context, output_supports_colors, false, default).

%% -------------------------------------------------------------------
%%
%% RABBITMQ_PRODUCT_NAME
%%   Override the product name
%%   Default: unset (i.e. "RabbitMQ")
%%
%% RABBITMQ_PRODUCT_VERSION
%%   Override the product version
%%   Default: unset (i.e. `rabbit` application version).
%%
%% RABBITMQ_MOTD_FILE
%%   Indicate a filename containing a "message of the day" to add to
%%   the banners, both the logged and the printed ones.
%%   Default: (Unix) ${SYS_PREFIX}/etc/rabbitmq/motd
%%         (Windows) ${RABBITMQ_BASE}\motd.txt

product_name(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_PRODUCT_NAME") of
        false when Remote =:= offline ->
            update_context(Context, product_name, undefined, default);
        false ->
            product_name_from_node(Context);
        Value ->
            update_context(Context, product_name, Value, environment)
    end;
product_name(Context) ->
    product_name_from_env(Context).

product_name_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_PRODUCT_NAME") of
        false ->
            update_context(Context, product_name, undefined, default);
        Value ->
            update_context(Context, product_name, Value, environment)
    end.

product_name_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = (catch query_remote(Remote, rabbit, product_name, [])),
    case Ret of
        {badrpc, nodedown} ->
            update_context(Context, product_name, undefined, default);
        {query, _, _} ->
            update_context(Context, product_name, undefined, default);
        Value  ->
            update_context(Context, product_name, Value, remote_node)
    end.

product_version(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_PRODUCT_VERSION") of
        false when Remote =:= offline ->
            update_context(Context, product_version, undefined, default);
        false ->
            product_version_from_node(Context);
        Value ->
            update_context(Context, product_version, Value, environment)
    end;
product_version(Context) ->
    product_version_from_env(Context).

product_version_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_PRODUCT_VERSION") of
        false ->
            update_context(Context, product_version, undefined, default);
        Value ->
            update_context(Context, product_version, Value, environment)
    end.

product_version_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = (catch query_remote(Remote, rabbit, product_version, [])),
    case Ret of
        {badrpc, _} ->
            update_context(Context, product_version, undefined, default);
        {query, _, _} ->
            update_context(Context, product_version, undefined, default);
        Value ->
            update_context(Context, product_version, Value, remote_node)
    end.

motd_file(#{from_remote_node := Remote} = Context) ->
    case get_prefixed_env_var("RABBITMQ_MOTD_FILE") of
        false when Remote =:= offline ->
            update_context(Context, motd_file, undefined, default);
        false ->
            motd_file_from_node(Context);
        Value ->
            File = normalize_path(Value),
            update_context(Context, motd_file, File, environment)
    end;
motd_file(Context) ->
    motd_file_from_env(Context).

motd_file_from_env(Context) ->
    case get_prefixed_env_var("RABBITMQ_MOTD_FILE") of
        false ->
            File = get_default_motd_file(Context),
            update_context(Context, motd_file, File, default);
        Value ->
            File = normalize_path(Value),
            update_context(Context, motd_file, File, environment)
    end.

get_default_motd_file(#{os_type := {unix, _},
                        config_base_dir := ConfigBaseDir}) ->
    filename:join(ConfigBaseDir, "motd");
get_default_motd_file(#{os_type := {win32, _},
                        config_base_dir := ConfigBaseDir}) ->
    filename:join(ConfigBaseDir, "motd.txt").

motd_file_from_node(#{from_remote_node := Remote} = Context) ->
    Ret = (catch query_remote(Remote, rabbit, motd_file, [])),
    case Ret of
        {badrpc, _} ->
            update_context(Context, motd_file, undefined, default);
        {query, _, _} ->
            update_context(Context, motd_file, undefined, default);
        File ->
            update_context(Context, motd_file, File, remote_node)
    end.

%% -------------------------------------------------------------------
%% Loading of rabbitmq-env.conf.
%% -------------------------------------------------------------------

load_conf_env_file(#{os_type := {unix, _},
                     sys_prefix := SysPrefix} = Context) ->
    {ConfEnvFile, Origin} =
    case get_prefixed_env_var("RABBITMQ_CONF_ENV_FILE") of
        false ->
            File = filename:join(
                     [SysPrefix, "etc", "rabbitmq", "rabbitmq-env.conf"]),
            {File, default};
        Value ->
            {normalize_path(Value), environment}
    end,
    Context1 = update_context(Context, conf_env_file, ConfEnvFile, Origin),
    case loading_conf_env_file_enabled(Context1) of
        true ->
            case filelib:is_regular(ConfEnvFile) of
                false ->
                    ?LOG_DEBUG(
                       "No $RABBITMQ_CONF_ENV_FILE (~ts)", [ConfEnvFile],
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    Context1;
                true ->
                    case os:find_executable("sh") of
                        false -> Context1;
                        Sh    -> do_load_conf_env_file(Context1,
                                                       Sh,
                                                       ConfEnvFile)
                    end
            end;
        false ->
            ?LOG_DEBUG(
               "Loading of $RABBITMQ_CONF_ENV_FILE (~ts) is disabled",
               [ConfEnvFile],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            Context1
    end;
load_conf_env_file(#{os_type := {win32, _},
                     rabbitmq_base := RabbitmqBase} = Context) ->
    {ConfEnvFile, Origin} =
    case get_prefixed_env_var("RABBITMQ_CONF_ENV_FILE") of
        false ->
            File = filename:join([RabbitmqBase, "rabbitmq-env-conf.bat"]),
            {File, default};
        Value ->
            {normalize_path(Value), environment}
    end,
    Context1 = update_context(Context, conf_env_file, ConfEnvFile, Origin),
    case loading_conf_env_file_enabled(Context1) of
        true ->
            case filelib:is_regular(ConfEnvFile) of
                false ->
                    ?LOG_DEBUG(
                       "No $RABBITMQ_CONF_ENV_FILE (~ts)", [ConfEnvFile],
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    Context1;
                true ->
                    case os:find_executable("cmd.exe") of
                        false ->
                            Cmd = os:getenv("ComSpec"),
                            CmdExists =
                              Cmd =/= false andalso
                              filelib:is_regular(Cmd),
                            case CmdExists of
                                false -> Context1;
                                true  -> do_load_conf_env_file(Context1,
                                                               Cmd,
                                                               ConfEnvFile)
                            end;
                        Cmd ->
                            do_load_conf_env_file(Context1, Cmd, ConfEnvFile)
                    end
            end;
        false ->
            ?LOG_DEBUG(
               "Loading of $RABBITMQ_CONF_ENV_FILE (~ts) is disabled",
               [ConfEnvFile],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            Context1
    end;
load_conf_env_file(Context) ->
    Context.

-spec loading_conf_env_file_enabled(map()) -> boolean().

-ifdef(TEST).
loading_conf_env_file_enabled(_) ->
    persistent_term:get({?MODULE, load_conf_env_file}, true).
-else.
loading_conf_env_file_enabled(_) ->
    %% When this module is built without `TEST` defined, we want this
    %% function to always return true. However, this makes Dialyzer
    %% think it can only return true: this is not the case when the
    %% module is compiled with `TEST` defined. The following line is
    %% here to trick Dialyzer.
    erlang:get({?MODULE, always_undefined}) =:= undefined.
-endif.

do_load_conf_env_file(#{os_type := {unix, _}} = Context, Sh, ConfEnvFile) ->
    ?LOG_DEBUG(
       "Sourcing $RABBITMQ_CONF_ENV_FILE: ~ts", [ConfEnvFile],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    %% The script below sources the `CONF_ENV_FILE` file, then it shows a
    %% marker line and all environment variables.
    %%
    %% The marker line is useful to distinguish any output from the sourced
    %% script from the variables we are interested in.
    Marker = vars_list_marker(),
    Script = rabbit_misc:format(
               ". \"~ts\" && "
               "echo \"~s\" && "
               "set", [ConfEnvFile, Marker]),

    #{sys_prefix := SysPrefix,
      rabbitmq_home := RabbitmqHome} = Context,
    MainConfigFile = re:replace(
                       get_default_main_config_file(Context),
                       "\\.(conf|config)$", "", [{return, list}]),

    %% The variables below are those the `CONF_ENV_FILE` file can expect.
    Env = [
           {"SYS_PREFIX", SysPrefix},
           {"RABBITMQ_HOME", RabbitmqHome},
           {"CONFIG_FILE", MainConfigFile},
           {"ADVANCED_CONFIG_FILE", get_default_advanced_config_file(Context)},
           {"MNESIA_BASE", get_default_mnesia_base_dir(Context)},
           {"ENABLED_PLUGINS_FILE", get_default_enabled_plugins_file(Context)},
           {"PLUGINS_DIR", get_default_plugins_path_from_env(Context)},
           {"CONF_ENV_FILE_PHASE", "rabbtimq-prelaunch"}
          ],

    Args = ["-ex", "-c", Script],
    Opts = [{args, Args},
            {env, Env},
            binary,
            use_stdio,
            stderr_to_stdout,
            exit_status],
    Port = erlang:open_port({spawn_executable, Sh}, Opts),
    collect_conf_env_file_output(Context, Port, Marker, <<>>);
do_load_conf_env_file(#{os_type := {win32, _}} = Context, Cmd, ConfEnvFile) ->
    %% rabbitmq/rabbitmq-common#392
    ?LOG_DEBUG(
       "Executing $RABBITMQ_CONF_ENV_FILE: ~ts", [ConfEnvFile],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    %% The script below executes the `CONF_ENV_FILE` file, then it shows a
    %% marker line and all environment variables.
    %%
    %% The marker line is useful to distinguish any output from the sourced
    %% script from the variables we are interested in.
    %%
    %% Arguments are split into a list of strings to support a filename with
    %% whitespaces in the path.
    Marker = vars_list_marker(),
    Script = [ConfEnvFile, "&&",
              "echo", Marker, "&&",
              "set"],

    #{rabbitmq_base := RabbitmqBase,
      rabbitmq_home := RabbitmqHome} = Context,
    MainConfigFile = re:replace(
                       get_default_main_config_file(Context),
                       "\\.(conf|config)$", "", [{return, list}]),

    %% The variables below are those the `CONF_ENV_FILE` file can expect.
    Env = [
           {"RABBITMQ_BASE", RabbitmqBase},
           {"RABBITMQ_HOME", RabbitmqHome},
           {"CONFIG_FILE", MainConfigFile},
           {"ADVANCED_CONFIG_FILE", get_default_advanced_config_file(Context)},
           {"MNESIA_BASE", get_default_mnesia_base_dir(Context)},
           {"ENABLED_PLUGINS_FILE", get_default_enabled_plugins_file(Context)},
           {"PLUGINS_DIR", get_default_plugins_path_from_env(Context)},
           {"CONF_ENV_FILE_PHASE", "rabbtimq-prelaunch"}
          ],

    Args = ["/Q", "/C" | Script],
    Opts = [{args, Args},
            {env, Env},
            hide,
            binary,
            stderr_to_stdout,
            exit_status],
    Port = erlang:open_port({spawn_executable, Cmd}, Opts),
    collect_conf_env_file_output(Context, Port, "\"" ++ Marker ++ "\" ", <<>>).

vars_list_marker() ->
    rabbit_misc:format(
      "-----BEGIN VARS LIST FOR PID ~s-----", [os:getpid()]).

collect_conf_env_file_output(Context, Port, Marker, Output) ->
    receive
        {Port, {exit_status, ExitStatus}} ->
            Lines = post_port_cmd_output(Context, Output, ExitStatus),
            case ExitStatus of
                0 -> parse_conf_env_file_output(Context, Marker, Lines);
                _ -> Context
            end;
        {Port, {data, Chunk}} ->
            collect_conf_env_file_output(
              Context, Port, Marker, [Output, Chunk])
    end.

post_port_cmd_output(#{os_type := {OSType, _}}, Output, ExitStatus) ->
    ?LOG_DEBUG(
       "$RABBITMQ_CONF_ENV_FILE exit status: ~b",
       [ExitStatus],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    DecodedOutput = unicode:characters_to_list(Output),
    LineSep = case OSType of
                  win32 -> "\r\n";
                  _     -> "\n"
              end,
    Lines = string:split(string:trim(DecodedOutput), LineSep, all),
    ?LOG_DEBUG(
       "$RABBITMQ_CONF_ENV_FILE output:~n~ts",
       [string:join([io_lib:format("  ~ts", [Line]) || Line <- Lines], "\n")],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    Lines.

parse_conf_env_file_output(Context, _, []) ->
    Context;
parse_conf_env_file_output(Context, Marker, [Marker | Lines]) ->
    %% Found our marker, let's parse variables.
    parse_conf_env_file_output1(Context, Lines);
parse_conf_env_file_output(Context, Marker, [_ | Lines]) ->
    parse_conf_env_file_output(Context, Marker, Lines).

parse_conf_env_file_output1(Context, Lines) ->
    Vars = parse_conf_env_file_output2(Lines, #{}),
    %% Re-export variables.
    lists:foreach(
      fun(Var) ->
              IsUsed = var_is_used(Var),
              IsSet = var_is_set(Var),
              case IsUsed andalso not IsSet of
                  true ->
                      ?LOG_DEBUG(
                         "$RABBITMQ_CONF_ENV_FILE: re-exporting variable $~s",
                         [Var],
                         #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                      os:putenv(Var, maps:get(Var, Vars));
                  false ->
                      ok
              end
      end, lists:sort(maps:keys(Vars))),
    Context.

parse_conf_env_file_output2([], Vars) ->
    Vars;
parse_conf_env_file_output2([Line | Lines], Vars) ->
    SetXOutput = is_sh_set_x_output(Line),
    ShFunction = is_sh_function(Line, Lines),
    if
        SetXOutput ->
            parse_conf_env_file_output2(Lines, Vars);
        ShFunction ->
            skip_sh_function(Lines, Vars);
        true ->
            case string:split(Line, "=") of
                [Var, IncompleteValue] ->
                    {Value, Lines1} = parse_sh_literal(IncompleteValue, Lines, ""),
                    Vars1 = Vars#{Var => Value},
                    parse_conf_env_file_output2(Lines1, Vars1);
                _ ->
                    %% Parsing failed somehow.
                    ?LOG_WARNING(
                       "Failed to parse $RABBITMQ_CONF_ENV_FILE output: ~p",
                       [Line],
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    #{}
            end
    end.

is_sh_set_x_output(Line) ->
    re:run(Line, "^\\++ ", [{capture, none}]) =:= match.

is_sh_function(_, []) ->
    false;
is_sh_function(Line, Lines) ->
    re:run(Line, "\\s\\(\\)\\s*$", [{capture, none}]) =:= match
    andalso
    re:run(hd(Lines), "^\\s*\\{\\s*$", [{capture, none}]) =:= match.

parse_sh_literal([$' | SingleQuoted], Lines, Literal) ->
    parse_single_quoted_literal(SingleQuoted, Lines, Literal);
parse_sh_literal([$" | DoubleQuoted], Lines, Literal) ->
    parse_double_quoted_literal(DoubleQuoted, Lines, Literal);
parse_sh_literal([$$, $' | DollarSingleQuoted], Lines, Literal) ->
    parse_dollar_single_quoted_literal(DollarSingleQuoted, Lines, Literal);
parse_sh_literal([], Lines, Literal) ->
    %% We reached the end of the literal.
    {lists:reverse(Literal), Lines};
parse_sh_literal(Unquoted, Lines, Literal) ->
    parse_unquoted_literal(Unquoted, Lines, Literal).

parse_unquoted_literal([$\\], [Line | Lines], Literal) ->
    %% The newline character is escaped: it means line continuation.
    parse_unquoted_literal(Line, Lines, Literal);
parse_unquoted_literal([$\\, C | Rest], Lines, Literal) ->
    %% This is an escaped character, so we "eat" the two characters but append
    %% only the escaped one.
    parse_unquoted_literal(Rest, Lines, [C | Literal]);
parse_unquoted_literal([C | _] = Rest, Lines, Literal)
  when C =:= $' orelse C =:= $" ->
    %% We reached the end of the unquoted literal and the beginning of a quoted
    %% literal. Both are concatenated.
    parse_sh_literal(Rest, Lines, Literal);
parse_unquoted_literal([C | Rest], Lines, Literal) ->
    parse_unquoted_literal(Rest, Lines, [C | Literal]);
parse_unquoted_literal([], Lines, Literal) ->
    %% We reached the end of the unquoted literal.
    parse_sh_literal([], Lines, Literal).

parse_single_quoted_literal([$' | Rest], Lines, Literal) ->
    %% We reached the closing single quote.
    parse_sh_literal(Rest, Lines, Literal);
parse_single_quoted_literal([], [Line | Lines], Literal) ->
    %% We reached the end of line before finding the closing single
    %% quote. The literal continues on the next line and includes that
    %% newline character.
    parse_single_quoted_literal(Line, Lines, [$\n | Literal]);
parse_single_quoted_literal([C | Rest], Lines, Literal) ->
    parse_single_quoted_literal(Rest, Lines, [C | Literal]).

parse_double_quoted_literal([$\\], [Line | Lines], Literal) ->
    %% The newline character is escaped: it means line continuation.
    parse_double_quoted_literal(Line, Lines, Literal);
parse_double_quoted_literal([$\\, C | Rest], Lines, Literal)
  when C =:= $$ orelse C =:= $` orelse C =:= $" orelse C =:= $\\ ->
    %% This is an escaped character, so we "eat" the two characters but append
    %% only the escaped one.
    parse_double_quoted_literal(Rest, Lines, [C | Literal]);
parse_double_quoted_literal([$" | Rest], Lines, Literal) ->
    %% We reached the closing double quote.
    parse_sh_literal(Rest, Lines, Literal);
parse_double_quoted_literal([], [Line | Lines], Literal) ->
    %% We reached the end of line before finding the closing double
    %% quote. The literal continues on the next line and includes that
    %% newline character.
    parse_double_quoted_literal(Line, Lines, [$\n | Literal]);
parse_double_quoted_literal([C | Rest], Lines, Literal) ->
    parse_double_quoted_literal(Rest, Lines, [C | Literal]).

-define(IS_OCTAL(C), C >= $0 andalso C < $8).
-define(IS_HEX(C),
        (C >= $0 andalso C =< $9) orelse
        (C >= $a andalso C =< $f) orelse
        (C >= $A andalso C =< $F)).

parse_dollar_single_quoted_literal([$\\, C1, C2, C3 | Rest], Lines, Literal)
  when ?IS_OCTAL(C1) andalso ?IS_OCTAL(C2) andalso ?IS_OCTAL(C3) ->
    %% An octal-based escaped character.
    C = octal_to_character([C1, C2, C3]),
    parse_dollar_single_quoted_literal(Rest, Lines, [C | Literal]);
parse_dollar_single_quoted_literal([$\\, $x, C1, C2 | Rest], Lines, Literal)
  when ?IS_HEX(C1) andalso ?IS_HEX(C2) ->
    %% A hex-based escaped character.
    C = hex_to_character([C1, C2]),
    parse_dollar_single_quoted_literal(Rest, Lines, [C | Literal]);
parse_dollar_single_quoted_literal([$\\, $u,
                                    C1, C2, C3, C4 | Rest],
                                   Lines, Literal)
  when ?IS_HEX(C1) andalso ?IS_HEX(C2) andalso
       ?IS_HEX(C3) andalso ?IS_HEX(C4) ->
    %% A hex-based escaped character.
    C = hex_to_character([C1, C2, C3, C4]),
    parse_dollar_single_quoted_literal(Rest, Lines, [C | Literal]);
parse_dollar_single_quoted_literal([$\\, $U,
                                    C1, C2, C3, C4,
                                    C5, C6, C7, C8 | Rest],
                                   Lines, Literal)
  when ?IS_HEX(C1) andalso ?IS_HEX(C2) andalso
       ?IS_HEX(C3) andalso ?IS_HEX(C4) andalso
       ?IS_HEX(C5) andalso ?IS_HEX(C6) andalso
       ?IS_HEX(C7) andalso ?IS_HEX(C8) ->
    %% A hex-based escaped character.
    C = hex_to_character([C1, C2, C3, C4, C5, C6, C7, C8]),
    parse_dollar_single_quoted_literal(Rest, Lines, [C | Literal]);
parse_dollar_single_quoted_literal([$\\, C1 | Rest], Lines, Literal)
  when C1 =:= $a orelse
       C1 =:= $b orelse
       C1 =:= $e orelse
       C1 =:= $E orelse
       C1 =:= $f orelse
       C1 =:= $n orelse
       C1 =:= $r orelse
       C1 =:= $t orelse
       C1 =:= $v orelse
       C1 =:= $\\ orelse
       C1 =:= $' orelse
       C1 =:= $" orelse
       C1 =:= $? ->
    %% This is an escaped character, so we "eat" the two characters but append
    %% only the escaped one.
    C = esc_to_character(C1),
    parse_dollar_single_quoted_literal(Rest, Lines, [C | Literal]);
parse_dollar_single_quoted_literal([$'], Lines, Literal) ->
    %% We reached the closing single quote.
    {lists:reverse(Literal), Lines};
parse_dollar_single_quoted_literal([], [Line | Lines], Literal) ->
    %% We reached the end of line before finding the closing single
    %% quote. The literal continues on the next line and includes that
    %% newline character.
    parse_dollar_single_quoted_literal(Line, Lines, [$\n | Literal]);
parse_dollar_single_quoted_literal([C | Rest], Lines, Literal) ->
    parse_dollar_single_quoted_literal(Rest, Lines, [C | Literal]).

octal_to_character(List) ->
    octal_to_character(List, 0).

octal_to_character([D | Rest], C) when ?IS_OCTAL(D) ->
    octal_to_character(Rest, C * 8 + D - $0);
octal_to_character([], C) ->
    C.

hex_to_character(List) ->
    hex_to_character(List, 0).

hex_to_character([D | Rest], C) ->
    hex_to_character(Rest, C * 16 + hex_to_int(D));
hex_to_character([], C) ->
    C.

hex_to_int(C) when C >= $0 andalso C =< $9 -> C - $0;
hex_to_int(C) when C >= $a andalso C =< $f -> 10 + C - $a;
hex_to_int(C) when C >= $A andalso C =< $F -> 10 + C - $A.

esc_to_character($a) -> 7;   % Bell
esc_to_character($b) -> 8;   % Backspace
esc_to_character($e) -> 27;  % Esc
esc_to_character($E) -> 27;  % Esc
esc_to_character($f) -> 12;  % Form feed
esc_to_character($n) -> $\n; % Newline
esc_to_character($r) -> 13;  % Carriage return
esc_to_character($t) -> 9;   % Horizontal tab
esc_to_character($v) -> 11;  % Vertical tab
esc_to_character(C)  -> C.

skip_sh_function(["}" | Lines], Vars) ->
    parse_conf_env_file_output2(Lines, Vars);
skip_sh_function([_ | Lines], Vars) ->
    skip_sh_function(Lines, Vars).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

get_env_var(VarName) ->
    get_env_var(VarName, []).

get_env_var(VarName, Options) ->
    KeepEmptyString = lists:member(keep_empty_string_as_is, Options),
    case os:getenv(VarName) of
        false                       -> false;
        "" when not KeepEmptyString -> false;
        Value                       -> Value
    end.

get_prefixed_env_var(VarName) ->
    get_prefixed_env_var(VarName, []).

get_prefixed_env_var("RABBITMQ_" ++ Suffix = VarName,
                     Options) ->
    case get_env_var(VarName, Options) of
        false -> get_env_var(Suffix, Options);
        Value -> Value
    end.

var_is_used("RABBITMQ_" ++ _ = PrefixedVar) ->
    lists:member(PrefixedVar, ?USED_ENV_VARS);
var_is_used("HOME") ->
    false;
var_is_used(Var) ->
    lists:member("RABBITMQ_" ++ Var, ?USED_ENV_VARS).

%% The $RABBITMQ_* variables have precedence over their un-prefixed equivalent.
%% Therefore, when we check if $RABBITMQ_* is set, we only look at this
%% variable. However, when we check if an un-prefixed variable is set, we first
%% look at its $RABBITMQ_* variant.
var_is_set("RABBITMQ_" ++ _ = PrefixedVar) ->
    os:getenv(PrefixedVar) /= false;
var_is_set(Var) ->
    os:getenv("RABBITMQ_" ++ Var) /= false orelse
    os:getenv(Var) /= false.

value_is_yes(Value) when is_list(Value) orelse is_binary(Value) ->
    Options = [{capture, none}, caseless],
    re:run(string:trim(Value), "^(1|yes|true)$", Options) =:= match;
value_is_yes(_) ->
    false.

normalize_path("" = Path) ->
    Path;
normalize_path(Path) ->
    filename:join(filename:split(Path)).

this_module_dir() ->
    File = code:which(?MODULE),
    %% Possible locations:
    %%   - the rabbit_common plugin (as an .ez archive):
    %%     .../plugins/rabbit_common-$version.ez/rabbit_common-$version/ebin
    %%   - the rabbit_common plugin (as a directory):
    %%     .../plugins/rabbit_common-$version/ebin
    %%   - the CLI:
    %%     .../escript/$cli
    filename:dirname(File).

maybe_setup_dist_for_remote_query(
  #{from_remote_node := offline} = Context) ->
    Context;
maybe_setup_dist_for_remote_query(
  #{from_remote_node := {RemoteNode, _}} = Context) ->
    {NamePart, HostPart} = rabbit_nodes_common:parts(RemoteNode),
    NameType = rabbit_nodes_common:name_type(RemoteNode),
    ok = rabbit_nodes_common:ensure_epmd(),
    Context1 = setup_dist_for_remote_query(
                 Context, NamePart, HostPart, NameType, 50),
    case is_rabbitmq_loaded_on_remote_node(Context1) of
        true  -> Context1;
        false -> maybe_stop_dist_for_remote_query(
                   update_context(Context, from_remote_node, offline))
    end;
maybe_setup_dist_for_remote_query(Context) ->
    Context.

setup_dist_for_remote_query(
  #{dist_started_for_remote_query := true} = Context,
  _, _, _, _) ->
    Context;
setup_dist_for_remote_query(Context, _, _, _, 0) ->
    Context;
setup_dist_for_remote_query(#{from_remote_node := {Remote, _}} = Context,
                            NamePart, HostPart, NameType,
                            Attempts) ->
    RndNamePart = NamePart ++ "_ctl_" ++ integer_to_list(rand:uniform(100)),
    Nodename = rabbit_nodes_common:make({RndNamePart, HostPart}),
    case net_kernel:start([Nodename, NameType]) of
        {ok, _} ->
            update_context(Context, dist_started_for_remote_query, true);
        {error, {already_started, _}} ->
            Context;
        {error, {{already_started, _}, _}} ->
            Context;
        Error ->
            logger:error(
              "rabbit_env: Failed to setup distribution (as ~s) to "
              "query node ~s: ~p",
              [Nodename, Remote, Error]),
            setup_dist_for_remote_query(Context,
                                        NamePart, HostPart, NameType,
                                        Attempts - 1)
    end.

is_rabbitmq_loaded_on_remote_node(
  #{from_remote_node := Remote}) ->
    case query_remote(Remote, application, loaded_applications, []) of
        {ok, Apps} ->
            lists:keymember(mnesia, 1, Apps) andalso
            lists:keymember(rabbit, 1, Apps);
        _ ->
            false
    end.

maybe_stop_dist_for_remote_query(
  #{dist_started_for_remote_query := true} = Context) ->
    net_kernel:stop(),
    maps:remove(dist_started_for_remote_query, Context);
maybe_stop_dist_for_remote_query(Context) ->
    Context.

query_remote({RemoteNode, Timeout}, Mod, Func, Args) ->
    Ret = rpc:call(RemoteNode, Mod, Func, Args, Timeout),
    case Ret of
        {badrpc, nodedown} = Error -> Error;
        {badrpc, _} = Error        -> throw({query, RemoteNode, Error});
        _                          -> {ok, Ret}
    end.
