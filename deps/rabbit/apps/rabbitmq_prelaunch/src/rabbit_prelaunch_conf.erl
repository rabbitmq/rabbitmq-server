-module(rabbit_prelaunch_conf).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/zip.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1,
         get_config_state/0,
         generate_config_from_cuttlefish_files/3,
         decrypt_config/1]).

-ifdef(TEST).
-export([decrypt_config/2]).
-endif.

setup(Context) ->
    ?LOG_DEBUG(
       "\n== Configuration ==",
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    %% TODO: Check if directories/files are inside Mnesia dir.

    ok = set_default_config(),

    AdditionalConfigFiles = find_additional_config_files(Context),
    AdvancedConfigFile = find_actual_advanced_config_file(Context),
    State = case find_actual_main_config_file(Context) of
                {MainConfigFile, erlang} ->
                    Config = load_cuttlefish_config_file(Context,
                                                         AdditionalConfigFiles,
                                                         MainConfigFile),
                    Apps = [App || {App, _} <- Config],
                    decrypt_config(Apps),
                    #{config_files => AdditionalConfigFiles,
                      config_advanced_file => MainConfigFile};
                {MainConfigFile, cuttlefish} ->
                    ConfigFiles = [MainConfigFile | AdditionalConfigFiles],
                    Config = load_cuttlefish_config_file(Context,
                                                         ConfigFiles,
                                                         AdvancedConfigFile),
                    Apps = [App || {App, _} <- Config],
                    decrypt_config(Apps),
                    #{config_files => ConfigFiles,
                      config_advanced_file => AdvancedConfigFile};
                undefined when AdditionalConfigFiles =/= [] ->
                    ConfigFiles = AdditionalConfigFiles,
                    Config = load_cuttlefish_config_file(Context,
                                                         ConfigFiles,
                                                         AdvancedConfigFile),
                    Apps = [App || {App, _} <- Config],
                    decrypt_config(Apps),
                    #{config_files => ConfigFiles,
                      config_advanced_file => AdvancedConfigFile};
                undefined when AdvancedConfigFile =/= undefined ->
                    ?LOG_WARNING(
                      "Using RABBITMQ_ADVANCED_CONFIG_FILE: ~s",
                      [AdvancedConfigFile],
                      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    Config = load_cuttlefish_config_file(Context,
                                                         AdditionalConfigFiles,
                                                         AdvancedConfigFile),
                    Apps = [App || {App, _} <- Config],
                    decrypt_config(Apps),
                    #{config_files => AdditionalConfigFiles,
                      config_advanced_file => AdvancedConfigFile};
                undefined ->
                    #{config_files => [],
                      config_advanced_file => undefined}
            end,
    ok = set_credentials_obfuscation_secret(),
    ?LOG_DEBUG(
      "Saving config state to application env: ~p", [State],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    store_config_state(State).

store_config_state(ConfigState) ->
    persistent_term:put({rabbitmq_prelaunch, config_state}, ConfigState).

get_config_state() ->
    persistent_term:get({rabbitmq_prelaunch, config_state}, undefined).

%% -------------------------------------------------------------------
%% Configuration loading.
%% -------------------------------------------------------------------

set_default_config() ->
    ?LOG_DEBUG("Setting default config",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    Config = [
              {ra,
               [
                {wal_max_size_bytes, 536870912}, %% 5 * 2 ^ 20
                {wal_max_batch_size, 4096}
               ]},
              {aten,
               [
                %% a greater poll interval has shown to trigger fewer false
                %% positive leader elections in quorum queues. The cost is slightly
                %% longer detection time when a genuine network issue occurs.
                %% Ra still uses erlang monitors of course so whenever a connection
                %% goes down it is still immediately detected
                {poll_interval, 5000}
               ]},
              {syslog,
               [{app_name, "rabbitmq-server"}]},
              {sysmon_handler,
               [{process_limit, 100},
                {port_limit, 100},
                {gc_ms_limit, 0},
                {schedule_ms_limit, 0},
                {heap_word_limit, 0},
                {busy_port, false},
                {busy_dist_port, true}]}
             ],
    apply_erlang_term_based_config(Config).

find_actual_main_config_file(#{main_config_file := File}) ->
    case filelib:is_regular(File) of
        true ->
            Format = case filename:extension(File) of
                ".conf"   -> cuttlefish;
                ".config" -> erlang;
                _         -> determine_config_format(File)
            end,
            {File, Format};
        false ->
            OldFormatFile = File ++ ".config",
            NewFormatFile = File ++ ".conf",
            case filelib:is_regular(OldFormatFile) of
                true ->
                    case filelib:is_regular(NewFormatFile) of
                        true ->
                            ?LOG_WARNING(
                              "Both old (.config) and new (.conf) format "
                              "config files exist.",
                              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                            ?LOG_WARNING(
                              "Using the old format config file: ~s",
                              [OldFormatFile],
                              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                            ?LOG_WARNING(
                              "Please update your config files to the new "
                              "format and remove the old file.",
                              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                            ok;
                        false ->
                            ok
                    end,
                    {OldFormatFile, erlang};
                false ->
                    case filelib:is_regular(NewFormatFile) of
                        true  -> {NewFormatFile, cuttlefish};
                        false -> undefined
                    end
            end
    end.

find_additional_config_files(#{additional_config_files := Pattern})
  when Pattern =/= undefined ->
    Pattern1 = case filelib:is_dir(Pattern) of
                   true  -> filename:join(Pattern, "*");
                   false -> Pattern
               end,
    OnlyFiles = [File ||
                 File <- filelib:wildcard(Pattern1),
                 filelib:is_regular(File)],
    lists:sort(OnlyFiles);
find_additional_config_files(_) ->
    [].

find_actual_advanced_config_file(#{advanced_config_file := File}) ->
    case filelib:is_regular(File) of
        true  -> File;
        false -> undefined
    end.

determine_config_format(File) ->
    case filelib:file_size(File) of
        0 ->
            cuttlefish;
        _ ->
            case file:consult(File) of
                {ok, _} -> erlang;
                _       -> cuttlefish
            end
    end.

load_cuttlefish_config_file(Context,
                            ConfigFiles,
                            AdvancedConfigFile) ->
    Config = generate_config_from_cuttlefish_files(
               Context, ConfigFiles, AdvancedConfigFile),
    apply_erlang_term_based_config(Config),
    Config.

generate_config_from_cuttlefish_files(Context,
                                      ConfigFiles,
                                      AdvancedConfigFile) ->
    %% Load schemas.
    SchemaFiles = find_cuttlefish_schemas(Context),
    case SchemaFiles of
        [] ->
            ?LOG_ERROR(
              "No configuration schema found", [],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({error, no_configuration_schema_found});
        _ ->
            ?LOG_DEBUG(
              "Configuration schemas found:~n", [],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            lists:foreach(
              fun(SchemaFile) ->
                      ?LOG_DEBUG("  - ~ts", [SchemaFile],
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
              end,
              SchemaFiles),
            ok
    end,
    Schema = cuttlefish_schema:files(SchemaFiles),

    %% Load configuration.
    ?LOG_DEBUG(
      "Loading configuration files (Cuttlefish based):",
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    lists:foreach(
      fun(ConfigFile) ->
              ?LOG_DEBUG("  - ~ts", [ConfigFile],
                         #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
      end, ConfigFiles),
    case cuttlefish_conf:files(ConfigFiles) of
        {errorlist, Errors} ->
            ?LOG_ERROR("Error parsing configuration:",
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            lists:foreach(
              fun(Error) ->
                      ?LOG_ERROR(
                        "  - ~ts",
                        [cuttlefish_error:xlate(Error)],
                        #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
              end, Errors),
            ?LOG_ERROR(
              "Are these files using the Cuttlefish format?",
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({error, failed_to_parse_configuration_file});
        Config0 ->
            %% Finalize configuration, based on the schema.
            Config = case cuttlefish_generator:map(Schema, Config0) of
                         {error, Phase, {errorlist, Errors}} ->
                             %% TODO
                             ?LOG_ERROR(
                               "Error preparing configuration in phase ~ts:",
                               [Phase],
                               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                             lists:foreach(
                               fun(Error) ->
                                       ?LOG_ERROR(
                                         "  - ~ts",
                                         [cuttlefish_error:xlate(Error)],
                                         #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
                               end, Errors),
                             throw(
                               {error, failed_to_prepare_configuration});
                         ValidConfig ->
                             proplists:delete(vm_args, ValidConfig)
                     end,

            %% Apply advanced configuration overrides, if any.
            override_with_advanced_config(Config, AdvancedConfigFile)
    end.

find_cuttlefish_schemas(Context) ->
    Apps = list_apps(Context),
    ?LOG_DEBUG(
      "Looking up configuration schemas in the following applications:",
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    find_cuttlefish_schemas(Apps, []).

find_cuttlefish_schemas([App | Rest], AllSchemas) ->
    Schemas = list_schemas_in_app(App),
    find_cuttlefish_schemas(Rest, AllSchemas ++ Schemas);
find_cuttlefish_schemas([], AllSchemas) ->
    lists:sort(fun(A,B) -> A < B end, AllSchemas).

list_apps(#{os_type := {win32, _}, plugins_path := PluginsPath}) ->
    PluginsDirs = lists:usort(string:lexemes(PluginsPath, ";")),
    list_apps1(PluginsDirs, []);
list_apps(#{plugins_path := PluginsPath}) ->
    PluginsDirs = lists:usort(string:lexemes(PluginsPath, ":")),
    list_apps1(PluginsDirs, []).


list_apps1([Dir | Rest], Apps) ->
    case file:list_dir(Dir) of
        {ok, Filenames} ->
            NewApps = [list_to_atom(
                         hd(
                           string:split(filename:basename(F, ".ex"), "-")))
                       || F <- Filenames],
            Apps1 = lists:umerge(Apps, lists:sort(NewApps)),
            list_apps1(Rest, Apps1);
        {error, Reason} ->
            ?LOG_DEBUG(
              "Failed to list directory \"~ts\" content: ~ts",
              [Dir, file:format_error(Reason)],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            list_apps1(Rest, Apps)
    end;
list_apps1([], AppInfos) ->
    AppInfos.

list_schemas_in_app(App) ->
    {Loaded, Unload} = case application:load(App) of
                           ok                           -> {true, true};
                           {error, {already_loaded, _}} -> {true, false};
                           {error, Reason}              -> {Reason, false}
                       end,
    List = case Loaded of
               true ->
                   case code:priv_dir(App) of
                       {error, bad_name} ->
                           ?LOG_DEBUG(
                             "  [ ] ~s (no readable priv dir)", [App],
                             #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                           [];
                       PrivDir ->
                           SchemaDir = filename:join([PrivDir, "schema"]),
                           do_list_schemas_in_app(App, SchemaDir)
                   end;
               Reason1 ->
                   ?LOG_DEBUG(
                     "  [ ] ~s (failed to load application: ~p)",
                     [App, Reason1],
                     #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                   []
           end,
    case Unload of
        true  -> _ = application:unload(App),
                 ok;
        false -> ok
    end,
    List.

do_list_schemas_in_app(App, SchemaDir) ->
    case erl_prim_loader:list_dir(SchemaDir) of
        {ok, Files} ->
            ?LOG_DEBUG("  [x] ~s", [App],
                       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            [filename:join(SchemaDir, File)
             || [C | _] = File <- Files,
                C =/= $.];
        error ->
            ?LOG_DEBUG(
              "  [ ] ~s (no readable schema dir)", [App],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            []
    end.

override_with_advanced_config(Config, undefined) ->
    Config;
override_with_advanced_config(Config, AdvancedConfigFile) ->
    ?LOG_DEBUG(
      "Override with advanced configuration file \"~ts\"",
      [AdvancedConfigFile],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case file:consult(AdvancedConfigFile) of
        {ok, [AdvancedConfig]} ->
            cuttlefish_advanced:overlay(Config, AdvancedConfig);
        {ok, OtherTerms} ->
            ?LOG_ERROR(
              "Failed to load advanced configuration file \"~ts\", "
              "incorrect format: ~p",
              [AdvancedConfigFile, OtherTerms],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({error, failed_to_parse_advanced_configuration_file});
        {error, Reason} ->
            ?LOG_ERROR(
              "Failed to load advanced configuration file \"~ts\": ~ts",
              [AdvancedConfigFile, file:format_error(Reason)],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({error, failed_to_read_advanced_configuration_file})
    end.

apply_erlang_term_based_config([{_, []} | Rest]) ->
    apply_erlang_term_based_config(Rest);
apply_erlang_term_based_config([{App, Vars} | Rest]) ->
    ?LOG_DEBUG("  Applying configuration for '~s':", [App],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = apply_app_env_vars(App, Vars),
    apply_erlang_term_based_config(Rest);
apply_erlang_term_based_config([]) ->
    ok.

apply_app_env_vars(App, [{Var, Value} | Rest]) ->
    ?LOG_DEBUG("    - ~s = ~p", [Var, Value],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = application:set_env(App, Var, Value, [{persistent, true}]),
    apply_app_env_vars(App, Rest);
apply_app_env_vars(_, []) ->
    ok.

set_credentials_obfuscation_secret() ->
    ?LOG_DEBUG(
      "Refreshing credentials obfuscation configuration from env: ~p",
      [application:get_all_env(credentials_obfuscation)],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = credentials_obfuscation:refresh_config(),
    CookieBin = rabbit_data_coercion:to_binary(erlang:get_cookie()),
    ?LOG_DEBUG(
      "Setting credentials obfuscation secret to '~s'", [CookieBin],
      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = credentials_obfuscation:set_secret(CookieBin).

%% -------------------------------------------------------------------
%% Config decryption.
%% -------------------------------------------------------------------

decrypt_config(Apps) ->
    ?LOG_DEBUG("Decoding encrypted config values (if any)",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ConfigEntryDecoder = application:get_env(rabbit, config_entry_decoder, []),
    decrypt_config(Apps, ConfigEntryDecoder).

decrypt_config([], _) ->
    ok;
decrypt_config([App | Apps], Algo) ->
    Algo1 = decrypt_app(App, application:get_all_env(App), Algo),
    decrypt_config(Apps, Algo1).

decrypt_app(_, [], Algo) ->
    Algo;
decrypt_app(App, [{Key, Value} | Tail], Algo) ->
    Algo2 = try
                case decrypt(Value, Algo) of
                    {Value, Algo1} ->
                        Algo1;
                    {NewValue, Algo1} ->
                        ?LOG_DEBUG(
                          "Value of `~s` decrypted", [Key],
                          #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                        ok = application:set_env(App, Key, NewValue,
                                                 [{persistent, true}]),
                        Algo1
                end
            catch
                throw:{bad_config_entry_decoder, _} = Error ->
                    throw(Error);
                _:Msg ->
                    throw({config_decryption_error, {key, Key}, Msg})
            end,
    decrypt_app(App, Tail, Algo2).

decrypt({encrypted, _} = EncValue,
        {Cipher, Hash, Iterations, PassPhrase} = Algo) ->
    {rabbit_pbe:decrypt_term(Cipher, Hash, Iterations, PassPhrase, EncValue),
     Algo};
decrypt({encrypted, _} = EncValue,
        ConfigEntryDecoder)
  when is_list(ConfigEntryDecoder) ->
    Algo = config_entry_decoder_to_algo(ConfigEntryDecoder),
    decrypt(EncValue, Algo);
decrypt(List, Algo) when is_list(List) ->
    decrypt_list(List, Algo, []);
decrypt(Value, Algo) ->
    {Value, Algo}.

%% We make no distinction between strings and other lists.
%% When we receive a string, we loop through each element
%% and ultimately return the string unmodified, as intended.
decrypt_list([], Algo, Acc) ->
    {lists:reverse(Acc), Algo};
decrypt_list([{Key, Value} | Tail], Algo, Acc)
  when Key =/= encrypted ->
    {Value1, Algo1} = decrypt(Value, Algo),
    decrypt_list(Tail, Algo1, [{Key, Value1} | Acc]);
decrypt_list([Value | Tail], Algo, Acc) ->
    {Value1, Algo1} = decrypt(Value, Algo),
    decrypt_list(Tail, Algo1, [Value1 | Acc]).

config_entry_decoder_to_algo(ConfigEntryDecoder) ->
    case get_passphrase(ConfigEntryDecoder) of
        undefined ->
            throw({bad_config_entry_decoder, missing_passphrase});
        PassPhrase ->
            {
             proplists:get_value(
               cipher, ConfigEntryDecoder, rabbit_pbe:default_cipher()),
             proplists:get_value(
               hash, ConfigEntryDecoder, rabbit_pbe:default_hash()),
             proplists:get_value(
               iterations, ConfigEntryDecoder,
               rabbit_pbe:default_iterations()),
             PassPhrase
            }
    end.

get_passphrase(ConfigEntryDecoder) ->
    ?LOG_DEBUG("Getting encrypted config passphrase",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case proplists:get_value(passphrase, ConfigEntryDecoder) of
        prompt ->
            IoDevice = get_input_iodevice(),
            ok = io:setopts(IoDevice, [{echo, false}]),
            PP = lists:droplast(io:get_line(IoDevice,
                "\nPlease enter the passphrase to unlock encrypted "
                "configuration entries.\n\nPassphrase: ")),
            ok = io:setopts(IoDevice, [{echo, true}]),
            io:format(IoDevice, "~n", []),
            PP;
        {file, Filename} ->
            {ok, File} = rabbit_misc:raw_read_file(Filename),
            [PP|_] = binary:split(File, [<<"\r\n">>, <<"\n">>]),
            PP;
        PP ->
            PP
    end.

%% This function retrieves the correct IoDevice for requesting
%% input. The problem with using the default IoDevice is that
%% the Erlang shell prevents us from getting the input.
%%
%% Instead we therefore look for the io process used by the
%% shell and if it can't be found (because the shell is not
%% started e.g with -noshell) we use the 'user' process.
%%
%% This function will not work when either -oldshell or -noinput
%% options are passed to erl.
get_input_iodevice() ->
    case whereis(user) of
        undefined ->
            user;
        User ->
            case group:interfaces(User) of
                [] ->
                    user;
                [{user_drv, Drv}] ->
                    case user_drv:interfaces(Drv) of
                        []                          -> user;
                        [{current_group, IoDevice}] -> IoDevice
                    end
            end
    end.
