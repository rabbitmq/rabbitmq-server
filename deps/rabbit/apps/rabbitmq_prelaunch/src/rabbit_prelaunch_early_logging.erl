%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_early_logging).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup_early_logging/1,
         default_formatter/1,
         default_console_formatter/1,
         default_file_formatter/1,
         default_journald_formatter/1,
         default_syslog_formatter/1,
         enable_quick_dbg/1,
         use_colored_logging/0,
         use_colored_logging/1,
         translate_categories_conf/2,
         translate_formatter_conf/2,
         translate_journald_fields_conf/2]).
-export([filter_log_event/2]).

-ifdef(TEST).
-export([levels/0,
         determine_prefix/1]).
-endif.

-export_type([category_name/0,
              console_props/0,
              exchange_props/0,
              file_props/0,
              journald_props/0,
              syslog_props/0,
              main_log_env/0,
              per_cat_env/0,
              default_cat_env/0,
              log_app_env/0]).

-define(CONFIGURED_KEY, {?MODULE, configured}).

setup_early_logging(#{log_levels := undefined} = Context) ->
    setup_early_logging(Context#{log_levels => get_default_log_level()});
setup_early_logging(Context) ->
    case is_configured() of
        true  -> ok;
        false -> do_setup_early_logging(Context)
    end.

get_default_log_level() ->
    #{"prelaunch" => notice}.

do_setup_early_logging(#{log_levels := LogLevels} = Context) ->
    add_rmqlog_filter(LogLevels),
    ok = logger:update_handler_config(
           default, main_handler_config(Context)).

is_configured() ->
    persistent_term:get(?CONFIGURED_KEY, false).

add_rmqlog_filter(LogLevels) ->
    add_erlang_specific_filters(LogLevels),
    FilterConfig0 = lists:foldl(
                      fun
                          ({_, V}, FC) when is_boolean(V) -> FC;
                          ({K, V}, FC) when is_atom(K) -> FC#{K => V};
                          ({K, V}, FC) -> FC#{list_to_atom(K) => V}
                      end, #{}, maps:to_list(LogLevels)),
    FilterConfig1 = case maps:is_key(global, FilterConfig0) of
                        true  -> FilterConfig0;
                        false -> FilterConfig0#{global => ?DEFAULT_LOG_LEVEL}
                    end,
    ok = logger:add_handler_filter(
           default, ?FILTER_NAME, {fun filter_log_event/2, FilterConfig1}),
    ok = logger:set_primary_config(level, all),
    ok = persistent_term:put(?CONFIGURED_KEY, true).

add_erlang_specific_filters(_) ->
    _ = logger:add_handler_filter(
          default, progress_reports, {fun logger_filters:progress/2, stop}),
    ok.

filter_log_event(
  #{meta := #{domain := ?RMQLOG_DOMAIN_GLOBAL}} = LogEvent,
  FilterConfig) ->
    MinLevel = get_min_level(global, FilterConfig),
    do_filter_log_event(LogEvent, MinLevel);
filter_log_event(
  #{meta := #{domain := [?RMQLOG_SUPER_DOMAIN_NAME, CatName | _]}} = LogEvent,
  FilterConfig) ->
    MinLevel = get_min_level(CatName, FilterConfig),
    do_filter_log_event(LogEvent, MinLevel);
filter_log_event(
  #{meta := #{domain := [CatName | _]}} = LogEvent,
  FilterConfig) ->
    MinLevel = get_min_level(CatName, FilterConfig),
    do_filter_log_event(LogEvent, MinLevel);
filter_log_event(LogEvent, FilterConfig) ->
    MinLevel = get_min_level(global, FilterConfig),
    do_filter_log_event(LogEvent, MinLevel).

get_min_level(global, FilterConfig) ->
    maps:get(global, FilterConfig, none);
get_min_level(CatName, FilterConfig) ->
    case maps:is_key(CatName, FilterConfig) of
        true  -> maps:get(CatName, FilterConfig);
        false -> get_min_level(global, FilterConfig)
    end.

do_filter_log_event(_, none) ->
    stop;
do_filter_log_event(#{level := Level} = LogEvent, MinLevel) ->
    case logger:compare_levels(Level, MinLevel) of
        lt -> stop;
        _  -> LogEvent
    end.

main_handler_config(Context) ->
    #{filter_default => log,
      formatter => default_formatter(Context)}.

default_formatter(#{log_levels := #{json := true}}) ->
    {rabbit_logger_json_fmt, #{}};
default_formatter(Context) ->
    Color = use_colored_logging(Context),
    {rabbit_logger_text_fmt, #{use_colors => Color}}.

default_console_formatter(Context) ->
    default_formatter(Context).

default_file_formatter(Context) ->
    default_formatter(Context#{output_supports_colors => false}).

default_journald_formatter(_Context) ->
    {rabbit_logger_text_fmt, #{prefix_format => [],
                               use_colors => false}}.

default_syslog_formatter(Context) ->
    {Module, Config} = default_file_formatter(Context),
    case Module of
        rabbit_logger_text_fmt -> {Module, Config#{prefix_format => []}};
        rabbit_logger_json_fmt -> {Module, Config}
    end.

use_colored_logging() ->
    use_colored_logging(rabbit_prelaunch:get_context()).

use_colored_logging(#{log_levels := #{color := true},
                      output_supports_colors := true}) ->
    true;
use_colored_logging(_) ->
    false.

enable_quick_dbg(#{dbg_output := Output, dbg_mods := Mods}) ->
    case Output of
        stdout -> {ok, _} = dbg:tracer(),
                  ok;
        _      -> {ok, _} = dbg:tracer(port, dbg:trace_port(file, Output)),
                  ok
    end,
    {ok, _} = dbg:p(all, c),
    lists:foreach(fun(M) -> {ok, _} = dbg:tp(M, cx) end, Mods).

%% -------------------------------------------------------------------
%% Internal function used by our Cuttlefish schema.
%% -------------------------------------------------------------------

-type category_name() :: atom().
%% The name of a log category.
%% Erlang Logger uses the concept of "domain" which is an ordered list of
%% atoms. A category is mapped to the domain `[?RMQLOG_SUPER_DOMAIN_NAME,
%% Category]'. In other words, a category is a subdomain of the `rabbitmq'
%% domain.

-type console_props() :: [{level, logger:level()} |
                          {enabled, boolean()} |
                          {stdio, stdout | stderr} |
                          {formatter, {atom(), term()}}].
%% Console properties are the parameters in the configuration file for a
%% console-based handler.

-type exchange_props() :: [{level, logger:level()} |
                           {enabled, boolean()} |
                           {formatter, {atom(), term()}}].
%% Exchange properties are the parameters in the configuration file for an
%% exchange-based handler.

-type file_props() :: file:filename() |
                      [{level, logger:level()} |
                       {file, file:filename() | false} |
                       {date, string()} |
                       {size, non_neg_integer()} |
                       {count, non_neg_integer()} |
                       {formatter, {atom(), term()}}].
%% File properties are the parameters in the configuration file for a
%% file-based handler.

-type journald_props() :: [{level, logger:level()} |
                           {enabled, boolean()} |
                           {fields, proplists:proplist()}].
%% journald properties are the parameters in the configuration file for a
%% journald-based handler.

-type syslog_props() :: [{level, logger:level()} |
                         {enabled, boolean()} |
                         {formatter, {atom(), term()}}].
%% Syslog properties are the parameters in the configuration file for a
%% syslog-based handler.

-type main_log_env() :: [{level, logger:level()} |
                         {console, console_props()} |
                         {exchange, exchange_props()} |
                         {file, file_props()} |
                         {journald, journald_props()} |
                         {syslog, syslog_props()}].
%% The main log environment is the parameters in the configuration file for
%% the main log handler (i.e. where all messages go by default).

-type per_cat_env() :: main_log_env().
%% A per-category log environment is the parameters in the configuration file
%% for a specific category log handler. There can be one per category.

-type default_cat_env() :: [{level, logger:level()}].
%% The `default' category log environment is special (read: awkward) in the
%% configuration file. It is used to change the log level of the main log
%% handler.

-type categories_env() :: [{default, default_cat_env()} |
                           {category_name(), per_cat_env()}].
%% The environment for all categories, plus the "default" category environment.

-type log_app_env() :: [main_log_env() |
                        {categories, categories_env()}].
%% The value for the `log' key in the `rabbit' application environment.

-spec translate_categories_conf(string(), cuttlefish_conf:conf()) ->
    [{default, default_cat_env()} |
     {category_name(), per_cat_env()}].
%% @doc
%% Called from the Cuttlefish schema to derive the actual categories
%% configuration from several Cuttlefish variables.

translate_categories_conf(VarPrefix, Conf) ->
    try
        PerCatConfig0 = #{},
        %% TODO: Add exchange, journald and syslog output variables.
        Suffixes = ["level",
                    "console",
                    "console.level",
                    "console.stdio",
                    "console.formatter",
                    "file",
                    "file.level",
                    "file.rotation.date",
                    "file.rotation.size",
                    "file.rotation.count",
                    "file.formatter"
                   ],
        translate_each_category_variable_conf(
          VarPrefix, Suffixes, Conf, PerCatConfig0)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(
               rabbit_prelaunch_errors:format_exception(
                 Class, Reason, Stacktrace),
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({configuration_translation_failure, Reason})
    end.

translate_each_category_variable_conf(
  VarPrefix, [Suffix | Rest], Conf, PerCatConfig) ->
    PrefixPattern = string:tokens(VarPrefix, "."),
    Values = lists:filter(
               fun({Var, _Value}) ->
                       Pattern = PrefixPattern ++ string:tokens(Suffix, "."),
                       cuttlefish_variable:is_fuzzy_match(Var, Pattern)
               end,
               Conf),
    PerCatConfig1 = lists:foldl(
                      fun({VarTokens, Value}, PCC) ->
                              RelevantVarTokens = lists:nthtail(
                                                    length(PrefixPattern) - 1,
                                                    VarTokens),
                              translate_category_variable_conf(
                                VarTokens,
                                RelevantVarTokens,
                                Value,
                                PCC,
                                Conf)
                      end,
                      PerCatConfig,
                      Values),
    translate_each_category_variable_conf(VarPrefix, Rest, Conf, PerCatConfig1);
translate_each_category_variable_conf(_, [], _, PerCatConfig) ->
    maps:to_list(PerCatConfig).

translate_category_variable_conf(
  _VarTokens, ["default" | _] = RelevantVarTokens, Value,
  PerCatConfig, _Conf) ->
    case RelevantVarTokens of
        ["default", "level"] -> PerCatConfig#{default => [{level, Value}]};
        _                    -> PerCatConfig
    end;
translate_category_variable_conf(
  VarTokens, [CatNameS, OutputS, "formatter"], _Value,
  PerCatConfig, Conf) ->
    case is_category(CatNameS) of
        true ->
            Var = string:join(VarTokens, "."),
            Value1 = translate_formatter_conf(Var, Conf),
            Output = list_to_atom(OutputS),
            add_category_prop(PerCatConfig, CatNameS, [Output, formatter], Value1);
        false ->
            PerCatConfig
    end;
translate_category_variable_conf(
  _VarTokens, [CatNameS | _] = RelevantVarTokens, Value,
  PerCatConfig, _Conf) ->
    case is_category(CatNameS) of
        true ->
            PropPath = translate_prop_path(RelevantVarTokens),
            add_category_prop(PerCatConfig, CatNameS, PropPath, Value);
        false ->
            PerCatConfig
    end.

is_category("console")  -> false;
is_category("exchange") -> false;
is_category("file")     -> false;
is_category("journald") -> false;
is_category("syslog")   -> false;
is_category("level")    -> false;
is_category("default")  -> false;
is_category(_)          -> true.

translate_prop_path([_, "console"])                   -> [console, enabled];
translate_prop_path([_, "file"])                      -> [file, file];
translate_prop_path([_, "file", "rotation", "date"])  -> [file, date];
translate_prop_path([_, "file", "rotation", "size"])  -> [file, size];
translate_prop_path([_, "file", "rotation", "count"]) -> [file, count];
translate_prop_path([_ | Rest])                       ->
    [list_to_atom(PropNameS) || PropNameS <- Rest].

add_category_prop(PerCatConfig, CatNameS, PropPath, Value) ->
    CatName = list_to_atom(CatNameS),
    CatProps = case PerCatConfig of
                   #{CatName := CatProps0} -> CatProps0;
                   _                       -> []
               end,
    CatProps1 = add_category_prop1(PropPath, Value, CatProps),
    PerCatConfig#{CatName => CatProps1}.

add_category_prop1([PropName], Value, Props) ->
    Prop = {PropName, Value},
    lists:keystore(PropName, 1, Props, Prop);
add_category_prop1([PropName | Rest], Value, Props) ->
    SubProps = proplists:get_value(PropName, Props, []),
    Prop = {PropName, add_category_prop1(Rest, Value, SubProps)},
    lists:keystore(PropName, 1, Props, Prop).

-spec translate_formatter_conf(string(), cuttlefish_conf:conf()) ->
    {rabbit_logger_text_fmt, formatter_plaintext_conf()} |
    {rabbit_logger_json_fmt, formatter_json_conf()}.
%% @doc
%% Called from the Cuttlefish schema to derive the actual formatter
%% configuration from several Cuttlefish variables.

translate_formatter_conf(Var, Conf) when is_list(Var) ->
    try
        %% Part of the formatter configuration is common to all the
        %% formatters, the rest is formatter-specific.
        GenericConfig = translate_generic_conf(Var, Conf),
        case cuttlefish:conf_get(Var, Conf) of
            plaintext ->
                translate_plaintext_formatter_conf(Var, Conf, GenericConfig);
            json ->
                translate_json_formatter_conf(Var, Conf, GenericConfig)
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(
               rabbit_prelaunch_errors:format_exception(
                 Class, Reason, Stacktrace),
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({configuration_translation_failure, Reason})
    end.

-type time_format_string_arg() :: year | month | day |
                                  hour | minute | second |
                                  {second_fractional, non_neg_integer()}.
-type time_format() :: {rfc3339, char(), string() | integer()} |
                       {epoch, secs | usecs, binary | int} |
                       {local | universal,
                        string(),
                        [time_format_string_arg()]}.
-type level_format() :: lc | uc | lc3 | uc3 | lc4 | uc4.
-type formatter_generic_conf() :: #{time_format := time_format(),
                                    level_format := level_format(),
                                    single_line := boolean()}.

-spec translate_generic_conf(string(), cuttlefish_conf:conf()) ->
    formatter_generic_conf().
%% @doc
%% Handles variables common to all formatters.

translate_generic_conf(Var, Conf) ->
    %% log.*.formatter.time_format
    %% It accepts either a "named pattern" like `rfc3339_T' or a custom
    %% pattern.
    Formatter = cuttlefish:conf_get(Var, Conf),
    TimeFormat = case cuttlefish:conf_get(Var ++ ".time_format", Conf) of
                     rfc3339_T ->
                         {rfc3339, $T, ""};
                     rfc3339_space ->
                         {rfc3339, $\s, ""};
                     epoch_secs when Formatter =:= json ->
                         {epoch, secs, int};
                     epoch_usecs when Formatter =:= json ->
                         {epoch, usecs, int};
                     epoch_secs ->
                         {epoch, secs, binary};
                     epoch_usecs ->
                         {epoch, usecs, binary};
                     lager_default ->
                         {local,
                          "~4..0b-~2..0b-~2..0b "
                          "~2..0b:~2..0b:~2..0b.~3..0b",
                          [year, month, day,
                           hour, minute, second,
                           {second_fractional, 3}]}
                 end,

    %% log.*.formatter.level_format
    %% It determines basically if the level should be printed in lowercase or
    %% uppercase, and fully or truncated (to align messages horizontally).
    LevelFormat = cuttlefish:conf_get(Var ++ ".level_format", Conf),

    %% log.*.formatter.single_line
    %% It tells if multi-line messages should be kept as-is or reformatted to
    %% stay on a single line.
    SingleLine = cuttlefish:conf_get(Var ++ ".single_line", Conf),

    #{time_format => TimeFormat,
      level_format => LevelFormat,
      single_line => SingleLine}.

-type line_format() :: [atom() | string()].
-type color_esc_seqs() :: #{logger:level() => string()}.
-type formatter_plaintext_conf() :: #{time_format := time_format(),
                                      level_format := level_format(),
                                      single_line := boolean(),
                                      prefix_format := line_format(),
                                      line_format := line_format(),
                                      use_colors := boolean(),
                                      color_esc_seqs := color_esc_seqs()}.

-spec translate_plaintext_formatter_conf(
        string(), cuttlefish_conf:conf(), formatter_generic_conf()) ->
    {rabbit_logger_text_fmt, formatter_plaintext_conf()}.
%% @doc
%% Handles variables specific to the plaintext formatter.

translate_plaintext_formatter_conf(Var, Conf, GenericConfig) ->
    %% log.*.formatter.plaintext.format
    %% This is a variable-based string used to indicate the message format.
    %% Here, we parse that pattern to make it easier and more efficient for
    %% the formatter to format the final message.
    Format0 = cuttlefish:conf_get(Var ++ ".plaintext.format", Conf),
    Format = prepare_fmt_format(Format0),
    {PrefixFormat, LineFormat} = determine_prefix(Format),

    %% log.console.use_colors
    %% log.console.color_esc_seqs
    %% Those variables indicates if colors should be used and which one. They
    %% are specific to the console handler.
    {UseColors, ColorEscSeqs} = translate_colors_conf(Var, Conf),

    Mod = rabbit_logger_text_fmt,
    Config = GenericConfig#{prefix_format => PrefixFormat,
                            line_format => LineFormat,
                            use_colors => UseColors,
                            color_esc_seqs => ColorEscSeqs},
    {Mod, Config}.

-spec prepare_fmt_format(string()) -> [atom() | string()].
%% @doc
%% Parse the pattern and prepare a list which makes it easy for the formatter
%% to format the final message.
%%
%% The initial pattern will use variables; for example:
%% `$time [$level] $pid - $msg'
%%
%% Once parsed, the pattern will look like:
%% `[time, " [", level, "] ", pid, " - ", msg]'
%%
%% Variables are taken from the log event structure: `msg' and `level' are
%% taken from the top-level, other variables come from the `meta' map.

prepare_fmt_format(Format) ->
    prepare_fmt_format(Format, []).

prepare_fmt_format([$$ | Rest], Parsed) ->
    {match, [Var, Rest1]} = re:run(Rest, "^([a-zA_Z0-9_]+)(.*)",
                                   [{capture, all_but_first, list}]),
    Var1 = list_to_atom(Var),
    prepare_fmt_format(Rest1, [Var1 | Parsed]);
prepare_fmt_format(Rest, Parsed) when Rest =/= "" ->
    %% We made sure in the guard expression that `Rest' contains at least
    %% onecharacter. The following regex "eats" at least that character. This
    %% avoids an infinite loop which would happen if the returned `String' was
    %% empty and `Rest1' would be the same as `Rest'.
    {match, [String, Rest1]} = re:run(Rest, "^(.[^$]*)(.*)",
                                      [{capture, all_but_first, list}]),
    prepare_fmt_format(Rest1, [String | Parsed]);
prepare_fmt_format("", Parsed) ->
    lists:reverse(Parsed).

determine_prefix(Format) ->
    %% Based on where the `msg' variable is, we determine the prefix of the
    %% message. This is later used by the formatter to repeat the prefix for
    %% each line making a multi-line message.
    %%
    %% If `msg' is not logged at all, we consider the line has no prefix.
    {PrefixFormat0, LineFormat0} =
    lists:foldl(
      fun
          (msg, {PF, LF})       -> {PF, LF ++ [msg]};
          (Elem, {PF, [] = LF}) -> {PF ++ [Elem], LF};
          (Elem, {PF, LF})      -> {PF, LF ++ [Elem]}
      end, {[], []}, Format),
    case {PrefixFormat0, LineFormat0} of
        {_, []} -> {[], PrefixFormat0};
        _       -> {PrefixFormat0, LineFormat0}
    end.

-spec translate_colors_conf(string(), cuttlefish_conf:conf()) ->
    {boolean(), map()}.
%% @doc
%% Computes the color configuration.
%%
%% The function uses the following two variables:
%% `log.console.use_colors'
%% `log.console.color_esc_seqs'
%%
%% It does not verify what escape sequences are actually configured. It is
%% entirely possible to play with the cursor position or other control
%% characters.
%%
%% This is only valid for the console output.

translate_colors_conf("log.console.formatter", Conf) ->
    {
     cuttlefish:conf_get("log.console.use_colors", Conf),
     lists:foldl(
       fun(Lvl, Acc) ->
               LvlS = atom_to_list(Lvl),
               Key = "log.console.color_esc_seqs." ++ LvlS,
               RawVal = cuttlefish:conf_get(Key, Conf),
               %% The ESC character will be escaped if the user entered the
               %% string "\033" for instance. We need to convert it back to an
               %% actual ESC character.
               Val = re:replace(
                       RawVal,
                       "\\\\(e|033)",
                       "\033",
                       [global, {return, list}]),
               Acc#{Lvl => Val}
       end,
       #{},
       levels())
    };
translate_colors_conf(_, _) ->
    {false, #{}}.

-type json_field_map() :: [{atom(), atom()} | {atom() | '$REST', false}].
-type json_verbosity_map() :: #{logger:level() => non_neg_integer(),
                                '$REST' => non_neg_integer()}.
-type formatter_json_conf() :: #{time_format := time_format(),
                                 level_format := level_format(),
                                 single_line := boolean(),
                                 field_map := json_field_map(),
                                 verbosity_map := json_verbosity_map()}.

-spec translate_json_formatter_conf(
        string(), cuttlefish_conf:conf(), map()) ->
    {rabbit_logger_json_fmt, formatter_json_conf()}.
%% @doc
%% Handles variables specific to the JSON formatter.

translate_json_formatter_conf(Var, Conf, GenericConfig) ->
    %% log.*.formatter.json.field_map
    %% It indicates several things:
    %%   - the order of fields; non-mentionned fields go unordered at the end
    %%     of the JSON object
    %%   - if fields should be renamed
    %%   - if fields should be removed from the final object
    RawFieldMapping = cuttlefish:conf_get(Var ++ ".json.field_map", Conf),
    FieldMapping = parse_json_field_mapping(RawFieldMapping),

    %% log.*.formatter.json.verbosity_map
    %% It indicates if a `verbosity' field should be added and how its value
    %% should be derived from `level'.
    RawVerbMapping = cuttlefish:conf_get(
                       Var ++ ".json.verbosity_map", Conf),
    VerbMapping = parse_json_verbosity_mapping(RawVerbMapping),

    Mod = rabbit_logger_json_fmt,
    Config = GenericConfig#{field_map => FieldMapping,
                            verbosity_map => VerbMapping},
    {Mod, Config}.

-spec parse_json_field_mapping(string()) -> json_field_map().
%% @doc
%% Parses the JSON formatter field_map pattern.
%%
%% The pattern is of the form: `time:ts level msg *:-'.
%%
%% `time:ts' means the `time' field should be renamed to `ts'.
%%
%% `level' means that field should be kept as-is.
%%
%% `gl:-' means the `gl' field should be dropped.
%%
%% `*:-' means all non-mentionned fields should be dropped.
%%
%% The order of fields in the pattern is important: it tells the order of
%% fields in the final JSON object.

parse_json_field_mapping(RawMapping) ->
    parse_json_field_mapping(string:split(RawMapping, " ", all), []).

parse_json_field_mapping([Entry | Rest], Mapping) ->
    Mapping1 = case string:split(Entry, ":", leading) of
                   ["*", "-"] ->
                       [{'$REST', false} | Mapping];
                   [OldS, "-"] ->
                       Old = list_to_atom(OldS),
                       [{Old, false} | Mapping];
                   ["*", _] ->
                       throw({bad_json_mapping, Entry});
                   [OldS, NewS] ->
                       Old = list_to_atom(OldS),
                       New = list_to_atom(NewS),
                       [{Old, New} | Mapping];
                   [KeepS] ->
                       Keep = list_to_atom(KeepS),
                       [{Keep, Keep} | Mapping]
               end,
    parse_json_field_mapping(Rest, Mapping1);
parse_json_field_mapping([], Mapping) ->
    %% We parsed everything. Now we want to organize fields a bit:
    %%   - All `{atom(), atom()}' (kept or renamed fields) go at the
    %%     beginning, preserving their order
    %%   - All `{_, false}' (removed fields) go at the end
    {Renames0, Removes0} = lists:partition(
                             fun
                                 ({_, false}) -> false;
                                 (_)          -> true
                             end,
                             Mapping),
    Renames = lists:reverse(Renames0),
    %% If all non-mentionned fields are to be removed, only the `{$REST,
    %% false}' entry is useful.
    Removes = case lists:member({'$REST', false}, Removes0) of
                  true  -> [{'$REST', false}];
                  false -> Removes0
              end,
    Renames ++ Removes.

-spec parse_json_verbosity_mapping(string()) -> json_verbosity_map().
%% @doc
%% Parses the verbosity_map pattern.
%%
%% The pattern is of the form: `debug=2 info=1 *=0'.
%%
%% `debug=2' means that the verbosity of the debug level is 2.
%%
%% `*=0' means that the verbosity of all non-mentionned levels is 0.

parse_json_verbosity_mapping("") ->
    #{};
parse_json_verbosity_mapping(RawMapping) ->
    parse_json_verbosity_mapping(string:split(RawMapping, " ", all), #{}).

parse_json_verbosity_mapping([Entry | Rest], Mapping) ->
    Mapping1 = case string:split(Entry, "=", leading) of
                   ["*", VerbS] ->
                       Verb = list_to_integer(VerbS),
                       Mapping#{'$REST' => Verb};
                   [LvlS, VerbS] ->
                       Lvl = list_to_atom(LvlS),
                       Verb = list_to_integer(VerbS),
                       Mapping#{Lvl => Verb}
               end,
    parse_json_verbosity_mapping(Rest, Mapping1);
parse_json_verbosity_mapping([], #{'$REST' := Default} = Mapping) ->
    DefaultMapping = lists:foldl(
                       fun(Lvl, Acc) -> Acc#{Lvl => Default} end,
                       #{}, levels()),
    maps:merge(
      DefaultMapping,
      maps:remove('$REST', Mapping));
parse_json_verbosity_mapping([], Mapping) ->
    Mapping.

-spec translate_journald_fields_conf(string(), cuttlefish_conf:conf()) ->
    proplists:proplist().
%% @doc
%% Called from the Cuttlefish schema to create the actual journald handler
%% configuration.

translate_journald_fields_conf(Var, Conf) when is_list(Var) ->
    try
        RawFieldMapping = cuttlefish:conf_get(Var, Conf),
        parse_journald_field_mapping(RawFieldMapping)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(
               rabbit_prelaunch_errors:format_exception(
                 Class, Reason, Stacktrace),
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({configuration_translation_failure, Reason})
    end.

-spec parse_journald_field_mapping(string()) ->
    [atom() | {atom(), atom()}].
%% @doc
%% Parses the journald fields pattern.
%%
%% The pattern is of the form: `SYSLOG_IDENTIFIER="rabbitmq-server" pid
%% CODE_FILE=file'.
%%
%% `SYSLOG_IDENTIFIER="rabbitmq"' means the `SYSLOG_IDENTIFIER' field should
%% be set to the string `rabbitmq-server'.
%%
%% `pid' means that field should be kept as-is.
%%
%% `CODE_FILE=file' means the `CODE_FILE' field should be set to the value of
%% the `pid' field.

parse_journald_field_mapping(RawMapping) ->
    parse_journald_field_mapping(string:split(RawMapping, " ", all), []).

parse_journald_field_mapping([Entry | Rest], Mapping) ->
    Mapping1 = case string:split(Entry, "=", leading) of
                   [[$_ | _], _] ->
                       throw({bad_journald_mapping,
                              leading_underscore_forbidden,
                              Entry});
                   [Name, Value] ->
                       case re:run(Name, "^[A-Z0-9_]+$", [{capture, none}]) of
                           match ->
                               ReOpts = [{capture, all_but_first, list}],
                               case re:run(Value, "^\"(.+)\"$", ReOpts) of
                                   {match, [Data]} ->
                                       [{Name, Data} | Mapping];
                                   nomatch ->
                                       Field = list_to_atom(Value),
                                       [{Name, Field} | Mapping]
                               end;
                           nomatch ->
                               throw({bad_journald_mapping,
                                      name_with_invalid_characters,
                                      Entry})
                       end;
                   [FieldS] ->
                       Field = list_to_atom(FieldS),
                       [Field | Mapping]
               end,
    parse_journald_field_mapping(Rest, Mapping1);
parse_journald_field_mapping([], Mapping) ->
    lists:reverse(Mapping).

levels() ->
    [debug, info, notice, warning, error, critical, alert, emergency].
