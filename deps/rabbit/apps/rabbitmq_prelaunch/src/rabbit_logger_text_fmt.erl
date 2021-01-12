%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_logger_text_fmt).

-export([format/2]).

format(#{msg := Msg, meta := Meta} = LogEvent, Config) ->
    Prefix = format_prefix(LogEvent, Config),
    Color = pick_color(LogEvent, Config),
    FormattedMsg = format_msg(Msg, Meta, Config),
    prepend_prefix_to_msg_and_add_color(Prefix, Color, FormattedMsg, Config).

format_prefix(_, #{prefix := false}) ->
    none;
format_prefix(#{level := Level,
                meta := #{time := Timestamp,
                          pid := Pid}},
              Config) ->
    Time = format_time(Timestamp, Config),
    LevelName = level_name(Level, Config),
    io_lib:format("~ts [~ts] ~p", [Time, LevelName, Pid]).

level_name(Level, #{level_name := full}) ->
    Level;
level_name(Level, #{level_name := uc3}) ->
    level_3letter_uc_name(Level);
level_name(Level, #{level_name := lc3}) ->
    level_3letter_lc_name(Level);
level_name(Level, #{level_name := uc4}) ->
    level_4letter_uc_name(Level);
level_name(Level, #{level_name := lc4}) ->
    level_4letter_lc_name(Level);
level_name(Level, _) ->
    Level.

level_3letter_lc_name(debug)     -> "dbg";
level_3letter_lc_name(info)      -> "inf";
level_3letter_lc_name(notice)    -> "ntc";
level_3letter_lc_name(warning)   -> "wrn";
level_3letter_lc_name(error)     -> "err";
level_3letter_lc_name(critical)  -> "crt";
level_3letter_lc_name(alert)     -> "alt";
level_3letter_lc_name(emergency) -> "emg".

level_3letter_uc_name(debug)     -> "DBG";
level_3letter_uc_name(info)      -> "INF";
level_3letter_uc_name(notice)    -> "NTC";
level_3letter_uc_name(warning)   -> "WRN";
level_3letter_uc_name(error)     -> "ERR";
level_3letter_uc_name(critical)  -> "CRT";
level_3letter_uc_name(alert)     -> "ALT";
level_3letter_uc_name(emergency) -> "EMG".

level_4letter_lc_name(debug)     -> "dbug";
level_4letter_lc_name(info)      -> "info";
level_4letter_lc_name(notice)    -> "noti";
level_4letter_lc_name(warning)   -> "warn";
level_4letter_lc_name(error)     -> "erro";
level_4letter_lc_name(critical)  -> "crit";
level_4letter_lc_name(alert)     -> "alrt";
level_4letter_lc_name(emergency) -> "emgc".

level_4letter_uc_name(debug)     -> "DBUG";
level_4letter_uc_name(info)      -> "INFO";
level_4letter_uc_name(notice)    -> "NOTI";
level_4letter_uc_name(warning)   -> "WARN";
level_4letter_uc_name(error)     -> "ERRO";
level_4letter_uc_name(critical)  -> "CRIT";
level_4letter_uc_name(alert)     -> "ALRT";
level_4letter_uc_name(emergency) -> "EMGC".

format_time(Timestamp, _) ->
    Options = [{unit, microsecond},
               {time_designator, $\s}],
    calendar:system_time_to_rfc3339(Timestamp, Options).

format_msg({string, Chardata}, Meta, Config) ->
    format_msg({"~ts", [Chardata]}, Meta, Config);
format_msg({report, Report}, Meta, Config) ->
    FormattedReport = format_report(Report, Meta, Config),
    format_msg(FormattedReport, Meta, Config);
format_msg({Format, Args}, _, _) ->
    io_lib:format(Format, Args).

format_report(
  #{label := {application_controller, _}} = Report, Meta, Config) ->
    format_application_progress(Report, Meta, Config);
format_report(
  #{label := {supervisor, progress}} = Report, Meta, Config) ->
    format_supervisor_progress(Report, Meta, Config);
format_report(
  Report, #{report_cb := Cb} = Meta, Config) ->
    try
        case erlang:fun_info(Cb, arity) of
            {arity, 1} -> Cb(Report);
            {arity, 2} -> {"~ts", [Cb(Report, #{})]}
        end
    catch
        _:_:_ ->
            format_report(Report, maps:remove(report_cb, Meta), Config)
    end;
format_report(Report, _, _) ->
    logger:format_report(Report).

format_application_progress(#{label := {_, progress},
                              report := InternalReport}, _, _) ->
    Application = proplists:get_value(application, InternalReport),
    StartedAt = proplists:get_value(started_at, InternalReport),
    {"Application ~w started on ~0p",
     [Application, StartedAt]};
format_application_progress(#{label := {_, exit},
                              report := InternalReport}, _, _) ->
    Application = proplists:get_value(application, InternalReport),
    Exited = proplists:get_value(exited, InternalReport),
    {"Application ~w exited with reason: ~0p",
     [Application, Exited]}.

format_supervisor_progress(#{report := InternalReport}, _, _) ->
    Supervisor = proplists:get_value(supervisor, InternalReport),
    Started = proplists:get_value(started, InternalReport),
    Id = proplists:get_value(id, Started),
    Pid = proplists:get_value(pid, Started),
    Mfa = proplists:get_value(mfargs, Started),
    {"Supervisor ~w: child ~w started (~w): ~0p",
     [Supervisor, Id, Pid, Mfa]}.

pick_color(_, #{color := false}) ->
    {"", ""};
pick_color(#{level := Level}, #{color := true} = Config) ->
    ColorStart = level_to_color(Level, Config),
    ColorEnd = "\033[0m",
    {ColorStart, ColorEnd}.

level_to_color(debug, _)     -> "\033[38;5;246m";
level_to_color(info, _)      -> "";
level_to_color(notice, _)    -> "\033[38;5;87m";
level_to_color(warning, _)   -> "\033[38;5;214m";
level_to_color(error, _)     -> "\033[38;5;160m";
level_to_color(critical, _)  -> "\033[1;37m\033[48;5;20m";
level_to_color(alert, _)     -> "\033[1;37m\033[48;5;93m";
level_to_color(emergency, _) -> "\033[1;37m\033[48;5;196m".

prepend_prefix_to_msg_and_add_color(
  none, {ColorStart, ColorEnd}, FormattedMsg, Config) ->
    Lines = split_lines(FormattedMsg, Config),
    [case Line of
         "" -> [$\n];
         _  -> [ColorStart, Line, ColorEnd, $\n]
     end
     || Line <- Lines];
prepend_prefix_to_msg_and_add_color(
  Prefix, {ColorStart, ColorEnd}, FormattedMsg, Config) ->
    Lines = split_lines(FormattedMsg, Config),
    [case Line of
         "" -> [ColorStart, Prefix, ColorEnd, $\n];
         _  -> [ColorStart, Prefix, " ", Line, ColorEnd, $\n]
     end
     || Line <- Lines].

split_lines(FormattedMsg, _) ->
    FlattenMsg = lists:flatten(FormattedMsg),
    string:split(FlattenMsg, [$\n], all).
