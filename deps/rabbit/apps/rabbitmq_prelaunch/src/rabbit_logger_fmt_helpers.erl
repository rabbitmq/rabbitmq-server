%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_logger_fmt_helpers).

-export([format_time/2,
         format_level/2,
         format_msg/3]).

format_time(Timestamp, #{time_format := Format}) ->
    format_time1(Timestamp, Format);
format_time(Timestamp, _) ->
    format_time1(Timestamp, {rfc3339, $\s, ""}).

format_time1(Timestamp, {rfc3339, Sep, Offset}) ->
    Options = [{unit, microsecond},
               {offset, Offset},
               {time_designator, Sep}],
    calendar:system_time_to_rfc3339(Timestamp, Options);
format_time1(Timestamp, {epoch, secs, int}) ->
    Timestamp div 1000000;
format_time1(Timestamp, {epoch, usecs, int}) ->
    Timestamp;
format_time1(Timestamp, {epoch, secs, binary}) ->
    io_lib:format("~.6.0f", [Timestamp / 1000000]);
format_time1(Timestamp, {epoch, usecs, binary}) ->
    io_lib:format("~b", [Timestamp]);
format_time1(Timestamp, {LocalOrUniversal, Format, Args}) ->
    %% The format string and the args list is prepared by
    %% `rabbit_prelaunch_early_logging:translate_generic_conf()'.
    {{Year, Month, Day},
     {Hour, Minute, Second}} = case LocalOrUniversal of
                                   local ->
                                       calendar:system_time_to_local_time(
                                         Timestamp, microsecond);
                                   universal ->
                                       calendar:system_time_to_universal_time(
                                         Timestamp, microsecond)
                               end,
    Args1 = lists:map(
              fun
                  (year)       -> Year;
                  (month)      -> Month;
                  (day)        -> Day;
                  (hour)       -> Hour;
                  (minute)     -> Minute;
                  (second)     -> Second;
                  ({second_fractional,
                    Decimals}) -> second_fractional(Timestamp, Decimals)
              end, Args),
    io_lib:format(Format, Args1).

second_fractional(Timestamp, Decimals) ->
    (Timestamp rem 1000000) div (1000000 div round(math:pow(10, Decimals))).

format_level(Level, Config) ->
    format_level1(Level, Config).

format_level1(Level, #{level_format := lc}) ->
    level_lc_name(Level);
format_level1(Level, #{level_format := uc}) ->
    level_uc_name(Level);
format_level1(Level, #{level_format := lc3}) ->
    level_3letter_lc_name(Level);
format_level1(Level, #{level_format := uc3}) ->
    level_3letter_uc_name(Level);
format_level1(Level, #{level_format := lc4}) ->
    level_4letter_lc_name(Level);
format_level1(Level, #{level_format := uc4}) ->
    level_4letter_uc_name(Level);
format_level1(Level, _) ->
    level_4letter_lc_name(Level).

level_lc_name(debug)     -> "debug";
level_lc_name(info)      -> "info";
level_lc_name(notice)    -> "notice";
level_lc_name(warning)   -> "warning";
level_lc_name(error)     -> "error";
level_lc_name(critical)  -> "critical";
level_lc_name(alert)     -> "alert";
level_lc_name(emergency) -> "emergency".

level_uc_name(debug)     -> "DEBUG";
level_uc_name(info)      -> "INFO";
level_uc_name(notice)    -> "NOTICE";
level_uc_name(warning)   -> "WARNING";
level_uc_name(error)     -> "ERROR";
level_uc_name(critical)  -> "CRITICAL";
level_uc_name(alert)     -> "ALERT";
level_uc_name(emergency) -> "EMERGENCY".

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

format_msg(Msg, Meta, #{single_line := true} = Config) ->
      FormattedMsg = format_msg1(Msg, Meta, Config),
      %% The following behavior is the same as the one in the official
      %% `logger_formatter'; the code is taken from that module (as of
      %% c5ed910098e9c2787e2c3f9f462c84322064e00d in the master branch).
      FormattedMsg1 = string:strip(FormattedMsg, both),
      re:replace(
        FormattedMsg1,
        ",?\r?\n\s*",
        ", ",
        [{return, list}, global, unicode]);
format_msg(Msg, Meta, Config) ->
      format_msg1(Msg, Meta, Config).

format_msg1({string, Chardata}, Meta, Config) ->
    format_msg1({"~ts", [Chardata]}, Meta, Config);
format_msg1({report, Report}, Meta, Config) ->
    FormattedReport = format_report(Report, Meta, Config),
    format_msg1(FormattedReport, Meta, Config);
format_msg1({Format, Args}, _, _) ->
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
