%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_logger_json_fmt).

-export([format/2]).

format(
  #{msg := Msg,
    level := Level,
    meta := #{time := Timestamp} = Meta},
  Config) ->
    FormattedTimestamp = unicode:characters_to_binary(
                           format_time(Timestamp, Config)),
    FormattedMsg = unicode:characters_to_binary(
                     format_msg(Msg, Meta, Config)),
    FormattedMeta = format_meta(Meta, Config),
    Json = jsx:encode(
             [{time, FormattedTimestamp},
              {level, Level},
              {msg, FormattedMsg},
              {meta, FormattedMeta}]),
    [Json, $\n].

format_time(Timestamp, _) ->
    Options = [{unit, microsecond}],
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

format_meta(Meta, _) ->
    maps:fold(
      fun
          (time, _, Acc) ->
              Acc;
          (domain = Key, Components, Acc) ->
              Term = unicode:characters_to_binary(
                       string:join(
                         [atom_to_list(Cmp) || Cmp <- Components],
                         ".")),
              Acc#{Key => Term};
          (Key, Value, Acc) ->
              case convert_to_types_accepted_by_jsx(Value) of
                  false -> Acc;
                  Term  -> Acc#{Key => Term}
              end
      end, #{}, Meta).

convert_to_types_accepted_by_jsx(Term) when is_map(Term) ->
    maps:map(
      fun(_, Value) -> convert_to_types_accepted_by_jsx(Value) end,
      Term);
convert_to_types_accepted_by_jsx(Term) when is_list(Term) ->
    case io_lib:deep_char_list(Term) of
        true ->
            unicode:characters_to_binary(Term);
        false ->
            [convert_to_types_accepted_by_jsx(E) || E <- Term]
    end;
convert_to_types_accepted_by_jsx(Term) when is_tuple(Term) ->
    convert_to_types_accepted_by_jsx(erlang:tuple_to_list(Term));
convert_to_types_accepted_by_jsx(Term) when is_function(Term) ->
    String = erlang:fun_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) when is_pid(Term) ->
    String = erlang:pid_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) when is_port(Term) ->
    String = erlang:port_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) when is_reference(Term) ->
    String = erlang:ref_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) ->
    Term.
