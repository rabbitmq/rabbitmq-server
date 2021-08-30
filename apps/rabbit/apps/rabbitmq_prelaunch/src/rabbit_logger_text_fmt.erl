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
    FormattedMsg = rabbit_logger_fmt_helpers:format_msg(Msg, Meta, Config),
    prepend_prefix_to_msg_and_add_color(
      Prefix, Color, FormattedMsg, LogEvent, Config).

format_prefix(LogEvent, #{prefix_format := PrefixFormat} = Config) ->
    format_prefix(PrefixFormat, LogEvent, Config, []);
format_prefix(LogEvent, Config) ->
    %% Default prefix format.
    format_prefix([time, " [", level, "] ", pid, " "], LogEvent, Config, []).

format_prefix([String | Rest], LogEvent, Config, Prefix)
  when is_list(String) ->
    format_prefix(Rest, LogEvent, Config, [String | Prefix]);
format_prefix([Var | Rest], LogEvent, Config, Prefix)
  when is_atom(Var) ->
    String = format_var(Var, LogEvent, Config),
    format_prefix(Rest, LogEvent, Config, [String | Prefix]);
format_prefix([], _, _, Prefix) ->
    lists:reverse(Prefix).

format_var(level, #{level := Level}, Config) ->
    rabbit_logger_fmt_helpers:format_level(Level, Config);
format_var(time, #{meta := #{time := Timestamp}}, Config) ->
    rabbit_logger_fmt_helpers:format_time(Timestamp, Config);
format_var(Var, #{meta := Meta}, _) ->
    case maps:get(Var, Meta, undefined) of
        undefined ->
            io_lib:format("<unknown ~s>", [Var]);
        Value ->
            case io_lib:char_list(Value) of
                true  -> io_lib:format("~s", [Value]);
                false -> io_lib:format("~p", [Value])
            end
    end.

pick_color(#{level := Level}, #{use_colors := true} = Config) ->
    ColorStart = level_to_color(Level, Config),
    ColorEnd = "\033[0m",
    {ColorStart, ColorEnd};
pick_color(_, _) ->
    {"", ""}.

level_to_color(Level, #{color_esc_seqs := ColorEscSeqs}) ->
    maps:get(Level, ColorEscSeqs);
level_to_color(debug, _)     -> "\033[38;5;246m";
level_to_color(info, _)      -> "";
level_to_color(notice, _)    -> "\033[38;5;87m";
level_to_color(warning, _)   -> "\033[38;5;214m";
level_to_color(error, _)     -> "\033[38;5;160m";
level_to_color(critical, _)  -> "\033[1;37m\033[48;5;20m";
level_to_color(alert, _)     -> "\033[1;37m\033[48;5;93m";
level_to_color(emergency, _) -> "\033[1;37m\033[48;5;196m".

prepend_prefix_to_msg_and_add_color(
  Prefix, {ColorStart, ColorEnd}, FormattedMsg, LogEvent, Config) ->
    Lines = split_lines(FormattedMsg, Config),
    [[ColorStart,
      format_line(Prefix, Line, LogEvent, Config),
      ColorEnd,
      $\n]
     || Line <- Lines].

split_lines(FormattedMsg, _) ->
    FlattenMsg = lists:flatten(FormattedMsg),
    string:split(FlattenMsg, [$\n], all).

format_line(Prefix, Msg, LogEvent, #{line_format := Format} = Config) ->
    format_line(Format, Msg, LogEvent, Config, [Prefix]);
format_line(Prefix, Msg, LogEvent, Config) ->
    format_line([msg], Msg, LogEvent, Config, [Prefix]).

format_line([msg | Rest], Msg, LogEvent, Config, Line) ->
    format_line(Rest, Msg, LogEvent, Config, [Msg | Line]);
format_line([String | Rest], Msg, LogEvent, Config, Line)
  when is_list(String) ->
    format_line(Rest, Msg, LogEvent, Config, [String | Line]);
format_line([Var | Rest], Msg, LogEvent, Config, Line)
  when is_atom(Var) ->
    String = format_var(Var, LogEvent, Config),
    format_line(Rest, Msg, LogEvent, Config, [String | Line]);
format_line([], _, _, _, Line) ->
    remove_trailing_whitespaces(Line).

remove_trailing_whitespaces([Tail | Line]) ->
    Tail1 = string:strip(Tail, right),
    lists:reverse([Tail1 | Line]).
