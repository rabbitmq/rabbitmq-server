%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_pretty_stdout).

-export([display_table/3,
         format_table/3,
         isatty/0,
         ascii_color/1,
         ascii_color/2]).

-type cell() :: {iolist(), color()} | empty.
-type color() :: default | bright_white | red_bg | green | yellow.

-type row() :: [line_in_row()].
-type line_in_row() :: [cell()].

-type table() :: [row()].

-spec display_table(table(), boolean(), boolean()) -> ok.
%% @doc
%% Formats and pretty-prints a table using border-delimited cells.
%%
%% @param Rows A list of rows in the table. Each row contains one or
%%   more lines and each line is made of one or more cells.
%% @param UseColors Indicates if ANSI escape sequences for colors can
%%   be used.
%% @param UseLines Indicates if ANSI escape sequences for line drawing
%%   can be used.

display_table(Rows, UseColors, UseLines) ->
    [io:format("~s~n", [Line])
     || Line <- format_table(Rows, UseColors, UseLines)],
    ok.

-spec format_table(table(), boolean(), boolean()) -> [string()].
%% @doc
%% Formats a table using border-delimited cells.
%%
%% @param Rows A list of rows in the table. Each row contains one or
%%   more lines and each line is made of one or more cells.
%% @param UseColors Indicates if ANSI escape sequences for colors can
%%   be used.
%% @param UseLines Indicates if ANSI escape sequences for line drawing
%%   can be used.
%% @return A list of strings where each string is a line to be
%%   displayed. Lines do not have a trailing newline character.

format_table([[FirstLine | _] | _] = Rows, UseColors, UseLines) ->
    %% Compute columns width.
    ColsCount = length(FirstLine),
    ColsWidths = lists:foldl(
                   fun(Row, Acc1) ->
                           lists:foldl(
                             fun
                                 (empty, Acc2) ->
                                     Acc2;
                                 (Line, Acc2) ->
                                     lists:foldl(
                                       fun(Col, Acc3) ->
                                               MaxW = lists:nth(Col, Acc2),
                                               case lists:nth(Col, Line) of
                                                   empty ->
                                                       [MaxW | Acc3];
                                                   {Txt, _} ->
                                                       CurW =
                                                       string:length(Txt),
                                                       case CurW > MaxW of
                                                           true ->
                                                               [CurW | Acc3];
                                                           false ->
                                                               [MaxW | Acc3]
                                                       end
                                               end
                                       end,
                                       [], lists:seq(ColsCount, 1, -1))
                             end,
                             Acc1, Row)
                   end,
                   lists:duplicate(ColsCount, 0), Rows),
    %% Prepare format string used for the content.
    VerticalSep = case UseLines of
                      true  -> "\x1b(0x\x1b(B";
                      false -> "|"
                  end,
    FormatString = rabbit_misc:format(
                     "~s ~s ~s",
                     [VerticalSep,
                      lists:join(
                        " " ++ VerticalSep ++ " ",
                        lists:duplicate(ColsCount, "~s~s~s~s")),
                      VerticalSep]),
    %% Prepare borders (top, middle, bottom).
    TopBorder = case UseLines of
                    true ->
                        rabbit_misc:format(
                          "\x1b(0lq~sqk\x1b(B",
                          [lists:join(
                             "qwq",
                             lists:map(
                               fun(ColW) -> string:chars($q, ColW) end,
                               ColsWidths))]);
                    false ->
                        rabbit_misc:format(
                          "+-~s-+",
                          [lists:join(
                             "-+-",
                             lists:map(
                               fun(ColW) -> string:chars($-, ColW) end,
                               ColsWidths))])
                end,
    MiddleBorder = case UseLines of
                       true ->
                           rabbit_misc:format(
                             "\x1b(0tq~squ\x1b(B",
                             [lists:join(
                                "qnq",
                                lists:map(
                                  fun(ColW) -> string:chars($q, ColW) end,
                                  ColsWidths))]);
                       false ->
                           TopBorder
                   end,
    BottomBorder = case UseLines of
                       true ->
                           rabbit_misc:format(
                             "\x1b(0mq~sqj\x1b(B",
                             [lists:join(
                                "qvq",
                                lists:map(
                                  fun(ColW) -> string:chars($q, ColW) end,
                                  ColsWidths))]);
                       false ->
                           TopBorder
                   end,
    TopLine  = rabbit_misc:format("~s", [TopBorder]),
    Content = format_rows(Rows,
                          ColsWidths,
                          FormatString,
                          UseColors,
                          MiddleBorder,
                          []),
    BottomLine = rabbit_misc:format("~s", [BottomBorder]),
    [TopLine] ++ Content ++ [BottomLine].

format_rows([Row | Rest],
             ColsWidths, FormatString, UseColors, MiddleBorder,
             AllLines) ->
    Lines = lists:map(
              fun(Line) ->
                      rabbit_misc:format(
                        FormatString,
                        lists:append(
                          lists:map(
                            fun(Col) ->
                                    Line1 = case Line of
                                                empty ->
                                                    lists:duplicate(
                                                      length(ColsWidths),
                                                      empty);
                                                _ ->
                                                    Line
                                            end,
                                    {Value, Color} =
                                      case lists:nth(Col, Line1) of
                                          empty  -> {"", ""};
                                          {V, C} -> {V, C}
                                      end,
                                    ColW = lists:nth(Col, ColsWidths),
                                    PaddingLen = ColW - string:length(Value),
                                    Padding = string:chars($\s, PaddingLen),
                                    case is_atom(Color) of
                                        true ->
                                            [ascii_color(Color, UseColors),
                                             Value,
                                             ascii_color(default, UseColors),
                                             Padding];
                                        false ->
                                            ["",
                                             Color,
                                             "",
                                             Padding]
                                    end
                            end,
                            lists:seq(1, length(ColsWidths)))))
              end,
              Row),
    Lines1 = case Rest of
                 [] -> Lines;
                 _  -> Lines ++ [rabbit_misc:format("~s", [MiddleBorder])]
             end,
    format_rows(Rest, ColsWidths, FormatString, UseColors, MiddleBorder,
                AllLines ++ Lines1);
format_rows([], _, _, _, _, AllLines) ->
    AllLines.

-spec isatty() -> boolean().
%% @doc
%% Guess if stdout it a TTY which handles colors and line drawing.
%%
%% Really it only check is `$TERM' is defined, so it is incorrect
%% at best. And even if it is set, we have no idea if it has the
%% appropriate capabilities.
%%
%% Consider this function a plateholder for some real code.
%%
%% @return `true' if it is a TTY, `false' otherwise.

isatty() ->
    case os:getenv("TERM") of
        %% We don't have access to isatty(3), so let's
        %% assume that is $TERM is defined, we can use
        %% colors and drawing characters.
        false -> false;
        _     -> true
    end.

-spec ascii_color(color()) -> string().
%% @doc
%% Returns the ANSI escape sequence for a given color name.
%%
%% @param Color Name of the color.
%% @return String containing the escape sequence.

ascii_color(default)      -> "\033[0m";
ascii_color(bright_white) -> "\033[1m";
ascii_color(red_bg)       -> "\033[1;37;41m";
ascii_color(green)        -> "\033[32m";
ascii_color(yellow)       -> "\033[33m".

-spec ascii_color(color(), UseColors :: boolean()) -> string().
%% @doc
%% Returns the ANSI escape sequence for a given color name, or the empty
%% string if colors are disabled.
%%
%% @param Color Name of the color.
%% @param UseColors Flag to indicate is colors can actually be used.
%% @return String containing the escape sequence (or an empty string).

ascii_color(Name, true) -> ascii_color(Name);
ascii_color(_, false)   -> "".
