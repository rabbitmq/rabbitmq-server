%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_dialyzer).

-export([create_basic_plt/1, add_to_plt/2, dialyze_files/2,
         halt_with_code/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(create_basic_plt/1 :: (file:filename()) -> 'ok').
-spec(add_to_plt/2 :: (file:filename(), string()) -> 'ok').
-spec(dialyze_files/2 :: (file:filename(), string()) -> 'ok').
-spec(halt_with_code/1 :: (atom()) -> no_return()).

-endif.

%%----------------------------------------------------------------------------

create_basic_plt(BasicPltPath) ->
    OptsRecord = dialyzer_options:build(
                   [{analysis_type, plt_build},
                    {output_plt, BasicPltPath},
                    {files_rec, otp_apps_dependencies_paths()}]),
    dialyzer_cl:start(OptsRecord),
    ok.

add_to_plt(PltPath, FilesString) ->
    Files = string:tokens(FilesString, " "),
    DialyzerWarnings = dialyzer:run([{analysis_type, plt_add},
                                     {init_plt, PltPath},
                                     {output_plt, PltPath},
                                     {files, Files}]),
    print_warnings(DialyzerWarnings, fun dialyzer:format_warning/1),
    ok.

dialyze_files(PltPath, ModifiedFiles) ->
    Files = string:tokens(ModifiedFiles, " "),
    DialyzerWarnings = dialyzer:run([{init_plt, PltPath},
                                     {files, Files},
                                     {warnings, [behaviours,
                                                 race_conditions]}]),
    case DialyzerWarnings of
        [] -> io:format("~nOk~n");
        _  -> io:format("~n~nFAILED with the following ~p warnings:~n~n",
                        [length(DialyzerWarnings)]),
              print_warnings(DialyzerWarnings, fun dialyzer:format_warning/1)
    end,
    ok.

print_warnings(Warnings, FormatFun) ->
    [io:format("~s~n", [FormatFun(W)]) || W <- Warnings],
    io:format("~n").

otp_apps_dependencies_paths() ->
    [code:lib_dir(App, ebin) ||
        App <- [kernel, stdlib, sasl, mnesia, os_mon, ssl, eunit, tools]].

halt_with_code(ok) ->
    halt();
halt_with_code(fail) ->
    halt(1).
