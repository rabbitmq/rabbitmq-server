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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_error_logger_file_h).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2,
         code_change/3]).

%% rabbit_error_logger_file_h is a wrapper around the error_logger_file_h
%% module because the original's init/1 does not match properly
%% with the result of closing the old handler when swapping handlers.
%% The first init/1 additionally allows for simple log rotation
%% when the suffix is not the empty string.
%% The original init/2 also opened the file in 'write' mode, thus
%% overwriting old logs.  To remedy this, init/2 from
%% lib/stdlib/src/error_logger_file_h.erl from R14B3 was copied as
%% init_file/2 and changed so that it opens the file in 'append' mode.

%% Used only when swapping handlers in log rotation
init({{File, Suffix}, []}) ->
    case rabbit_file:append_file(File, Suffix) of
        ok -> file:delete(File),
              ok;
        {error, Error} ->
            rabbit_log:error("Failed to append contents of "
                             "log file '~s' to '~s':~n~p~n",
                             [File, [File, Suffix], Error])
    end,
    init(File);
%% Used only when swapping handlers and the original handler
%% failed to terminate or was never installed
init({{File, _}, error}) ->
    init(File);
%% Used only when swapping handlers without performing
%% log rotation
init({File, []}) ->
    init(File);
%% Used only when taking over from the tty handler
init({{File, []}, _}) ->
    init(File);
init({File, {error_logger, Buf}}) ->
    rabbit_file:ensure_parent_dirs_exist(File),
    init_file(File, {error_logger, Buf});
init(File) ->
    rabbit_file:ensure_parent_dirs_exist(File),
    init_file(File, []).

init_file(File, {error_logger, Buf}) ->
    case init_file(File, error_logger) of
        {ok, {Fd, File, PrevHandler}} ->
            [handle_event(Event, {Fd, File, PrevHandler}) ||
                {_, Event} <- lists:reverse(Buf)],
            {ok, {Fd, File, PrevHandler}};
        Error ->
            Error
    end;
init_file(File, PrevHandler) ->
    process_flag(trap_exit, true),
    case file:open(File, [append]) of
        {ok,Fd} -> {ok, {Fd, File, PrevHandler}};
        Error   -> Error
    end.

handle_event(Event, State) ->
    error_logger_file_h:handle_event(Event, State).

handle_info(Event, State) ->
    error_logger_file_h:handle_info(Event, State).

handle_call(Event, State) ->
    error_logger_file_h:handle_call(Event, State).

terminate(Reason, State) ->
    error_logger_file_h:terminate(Reason, State).

code_change(OldVsn, State, Extra) ->
    error_logger_file_h:code_change(OldVsn, State, Extra).
