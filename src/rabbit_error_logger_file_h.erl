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

-module(rabbit_error_logger_file_h).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

%% rabbit_error_logger_file_h is a wrapper around the error_logger_file_h
%% module because the original's init/1 does not match properly
%% with the result of closing the old handler when swapping handlers.
%% The first init/1 additionally allows for simple log rotation
%% when the suffix is not the empty string.

%% Used only when swapping handlers in log rotation
init({{File, Suffix}, []}) ->
    case rabbit_misc:append_file(File, Suffix) of
        ok  -> ok;
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
init({File, _Type} = FileInfo) ->
    rabbit_misc:ensure_parent_dirs_exist(File),
    error_logger_file_h:init(FileInfo);
init(File) ->
    rabbit_misc:ensure_parent_dirs_exist(File),
    error_logger_file_h:init(File).

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
