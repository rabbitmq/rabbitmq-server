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

-module(rabbit_msg_store_misc).

-export([open_file/3, preallocate/3, truncate_and_extend_file/3,
         form_filename/2, filenum_to_name/1, scan_file_for_valid_messages/2]).

-include("rabbit_msg_store.hrl").


%%----------------------------------------------------------------------------

open_file(Dir, FileName, Mode) ->
    file_handle_cache:open(form_filename(Dir, FileName), ?BINARY_MODE ++ Mode,
                           [{write_buffer, ?HANDLE_CACHE_BUFFER_SIZE}]).

%%----------------------------------------------------------------------------

preallocate(Hdl, FileSizeLimit, FinalPos) ->
    {ok, FileSizeLimit} = file_handle_cache:position(Hdl, FileSizeLimit),
    ok = file_handle_cache:truncate(Hdl),
    {ok, FinalPos} = file_handle_cache:position(Hdl, FinalPos),
    ok.

truncate_and_extend_file(FileHdl, Lowpoint, Highpoint) ->
    {ok, Lowpoint} = file_handle_cache:position(FileHdl, Lowpoint),
    ok = file_handle_cache:truncate(FileHdl),
    ok = preallocate(FileHdl, Highpoint, Lowpoint).

form_filename(Dir, Name) -> filename:join(Dir, Name).

filenum_to_name(File) -> integer_to_list(File) ++ ?FILE_EXTENSION.

scan_file_for_valid_messages(Dir, FileName) ->
    case open_file(Dir, FileName, ?READ_MODE) of
        {ok, Hdl} ->
            Valid = rabbit_msg_file:scan(Hdl),
            %% if something really bad's happened, the close could fail,
            %% but ignore
            file_handle_cache:close(Hdl),
            Valid;
        {error, enoent} -> {ok, [], 0};
        {error, Reason} -> throw({error,
                                  {unable_to_scan_file, FileName, Reason}})
    end.
