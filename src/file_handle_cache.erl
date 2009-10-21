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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(file_handle_cache).

-export([init/0, open/4, close/2, release/2, read/4, write/4, sync/2,
         position/3, truncate/2, with_file_handle_at/4, sync_to_offset/3]).

-record(file,
        { reader_count,
          has_writer,
          path
        }).

-record(handle,
        { hdl,
          offset,
          trusted_offset,
          write_buffer_size,
          write_buffer,
          at_eof,
          is_append,
          mode,
          global_key
        }).

open(Path, Mode, Options, State) ->
    Path1 = filename:absname(Path),
    case get({Path1, fhc_path}) of
        {gref, GRef} ->
            #file { reader_count = RCount, has_writer = HasWriter }
                = File = get({GRef, fhc_file}),
            Mode1 = lists:usort(Mode),
            IsWriter = is_writer(Mode1),
            case IsWriter andalso HasWriter of
                true ->
                    {{error, writer_exists}, State};
                false ->
                    RCount1 = case is_reader(Mode1) of
                                  true -> RCount + 1;
                                  false -> RCount
                              end,
                    put({Path1, fhc_file},
                        File #file { reader_count = RCount1,
                                     has_writer = HasWriter orelse IsWriter }),
                    open1(Path1, Mode1, Options, GRef, State)
            end;
        undefined ->
            GRef = make_ref(),
            put({Path1, fhc_path}, {gref, GRef}),
            put({GRef, fhc_file},
                #file { reader_count = 0, has_writer = false, path = Path1 }),
            open(Path, Mode, Options, State)
    end.

close(Ref, State) ->
    case erase({Ref, fhc_handle}) of
        undefined -> {ok, State};
        Handle ->
            case write_buffer(Handle) of
                {ok, #handle { hdl = Hdl, mode = Mode, global_key = GRef }} ->
                    ok = file:sync(Hdl),
                    ok = file:close(Hdl),
                    IsReader = is_reader(Mode),
                    IsWriter = is_writer(Mode),
                    #file { reader_count = RCount, has_writer = HasWriter,
                            path = Path } = File = get({GRef, fhc_file}),
                    RCount1 = case IsReader of
                                  true -> RCount - 1;
                                  false -> RCount
                              end, 
                    HasWriter1 = HasWriter andalso not IsWriter,
                    case RCount1 == 0 andalso not HasWriter1 of
                        true -> erase({GRef, fhc_file}),
                                erase({Path, fhc_path});
                        false -> put({GRef, fhc_file},
                                     File #file { reader_count = RCount1,
                                                  has_writer = HasWriter1 })
                    end,
                    {ok, State};
                {Error, Handle1} ->
                    put({Ref, fhc_handle}, Handle1),
                    {Error, State}
            end
    end.

release(_Ref, State) -> %% noop just for now
    {ok, State}.

read(Ref, NewOffset, Count, State) ->
    case get({Ref, fhc_handle}) of
        undefined -> {{error, not_open}, State};
        Handle ->
            case write_buffer(Handle) of
                {ok, Handle1 = #handle { hdl = Hdl, at_eof = AtEoF,
                                         offset = Offset }} ->
                    {AtEoF1, Offset1} =
                        maybe_position(Hdl, AtEoF, Offset, NewOffset),
                    Handle2 = Handle1 #handle { at_eof = AtEoF1,
                                                offset = Offset1 },
                    {Handle3, Result} =
                        case file:read(Hdl, Count) of
                            {ok, Data} -> {Handle2, {ok, Data}};
                            eof -> {Handle2 #handle { at_eof = true }, eof};
                            {error, Reason} -> {Handle2, {error, Reason}}
                        end,
                    put({Ref, fhc_handle}, Handle3),
                    {Result, State};
                {Error, Handle1} ->
                    put({Ref, fhc_handle}, Handle1),
                    {Error, State}
            end
    end.

open1(Path, Mode, Options, GRef, State) ->
    case file:open(Path, Mode) of
        {ok, Hdl} ->
            WriteBufferSize =
                case proplists:get_value(write_buffer, Options, unbuffered) of
                    unbuffered -> 0;
                    infinity -> infinity;
                    N when is_integer(N) -> N
                end,
            Ref = make_ref(),
            put({Ref, fhc_handle},
                #handle { hdl = Hdl, offset = 0, trusted_offset = 0,
                          write_buffer_size = WriteBufferSize,
                          write_buffer = [], at_eof = false,
                          is_append = lists:member(append, Mode), mode = Mode,
                          global_key = GRef }),
            {{ok, Ref}, State};
        {error, Reason} ->
            {{error, Reason}, State}
    end.

write_buffer(Handle = #handle { write_buffer = [] }) ->
    Handle;
write_buffer(Handle = #handle { hdl = Hdl, offset = Offset,
                                write_buffer = WriteBuffer,
                                is_append = IsAppend, at_eof = AtEoF }) ->
    DataSize = size_of_write_data(WriteBuffer),
    case file:write(Hdl, lists:reverse(WriteBuffer)) of
        ok ->
            Offset1 = case IsAppend of
                          true -> Offset;
                          false -> Offset + DataSize
                      end,
            AtEoF1 = AtEoF andalso not IsAppend,
            {ok, Handle #handle { offset = Offset1, write_buffer = [],
                                  at_eof = AtEoF1 }};
        {error, Reason} ->
            {{error, Reason}, Handle}
    end.

size_of_write_data(Data) ->
    size_of_write_data(Data, 0).

size_of_write_data([], Acc) ->
    Acc;
size_of_write_data([A|B], Acc) ->
    size_of_write_data(B, size_of_write_data(A, Acc));
size_of_write_data(Bin, Acc) when is_binary(Bin) ->
    size(Bin) + Acc.

is_reader(Mode) ->
    lists:member(read, Mode).

is_writer(Mode) ->
    lists:member(write, Mode) orelse lists:member(append, Mode).

%% maybe_position(Hdl, AtEof, CurOffset, DesiredOffset)
maybe_position(_Hdl, AtEof, CurOffset, cur) ->
    {AtEof, CurOffset};
maybe_position(_Hdl, true, CurOffset, eof) ->
    {true, CurOffset};
maybe_position(_Hdl, AtEof, CurOffset, CurOffset) ->
    {AtEof, CurOffset};
maybe_position(Hdl, true, CurOffset, DesiredOffset)
  when DesiredOffset >= CurOffset ->
    {ok, Offset} = file:position(Hdl, DesiredOffset),
    {true, Offset};
%% because we can't really track size, we could well end up at EoF and not know
maybe_position(Hdl, _AtEoF, _CurOffset, DesiredOffset) ->
    {ok, Offset} = file:position(Hdl, DesiredOffset),
    {false, Offset}.
