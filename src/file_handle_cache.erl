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
         position/3, truncate/2, last_sync_offset/2]).

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
          write_buffer_size_limit,
          write_buffer,
          at_eof,
          is_append,
          is_write,
          is_read,
          mode,
          global_key,
          last_used_at
        }).

open(Path, Mode, Options, State) ->
    Path1 = filename:absname(Path),
    case get({Path1, fhc_path}) of
        {gref, GRef} ->
            #file { reader_count = RCount, has_writer = HasWriter }
                = File = get({GRef, fhc_file}),
            Mode1 = lists:usort(Mode),
            IsWriter = is_writer(Mode1) orelse is_appender(Mode),
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
        #handle { is_read = false } -> {{error, not_open_for_reading}, State};
        Handle ->
            case write_buffer(Handle) of
                {ok, Handle1 = #handle { hdl = Hdl, at_eof = AtEoF,
                                         offset = Offset }} ->
                    {AtEoF1, NeedsSeek} = needs_seek(AtEoF, Offset, NewOffset),
                    {ok, Offset1} = case NeedsSeek of
                                        true -> file:position(Hdl, NewOffset);
                                        false -> {ok, Offset}
                                    end,
                    Handle2 = Handle1 #handle { at_eof = AtEoF1,
                                                offset = Offset1,
                                                last_used_at = now() },
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

write(Ref, NewOffset, Data, State) ->
    case get({Ref, fhc_handle}) of
        undefined -> {{error, not_open}, State};
        Handle = #handle { is_append = true, is_write = false } ->
            {Result, Handle1} = write_to_buffer(Data, Handle),
            put({Ref, fhc_handle}, Handle1),
            {Result, State};
        Handle = #handle { is_append = false, is_write = true, at_eof = AtEoF,
                           offset = Offset, write_buffer_size = BufSize } ->
            %% If we wrote the buffer out now, where would we end up?
            %% Note that if AtEoF == true then it would still be true
            %% after writing the buffer out, but if AtEoF == false,
            %% it's possible it should be true after writing the
            %% buffer out, but we won't know about it.
            VirtualOffset = Offset + BufSize,
            %% AtEoF1 says "after writing the buffer out, we will be
            %% at VirtualOffset. At that point, we travel to
            %% NewOffset. When we get there, will we be at eof?"
            {AtEoF1, NeedsSeek} = needs_seek(AtEoF, VirtualOffset, NewOffset),
            {Error, Handle1} =
                case NeedsSeek of
                    %% Now if we don't seek, we don't write the buffer
                    %% out. This means we'll still be at Offset, and
                    %% AtEoF still applies. We need to add the data to
                    %% the buffer and leave it at that.
                    false -> {ok, Handle};
                    %% If we do seek, then we write the buffer out,
                    %% which means that AtEoF1 applies, because after
                    %% writing the buffer out, we'll be at
                    %% VirtualOffset, and then want to get to
                    %% NewOffset.
                    true ->                
                        case write_buffer(Handle) of
                            {ok, Handle2 = #handle { hdl = Hdl }} ->
                                {ok, Offset2} = file:position(Hdl, NewOffset),
                                {ok, Handle2 #handle { offset = Offset2,
                                                       at_eof = AtEoF1 }};
                            {Error1, Handle2} -> {Error1, Offset, Handle2}
                        end
                end,
            case Error of
                ok -> {Result, Handle3} = write_to_buffer(Data, Handle1),
                      put({Ref, fhc_handle}, Handle3),
                      {Result, State};
                _ -> put({Ref, fhc_handle}, Handle1),
                     {Error, State}
            end;
        _ -> {{error, not_open_for_writing}, State}
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
                          write_buffer_size = 0,
                          write_buffer_size_limit = WriteBufferSize,
                          write_buffer = [], at_eof = false,
                          is_append = is_appender(Mode), mode = Mode,
                          is_write = is_writer(Mode), is_read = is_reader(Mode),
                          global_key = GRef, last_used_at = now() }),
            {{ok, Ref}, State};
        {error, Reason} ->
            {{error, Reason}, State}
    end.

write_to_buffer(Data, Handle = #handle { hdl = Hdl,
                                         write_buffer_size_limit = 0 }) ->
    {file:write(Hdl, Data), Handle #handle { last_used_at = now() }};
write_to_buffer(Data, Handle =
                #handle { write_buffer = WriteBuffer,
                          write_buffer_size = Size,
                          write_buffer_size_limit = infinity }) ->
    {ok, Handle #handle { write_buffer_size = Size + size_of_write_data(Data),
                          write_buffer = [ Data | WriteBuffer ],
                          last_used_at = now() }};
write_to_buffer(Data, Handle =
                #handle { write_buffer = WriteBuffer,
                          write_buffer_size = Size,
                          write_buffer_size_limit = Limit }) ->
    Size1 = Size + size_of_write_data(Data),
    Handle1 = Handle #handle { write_buffer = [ Data | WriteBuffer ],
                               write_buffer_size = Size1,
                               last_used_at = now() },
    case Size1 > Limit of
        true -> write_buffer(Handle1);
        false -> {ok, Handle1}
    end.

write_buffer(Handle = #handle { write_buffer = [] }) ->
    {ok, Handle};
write_buffer(Handle = #handle { hdl = Hdl, offset = Offset,
                                write_buffer = WriteBuffer,
                                write_buffer_size = DataSize,
                                is_append = IsAppend, at_eof = AtEoF }) ->
    case file:write(Hdl, lists:reverse(WriteBuffer)) of
        ok ->
            Offset1 = case IsAppend of
                          true -> Offset;
                          false -> Offset + DataSize
                      end,
            AtEoF1 = AtEoF andalso not IsAppend,
            {ok, Handle #handle { offset = Offset1, write_buffer = [],
                                  write_buffer_size = 0, at_eof = AtEoF1 }};
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

is_reader(Mode) -> lists:member(read, Mode).

is_writer(Mode) -> lists:member(write, Mode).

is_appender(Mode) -> lists:member(append, Mode).

needs_seek(AtEof, _CurOffset, DesiredOffset)
  when DesiredOffset == cur orelse DesiredOffset == {cur, 0} ->
    {AtEof, false};
needs_seek(true, _CurOffset, DesiredOffset)
  when DesiredOffset == eof orelse DesiredOffset == {eof, 0} ->
    {true, false};
needs_seek(false, _CurOffset, DesiredOffset)
  when DesiredOffset == eof orelse DesiredOffset == {eof, 0} ->
    {true, true};
needs_seek(AtEof, 0, DesiredOffset)
  when DesiredOffset == bof orelse DesiredOffset == {bof, 0} ->
    {AtEof, false};
needs_seek(AtEof, CurOffset, CurOffset) ->
    {AtEof, false};
needs_seek(true, CurOffset, {bof, DesiredOffset})
  when DesiredOffset >= CurOffset ->
    {true, true};
needs_seek(true, _CurOffset, {cur, DesiredOffset})
  when DesiredOffset > 0 ->
    {true, true};
needs_seek(true, CurOffset, DesiredOffset) %% same as {bof, DO}
  when is_integer(DesiredOffset) andalso DesiredOffset >= CurOffset ->
    {true, true};
%% because we can't really track size, we could well end up at EoF and not know
needs_seek(_AtEoF, _CurOffset, _DesiredOffset) ->
    {false, true}.
