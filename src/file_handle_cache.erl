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

-behaviour(gen_server2).

-export([open/3, close/1, read/2, append/2, sync/1, position/2, truncate/1,
         last_sync_offset/1, current_virtual_offset/1, current_raw_offset/1,
         append_write_buffer/1, copy/3]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-record(file,
        { reader_count,
          has_writer,
          path
        }).

-record(handle,
        { hdl,
          offset,
          trusted_offset,
          is_dirty,
          write_buffer_size,
          write_buffer_size_limit,
          write_buffer,
          at_eof,
          is_write,
          is_read,
          mode,
          options,
          global_key,
          last_used_at
        }).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ref() :: any()).
-type(error() :: {'error', any()}).
-type(ok_or_error() :: ('ok' | error())).
-type(position() :: ('bof' | 'eof' | {'bof',integer()} | {'eof',integer()}
                     | {'cur',integer()} | integer())).

-spec(open/3 :: (string(), [any()], [any()]) -> ({'ok', ref()} | error())).
-spec(close/1 :: (ref()) -> ('ok' | error())).
-spec(read/2 :: (ref(), integer()) ->
             ({'ok', ([char()]|binary())} | eof | error())). 
-spec(append/2 :: (ref(), iodata()) -> ok_or_error()).
-spec(sync/1 :: (ref()) ->  ok_or_error()).
-spec(position/2 :: (ref(), position()) ->
             ({'ok', non_neg_integer()} | error())).
-spec(truncate/1 :: (ref()) -> ok_or_error()).
-spec(last_sync_offset/1 :: (ref()) -> ({'ok', integer()} | error())).
-spec(current_virtual_offset/1 :: (ref()) -> ({'ok', integer()} | error())).
-spec(current_raw_offset/1 :: (ref()) -> ({'ok', integer()} | error())).
-spec(append_write_buffer/1 :: (ref()) -> ok_or_error()).
-spec(copy/3 :: (ref(), ref(), non_neg_integer()) ->
             ({'ok', integer()} | error())).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], [{timeout, infinity}]).

open(Path, Mode, Options) ->
    case is_appender(Mode) of
        true -> {error, append_not_supported};
        false ->
            Path1 = filename:absname(Path),
            case get({Path1, fhc_path}) of
                {gref, GRef} ->
                    #file { reader_count = RCount, has_writer = HasWriter }
                        = File = get({GRef, fhc_file}),
                    Mode1 = lists:usort(Mode),
                    IsWriter = is_writer(Mode1),
                    case IsWriter andalso HasWriter of
                        true ->
                            {error, writer_exists};
                        false ->
                            RCount1 = case is_reader(Mode1) of
                                          true -> RCount + 1;
                                          false -> RCount
                                      end,
                            put({GRef, fhc_file},
                                File #file {
                                  reader_count = RCount1,
                                  has_writer = HasWriter orelse IsWriter }),
                            Ref = make_ref(),
                            case open1(Path1, Mode1, Options, Ref, GRef) of
                                {ok, _Handle} -> {ok, Ref};
                                Error -> Error
                            end
                    end;
                undefined ->
                    GRef = make_ref(),
                    put({Path1, fhc_path}, {gref, GRef}),
                    put({GRef, fhc_file},
                        #file { reader_count = 0, has_writer = false,
                                path = Path1 }),
                    open(Path, Mode, Options)
            end
    end.

close(Ref) ->
    case erase({Ref, fhc_handle}) of
        undefined -> ok;
        Handle ->
            case write_buffer(Handle) of
                {ok, #handle { hdl = Hdl, global_key = GRef, is_dirty = IsDirty,
                               is_read = IsReader, is_write = IsWriter,
                               last_used_at = Then }} ->
                    case Hdl of
                        closed -> ok;
                        _ -> ok = case IsDirty of
                                      true -> file:sync(Hdl);
                                      false -> ok
                                  end,
                             ok = file:close(Hdl),
                             with_age_tree(
                               fun (Tree) ->
                                       Tree1 = gb_trees:delete(Then, Tree),
                                       Oldest =
                                           case gb_trees:is_empty(Tree1) of
                                               true ->
                                                   undefined;
                                               false ->
                                                   {Oldest1, _Ref} =
                                                       gb_trees:smallest(Tree1),
                                                   Oldest1
                                           end,
                                       gen_server2:cast(
                                         ?SERVER, {self(), close, Oldest}),
                                       Tree1
                               end)
                    end,
                    #file { reader_count = RCount, has_writer = HasWriter,
                            path = Path } = File = get({GRef, fhc_file}),
                    RCount1 = case IsReader of
                                  true -> RCount - 1;
                                  false -> RCount
                              end,
                    HasWriter1 = HasWriter andalso not IsWriter,
                    case RCount1 =:= 0 andalso not HasWriter1 of
                        true -> erase({GRef, fhc_file}),
                                erase({Path, fhc_path});
                        false -> put({GRef, fhc_file},
                                     File #file { reader_count = RCount1,
                                                  has_writer = HasWriter1 })
                    end,
                    ok;
                {Error, Handle1} ->
                    put_handle(Ref, Handle1),
                    Error
            end
    end.

read(Ref, Count) ->
    case get_or_reopen(Ref) of
        {ok, #handle { is_read = false }} ->
            {error, not_open_for_reading};
        {ok, Handle} ->
            {Result, Handle1} =
                case write_buffer(Handle) of
                    {ok, Handle2 = #handle { hdl = Hdl, offset = Offset }} ->
                        case file:read(Hdl, Count) of
                            {ok, Data} = Obj ->
                                Size = iolist_size(Data),
                                {Obj,
                                 Handle2 #handle { offset = Offset + Size }};
                            eof -> {eof, Handle2 #handle { at_eof = true }};
                            Error -> {Error, Handle2}
                        end;
                    {Error, Handle2} -> {Error, Handle2}
                end,
            put_handle(Ref, Handle1),
            Result;
        Error -> Error
    end.

append(Ref, Data) ->
    case get_or_reopen(Ref) of
        {ok, #handle { is_write = false }} ->
            {error, not_open_for_writing};
        {ok, Handle} ->
            {Result, Handle1} =
                case maybe_seek(eof, Handle) of
                    {{ok, _Offset}, Handle2 = #handle { at_eof = true }} ->
                        write_to_buffer(Data, Handle2);
                    {{error, _} = Error, Handle2} ->
                        {Error, Handle2}
                end,
            put_handle(Ref, Handle1),
            Result;
        Error -> Error
    end.

sync(Ref) ->
    case get_or_reopen(Ref) of
        {ok, #handle { is_dirty = false, write_buffer = [] }} ->
            ok;
        {ok, Handle} ->
            %% write_buffer will set is_dirty, or leave it set if buffer empty
            {Result, Handle1} =
                case write_buffer(Handle) of
                    {ok, Handle2 = #handle {
                           hdl = Hdl, offset = Offset, is_dirty = true }} ->
                        case file:sync(Hdl) of
                            ok -> {ok,
                                   Handle2 #handle { trusted_offset = Offset,
                                                     is_dirty = false }};
                            Error -> {Error, Handle2}
                        end;
                    Error -> {Error, Handle}
                end,
            put_handle(Ref, Handle1),
            Result;
        Error -> Error
    end.

position(Ref, NewOffset) ->
    case get_or_reopen(Ref) of
        {ok, Handle} ->
            {Result, Handle1} =
                case write_buffer(Handle) of
                    {ok, Handle2} -> maybe_seek(NewOffset, Handle2);
                    {Error, Handle2} -> {Error, Handle2}
                end,
            put_handle(Ref, Handle1),
            Result;
        Error -> Error
    end.

truncate(Ref) ->
    case get_or_reopen(Ref) of
        {ok, #handle { is_write = false }} ->
            {error, not_open_for_writing};
        {ok, Handle} ->
            {Result, Handle1} =
                case write_buffer(Handle) of
                    {ok,
                     Handle2 = #handle { hdl = Hdl, offset = Offset,
                                         trusted_offset = TrustedOffset }} ->
                        case file:truncate(Hdl) of
                            ok ->
                                {ok,
                                 Handle2 #handle {
                                   at_eof = true,
                                   trusted_offset = lists:min([Offset,
                                                               TrustedOffset])
                                  }};
                            Error -> {Error, Handle2}
                        end;
                    {Error, Handle2} -> {Error, Handle2}
                end,
            put_handle(Ref, Handle1),
            Result;
        Error -> Error
    end.

last_sync_offset(Ref) ->
    case get_or_reopen(Ref) of
        {ok, #handle { trusted_offset = TrustedOffset }} -> {ok, TrustedOffset};
        Error -> Error
    end.

current_virtual_offset(Ref) ->
    case get_or_reopen(Ref) of
        {ok, #handle { at_eof = true, is_write = true, offset = Offset,
                       write_buffer_size = Size }} ->
            {ok, Offset + Size};
        {ok, #handle { offset = Offset }} -> {ok, Offset};
        Error -> Error
    end.

current_raw_offset(Ref) ->
    case get_or_reopen(Ref) of
        {ok, #handle { offset = Offset }} -> {ok, Offset};
        Error -> Error
    end.    

append_write_buffer(Ref) ->
    case get_or_reopen(Ref) of
        {ok, Handle} ->
            {Result, Handle1} = write_buffer(Handle),
            put_handle(Ref, Handle1),
            Result;
        Error -> Error
    end.

copy(Src, Dest, Count) ->
    case get_or_reopen(Src) of
        {ok, SHandle = #handle { is_read = true }} ->
            case get_or_reopen(Dest) of
                {ok, DHandle = #handle { is_write = true }} ->
                    {Result, SHandle1, DHandle1} =
                        case write_buffer(SHandle) of
                            {ok, SHandle2 = #handle { hdl = SHdl,
                                                      offset = SOffset }} ->
                                case write_buffer(DHandle) of
                                    {ok,
                                     DHandle2 = #handle { hdl = DHdl,
                                                          offset = DOffset }} ->
                                        Result1 = file:copy(SHdl, DHdl, Count),
                                        case Result1 of
                                            {ok, Count1} ->
                                                {Result1,
                                                 SHandle2 #handle {
                                                   offset = SOffset + Count1 },
                                                 DHandle2 #handle {
                                                   offset = DOffset + Count1 }};
                                            Error ->
                                                {Error, SHandle2, DHandle2}
                                        end;
                                    Error -> {Error, SHandle2, DHandle}
                                end;
                            Error -> {Error, SHandle, DHandle}
                        end,
                    put_handle(Src, SHandle1),
                    put_handle(Dest, DHandle1),
                    Result;
                {ok, _} -> {error, destination_not_open_for_writing};
                Error -> Error
            end;
        {ok, _} -> {error, source_not_open_for_reading};
        Error -> Error
    end.
                                

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

get_or_reopen(Ref) ->
    case get({Ref, fhc_handle}) of
        undefined -> {error, not_open, Ref};
        #handle { hdl = closed, mode = Mode, global_key = GRef,
                  options = Options } ->
            #file { path = Path } = get({GRef, fhc_file}),
            open1(Path, Mode, Options, Ref, GRef);
        Handle ->
            {ok, Handle}
    end.

get_or_create_age_tree() ->
    case get(fhc_age_tree) of
        undefined -> gb_trees:empty();
        AgeTree -> AgeTree
    end.

with_age_tree(Fun) ->
    put(fhc_age_tree, Fun(get_or_create_age_tree())).

put_handle(Ref, Handle = #handle { last_used_at = Then }) ->
    Now = now(),
    with_age_tree(
      fun (Tree) ->
              gb_trees:insert(Now, Ref, gb_trees:delete(Then, Tree))
      end),
    put({Ref, fhc_handle}, Handle #handle { last_used_at = Now }).

open1(Path, Mode, Options, Ref, GRef) ->
    case file:open(Path, Mode) of
        {ok, Hdl} ->
            WriteBufferSize =
                case proplists:get_value(write_buffer, Options, unbuffered) of
                    unbuffered -> 0;
                    infinity -> infinity;
                    N when is_integer(N) -> N
                end,
            Now = now(),
            Handle =
                #handle { hdl = Hdl, offset = 0, trusted_offset = 0,
                          write_buffer_size = 0, options = Options,
                          write_buffer_size_limit = WriteBufferSize,
                          write_buffer = [], at_eof = false, mode = Mode,
                          is_write = is_writer(Mode), is_read = is_reader(Mode),
                          global_key = GRef, last_used_at = Now,
                          is_dirty = false },
            put({Ref, fhc_handle}, Handle),
            with_age_tree(fun (Tree) ->
                                  Tree1 = gb_trees:insert(Now, Ref, Tree),
                                  {Oldest, _Ref} = gb_trees:smallest(Tree1),
                                  gen_server2:cast(?SERVER,
                                                   {self(), open, Oldest}),
                                  Tree1
                          end),
            {ok, Handle};
        {error, Reason} ->
            {error, Reason}
    end.

maybe_seek(NewOffset, Handle = #handle { hdl = Hdl, at_eof = AtEoF,
                                         offset = Offset }) ->
    {AtEoF1, NeedsSeek} = needs_seek(AtEoF, Offset, NewOffset),
    Result = case NeedsSeek of
                 true -> file:position(Hdl, NewOffset);
                 false -> {ok, Offset}
             end,
    case Result of
        {ok, Offset1} ->
            {Result, Handle #handle { at_eof = AtEoF1, offset = Offset1 }};
        {error, _} = Error -> {Error, Handle}
    end.

write_to_buffer(Data, Handle = #handle { hdl = Hdl, offset = Offset,
                                         write_buffer_size_limit = 0 }) ->
    Offset1 = Offset + iolist_size(Data),
    {file:write(Hdl, Data),
     Handle #handle { is_dirty = true, offset = Offset1 }};
write_to_buffer(Data, Handle =
                #handle { write_buffer = WriteBuffer,
                          write_buffer_size = Size,
                          write_buffer_size_limit = Limit }) ->
    Size1 = Size + iolist_size(Data),
    Handle1 = Handle #handle { write_buffer = [ Data | WriteBuffer ],
                               write_buffer_size = Size1 },
    case Limit /= infinity andalso Size1 > Limit of
        true -> write_buffer(Handle1);
        false -> {ok, Handle1}
    end.

write_buffer(Handle = #handle { write_buffer = [] }) ->
    {ok, Handle};
write_buffer(Handle = #handle { hdl = Hdl, offset = Offset,
                                write_buffer = WriteBuffer,
                                write_buffer_size = DataSize,
                                at_eof = true }) ->
    case file:write(Hdl, lists:reverse(WriteBuffer)) of
        ok ->
            Offset1 = Offset + DataSize,
            {ok, Handle #handle { offset = Offset1, write_buffer = [],
                                  write_buffer_size = 0, is_dirty = true }};
        {error, _} = Error ->
            {Error, Handle}
    end.

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

%%----------------------------------------------------------------------------
%% gen_server
%%----------------------------------------------------------------------------

init([]) ->
    {ok, state}.

handle_call(_Msg, _From, State) ->
    {reply, message_not_understood, State}.

handle_cast(Msg, State) ->
    io:format("~p~n", [Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
