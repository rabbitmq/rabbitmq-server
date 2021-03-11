%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(file_handle_cache).

%% A File Handle Cache
%%
%% This extends a subset of the functionality of the Erlang file
%% module. In the below, we use "file handle" to specifically refer to
%% file handles, and "file descriptor" to refer to descriptors which
%% are not file handles, e.g. sockets.
%%
%% Some constraints
%% 1) This supports one writer, multiple readers per file. Nothing
%% else.
%% 2) Do not open the same file from different processes. Bad things
%% may happen, especially for writes.
%% 3) Writes are all appends. You cannot write to the middle of a
%% file, although you can truncate and then append if you want.
%% 4) There are read and write buffers. Feel free to use the read_ahead
%% mode, but beware of the interaction between that buffer and the write
%% buffer.
%%
%% Some benefits
%% 1) You do not have to remember to call sync before close
%% 2) Buffering is much more flexible than with the plain file module,
%% and you can control when the buffer gets flushed out. This means
%% that you can rely on reads-after-writes working, without having to
%% call the expensive sync.
%% 3) Unnecessary calls to position and sync get optimised out.
%% 4) You can find out what your 'real' offset is, and what your
%% 'virtual' offset is (i.e. where the hdl really is, and where it
%% would be after the write buffer is written out).
%%
%% There is also a server component which serves to limit the number
%% of open file descriptors. This is a hard limit: the server
%% component will ensure that clients do not have more file
%% descriptors open than it's configured to allow.
%%
%% On open, the client requests permission from the server to open the
%% required number of file handles. The server may ask the client to
%% close other file handles that it has open, or it may queue the
%% request and ask other clients to close file handles they have open
%% in order to satisfy the request. Requests are always satisfied in
%% the order they arrive, even if a latter request (for a small number
%% of file handles) can be satisfied before an earlier request (for a
%% larger number of file handles). On close, the client sends a
%% message to the server. These messages allow the server to keep
%% track of the number of open handles. The client also keeps a
%% gb_tree which is updated on every use of a file handle, mapping the
%% time at which the file handle was last used (timestamp) to the
%% handle. Thus the smallest key in this tree maps to the file handle
%% that has not been used for the longest amount of time. This
%% smallest key is included in the messages to the server. As such,
%% the server keeps track of when the least recently used file handle
%% was used *at the point of the most recent open or close* by each
%% client.
%%
%% Note that this data can go very out of date, by the client using
%% the least recently used handle.
%%
%% When the limit is exceeded (i.e. the number of open file handles is
%% at the limit and there are pending 'open' requests), the server
%% calculates the average age of the last reported least recently used
%% file handle of all the clients. It then tells all the clients to
%% close any handles not used for longer than this average, by
%% invoking the callback the client registered. The client should
%% receive this message and pass it into
%% set_maximum_since_use/1. However, it is highly possible this age
%% will be greater than the ages of all the handles the client knows
%% of because the client has used its file handles in the mean
%% time. Thus at this point the client reports to the server the
%% current timestamp at which its least recently used file handle was
%% last used. The server will check two seconds later that either it
%% is back under the limit, in which case all is well again, or if
%% not, it will calculate a new average age. Its data will be much
%% more recent now, and so it is very likely that when this is
%% communicated to the clients, the clients will close file handles.
%% (In extreme cases, where it's very likely that all clients have
%% used their open handles since they last sent in an update, which
%% would mean that the average will never cause any file handles to
%% be closed, the server can send out an average age of 0, resulting
%% in all available clients closing all their file handles.)
%%
%% Care is taken to ensure that (a) processes which are blocked
%% waiting for file descriptors to become available are not sent
%% requests to close file handles; and (b) given it is known how many
%% file handles a process has open, when the average age is forced to
%% 0, close messages are only sent to enough processes to release the
%% correct number of file handles and the list of processes is
%% randomly shuffled. This ensures we don't cause processes to
%% needlessly close file handles, and ensures that we don't always
%% make such requests of the same processes.
%%
%% The advantage of this scheme is that there is only communication
%% from the client to the server on open, close, and when in the
%% process of trying to reduce file handle usage. There is no
%% communication from the client to the server on normal file handle
%% operations. This scheme forms a feed-back loop - the server does
%% not care which file handles are closed, just that some are, and it
%% checks this repeatedly when over the limit.
%%
%% Handles which are closed as a result of the server are put into a
%% "soft-closed" state in which the handle is closed (data flushed out
%% and sync'd first) but the state is maintained. The handle will be
%% fully reopened again as soon as needed, thus users of this library
%% do not need to worry about their handles being closed by the server
%% - reopening them when necessary is handled transparently.
%%
%% The server also supports obtain, release and transfer. obtain/{0,1}
%% blocks until a file descriptor is available, at which point the
%% requesting process is considered to 'own' more descriptor(s).
%% release/{0,1} is the inverse operation and releases previously obtained
%% descriptor(s). transfer/{1,2} transfers ownership of file descriptor(s)
%% between processes. It is non-blocking. Obtain has a
%% lower limit, set by the ?OBTAIN_LIMIT/1 macro. File handles can use
%% the entire limit, but will be evicted by obtain calls up to the
%% point at which no more obtain calls can be satisfied by the obtains
%% limit. Thus there will always be some capacity available for file
%% handles. Processes that use obtain are never asked to return them,
%% and they are not managed in any way by the server. It is simply a
%% mechanism to ensure that processes that need file descriptors such
%% as sockets can do so in such a way that the overall number of open
%% file descriptors is managed.
%%
%% The callers of register_callback/3, obtain, and the argument of
%% transfer are monitored, reducing the count of handles in use
%% appropriately when the processes terminate.

-behaviour(gen_server2).

-export([register_callback/3]).
-export([open/3, close/1, read/2, append/2, needs_sync/1, sync/1, position/2,
         truncate/1, current_virtual_offset/1, current_raw_offset/1, flush/1,
         copy/3, set_maximum_since_use/1, delete/1, clear/1,
         open_with_absolute_path/3]).
-export([obtain/0, obtain/1, release/0, release/1, transfer/1, transfer/2,
         set_limit/1, get_limit/0, info_keys/0, with_handle/1, with_handle/2,
         info/0, info/1, clear_read_cache/0, clear_process_read_cache/0]).
-export([set_reservation/0, set_reservation/1, release_reservation/0]).
-export([ulimit/0]).

-export([start_link/0, start_link/2, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3, prioritise_cast/3]).

-define(SERVER, ?MODULE).
%% Reserve 3 handles for ra usage: wal, segment writer and a dets table
-define(RESERVED_FOR_OTHERS, 100 + 3).

-define(FILE_HANDLES_LIMIT_OTHER, 1024).
-define(FILE_HANDLES_CHECK_INTERVAL, 2000).

-define(OBTAIN_LIMIT(LIMIT), trunc((LIMIT * 0.9) - 2)).
-define(CLIENT_ETS_TABLE, file_handle_cache_client).
-define(ELDERS_ETS_TABLE, file_handle_cache_elders).

%%----------------------------------------------------------------------------

-record(file,
        { reader_count,
          has_writer
        }).

-record(handle,
        { hdl,
          ref,
          offset,
          is_dirty,
          write_buffer_size,
          write_buffer_size_limit,
          write_buffer,
          read_buffer,
          read_buffer_pos,
          read_buffer_rem,        %% Num of bytes from pos to end
          read_buffer_size,       %% Next size of read buffer to use
          read_buffer_size_limit, %% Max size of read buffer to use
          read_buffer_usage,      %% Bytes we have read from it, for tuning
          at_eof,
          path,
          mode,
          options,
          is_write,
          is_read,
          last_used_at
        }).

-record(fhc_state,
        { elders,
          limit,
          open_count,
          open_pending,
          obtain_limit, %%socket
          obtain_count_socket,
          obtain_count_file,
          obtain_pending_socket,
          obtain_pending_file,
          clients,
          timer_ref,
          alarm_set,
          alarm_clear,
          reserve_count_socket,
          reserve_count_file
        }).

-record(cstate,
        { pid,
          callback,
          opened,
          obtained_socket,
          obtained_file,
          blocked,
          pending_closes,
          reserved_socket,
          reserved_file
        }).

-record(pending,
        { kind,
          pid,
          requested,
          from
        }).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-type ref() :: any().
-type ok_or_error() :: 'ok' | {'error', any()}.
-type val_or_error(T) :: {'ok', T} | {'error', any()}.
-type position() :: ('bof' | 'eof' | non_neg_integer() |
                     {('bof' |'eof'), non_neg_integer()} |
                     {'cur', integer()}).
-type offset() :: non_neg_integer().

-spec register_callback(atom(), atom(), [any()]) -> 'ok'.
-spec open
        (file:filename(), [any()],
         [{'write_buffer', (non_neg_integer() | 'infinity' | 'unbuffered')} |
          {'read_buffer', (non_neg_integer() | 'unbuffered')}]) ->
            val_or_error(ref()).
-spec open_with_absolute_path
        (file:filename(), [any()],
         [{'write_buffer', (non_neg_integer() | 'infinity' | 'unbuffered')} |
          {'read_buffer', (non_neg_integer() | 'unbuffered')}]) ->
            val_or_error(ref()).
-spec close(ref()) -> ok_or_error().
-spec read
        (ref(), non_neg_integer()) -> val_or_error([char()] | binary()) | 'eof'.
-spec append(ref(), iodata()) -> ok_or_error().
-spec sync(ref()) ->  ok_or_error().
-spec position(ref(), position()) -> val_or_error(offset()).
-spec truncate(ref()) -> ok_or_error().
-spec current_virtual_offset(ref()) -> val_or_error(offset()).
-spec current_raw_offset(ref()) -> val_or_error(offset()).
-spec flush(ref()) -> ok_or_error().
-spec copy(ref(), ref(), non_neg_integer()) -> val_or_error(non_neg_integer()).
-spec delete(ref()) -> ok_or_error().
-spec clear(ref()) -> ok_or_error().
-spec set_maximum_since_use(non_neg_integer()) -> 'ok'.
-spec obtain() -> 'ok'.
-spec obtain(non_neg_integer()) -> 'ok'.
-spec release() -> 'ok'.
-spec release(non_neg_integer()) -> 'ok'.
-spec transfer(pid()) -> 'ok'.
-spec transfer(pid(), non_neg_integer()) -> 'ok'.
-spec with_handle(fun(() -> A)) -> A.
-spec with_handle(non_neg_integer(), fun(() -> A)) -> A.
-spec set_limit(non_neg_integer()) -> 'ok'.
-spec get_limit() -> non_neg_integer().
-spec info_keys() -> rabbit_types:info_keys().
-spec info() -> rabbit_types:infos().
-spec info([atom()]) -> rabbit_types:infos().
-spec ulimit() -> 'unknown' | non_neg_integer().

%%----------------------------------------------------------------------------
-define(INFO_KEYS, [total_limit, total_used, sockets_limit, sockets_used]).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    start_link(fun alarm_handler:set_alarm/1, fun alarm_handler:clear_alarm/1).

start_link(AlarmSet, AlarmClear) ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [AlarmSet, AlarmClear],
                           [{timeout, infinity}]).

register_callback(M, F, A)
  when is_atom(M) andalso is_atom(F) andalso is_list(A) ->
    gen_server2:cast(?SERVER, {register_callback, self(), {M, F, A}}).

open(Path, Mode, Options) ->
    open_with_absolute_path(filename:absname(Path), Mode, Options).

open_with_absolute_path(Path, Mode, Options) ->
    File1 = #file { reader_count = RCount, has_writer = HasWriter } =
        case get({Path, fhc_file}) of
            File = #file {} -> File;
            undefined       -> #file { reader_count = 0,
                                       has_writer = false }
        end,
    Mode1 = append_to_write(Mode),
    IsWriter = is_writer(Mode1),
    case IsWriter andalso HasWriter of
        true  -> {error, writer_exists};
        false -> {ok, Ref} = new_closed_handle(Path, Mode1, Options),
                 case get_or_reopen_timed([{Ref, new}]) of
                     {ok, [_Handle1]} ->
                         RCount1 = case is_reader(Mode1) of
                                       true  -> RCount + 1;
                                       false -> RCount
                                   end,
                         HasWriter1 = HasWriter orelse IsWriter,
                         put({Path, fhc_file},
                             File1 #file { reader_count = RCount1,
                                           has_writer = HasWriter1 }),
                         {ok, Ref};
                     Error ->
                         erase({Ref, fhc_handle}),
                         Error
                 end
    end.

close(Ref) ->
    case erase({Ref, fhc_handle}) of
        undefined -> ok;
        Handle    -> case hard_close(Handle) of
                         ok               -> ok;
                         {Error, Handle1} -> put_handle(Ref, Handle1),
                                             Error
                     end
    end.

read(Ref, Count) ->
    with_flushed_handles(
      [Ref], keep,
      fun ([#handle { is_read = false }]) ->
              {error, not_open_for_reading};
          ([#handle{read_buffer_size_limit = 0,
                    hdl = Hdl, offset = Offset} = Handle]) ->
              %% The read buffer is disabled. This is just an
              %% optimization: the clauses below can handle this case.
              case prim_file_read(Hdl, Count) of
                  {ok, Data} -> {{ok, Data},
                                 [Handle#handle{offset = Offset+size(Data)}]};
                  eof        -> {eof, [Handle #handle { at_eof = true }]};
                  Error      -> {Error, Handle}
              end;
          ([Handle = #handle{read_buffer       = Buf,
                             read_buffer_pos   = BufPos,
                             read_buffer_rem   = BufRem,
                             read_buffer_usage = BufUsg,
                             offset            = Offset}])
            when BufRem >= Count ->
              <<_:BufPos/binary, Res:Count/binary, _/binary>> = Buf,
              {{ok, Res}, [Handle#handle{offset            = Offset + Count,
                                         read_buffer_pos   = BufPos + Count,
                                         read_buffer_rem   = BufRem - Count,
                                         read_buffer_usage = BufUsg + Count }]};
          ([Handle0]) ->
              maybe_reduce_read_cache([Ref]),
              Handle = #handle{read_buffer      = Buf,
                               read_buffer_pos  = BufPos,
                               read_buffer_rem  = BufRem,
                               read_buffer_size = BufSz,
                               hdl              = Hdl,
                               offset           = Offset}
                  = tune_read_buffer_limit(Handle0, Count),
              WantedCount = Count - BufRem,
              case prim_file_read(Hdl, max(BufSz, WantedCount)) of
                  {ok, Data} ->
                      <<_:BufPos/binary, BufTl/binary>> = Buf,
                      ReadCount = size(Data),
                      case ReadCount < WantedCount of
                          true ->
                              OffSet1 = Offset + BufRem + ReadCount,
                              {{ok, <<BufTl/binary, Data/binary>>},
                               [reset_read_buffer(
                                  Handle#handle{offset = OffSet1})]};
                          false ->
                              <<Hd:WantedCount/binary, _/binary>> = Data,
                              OffSet1 = Offset + BufRem + WantedCount,
                              BufRem1 = ReadCount - WantedCount,
                              {{ok, <<BufTl/binary, Hd/binary>>},
                               [Handle#handle{offset            = OffSet1,
                                              read_buffer       = Data,
                                              read_buffer_pos   = WantedCount,
                                              read_buffer_rem   = BufRem1,
                                              read_buffer_usage = WantedCount}]}
                      end;
                  eof ->
                      {eof, [Handle #handle { at_eof = true }]};
                  Error ->
                      {Error, [reset_read_buffer(Handle)]}
              end
      end).

append(Ref, Data) ->
    with_handles(
      [Ref],
      fun ([#handle { is_write = false }]) ->
              {error, not_open_for_writing};
          ([Handle]) ->
              case maybe_seek(eof, Handle) of
                  {{ok, _Offset}, #handle { hdl = Hdl, offset = Offset,
                                            write_buffer_size_limit = 0,
                                            at_eof = true } = Handle1} ->
                      Offset1 = Offset + iolist_size(Data),
                      {prim_file_write(Hdl, Data),
                       [Handle1 #handle { is_dirty = true, offset = Offset1 }]};
                  {{ok, _Offset}, #handle { write_buffer = WriteBuffer,
                                            write_buffer_size = Size,
                                            write_buffer_size_limit = Limit,
                                            at_eof = true } = Handle1} ->
                      WriteBuffer1 = [Data | WriteBuffer],
                      Size1 = Size + iolist_size(Data),
                      Handle2 = Handle1 #handle { write_buffer = WriteBuffer1,
                                                  write_buffer_size = Size1 },
                      case Limit =/= infinity andalso Size1 > Limit of
                          true  -> {Result, Handle3} = write_buffer(Handle2),
                                   {Result, [Handle3]};
                          false -> {ok, [Handle2]}
                      end;
                  {{error, _} = Error, Handle1} ->
                      {Error, [Handle1]}
              end
      end).

sync(Ref) ->
    with_flushed_handles(
      [Ref], keep,
      fun ([#handle { is_dirty = false, write_buffer = [] }]) ->
              ok;
          ([Handle = #handle { hdl = Hdl,
                               is_dirty = true, write_buffer = [] }]) ->
              case prim_file_sync(Hdl) of
                  ok    -> {ok, [Handle #handle { is_dirty = false }]};
                  Error -> {Error, [Handle]}
              end
      end).

needs_sync(Ref) ->
    %% This must *not* use with_handles/2; see bug 25052
    case get({Ref, fhc_handle}) of
        #handle { is_dirty = false, write_buffer = [] } -> false;
        #handle {}                                      -> true
    end.

position(Ref, NewOffset) ->
    with_flushed_handles(
      [Ref], keep,
      fun ([Handle]) -> {Result, Handle1} = maybe_seek(NewOffset, Handle),
                        {Result, [Handle1]}
      end).

truncate(Ref) ->
    with_flushed_handles(
      [Ref],
      fun ([Handle1 = #handle { hdl = Hdl }]) ->
              case prim_file:truncate(Hdl) of
                  ok    -> {ok, [Handle1 #handle { at_eof = true }]};
                  Error -> {Error, [Handle1]}
              end
      end).

current_virtual_offset(Ref) ->
    with_handles([Ref], fun ([#handle { at_eof = true, is_write = true,
                                        offset = Offset,
                                        write_buffer_size = Size }]) ->
                                {ok, Offset + Size};
                            ([#handle { offset = Offset }]) ->
                                {ok, Offset}
                        end).

current_raw_offset(Ref) ->
    with_handles([Ref], fun ([Handle]) -> {ok, Handle #handle.offset} end).

flush(Ref) ->
    with_flushed_handles([Ref], fun ([Handle]) -> {ok, [Handle]} end).

copy(Src, Dest, Count) ->
    with_flushed_handles(
      [Src, Dest],
      fun ([SHandle = #handle { is_read  = true, hdl = SHdl, offset = SOffset },
            DHandle = #handle { is_write = true, hdl = DHdl, offset = DOffset }]
          ) ->
              case prim_file:copy(SHdl, DHdl, Count) of
                  {ok, Count1} = Result1 ->
                      {Result1,
                       [SHandle #handle { offset = SOffset + Count1 },
                        DHandle #handle { offset = DOffset + Count1,
                                          is_dirty = true }]};
                  Error ->
                      {Error, [SHandle, DHandle]}
              end;
          (_Handles) ->
              {error, incorrect_handle_modes}
      end).

delete(Ref) ->
    case erase({Ref, fhc_handle}) of
        undefined ->
            ok;
        Handle = #handle { path = Path } ->
            case hard_close(Handle #handle { is_dirty = false,
                                             write_buffer = [] }) of
                ok               -> prim_file:delete(Path);
                {Error, Handle1} -> put_handle(Ref, Handle1),
                                    Error
            end
    end.

clear(Ref) ->
    with_handles(
      [Ref],
      fun ([#handle { at_eof = true, write_buffer_size = 0, offset = 0 }]) ->
              ok;
          ([Handle]) ->
              case maybe_seek(bof, Handle#handle{write_buffer      = [],
                                                 write_buffer_size = 0}) of
                  {{ok, 0}, Handle1 = #handle { hdl = Hdl }} ->
                      case prim_file:truncate(Hdl) of
                          ok    -> {ok, [Handle1 #handle { at_eof = true }]};
                          Error -> {Error, [Handle1]}
                      end;
                  {{error, _} = Error, Handle1} ->
                      {Error, [Handle1]}
              end
      end).

set_maximum_since_use(MaximumAge) ->
    Now = erlang:monotonic_time(),
    case lists:foldl(
           fun ({{Ref, fhc_handle},
                 Handle = #handle { hdl = Hdl, last_used_at = Then }}, Rep) ->
                   case Hdl =/= closed andalso
                        erlang:convert_time_unit(Now - Then,
                                                 native,
                                                 micro_seconds)
                          >= MaximumAge of
                       true  -> soft_close(Ref, Handle) orelse Rep;
                       false -> Rep
                   end;
               (_KeyValuePair, Rep) ->
                   Rep
           end, false, get()) of
        false -> age_tree_change(), ok;
        true  -> ok
    end.

obtain()          -> obtain(1).
set_reservation() -> set_reservation(1).
release()         -> release(1).
release_reservation() -> release_reservation(file).
transfer(Pid)     -> transfer(Pid, 1).

obtain(Count)          -> obtain(Count, socket).
set_reservation(Count) -> set_reservation(Count, file).
release(Count)         -> release(Count, socket).

with_handle(Fun) ->
    with_handle(1, Fun).

with_handle(N, Fun) ->
    ok = obtain(N, file),
    try Fun()
    after ok = release(N, file)
    end.

obtain(Count, Type) when Count > 0 ->
    %% If the FHC isn't running, obtains succeed immediately.
    case whereis(?SERVER) of
        undefined -> ok;
        _         -> gen_server2:call(
                       ?SERVER, {obtain, Count, Type, self()}, infinity)
    end.

set_reservation(Count, Type) when Count > 0 ->
    %% If the FHC isn't running, reserve succeed immediately.
    case whereis(?SERVER) of
        undefined -> ok;
        _         -> gen_server2:cast(?SERVER, {set_reservation, Count, Type, self()})
    end.

release(Count, Type) when Count > 0 ->
    gen_server2:cast(?SERVER, {release, Count, Type, self()}).

release_reservation(Type) ->
    gen_server2:cast(?SERVER, {release_reservation, Type, self()}).

transfer(Pid, Count) when Count > 0 ->
    gen_server2:cast(?SERVER, {transfer, Count, self(), Pid}).

set_limit(Limit) ->
    gen_server2:call(?SERVER, {set_limit, Limit}, infinity).

get_limit() ->
    gen_server2:call(?SERVER, get_limit, infinity).

info_keys() -> ?INFO_KEYS.

info() -> info(?INFO_KEYS).
info(Items) -> gen_server2:call(?SERVER, {info, Items}, infinity).

clear_read_cache() ->
    gen_server2:cast(?SERVER, clear_read_cache).

clear_process_read_cache() ->
    [
     begin
         Handle1 = reset_read_buffer(Handle),
         put({Ref, fhc_handle}, Handle1)
     end ||
        {{Ref, fhc_handle}, Handle} <- get(),
        size(Handle#handle.read_buffer) > 0
    ].

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

prim_file_read(Hdl, Size) ->
    file_handle_cache_stats:update(
      io_read, Size, fun() -> prim_file:read(Hdl, Size) end).

prim_file_write(Hdl, Bytes) ->
    file_handle_cache_stats:update(
      io_write, iolist_size(Bytes), fun() -> prim_file:write(Hdl, Bytes) end).

prim_file_sync(Hdl) ->
    file_handle_cache_stats:update(io_sync, fun() -> prim_file:sync(Hdl) end).

prim_file_position(Hdl, NewOffset) ->
    file_handle_cache_stats:update(
      io_seek, fun() -> prim_file:position(Hdl, NewOffset) end).

is_reader(Mode) -> lists:member(read, Mode).

is_writer(Mode) -> lists:member(write, Mode).

append_to_write(Mode) ->
    case lists:member(append, Mode) of
        true  -> [write | Mode -- [append, write]];
        false -> Mode
    end.

with_handles(Refs, Fun) ->
    with_handles(Refs, reset, Fun).

with_handles(Refs, ReadBuffer, Fun) ->
    case get_or_reopen_timed([{Ref, reopen} || Ref <- Refs]) of
        {ok, Handles0} ->
            Handles = case ReadBuffer of
                          reset -> [reset_read_buffer(H) || H <- Handles0];
                          keep  -> Handles0
                      end,
            case Fun(Handles) of
                {Result, Handles1} when is_list(Handles1) ->
                    _ = lists:zipwith(fun put_handle/2, Refs, Handles1),
                    Result;
                Result ->
                    Result
            end;
        Error ->
            Error
    end.

with_flushed_handles(Refs, Fun) ->
    with_flushed_handles(Refs, reset, Fun).

with_flushed_handles(Refs, ReadBuffer, Fun) ->
    with_handles(
      Refs, ReadBuffer,
      fun (Handles) ->
              case lists:foldl(
                     fun (Handle, {ok, HandlesAcc}) ->
                             {Res, Handle1} = write_buffer(Handle),
                             {Res, [Handle1 | HandlesAcc]};
                         (Handle, {Error, HandlesAcc}) ->
                             {Error, [Handle | HandlesAcc]}
                     end, {ok, []}, Handles) of
                  {ok, Handles1} ->
                      Fun(lists:reverse(Handles1));
                  {Error, Handles1} ->
                      {Error, lists:reverse(Handles1)}
              end
      end).

get_or_reopen_timed(RefNewOrReopens) ->
    file_handle_cache_stats:update(
      io_file_handle_open_attempt, fun() -> get_or_reopen(RefNewOrReopens) end).

get_or_reopen(RefNewOrReopens) ->
    case partition_handles(RefNewOrReopens) of
        {OpenHdls, []} ->
            {ok, [Handle || {_Ref, Handle} <- OpenHdls]};
        {OpenHdls, ClosedHdls} ->
            Oldest = oldest(get_age_tree(),
                            fun () -> erlang:monotonic_time() end),
            case gen_server2:call(?SERVER, {open, self(), length(ClosedHdls),
                                            Oldest}, infinity) of
                ok ->
                    case reopen(ClosedHdls) of
                        {ok, RefHdls}  -> sort_handles(RefNewOrReopens,
                                                       OpenHdls, RefHdls, []);
                        Error          -> Error
                    end;
                close ->
                    [soft_close(Ref, Handle) ||
                        {{Ref, fhc_handle}, Handle = #handle { hdl = Hdl }} <-
                            get(),
                        Hdl =/= closed],
                    get_or_reopen(RefNewOrReopens)
            end
    end.

reopen(ClosedHdls) -> reopen(ClosedHdls, get_age_tree(), []).

reopen([], Tree, RefHdls) ->
    put_age_tree(Tree),
    {ok, lists:reverse(RefHdls)};
reopen([{Ref, NewOrReopen, Handle = #handle { hdl          = closed,
                                              path         = Path,
                                              mode         = Mode0,
                                              offset       = Offset,
                                              last_used_at = undefined }} |
        RefNewOrReopenHdls] = ToOpen, Tree, RefHdls) ->
    Mode = case NewOrReopen of
               new    -> Mode0;
               reopen -> file_handle_cache_stats:update(io_reopen),
                         [read | Mode0]
           end,
    case prim_file:open(Path, Mode) of
        {ok, Hdl} ->
            Now = erlang:monotonic_time(),
            {{ok, _Offset}, Handle1} =
                maybe_seek(Offset, reset_read_buffer(
                                     Handle#handle{hdl              = Hdl,
                                                   offset           = 0,
                                                   last_used_at     = Now})),
            put({Ref, fhc_handle}, Handle1),
            reopen(RefNewOrReopenHdls, gb_trees:insert({Now, Ref}, true, Tree),
                   [{Ref, Handle1} | RefHdls]);
        Error ->
            %% NB: none of the handles in ToOpen are in the age tree
            Oldest = oldest(Tree, fun () -> undefined end),
            [gen_server2:cast(?SERVER, {close, self(), Oldest}) || _ <- ToOpen],
            put_age_tree(Tree),
            Error
    end.

partition_handles(RefNewOrReopens) ->
    lists:foldr(
      fun ({Ref, NewOrReopen}, {Open, Closed}) ->
              case get({Ref, fhc_handle}) of
                  #handle { hdl = closed } = Handle ->
                      {Open, [{Ref, NewOrReopen, Handle} | Closed]};
                  #handle {} = Handle ->
                      {[{Ref, Handle} | Open], Closed}
              end
      end, {[], []}, RefNewOrReopens).

sort_handles([], [], [], Acc) ->
    {ok, lists:reverse(Acc)};
sort_handles([{Ref, _} | RefHdls], [{Ref, Handle} | RefHdlsA], RefHdlsB, Acc) ->
    sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]);
sort_handles([{Ref, _} | RefHdls], RefHdlsA, [{Ref, Handle} | RefHdlsB], Acc) ->
    sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]).

put_handle(Ref, Handle = #handle { last_used_at = Then }) ->
    Now = erlang:monotonic_time(),
    age_tree_update(Then, Now, Ref),
    put({Ref, fhc_handle}, Handle #handle { last_used_at = Now }).

with_age_tree(Fun) -> put_age_tree(Fun(get_age_tree())).

get_age_tree() ->
    case get(fhc_age_tree) of
        undefined -> gb_trees:empty();
        AgeTree   -> AgeTree
    end.

put_age_tree(Tree) -> put(fhc_age_tree, Tree).

age_tree_update(Then, Now, Ref) ->
    with_age_tree(
      fun (Tree) ->
              gb_trees:insert({Now, Ref}, true,
                              gb_trees:delete_any({Then, Ref}, Tree))
      end).

age_tree_delete(Then, Ref) ->
    with_age_tree(
      fun (Tree) ->
              Tree1 = gb_trees:delete_any({Then, Ref}, Tree),
              Oldest = oldest(Tree1, fun () -> undefined end),
              gen_server2:cast(?SERVER, {close, self(), Oldest}),
              Tree1
      end).

age_tree_change() ->
    with_age_tree(
      fun (Tree) ->
              case gb_trees:is_empty(Tree) of
                  true  -> Tree;
                  false -> {{Oldest, _Ref}, _} = gb_trees:smallest(Tree),
                           gen_server2:cast(?SERVER, {update, self(), Oldest}),
                           Tree
              end
      end).

oldest(Tree, DefaultFun) ->
    case gb_trees:is_empty(Tree) of
        true  -> DefaultFun();
        false -> {{Oldest, _Ref}, _} = gb_trees:smallest(Tree),
                 Oldest
    end.

new_closed_handle(Path, Mode, Options) ->
    WriteBufferSize =
        case application:get_env(rabbit, fhc_write_buffering) of
            {ok, false} -> 0;
            {ok, true}  ->
                case proplists:get_value(write_buffer, Options, unbuffered) of
                    unbuffered           -> 0;
                    infinity             -> infinity;
                    N when is_integer(N) -> N
                end
        end,
    ReadBufferSize =
        case application:get_env(rabbit, fhc_read_buffering) of
            {ok, false} -> 0;
            {ok, true}  ->
                case proplists:get_value(read_buffer, Options, unbuffered) of
                    unbuffered             -> 0;
                    N2 when is_integer(N2) -> N2
                end
        end,
    Ref = make_ref(),
    put({Ref, fhc_handle}, #handle { hdl                     = closed,
                                     ref                     = Ref,
                                     offset                  = 0,
                                     is_dirty                = false,
                                     write_buffer_size       = 0,
                                     write_buffer_size_limit = WriteBufferSize,
                                     write_buffer            = [],
                                     read_buffer             = <<>>,
                                     read_buffer_pos         = 0,
                                     read_buffer_rem         = 0,
                                     read_buffer_size        = ReadBufferSize,
                                     read_buffer_size_limit  = ReadBufferSize,
                                     read_buffer_usage       = 0,
                                     at_eof                  = false,
                                     path                    = Path,
                                     mode                    = Mode,
                                     options                 = Options,
                                     is_write                = is_writer(Mode),
                                     is_read                 = is_reader(Mode),
                                     last_used_at            = undefined }),
    {ok, Ref}.

soft_close(Ref, Handle) ->
    {Res, Handle1} = soft_close(Handle),
    case Res of
        ok -> put({Ref, fhc_handle}, Handle1),
              true;
        _  -> put_handle(Ref, Handle1),
              false
    end.

soft_close(Handle = #handle { hdl = closed }) ->
    {ok, Handle};
soft_close(Handle) ->
    case write_buffer(Handle) of
        {ok, #handle { hdl         = Hdl,
                       ref         = Ref,
                       is_dirty    = IsDirty,
                       last_used_at = Then } = Handle1 } ->
            ok = case IsDirty of
                     true  -> prim_file_sync(Hdl);
                     false -> ok
                 end,
            ok = prim_file:close(Hdl),
            age_tree_delete(Then, Ref),
            {ok, Handle1 #handle { hdl            = closed,
                                   is_dirty       = false,
                                   last_used_at   = undefined }};
        {_Error, _Handle} = Result ->
            Result
    end.

hard_close(Handle) ->
    case soft_close(Handle) of
        {ok, #handle { path = Path,
                       is_read = IsReader, is_write = IsWriter }} ->
            #file { reader_count = RCount, has_writer = HasWriter } = File =
                get({Path, fhc_file}),
            RCount1 = case IsReader of
                          true  -> RCount - 1;
                          false -> RCount
                      end,
            HasWriter1 = HasWriter andalso not IsWriter,
            case RCount1 =:= 0 andalso not HasWriter1 of
                true  -> erase({Path, fhc_file});
                false -> put({Path, fhc_file},
                             File #file { reader_count = RCount1,
                                          has_writer = HasWriter1 })
            end,
            ok;
        {_Error, _Handle} = Result ->
            Result
    end.

maybe_seek(New, Handle = #handle{hdl              = Hdl,
                                 offset           = Old,
                                 read_buffer_pos  = BufPos,
                                 read_buffer_rem  = BufRem,
                                 at_eof           = AtEoF}) ->
    {AtEoF1, NeedsSeek} = needs_seek(AtEoF, Old, New),
    case NeedsSeek of
        true when is_number(New) andalso
                  ((New >= Old andalso New =< BufRem + Old)
                   orelse (New < Old andalso Old - New =< BufPos)) ->
            Diff = New - Old,
            {{ok, New}, Handle#handle{offset          = New,
                                      at_eof          = AtEoF1,
                                      read_buffer_pos = BufPos + Diff,
                                      read_buffer_rem = BufRem - Diff}};
        true ->
            case prim_file_position(Hdl, New) of
                {ok, Offset1} = Result ->
                    {Result, reset_read_buffer(Handle#handle{offset = Offset1,
                                                             at_eof = AtEoF1})};
                {error, _} = Error ->
                    {Error, Handle}
            end;
        false ->
            {{ok, Old}, Handle}
    end.

needs_seek( AtEoF, _CurOffset,  cur     ) -> {AtEoF, false};
needs_seek( AtEoF, _CurOffset,  {cur, 0}) -> {AtEoF, false};
needs_seek(  true, _CurOffset,  eof     ) -> {true , false};
needs_seek(  true, _CurOffset,  {eof, 0}) -> {true , false};
needs_seek( false, _CurOffset,  eof     ) -> {true , true };
needs_seek( false, _CurOffset,  {eof, 0}) -> {true , true };
needs_seek( AtEoF,          0,  bof     ) -> {AtEoF, false};
needs_seek( AtEoF,          0,  {bof, 0}) -> {AtEoF, false};
needs_seek( AtEoF,  CurOffset, CurOffset) -> {AtEoF, false};
needs_seek(  true,  CurOffset, {bof, DesiredOffset})
  when DesiredOffset >= CurOffset ->
    {true, true};
needs_seek(  true, _CurOffset, {cur, DesiredOffset})
  when DesiredOffset > 0 ->
    {true, true};
needs_seek(  true,  CurOffset, DesiredOffset) %% same as {bof, DO}
  when is_integer(DesiredOffset) andalso DesiredOffset >= CurOffset ->
    {true, true};
%% because we can't really track size, we could well end up at EoF and not know
needs_seek(_AtEoF, _CurOffset, _DesiredOffset) ->
    {false, true}.

write_buffer(Handle = #handle { write_buffer = [] }) ->
    {ok, Handle};
write_buffer(Handle = #handle { hdl = Hdl, offset = Offset,
                                write_buffer = WriteBuffer,
                                write_buffer_size = DataSize,
                                at_eof = true }) ->
    case prim_file_write(Hdl, lists:reverse(WriteBuffer)) of
        ok ->
            Offset1 = Offset + DataSize,
            {ok, Handle #handle { offset = Offset1, is_dirty = true,
                                  write_buffer = [], write_buffer_size = 0 }};
        {error, _} = Error ->
            {Error, Handle}
    end.

reset_read_buffer(Handle) ->
    Handle#handle{read_buffer     = <<>>,
                  read_buffer_pos = 0,
                  read_buffer_rem = 0}.

%% We come into this function whenever there's been a miss while
%% reading from the buffer - but note that when we first start with a
%% new handle the usage will be 0.  Therefore in that case don't take
%% it as meaning the buffer was useless, we just haven't done anything
%% yet!
tune_read_buffer_limit(Handle = #handle{read_buffer_usage = 0}, _Count) ->
    Handle;
%% In this head we have been using the buffer but now tried to read
%% outside it. So how did we do? If we used less than the size of the
%% buffer, make the new buffer the size of what we used before, but
%% add one byte (so that next time we can distinguish between getting
%% the buffer size exactly right and actually wanting more). If we
%% read 100% of what we had, then double it for next time, up to the
%% limit that was set when we were created.
tune_read_buffer_limit(Handle = #handle{read_buffer            = Buf,
                                        read_buffer_usage      = Usg,
                                        read_buffer_size       = Sz,
                                        read_buffer_size_limit = Lim}, Count) ->
    %% If the buffer is <<>> then we are in the first read after a
    %% reset, the read_buffer_usage is the total usage from before the
    %% reset. But otherwise we are in a read which read off the end of
    %% the buffer, so really the size of this read should be included
    %% in the usage.
    TotalUsg = case Buf of
                   <<>> -> Usg;
                   _    -> Usg + Count
               end,
    Handle#handle{read_buffer_usage = 0,
                  read_buffer_size  = erlang:min(case TotalUsg < Sz of
                                                     true  -> Usg + 1;
                                                     false -> Usg * 2
                                                 end, Lim)}.

maybe_reduce_read_cache(SparedRefs) ->
    case vm_memory_monitor:get_memory_use(bytes) of
        {_, infinity}                             -> ok;
        {MemUse, MemLimit} when MemUse < MemLimit -> ok;
        {MemUse, MemLimit}                        -> reduce_read_cache(
                                                       (MemUse - MemLimit) * 2,
                                                       SparedRefs)
    end.

reduce_read_cache(MemToFree, SparedRefs) ->
    Handles = lists:sort(
      fun({_, H1}, {_, H2}) -> H1 < H2 end,
      [{R, H} || {{R, fhc_handle}, H} <- get(),
                 not lists:member(R, SparedRefs)
                 andalso size(H#handle.read_buffer) > 0]),
    FreedMem = lists:foldl(
      fun
          (_, Freed) when Freed >= MemToFree ->
              Freed;
          ({Ref, #handle{read_buffer = Buf} = Handle}, Freed) ->
              Handle1 = reset_read_buffer(Handle),
              put({Ref, fhc_handle}, Handle1),
              Freed + size(Buf)
      end, 0, Handles),
    if
        FreedMem < MemToFree andalso SparedRefs =/= [] ->
            reduce_read_cache(MemToFree - FreedMem, []);
        true ->
            ok
    end.

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(total_limit,   #fhc_state{limit               = Limit}) -> Limit;
i(total_used,    State)                                   -> used(State);
i(sockets_limit, #fhc_state{obtain_limit        = Limit}) -> Limit;
i(sockets_used,  #fhc_state{obtain_count_socket = Count,
                            reserve_count_socket = RCount}) -> Count + RCount;
i(files_reserved,  #fhc_state{reserve_count_file   = RCount}) -> RCount;
i(Item, _) -> throw({bad_argument, Item}).

used(#fhc_state{open_count          = C1,
                obtain_count_socket = C2,
                obtain_count_file   = C3,
                reserve_count_socket = C4,
                reserve_count_file = C5}) -> C1 + C2 + C3 + C4 + C5.

%%----------------------------------------------------------------------------
%% gen_server2 callbacks
%%----------------------------------------------------------------------------

init([AlarmSet, AlarmClear]) ->
    _ = file_handle_cache_stats:init(),
    Limit = case application:get_env(file_handles_high_watermark) of
                {ok, Watermark} when (is_integer(Watermark) andalso
                                      Watermark > 0) ->
                    Watermark;
                _ ->
                    case ulimit() of
                        unknown  -> ?FILE_HANDLES_LIMIT_OTHER;
                        Lim      -> lists:max([2, Lim - ?RESERVED_FOR_OTHERS])
                    end
            end,
    ObtainLimit = obtain_limit(Limit),
    logger:info("Limiting to approx ~p file handles (~p sockets)",
                 [Limit, ObtainLimit]),
    Clients = ets:new(?CLIENT_ETS_TABLE, [set, private, {keypos, #cstate.pid}]),
    Elders = ets:new(?ELDERS_ETS_TABLE, [set, private]),
    {ok, #fhc_state { elders                = Elders,
                      limit                 = Limit,
                      open_count            = 0,
                      open_pending          = pending_new(),
                      obtain_limit          = ObtainLimit,
                      obtain_count_file     = 0,
                      obtain_pending_file   = pending_new(),
                      obtain_count_socket   = 0,
                      obtain_pending_socket = pending_new(),
                      clients               = Clients,
                      timer_ref             = undefined,
                      alarm_set             = AlarmSet,
                      alarm_clear           = AlarmClear,
                      reserve_count_file    = 0,
                      reserve_count_socket  = 0 }}.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {release, _, _, _}           -> 5;
        {release_reservation, _, _, _}   -> 5;
        _                            -> 0
    end.

handle_call({open, Pid, Requested, EldestUnusedSince}, From,
            State = #fhc_state { open_count   = Count,
                                 open_pending = Pending,
                                 elders       = Elders,
                                 clients      = Clients })
  when EldestUnusedSince =/= undefined ->
    true = ets:insert(Elders, {Pid, EldestUnusedSince}),
    Item = #pending { kind      = open,
                      pid       = Pid,
                      requested = Requested,
                      from      = From },
    ok = track_client(Pid, Clients),
    case needs_reduce(State #fhc_state { open_count = Count + Requested }) of
        true  -> case ets:lookup(Clients, Pid) of
                     [#cstate { opened = 0 }] ->
                         true = ets:update_element(
                                  Clients, Pid, {#cstate.blocked, true}),
                         {noreply,
                          reduce(State #fhc_state {
                                   open_pending = pending_in(Item, Pending) })};
                     [#cstate { opened = Opened }] ->
                         true = ets:update_element(
                                  Clients, Pid,
                                  {#cstate.pending_closes, Opened}),
                         {reply, close, State}
                 end;
        false -> {noreply, run_pending_item(Item, State)}
    end;

handle_call({obtain, N, Type, Pid}, From,
            State = #fhc_state { clients = Clients }) ->
    Count = obtain_state(Type, count, State),
    Pending = obtain_state(Type, pending, State),
    ok = track_client(Pid, Clients),
    Item = #pending { kind      = {obtain, Type}, pid  = Pid,
                      requested = N,              from = From },
    Enqueue = fun () ->
                      true = ets:update_element(Clients, Pid,
                                                {#cstate.blocked, true}),
                      set_obtain_state(Type, pending,
                                       pending_in(Item, Pending), State)
              end,
    {noreply,
        case obtain_limit_reached(Type, State) of
            true  -> Enqueue();
            false -> case needs_reduce(
                            set_obtain_state(Type, count, Count + 1, State)) of
                         true  -> reduce(Enqueue());
                         false -> adjust_alarm(
                                      State, run_pending_item(Item, State))
                     end
        end};

handle_call({set_limit, Limit}, _From, State) ->
    {reply, ok, adjust_alarm(
                  State, maybe_reduce(
                           process_pending(
                             State #fhc_state {
                               limit        = Limit,
                               obtain_limit = obtain_limit(Limit) })))};

handle_call(get_limit, _From, State = #fhc_state { limit = Limit }) ->
    {reply, Limit, State};

handle_call({info, Items}, _From, State) ->
    {reply, infos(Items, State), State}.

handle_cast({register_callback, Pid, MFA},
            State = #fhc_state { clients = Clients }) ->
    ok = track_client(Pid, Clients),
    true = ets:update_element(Clients, Pid, {#cstate.callback, MFA}),
    {noreply, State};

handle_cast({update, Pid, EldestUnusedSince},
            State = #fhc_state { elders = Elders })
  when EldestUnusedSince =/= undefined ->
    true = ets:insert(Elders, {Pid, EldestUnusedSince}),
    %% don't call maybe_reduce from here otherwise we can create a
    %% storm of messages
    {noreply, State};

handle_cast({release, N, Type, Pid}, State) ->
    State1 = process_pending(update_counts({obtain, Type}, Pid, -N, State)),
    {noreply, adjust_alarm(State, State1)};

handle_cast({close, Pid, EldestUnusedSince},
            State = #fhc_state { elders = Elders, clients = Clients }) ->
    true = case EldestUnusedSince of
               undefined -> ets:delete(Elders, Pid);
               _         -> ets:insert(Elders, {Pid, EldestUnusedSince})
           end,
    ets:update_counter(Clients, Pid, {#cstate.pending_closes, -1, 0, 0}),
    {noreply, adjust_alarm(State, process_pending(
                update_counts(open, Pid, -1, State)))};

handle_cast({transfer, N, FromPid, ToPid}, State) ->
    ok = track_client(ToPid, State#fhc_state.clients),
    {noreply, process_pending(
                update_counts({obtain, socket}, ToPid, +N,
                              update_counts({obtain, socket}, FromPid, -N,
                                            State)))};

handle_cast(clear_read_cache, State) ->
    _ = clear_process_read_cache(),
    {noreply, State};

handle_cast({release_reservation, Type, Pid}, State) ->
    State1 = process_pending(update_counts({reserve, Type}, Pid, 0, State)),
    {noreply, adjust_alarm(State, State1)};

handle_cast({set_reservation, N, Type, Pid},
            State = #fhc_state { clients = Clients }) ->
    ok = track_client(Pid, Clients),
    NewState = process_pending(update_counts({reserve, Type}, Pid, N, State)),
    {noreply, case needs_reduce(NewState) of
                  true  -> reduce(NewState);
                  false -> adjust_alarm(State, NewState)
              end}.

handle_info(check_counts, State) ->
    {noreply, maybe_reduce(State #fhc_state { timer_ref = undefined })};

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #fhc_state { elders                = Elders,
                                 open_count            = OpenCount,
                                 open_pending          = OpenPending,
                                 obtain_count_file     = ObtainCountF,
                                 obtain_count_socket   = ObtainCountS,
                                 obtain_pending_file   = ObtainPendingF,
                                 obtain_pending_socket = ObtainPendingS,
                                 reserve_count_file    = ReserveCountF,
                                 reserve_count_socket  = ReserveCountS,
                                 clients               = Clients }) ->
    [#cstate { opened          = Opened,
               obtained_file   = ObtainedFile,
               obtained_socket = ObtainedSocket,
               reserved_file   = ReservedFile,
               reserved_socket = ReservedSocket }] =
        ets:lookup(Clients, Pid),
    true = ets:delete(Clients, Pid),
    true = ets:delete(Elders, Pid),
    Fun = fun (#pending { pid = Pid1 }) -> Pid1 =/= Pid end,
    State1 = process_pending(
               State #fhc_state {
                 open_count            = OpenCount - Opened,
                 open_pending          = filter_pending(Fun, OpenPending),
                 obtain_count_file     = ObtainCountF - ObtainedFile,
                 obtain_count_socket   = ObtainCountS - ObtainedSocket,
                 obtain_pending_file   = filter_pending(Fun, ObtainPendingF),
                 obtain_pending_socket = filter_pending(Fun, ObtainPendingS),
                 reserve_count_file    = ReserveCountF - ReservedFile,
                 reserve_count_socket  = ReserveCountS - ReservedSocket}),
    {noreply, adjust_alarm(State, State1)}.

terminate(_Reason, State = #fhc_state { clients = Clients,
                                        elders  = Elders }) ->
    ets:delete(Clients),
    ets:delete(Elders),
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% pending queue abstraction helpers
%%----------------------------------------------------------------------------

queue_fold(Fun, Init, Q) ->
    case queue:out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> queue_fold(Fun, Fun(V, Init), Q1)
    end.

filter_pending(Fun, {Count, Queue}) ->
    {Delta, Queue1} =
        queue_fold(
          fun (Item = #pending { requested = Requested }, {DeltaN, QueueN}) ->
                  case Fun(Item) of
                      true  -> {DeltaN, queue:in(Item, QueueN)};
                      false -> {DeltaN - Requested, QueueN}
                  end
          end, {0, queue:new()}, Queue),
    {Count + Delta, Queue1}.

pending_new() ->
    {0, queue:new()}.

pending_in(Item = #pending { requested = Requested }, {Count, Queue}) ->
    {Count + Requested, queue:in(Item, Queue)}.

pending_out({0, _Queue} = Pending) ->
    {empty, Pending};
pending_out({N, Queue}) ->
    {{value, #pending { requested = Requested }} = Result, Queue1} =
        queue:out(Queue),
    {Result, {N - Requested, Queue1}}.

pending_count({Count, _Queue}) ->
    Count.

%%----------------------------------------------------------------------------
%% server helpers
%%----------------------------------------------------------------------------

obtain_limit(infinity) -> infinity;
obtain_limit(Limit)    -> case ?OBTAIN_LIMIT(Limit) of
                              OLimit when OLimit < 0 -> 0;
                              OLimit                 -> OLimit
                          end.

obtain_limit_reached(socket, State) -> obtain_limit_reached(State);
obtain_limit_reached(file,   State) -> needs_reduce(State).

obtain_limit_reached(#fhc_state{obtain_limit        = Limit,
                                obtain_count_socket = Count,
                                reserve_count_socket = RCount}) ->
    Limit =/= infinity andalso (RCount + Count) >= Limit.

obtain_state(file,   count,   #fhc_state{obtain_count_file     = N}) -> N;
obtain_state(socket, count,   #fhc_state{obtain_count_socket   = N}) -> N;
obtain_state(file,   pending, #fhc_state{obtain_pending_file   = N}) -> N;
obtain_state(socket, pending, #fhc_state{obtain_pending_socket = N}) -> N.

set_obtain_state(file,   count,   N, S) -> S#fhc_state{obtain_count_file = N};
set_obtain_state(socket, count,   N, S) -> S#fhc_state{obtain_count_socket = N};
set_obtain_state(file,   pending, N, S) -> S#fhc_state{obtain_pending_file = N};
set_obtain_state(socket, pending, N, S) -> S#fhc_state{obtain_pending_socket = N}.

adjust_alarm(OldState = #fhc_state { alarm_set   = AlarmSet,
                                     alarm_clear = AlarmClear }, NewState) ->
    case {obtain_limit_reached(OldState), obtain_limit_reached(NewState)} of
        {false, true} -> AlarmSet({file_descriptor_limit, []});
        {true, false} -> AlarmClear(file_descriptor_limit);
        _             -> ok
    end,
    NewState.

process_pending(State = #fhc_state { limit = infinity }) ->
    State;
process_pending(State) ->
    process_open(process_obtain(socket, process_obtain(file, State))).

process_open(State = #fhc_state { limit        = Limit,
                                  open_pending = Pending}) ->
    {Pending1, State1} = process_pending(Pending, Limit - used(State), State),
    State1 #fhc_state { open_pending = Pending1 }.

process_obtain(socket, State = #fhc_state { limit        = Limit,
                                            obtain_limit = ObtainLimit,
                                            open_count = OpenCount,
                                            obtain_count_socket = ObtainCount,
                                            obtain_pending_socket = Pending,
                                            obtain_count_file = ObtainCountF,
                                            reserve_count_file = ReserveCountF,
                                            reserve_count_socket = ReserveCount}) ->
    Quota = min(ObtainLimit - ObtainCount,
                Limit - (OpenCount + ObtainCount + ObtainCountF + ReserveCount + ReserveCountF)),
    {Pending1, State1} = process_pending(Pending, Quota, State),
    State1#fhc_state{obtain_pending_socket = Pending1};
process_obtain(file, State = #fhc_state { limit        = Limit,
                                          open_count = OpenCount,
                                          obtain_count_socket = ObtainCountS,
                                          obtain_count_file = ObtainCountF,
                                          obtain_pending_file = Pending,
                                          reserve_count_file = ReserveCountF,
                                          reserve_count_socket = ReserveCountS}) ->
    Quota = Limit - (OpenCount + ObtainCountS + ObtainCountF + ReserveCountF + ReserveCountS),
    {Pending1, State1} = process_pending(Pending, Quota, State),
    State1#fhc_state{obtain_pending_file = Pending1}.

process_pending(Pending, Quota, State) when Quota =< 0 ->
    {Pending, State};
process_pending(Pending, Quota, State) ->
    case pending_out(Pending) of
        {empty, _Pending} ->
            {Pending, State};
        {{value, #pending { requested = Requested }}, _Pending1}
          when Requested > Quota ->
            {Pending, State};
        {{value, #pending { requested = Requested } = Item}, Pending1} ->
            process_pending(Pending1, Quota - Requested,
                            run_pending_item(Item, State))
    end.

run_pending_item(#pending { kind      = Kind,
                            pid       = Pid,
                            requested = Requested,
                            from      = From },
                 State = #fhc_state { clients = Clients }) ->
    gen_server2:reply(From, ok),
    true = ets:update_element(Clients, Pid, {#cstate.blocked, false}),
    update_counts(Kind, Pid, Requested, State).

update_counts(open, Pid, Delta,
              State = #fhc_state { open_count          = OpenCount,
                                   clients             = Clients }) ->
    ets:update_counter(Clients, Pid, {#cstate.opened, Delta}),
    State #fhc_state { open_count = OpenCount + Delta};
update_counts({obtain, file}, Pid, Delta,
              State = #fhc_state {obtain_count_file   = ObtainCountF,
                                  clients             = Clients }) ->
    ets:update_counter(Clients, Pid, {#cstate.obtained_file, Delta}),
    State #fhc_state { obtain_count_file = ObtainCountF + Delta};
update_counts({obtain, socket}, Pid, Delta,
              State = #fhc_state {obtain_count_socket   = ObtainCountS,
                                  clients             = Clients }) ->
    ets:update_counter(Clients, Pid, {#cstate.obtained_socket, Delta}),
    State #fhc_state { obtain_count_socket = ObtainCountS + Delta};
update_counts({reserve, file}, Pid, NewReservation,
              State = #fhc_state {reserve_count_file   = ReserveCountF,
                                  clients             = Clients }) ->
    [#cstate{reserved_file = R}] = ets:lookup(Clients, Pid),
    Delta = NewReservation - R,
    ets:update_counter(Clients, Pid, {#cstate.reserved_file, Delta}),
    State #fhc_state { reserve_count_file = ReserveCountF + Delta};
update_counts({reserve, socket}, Pid, NewReservation,
              State = #fhc_state {reserve_count_socket   = ReserveCountS,
                                  clients             = Clients }) ->
    [#cstate{reserved_file = R}] = ets:lookup(Clients, Pid),
    Delta = NewReservation - R,
    ets:update_counter(Clients, Pid, {#cstate.reserved_socket, Delta}),
    State #fhc_state { reserve_count_socket = ReserveCountS + Delta}.

maybe_reduce(State) ->
    case needs_reduce(State) of
        true  -> reduce(State);
        false -> State
    end.

needs_reduce(#fhc_state { limit                 = Limit,
                          open_count            = OpenCount,
                          open_pending          = {OpenPending, _},
                          obtain_limit          = ObtainLimit,
                          obtain_count_socket   = ObtainCountS,
                          obtain_count_file     = ObtainCountF,
                          obtain_pending_file   = {ObtainPendingF, _},
                          obtain_pending_socket = {ObtainPendingS, _},
                          reserve_count_socket  = ReserveCountS,
                          reserve_count_file    = ReserveCountF}) ->
    Limit =/= infinity
        andalso (((OpenCount + ObtainCountS + ObtainCountF + ReserveCountS + ReserveCountF) > Limit)
                 orelse (OpenPending =/= 0)
                 orelse (ObtainPendingF =/= 0)
                 orelse (ObtainCountS < ObtainLimit
                         andalso (ObtainPendingS =/= 0))).

reduce(State = #fhc_state { open_pending          = OpenPending,
                            obtain_pending_file   = ObtainPendingFile,
                            obtain_pending_socket = ObtainPendingSocket,
                            elders                = Elders,
                            clients               = Clients,
                            timer_ref             = TRef }) ->
    Now = erlang:monotonic_time(),
    {CStates, Sum, ClientCount} =
        ets:foldl(fun ({Pid, Eldest}, {CStatesAcc, SumAcc, CountAcc} = Accs) ->
                          [#cstate { pending_closes = PendingCloses,
                                     opened         = Opened,
                                     blocked        = Blocked } = CState] =
                              ets:lookup(Clients, Pid),
                          TimeDiff = erlang:convert_time_unit(
                            Now - Eldest, native, micro_seconds),
                          case Blocked orelse PendingCloses =:= Opened of
                              true  -> Accs;
                              false -> {[CState | CStatesAcc],
                                        SumAcc + TimeDiff,
                                        CountAcc + 1}
                          end
                  end, {[], 0, 0}, Elders),
    case CStates of
        [] -> ok;
        _  -> case (Sum / ClientCount) -
                  (1000 * ?FILE_HANDLES_CHECK_INTERVAL) of
                  AverageAge when AverageAge > 0 ->
                      notify_age(CStates, AverageAge);
                  _ ->
                      notify_age0(Clients, CStates,
                                  pending_count(OpenPending) +
                                      pending_count(ObtainPendingFile) +
                                      pending_count(ObtainPendingSocket))
              end
    end,
    case TRef of
        undefined -> TRef1 = erlang:send_after(
                               ?FILE_HANDLES_CHECK_INTERVAL, ?SERVER,
                               check_counts),
                     State #fhc_state { timer_ref = TRef1 };
        _         -> State
    end.

notify_age(CStates, AverageAge) ->
    lists:foreach(
      fun (#cstate { callback = undefined }) -> ok;
          (#cstate { callback = {M, F, A} }) -> apply(M, F, A ++ [AverageAge])
      end, CStates).

notify_age0(Clients, CStates, Required) ->
    case [CState || CState <- CStates, CState#cstate.callback =/= undefined] of
        []            -> ok;
        Notifications -> S = rand:uniform(length(Notifications)),
                         {L1, L2} = lists:split(S, Notifications),
                         notify(Clients, Required, L2 ++ L1)
    end.

notify(_Clients, _Required, []) ->
    ok;
notify(_Clients, Required, _Notifications) when Required =< 0 ->
    ok;
notify(Clients, Required, [#cstate{ pid      = Pid,
                                    callback = {M, F, A},
                                    opened   = Opened } | Notifications]) ->
    apply(M, F, A ++ [0]),
    ets:update_element(Clients, Pid, {#cstate.pending_closes, Opened}),
    notify(Clients, Required - Opened, Notifications).

track_client(Pid, Clients) ->
    case ets:insert_new(Clients, #cstate { pid             = Pid,
                                           callback        = undefined,
                                           opened          = 0,
                                           obtained_file   = 0,
                                           obtained_socket = 0,
                                           blocked         = false,
                                           pending_closes  = 0,
                                           reserved_file   = 0,
                                           reserved_socket = 0 }) of
        true  -> _MRef = erlang:monitor(process, Pid),
                 ok;
        false -> ok
    end.


%% To increase the number of file descriptors: on Windows set ERL_MAX_PORTS
%% environment variable, on Linux set `ulimit -n`.
ulimit() ->
    IOStats = case erlang:system_info(check_io) of
        [Val | _] when is_list(Val) -> Val;
        Val when is_list(Val)       -> Val;
        _Other                      -> []
    end,
    case proplists:get_value(max_fds, IOStats) of
        MaxFds when is_integer(MaxFds) andalso MaxFds > 1 ->
            case os:type() of
                {win32, _OsName} ->
                    %% On Windows max_fds is twice the number of open files:
                    %%   https://github.com/yrashk/erlang/blob/e1282325ed75e52a98d5/erts/emulator/sys/win32/sys.c#L2459-2466
                    MaxFds div 2;
                _Any ->
                    %% For other operating systems trust Erlang.
                    MaxFds
            end;
        _ ->
            unknown
    end.
