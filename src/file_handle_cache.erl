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

-module(file_handle_cache).

%% A File Handle Cache
%%
%% This extends a subset of the functionality of the Erlang file
%% module.
%%
%% Some constraints
%% 1) This supports one writer, multiple readers per file. Nothing
%% else.
%% 2) Do not open the same file from different processes. Bad things
%% may happen.
%% 3) Writes are all appends. You cannot write to the middle of a
%% file, although you can truncate and then append if you want.
%% 4) Although there is a write buffer, there is no read buffer. Feel
%% free to use the read_ahead mode, but beware of the interaction
%% between that buffer and the write buffer.
%%
%% Some benefits
%% 1) You do not have to remember to call sync before close
%% 2) Buffering is much more flexible than with plain file module, and
%% you can control when the buffer gets flushed out. This means that
%% you can rely on reads-after-writes working, without having to call
%% the expensive sync.
%% 3) Unnecessary calls to position and sync get optimised out.
%% 4) You can find out what your 'real' offset is, and what your
%% 'virtual' offset is (i.e. where the hdl really is, and where it
%% would be after the write buffer is written out).
%% 5) You can find out what the offset was when you last sync'd.
%%
%% There is also a server component which serves to limit the number
%% of open file handles in a "soft" way - the server will never
%% prevent a client from opening a handle, but may immediately tell it
%% to close the handle. Thus you can set the limit to zero and it will
%% still all work correctly, it is just that effectively no caching
%% will take place. The operation of limiting is as follows:
%%
%% On open and close, the client sends messages to the server
%% informing it of opens and closes. This allows the server to keep
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
%% When the limit is reached, the server calculates the average age of
%% the last reported least recently used file handle of all the
%% clients. It then tells all the clients to close any handles not
%% used for longer than this average, by invoking the callback the
%% client registered. The client should receive this message and pass
%% it into set_maximum_since_use/1. However, it is highly possible
%% this age will be greater than the ages of all the handles the
%% client knows of because the client has used its file handles in the
%% mean time. Thus at this point the client reports to the server the
%% current timestamp at which its least recently used file handle was
%% last used. The server will check two seconds later that either it
%% is back under the limit, in which case all is well again, or if
%% not, it will calculate a new average age. Its data will be much
%% more recent now, and so it is very likely that when this is
%% communicated to the clients, the clients will close file handles.
%%
%% The advantage of this scheme is that there is only communication
%% from the client to the server on open, close, and when in the
%% process of trying to reduce file handle usage. There is no
%% communication from the client to the server on normal file handle
%% operations. This scheme forms a feed-back loop - the server does
%% not care which file handles are closed, just that some are, and it
%% checks this repeatedly when over the limit. Given the guarantees of
%% now(), even if there is just one file handle open, a limit of 1,
%% and one client, it is certain that when the client calculates the
%% age of the handle, it will be greater than when the server
%% calculated it, hence it should be closed.
%%
%% Handles which are closed as a result of the server are put into a
%% "soft-closed" state in which the handle is closed (data flushed out
%% and sync'd first) but the state is maintained. The handle will be
%% fully reopened again as soon as needed, thus users of this library
%% do not need to worry about their handles being closed by the server
%% - reopening them when necessary is handled transparently.
%%
%% The server also supports obtain and transfer. obtain/0 blocks until
%% a file descriptor is available. transfer/1 is transfers ownership
%% of a file descriptor between processes. It is non-blocking.
%%
%% The callers of register_callback/3, obtain/0, and the argument of
%% transfer/1 are monitored, reducing the count of handles in use
%% appropriately when the processes terminate.

-behaviour(gen_server).

-export([register_callback/3]).
-export([open/3, close/1, read/2, append/2, sync/1, position/2, truncate/1,
         last_sync_offset/1, current_virtual_offset/1, current_raw_offset/1,
         flush/1, copy/3, set_maximum_since_use/1, delete/1, clear/1]).
-export([obtain/0, transfer/1, set_limit/1, get_limit/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(RESERVED_FOR_OTHERS, 100).

%% Googling around suggests that Windows has a limit somewhere around
%% 16M, eg
%% http://blogs.technet.com/markrussinovich/archive/2009/09/29/3283844.aspx
%% however, it turns out that's only available through the win32
%% API. Via the C Runtime, we have just 512:
%% http://msdn.microsoft.com/en-us/library/6e3b887c%28VS.80%29.aspx
-define(FILE_HANDLES_LIMIT_WINDOWS, 512).
-define(FILE_HANDLES_LIMIT_OTHER, 1024).
-define(FILE_HANDLES_CHECK_INTERVAL, 2000).

-define(OBTAIN_LIMIT(LIMIT), trunc((LIMIT * 0.9) - 2)).
-define(CLIENT_ETS_TABLE, ?MODULE).

%%----------------------------------------------------------------------------

-record(file,
        { reader_count,
          has_writer
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
          obtain_limit,
          obtain_count,
          obtain_pending,
          clients,
          timer_ref
        }).

-record(cstate,
        { pid,
          callback,
          opened,
          obtained,
          blocked,
          pending_closes
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

-ifdef(use_specs).

-type(ref() :: any()).
-type(ok_or_error() :: 'ok' | {'error', any()}).
-type(val_or_error(T) :: {'ok', T} | {'error', any()}).
-type(position() :: ('bof' | 'eof' | non_neg_integer() |
                     {('bof' |'eof'), non_neg_integer()} |
                     {'cur', integer()})).
-type(offset() :: non_neg_integer()).

-spec(register_callback/3 :: (atom(), atom(), [any()]) -> 'ok').
-spec(open/3 ::
        (string(), [any()],
         [{'write_buffer', (non_neg_integer() | 'infinity' | 'unbuffered')}])
        -> val_or_error(ref())).
-spec(close/1 :: (ref()) -> ok_or_error()).
-spec(read/2 :: (ref(), non_neg_integer()) ->
             val_or_error([char()] | binary()) | 'eof').
-spec(append/2 :: (ref(), iodata()) -> ok_or_error()).
-spec(sync/1 :: (ref()) ->  ok_or_error()).
-spec(position/2 :: (ref(), position()) -> val_or_error(offset())).
-spec(truncate/1 :: (ref()) -> ok_or_error()).
-spec(last_sync_offset/1       :: (ref()) -> val_or_error(offset())).
-spec(current_virtual_offset/1 :: (ref()) -> val_or_error(offset())).
-spec(current_raw_offset/1     :: (ref()) -> val_or_error(offset())).
-spec(flush/1 :: (ref()) -> ok_or_error()).
-spec(copy/3 :: (ref(), ref(), non_neg_integer()) ->
             val_or_error(non_neg_integer())).
-spec(set_maximum_since_use/1 :: (non_neg_integer()) -> 'ok').
-spec(delete/1 :: (ref()) -> ok_or_error()).
-spec(clear/1 :: (ref()) -> ok_or_error()).
-spec(obtain/0 :: () -> 'ok').
-spec(transfer/1 :: (pid()) -> 'ok').
-spec(set_limit/1 :: (non_neg_integer()) -> 'ok').
-spec(get_limit/0 :: () -> non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], [{timeout, infinity}]).

register_callback(M, F, A)
  when is_atom(M) andalso is_atom(F) andalso is_list(A) ->
    gen_server:cast(?SERVER, {register_callback, self(), {M, F, A}}).

open(Path, Mode, Options) ->
    Path1 = filename:absname(Path),
    File1 = #file { reader_count = RCount, has_writer = HasWriter } =
        case get({Path1, fhc_file}) of
            File = #file {} -> File;
            undefined       -> #file { reader_count = 0,
                                       has_writer = false }
        end,
    Mode1 = append_to_write(Mode),
    IsWriter = is_writer(Mode1),
    case IsWriter andalso HasWriter of
        true  -> {error, writer_exists};
        false -> {ok, Ref} = new_closed_handle(Path1, Mode1, Options),
                 case get_or_reopen([{Ref, new}]) of
                     {ok, [_Handle1]} ->
                         RCount1 = case is_reader(Mode1) of
                                       true  -> RCount + 1;
                                       false -> RCount
                                   end,
                         HasWriter1 = HasWriter orelse IsWriter,
                         put({Path1, fhc_file},
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
      [Ref],
      fun ([#handle { is_read = false }]) ->
              {error, not_open_for_reading};
          ([Handle = #handle { hdl = Hdl, offset = Offset }]) ->
              case file:read(Hdl, Count) of
                  {ok, Data} = Obj -> Offset1 = Offset + iolist_size(Data),
                                      {Obj,
                                       [Handle #handle { offset = Offset1 }]};
                  eof              -> {eof, [Handle #handle { at_eof = true }]};
                  Error            -> {Error, [Handle]}
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
                      {file:write(Hdl, Data),
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
      [Ref],
      fun ([#handle { is_dirty = false, write_buffer = [] }]) ->
              ok;
          ([Handle = #handle { hdl = Hdl, offset = Offset,
                               is_dirty = true, write_buffer = [] }]) ->
              case file:sync(Hdl) of
                  ok    -> {ok, [Handle #handle { trusted_offset = Offset,
                                                  is_dirty = false }]};
                  Error -> {Error, [Handle]}
              end
      end).

position(Ref, NewOffset) ->
    with_flushed_handles(
      [Ref],
      fun ([Handle]) -> {Result, Handle1} = maybe_seek(NewOffset, Handle),
                        {Result, [Handle1]}
      end).

truncate(Ref) ->
    with_flushed_handles(
      [Ref],
      fun ([Handle1 = #handle { hdl = Hdl, offset = Offset,
                                trusted_offset = TOffset }]) ->
              case file:truncate(Hdl) of
                  ok    -> TOffset1 = lists:min([Offset, TOffset]),
                           {ok, [Handle1 #handle { trusted_offset = TOffset1,
                                                   at_eof = true }]};
                  Error -> {Error, [Handle1]}
              end
      end).

last_sync_offset(Ref) ->
    with_handles([Ref], fun ([#handle { trusted_offset = TOffset }]) ->
                                {ok, TOffset}
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
              case file:copy(SHdl, DHdl, Count) of
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
                ok               -> file:delete(Path);
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
              case maybe_seek(bof, Handle #handle { write_buffer = [],
                                                    write_buffer_size = 0 }) of
                  {{ok, 0}, Handle1 = #handle { hdl = Hdl }} ->
                      case file:truncate(Hdl) of
                          ok    -> {ok, [Handle1 #handle {trusted_offset = 0,
                                                          at_eof = true }]};
                          Error -> {Error, [Handle1]}
                      end;
                  {{error, _} = Error, Handle1} ->
                      {Error, [Handle1]}
              end
      end).

set_maximum_since_use(MaximumAge) ->
    Now = now(),
    case lists:foldl(
           fun ({{Ref, fhc_handle},
                 Handle = #handle { hdl = Hdl, last_used_at = Then }}, Rep) ->
                   Age = timer:now_diff(Now, Then),
                   case Hdl =/= closed andalso Age >= MaximumAge of
                       true  -> soft_close(Ref, Handle) orelse Rep;
                       false -> Rep
                   end;
               (_KeyValuePair, Rep) ->
                   Rep
           end, false, get()) of
        false -> age_tree_change(), ok;
        true  -> ok
    end.

obtain() ->
    gen_server:call(?SERVER, {obtain, self()}, infinity).

transfer(Pid) ->
    gen_server:cast(?SERVER, {transfer, self(), Pid}).

set_limit(Limit) ->
    gen_server:call(?SERVER, {set_limit, Limit}, infinity).

get_limit() ->
    gen_server:call(?SERVER, get_limit, infinity).

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

is_reader(Mode) -> lists:member(read, Mode).

is_writer(Mode) -> lists:member(write, Mode).

append_to_write(Mode) ->
    case lists:member(append, Mode) of
        true  -> [write | Mode -- [append, write]];
        false -> Mode
    end.

with_handles(Refs, Fun) ->
    case get_or_reopen([{Ref, reopen} || Ref <- Refs]) of
        {ok, Handles} ->
            case Fun(Handles) of
                {Result, Handles1} when is_list(Handles1) ->
                    lists:zipwith(fun put_handle/2, Refs, Handles1),
                    Result;
                Result ->
                    Result
            end;
        Error ->
            Error
    end.

with_flushed_handles(Refs, Fun) ->
    with_handles(
      Refs,
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

get_or_reopen(RefNewOrReopens) ->
    case partition_handles(RefNewOrReopens) of
        {OpenHdls, []} ->
            {ok, [Handle || {_Ref, Handle} <- OpenHdls]};
        {OpenHdls, ClosedHdls} ->
            Tree = get_age_tree(),
            Oldest = case gb_trees:is_empty(Tree) of
                         true  -> now();
                         false -> {Then, _Ref} = gb_trees:smallest(Tree),
                                  Then
                     end,
            case gen_server:call(?SERVER, {open, self(), length(ClosedHdls),
                                           Oldest}, infinity) of
                ok ->
                    case reopen(ClosedHdls, Tree, []) of
                        {ok, RefHdls}  -> sort_handles(RefNewOrReopens,
                                                       OpenHdls, RefHdls, []);
                        {error, Error} -> {error, Error}
                    end;
                close ->
                    [soft_close(Ref, Handle) ||
                        {{Ref, fhc_handle}, Handle = #handle { hdl = Hdl }} <-
                            get(),
                        Hdl =/= closed],
                    get_or_reopen(RefNewOrReopens)
            end
    end.

sort_handles([], [], [], Acc) ->
    {ok, lists:reverse(Acc)};
sort_handles([{Ref, _} | RefHdls], [{Ref, Handle} | RefHdlsA], RefHdlsB, Acc) ->
    sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]);
sort_handles([{Ref, _} | RefHdls], RefHdlsA, [{Ref, Handle} | RefHdlsB], Acc) ->
    sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]).

reopen([], Tree, RefHdls) ->
    put_age_tree(Tree),
    {ok, lists:reverse(RefHdls)};
reopen([{Ref, NewOrReopen, Handle} | RefNewOrReopenHdls], Tree, RefHdls) ->
    Now = now(),
    case open1(Ref, Handle #handle { last_used_at = Now }, NewOrReopen) of
        {ok, #handle {} = Handle1} ->
            reopen(RefNewOrReopenHdls, gb_trees:insert(Now, Ref, Tree),
                   [{Ref, Handle1} | RefHdls]);
        {error, Reason} ->
            put_age_tree(Tree),
            [age_tree_delete(Handle1 #handle.last_used_at) ||
                {_Ref1, _NewOrReopen1, Handle1} <-
                    [{Ref, NewOrReopen, Handle} | RefNewOrReopenHdls]],
            {error, Reason}
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

put_handle(Ref, Handle = #handle { last_used_at = Then }) ->
    Now = now(),
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
              gb_trees:insert(Now, Ref, gb_trees:delete_any(Then, Tree))
      end).

age_tree_delete(Then) ->
    with_age_tree(
      fun (Tree) ->
              Tree1 = gb_trees:delete_any(Then, Tree),
              Oldest = case gb_trees:is_empty(Tree1) of
                           true ->
                               undefined;
                           false ->
                               {Oldest1, _Ref} = gb_trees:smallest(Tree1),
                               Oldest1
                       end,
              gen_server:cast(?SERVER, {close, self(), Oldest}),
              Tree1
      end).

age_tree_change() ->
    with_age_tree(
      fun (Tree) ->
              case gb_trees:is_empty(Tree) of
                  true  -> Tree;
                  false -> {Oldest, _Ref} = gb_trees:smallest(Tree),
                           gen_server:cast(?SERVER, {update, self(), Oldest})
              end,
              Tree
      end).

new_closed_handle(Path, Mode, Options) ->
    WriteBufferSize =
        case proplists:get_value(write_buffer, Options, unbuffered) of
            unbuffered           -> 0;
            infinity             -> infinity;
            N when is_integer(N) -> N
        end,
    Ref = make_ref(),
    put({Ref, fhc_handle}, #handle { hdl                     = closed,
                                     offset                  = 0,
                                     trusted_offset          = 0,
                                     is_dirty                = false,
                                     write_buffer_size       = 0,
                                     write_buffer_size_limit = WriteBufferSize,
                                     write_buffer            = [],
                                     at_eof                  = false,
                                     path                    = Path,
                                     mode                    = Mode,
                                     options                 = Options,
                                     is_write                = is_writer(Mode),
                                     is_read                 = is_reader(Mode),
                                     last_used_at            = undefined }),
    {ok, Ref}.

open1(Ref, #handle { hdl = closed, path = Path, mode = Mode, offset = Offset } =
          Handle, NewOrReopen) ->
    Mode1 = case NewOrReopen of
                new    -> Mode;
                reopen -> [read | Mode]
            end,
    case file:open(Path, Mode1) of
        {ok, Hdl} ->
            {{ok, Offset1}, Handle1} =
                maybe_seek(Offset, Handle #handle { hdl = Hdl, offset = 0 }),
            Handle2 = Handle1 #handle { trusted_offset = Offset1 },
            put({Ref, fhc_handle}, Handle2),
            {ok, Handle2};
        {error, Reason} ->
            {error, Reason}
    end.

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
        {ok, #handle { hdl = Hdl, offset = Offset, is_dirty = IsDirty,
                       last_used_at = Then } = Handle1 } ->
            ok = case IsDirty of
                     true  -> file:sync(Hdl);
                     false -> ok
                 end,
            ok = file:close(Hdl),
            age_tree_delete(Then),
            {ok, Handle1 #handle { hdl = closed, trusted_offset = Offset,
                                   is_dirty = false }};
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

maybe_seek(NewOffset, Handle = #handle { hdl = Hdl, offset = Offset,
                                         at_eof = AtEoF }) ->
    {AtEoF1, NeedsSeek} = needs_seek(AtEoF, Offset, NewOffset),
    case (case NeedsSeek of
              true  -> file:position(Hdl, NewOffset);
              false -> {ok, Offset}
          end) of
        {ok, Offset1} = Result ->
            {Result, Handle #handle { offset = Offset1, at_eof = AtEoF1 }};
        {error, _} = Error ->
            {Error, Handle}
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
    case file:write(Hdl, lists:reverse(WriteBuffer)) of
        ok ->
            Offset1 = Offset + DataSize,
            {ok, Handle #handle { offset = Offset1, is_dirty = true,
                                  write_buffer = [], write_buffer_size = 0 }};
        {error, _} = Error ->
            {Error, Handle}
    end.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    Limit = case application:get_env(file_handles_high_watermark) of
                {ok, Watermark} when (is_integer(Watermark) andalso
                                      Watermark > 0) ->
                    Watermark;
                _ ->
                    ulimit()
            end,
    ObtainLimit = obtain_limit(Limit),
    error_logger:info_msg("Limiting to approx ~p file handles (~p sockets)~n",
                          [Limit, ObtainLimit]),
    Clients = ets:new(?CLIENT_ETS_TABLE, [set, private, {keypos, #cstate.pid}]),
    {ok, #fhc_state { elders         = dict:new(),
                      limit          = Limit,
                      open_count     = 0,
                      open_pending   = pending_new(),
                      obtain_limit   = ObtainLimit,
                      obtain_count   = 0,
                      obtain_pending = pending_new(),
                      clients        = Clients,
                      timer_ref      = undefined }}.

handle_call({open, Pid, Requested, EldestUnusedSince}, From,
            State = #fhc_state { open_count   = Count,
                                 open_pending = Pending,
                                 elders       = Elders,
                                 clients      = Clients })
  when EldestUnusedSince =/= undefined ->
    Elders1 = dict:store(Pid, EldestUnusedSince, Elders),
    Item = #pending { kind      = open,
                      pid       = Pid,
                      requested = Requested,
                      from      = From },
    ok = track_client(Pid, Clients),
    State1 = State #fhc_state { elders = Elders1 },
    case needs_reduce(State1 #fhc_state { open_count = Count + Requested }) of
        true  -> case ets:lookup(Clients, Pid) of
                     [#cstate { opened = 0 }] ->
                         true = ets:update_element(
                                  Clients, Pid, {#cstate.blocked, true}),
                         {noreply,
                          reduce(State1 #fhc_state {
                                   open_pending = pending_in(Item, Pending) })};
                     [#cstate { opened = Opened }] ->
                         true = ets:update_element(
                                  Clients, Pid,
                                  {#cstate.pending_closes, Opened}),
                         {reply, close, State1}
                 end;
        false -> {noreply, run_pending_item(Item, State1)}
    end;

handle_call({obtain, Pid}, From, State = #fhc_state { obtain_limit   = Limit,
                                                      obtain_count   = Count,
                                                      obtain_pending = Pending,
                                                      clients = Clients })
  when Limit =/= infinity andalso Count >= Limit ->
    ok = track_client(Pid, Clients),
    true = ets:update_element(Clients, Pid, {#cstate.blocked, true}),
    Item = #pending { kind = obtain, pid = Pid, requested = 1, from = From },
    {noreply, State #fhc_state { obtain_pending = pending_in(Item, Pending) }};
handle_call({obtain, Pid}, From, State = #fhc_state { obtain_count   = Count,
                                                      obtain_pending = Pending,
                                                      clients = Clients }) ->
    Item = #pending { kind = obtain, pid = Pid, requested = 1, from = From },
    ok = track_client(Pid, Clients),
    case needs_reduce(State #fhc_state { obtain_count = Count + 1 }) of
        true ->
            true = ets:update_element(Clients, Pid, {#cstate.blocked, true}),
            {noreply, reduce(State #fhc_state {
                               obtain_pending = pending_in(Item, Pending) })};
        false ->
            {noreply, run_pending_item(Item, State)}
    end;
handle_call({set_limit, Limit}, _From, State) ->
    {reply, ok, maybe_reduce(
                  process_pending(State #fhc_state {
                                    limit        = Limit,
                                    obtain_limit = obtain_limit(Limit) }))};
handle_call(get_limit, _From, State = #fhc_state { limit = Limit }) ->
    {reply, Limit, State}.

handle_cast({register_callback, Pid, MFA},
            State = #fhc_state { clients = Clients }) ->
    ok = track_client(Pid, Clients),
    true = ets:update_element(Clients, Pid, {#cstate.callback, MFA}),
    {noreply, State};

handle_cast({update, Pid, EldestUnusedSince},
            State = #fhc_state { elders = Elders })
  when EldestUnusedSince =/= undefined ->
    Elders1 = dict:store(Pid, EldestUnusedSince, Elders),
    %% don't call maybe_reduce from here otherwise we can create a
    %% storm of messages
    {noreply, State #fhc_state { elders = Elders1 }};

handle_cast({close, Pid, EldestUnusedSince},
            State = #fhc_state { elders = Elders, clients = Clients }) ->
    Elders1 = case EldestUnusedSince of
                  undefined -> dict:erase(Pid, Elders);
                  _         -> dict:store(Pid, EldestUnusedSince, Elders)
              end,
    ets:update_counter(Clients, Pid, {#cstate.pending_closes, -1, 0, 0}),
    {noreply, process_pending(
                update_counts(open, Pid, -1,
                              State #fhc_state { elders = Elders1 }))};

handle_cast({transfer, FromPid, ToPid}, State) ->
    ok = track_client(ToPid, State#fhc_state.clients),
    {noreply, process_pending(
                update_counts(obtain, ToPid, +1,
                              update_counts(obtain, FromPid, -1, State)))};

handle_cast(check_counts, State) ->
    {noreply, maybe_reduce(State #fhc_state { timer_ref = undefined })}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #fhc_state { elders         = Elders,
                                 open_count     = OpenCount,
                                 open_pending   = OpenPending,
                                 obtain_count   = ObtainCount,
                                 obtain_pending = ObtainPending,
                                 clients        = Clients }) ->
    [#cstate { opened = Opened, obtained = Obtained }] =
        ets:lookup(Clients, Pid),
    true = ets:delete(Clients, Pid),
    FilterFun = fun (#pending { pid = Pid1 }) -> Pid1 =/= Pid end,
    {noreply, process_pending(
                State #fhc_state {
                  open_count     = OpenCount - Opened,
                  open_pending   = filter_pending(FilterFun, OpenPending),
                  obtain_count   = ObtainCount - Obtained,
                  obtain_pending = filter_pending(FilterFun, ObtainPending),
                  elders         = dict:erase(Pid, Elders) })}.

terminate(_Reason, State = #fhc_state { clients = Clients }) ->
    ets:delete(Clients),
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
        queue_fold(fun (Item, {DeltaN, QueueN}) ->
                           case Fun(Item) of
                               true  -> {DeltaN, queue:in(Item, QueueN)};
                               false -> {DeltaN - requested(Item), QueueN}
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

pending_is_empty({0, _Queue}) ->
    true;
pending_is_empty({_N, _Queue}) ->
    false.

%%----------------------------------------------------------------------------
%% server helpers
%%----------------------------------------------------------------------------

obtain_limit(infinity) -> infinity;
obtain_limit(Limit)    -> case ?OBTAIN_LIMIT(Limit) of
                              OLimit when OLimit < 0 -> 0;
                              OLimit                 -> OLimit
                          end.

requested({_Kind, _Pid, Requested, _From}) ->
    Requested.

process_pending(State = #fhc_state { limit = infinity }) ->
    State;
process_pending(State) ->
    process_open(process_obtain(State)).

process_open(State = #fhc_state { limit        = Limit,
                                  open_pending = Pending,
                                  open_count   = OpenCount,
                                  obtain_count = ObtainCount }) ->
    {Pending1, State1} =
        process_pending(Pending, Limit - (ObtainCount + OpenCount), State),
    State1 #fhc_state { open_pending = Pending1 }.

process_obtain(State = #fhc_state { limit          = Limit,
                                    obtain_pending = Pending,
                                    obtain_limit   = ObtainLimit,
                                    obtain_count   = ObtainCount,
                                    open_count     = OpenCount }) ->
    Quota = lists:min([ObtainLimit - ObtainCount,
                       Limit - (ObtainCount + OpenCount)]),
    {Pending1, State1} = process_pending(Pending, Quota, State),
    State1 #fhc_state { obtain_pending = Pending1 }.

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
    gen_server:reply(From, ok),
    true = ets:update_element(Clients, Pid, {#cstate.blocked, false}),
    update_counts(Kind, Pid, Requested, State).

update_counts(Kind, Pid, Delta,
              State = #fhc_state { open_count   = OpenCount,
                                   obtain_count = ObtainCount,
                                   clients      = Clients }) ->
    {OpenDelta, ObtainDelta} = update_counts1(Kind, Pid, Delta, Clients),
    State #fhc_state { open_count   = OpenCount   + OpenDelta,
                       obtain_count = ObtainCount + ObtainDelta }.

update_counts1(open, Pid, Delta, Clients) ->
    ets:update_counter(Clients, Pid, {#cstate.opened, Delta}),
    {Delta, 0};
update_counts1(obtain, Pid, Delta, Clients) ->
    ets:update_counter(Clients, Pid, {#cstate.obtained, Delta}),
    {0, Delta}.

maybe_reduce(State) ->
    case needs_reduce(State) of
        true  -> reduce(State);
        false -> State
    end.

needs_reduce(#fhc_state { limit          = Limit,
                          open_count     = OpenCount,
                          open_pending   = OpenPending,
                          obtain_count   = ObtainCount,
                          obtain_limit   = ObtainLimit,
                          obtain_pending = ObtainPending }) ->
    Limit =/= infinity
        andalso ((OpenCount + ObtainCount > Limit)
                 orelse (not pending_is_empty(OpenPending))
                 orelse (ObtainCount < ObtainLimit
                         andalso not pending_is_empty(ObtainPending))).

reduce(State = #fhc_state { open_pending   = OpenPending,
                            obtain_pending = ObtainPending,
                            elders         = Elders,
                            clients        = Clients,
                            timer_ref      = TRef }) ->
    Now = now(),
    {CStates, Sum, ClientCount} =
        dict:fold(fun (Pid, Eldest, {CStatesAcc, SumAcc, CountAcc} = Accs) ->
                          [#cstate { pending_closes = PendingCloses,
                                     opened         = Opened,
                                     blocked        = Blocked } = CState] =
                              ets:lookup(Clients, Pid),
                          case Blocked orelse PendingCloses =:= Opened of
                              true  -> Accs;
                              false -> {[CState | CStatesAcc],
                                        SumAcc + timer:now_diff(Now, Eldest),
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
                                      pending_count(ObtainPending))
              end
    end,
    case TRef of
        undefined -> {ok, TRef1} = timer:apply_after(
                                     ?FILE_HANDLES_CHECK_INTERVAL,
                                     gen_server, cast, [?SERVER, check_counts]),
                     State #fhc_state { timer_ref = TRef1 };
        _         -> State
    end.

notify_age(CStates, AverageAge) ->
    lists:foreach(
      fun (#cstate { callback = undefined }) -> ok;
          (#cstate { callback = {M, F, A} }) -> apply(M, F, A ++ [AverageAge])
      end, CStates).

notify_age0(Clients, CStates, Required) ->
    Notifications =
        [CState || CState <- CStates, CState#cstate.callback =/= undefined],
    {L1, L2} = lists:split(random:uniform(length(Notifications)),
                           Notifications),
    notify(Clients, Required, L2 ++ L1).

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
    case ets:insert_new(Clients, #cstate { pid            = Pid,
                                           callback       = undefined,
                                           opened         = 0,
                                           obtained       = 0,
                                           blocked        = false,
                                           pending_closes = 0 }) of
        true  -> _MRef = erlang:monitor(process, Pid),
                 ok;
        false -> ok
    end.

%% For all unices, assume ulimit exists. Further googling suggests
%% that BSDs (incl OS X), solaris and linux all agree that ulimit -n
%% is file handles
ulimit() ->
    case os:type() of
        {win32, _OsName} ->
            ?FILE_HANDLES_LIMIT_WINDOWS - ?RESERVED_FOR_OTHERS;
        {unix, _OsName} ->
            %% Under Linux, Solaris and FreeBSD, ulimit is a shell
            %% builtin, not a command. In OS X, it's a command.
            %% Fortunately, os:cmd invokes the cmd in a shell env, so
            %% we're safe in all cases.
            case os:cmd("ulimit -n") of
                "unlimited" ->
                    infinity;
                String = [C|_] when $0 =< C andalso C =< $9 ->
                    Num = list_to_integer(
                            lists:takewhile(
                              fun (D) -> $0 =< D andalso D =< $9 end, String)) -
                        ?RESERVED_FOR_OTHERS,
                    lists:max([1, Num]);
                _ ->
                    %% probably a variant of
                    %% "/bin/sh: line 1: ulimit: command not found\n"
                    ?FILE_HANDLES_LIMIT_OTHER - ?RESERVED_FOR_OTHERS
            end;
        _ ->
            ?FILE_HANDLES_LIMIT_OTHER - ?RESERVED_FOR_OTHERS
    end.
