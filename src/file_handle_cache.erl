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
         append_write_buffer/1, copy/3, set_maximum_since_use/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([decrement/0, increment/0]).

-define(SERVER, ?MODULE).
-define(RESERVED_FOR_OTHERS, 50).
-define(FILE_HANDLES_LIMIT_WINDOWS, 10000000).
-define(FILE_HANDLES_LIMIT_OTHER, 1024).
-define(FILE_HANDLES_CHECK_INTERVAL, 2000).

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
          is_write,
          is_read,
          mode,
          options,
          path,
          last_used_at
        }).

-record(fhc_state,
        { elders,
          limit,
          count
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

-spec(open/3 ::
      (string(), [any()],
       [{'write_buffer', (non_neg_integer()|'infinity'|'unbuffered')}]) ->
             ({'ok', ref()} | error())).
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
-spec(set_maximum_since_use/1 :: (non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [],
                           [{timeout, infinity}]).

open(Path, Mode, Options) ->
    case is_appender(Mode) of
        true  ->
            {error, append_not_supported};
        false ->
            Path1 = filename:absname(Path),
            File1 = #file { reader_count = RCount, has_writer = HasWriter } =
                case get({Path1, fhc_file}) of
                    File = #file {} -> File;
                    undefined       -> File = #file { reader_count = 0,
                                                      has_writer = false },
                                       put({Path1, fhc_file}, File),
                                       File
                end,
            IsWriter = is_writer(Mode),
            case IsWriter andalso HasWriter of
                true  -> {error, writer_exists};
                false -> RCount1 = case is_reader(Mode) of
                                       true  -> RCount + 1;
                                       false -> RCount
                                   end,
                         HasWriter1 = HasWriter orelse IsWriter,
                         put({Path1, fhc_file},
                             File1 #file { reader_count = RCount1,
                                           has_writer = HasWriter1}),
                         Ref = make_ref(),
                         case open1(Path1, Mode, Options, Ref, bof) of
                             {ok, _Handle} -> {ok, Ref};
                             Error         -> Error
                         end
            end
    end.

close(Ref) ->
    case erase({Ref, fhc_handle}) of
        undefined -> ok;
        Handle    -> close1(Ref, Handle, hard)
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
                      case Limit /= infinity andalso Size1 > Limit of
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
                  ok ->
                      {ok, [Handle #handle { trusted_offset = Offset,
                                             is_dirty = false }]};
                  Error ->
                      {Error, [Handle]}
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
                                trusted_offset = TrustedOffset }]) ->
              case file:truncate(Hdl) of
                  ok    -> TrustedOffset1 = lists:min([Offset, TrustedOffset]),
                           {ok, [Handle1 #handle {
                                   at_eof = true,
                                   trusted_offset = TrustedOffset1 }]};
                  Error -> {Error, [Handle1]}
              end
      end).

last_sync_offset(Ref) ->
    with_handles([Ref], fun ([#handle { trusted_offset = TrustedOffset }]) ->
                                {ok, TrustedOffset}
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

append_write_buffer(Ref) ->
    with_flushed_handles([Ref], fun ([Handle]) -> {ok, [Handle]} end).

copy(Src, Dest, Count) ->
    with_flushed_handles(
      [Src, Dest],
      fun ([SHandle = #handle { is_read = true, hdl = SHdl, offset = SOffset },
            DHandle = #handle { is_write = true, hdl = DHdl, offset = DOffset }]
          ) ->
              case file:copy(SHdl, DHdl, Count) of
                  {ok, Count1} = Result1 ->
                      {Result1,
                       [SHandle #handle { offset = SOffset + Count1 },
                        DHandle #handle { offset = DOffset + Count1 }]};
                  Error ->
                      {Error, [SHandle, DHandle]}
              end;
          (_Handles) ->
              {error, incorrect_handle_modes}
      end).

set_maximum_since_use(MaximumAge) ->
    Now = now(),
    Report =
        lists:foldl(
          fun ({{Ref, fhc_handle},
                Handle = #handle { hdl = Hdl, last_used_at = Then }}, Rep) ->
              Age = timer:now_diff(Now, Then),
              case Hdl /= closed andalso Age >= MaximumAge of
                  true -> case close1(Ref, Handle, soft) of
                              {ok, Handle1} -> put({Ref, fhc_handle}, Handle1),
                                               false;
                              _             -> Rep
                          end;
                  false -> Rep
              end;
              (_KeyValuePair, Rep) ->
                  Rep
          end, true, get()),
    case Report of
        true -> report_eldest();
        false -> ok
    end.

decrement() ->
    gen_server2:cast(?SERVER, decrement).

increment() ->
    gen_server2:cast(?SERVER, increment).

%%----------------------------------------------------------------------------
%% Internal versions for the above
%%----------------------------------------------------------------------------


%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

is_reader(Mode) -> lists:member(read, Mode).

is_writer(Mode) -> lists:member(write, Mode).

is_appender(Mode) -> lists:member(append, Mode).

report_eldest() ->
    with_age_tree(
      fun (Tree) ->
              case gb_trees:is_empty(Tree) of
                  true  -> Tree;
                  false -> {Oldest, _Ref} = gb_trees:smallest(Tree),
                           gen_server2:cast(?SERVER, {update, self(), Oldest})
              end,
              Tree
      end),
    ok.

with_handles(Refs, Fun) ->
    ResHandles = lists:foldl(
                   fun (Ref, {ok, HandlesAcc}) ->
                           case get_or_reopen(Ref) of
                               {ok, Handle} -> {ok, [Handle | HandlesAcc]};
                               Error        -> Error
                           end;
                       (_Ref, Error) ->
                           Error
                   end, {ok, []}, Refs),
    case ResHandles of
        {ok, Handles} ->
            case erlang:apply(Fun, [lists:reverse(Handles)]) of
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
                      erlang:apply(Fun, [lists:reverse(Handles1)]);
                  {Error, Handles1} ->
                      {Error, lists:reverse(Handles1)}
              end
      end).

get_or_reopen(Ref) ->
    case get({Ref, fhc_handle}) of
        undefined ->
            {error, not_open, Ref};
        #handle { hdl = closed, mode = Mode, options = Options,
                  offset = Offset, path = Path } ->
            open1(Path, Mode, Options, Ref, Offset);
        Handle ->
            {ok, Handle}
    end.

get_or_create_age_tree() ->
    case get(fhc_age_tree) of
        undefined -> gb_trees:empty();
        AgeTree   -> AgeTree
    end.

with_age_tree(Fun) ->
    put(fhc_age_tree, Fun(get_or_create_age_tree())).

put_handle(Ref, Handle = #handle { last_used_at = Then }) ->
    Now = now(),
    with_age_tree(
      fun (Tree) -> gb_trees:insert(Now, Ref, gb_trees:delete(Then, Tree)) end),
    put({Ref, fhc_handle}, Handle #handle { last_used_at = Now }).

open1(Path, Mode, Options, Ref, Offset) ->
    case file:open(Path, Mode) of
        {ok, Hdl} ->
            WriteBufferSize =
                case proplists:get_value(write_buffer, Options, unbuffered) of
                    unbuffered           -> 0;
                    infinity             -> infinity;
                    N when is_integer(N) -> N
                end,
            Now = now(),
            Handle =
                #handle { hdl = Hdl, offset = 0, trusted_offset = Offset,
                          write_buffer_size = 0, options = Options,
                          write_buffer_size_limit = WriteBufferSize,
                          write_buffer = [], at_eof = false, mode = Mode,
                          is_write = is_writer(Mode), is_read = is_reader(Mode),
                          path = Path, last_used_at = Now,
                          is_dirty = false },
            {{ok, _Offset}, Handle1} = maybe_seek(Offset, Handle),
            put({Ref, fhc_handle}, Handle1),
            with_age_tree(fun (Tree) ->
                                  Tree1 = gb_trees:insert(Now, Ref, Tree),
                                  {Oldest, _Ref} = gb_trees:smallest(Tree1),
                                  gen_server2:cast(?SERVER,
                                                   {open, self(), Oldest}),
                                  Tree1
                          end),
            {ok, Handle1};
        {error, Reason} ->
            {error, Reason}
    end.

close1(Ref, Handle, SoftOrHard) ->
    case write_buffer(Handle) of
        {ok, #handle { hdl = Hdl, path = Path, is_dirty = IsDirty,
                       is_read = IsReader, is_write = IsWriter,
                       last_used_at = Then } = Handle1 } ->
            case Hdl of
                closed ->
                    ok;
                _ ->
                    ok = case IsDirty of
                             true  -> file:sync(Hdl);
                             false -> ok
                         end,
                    ok = file:close(Hdl),
                    with_age_tree(
                      fun (Tree) ->
                              Tree1 = gb_trees:delete(Then, Tree),
                              Oldest =
                                  case gb_trees:is_empty(Tree1) of
                                      true  ->  undefined;
                                      false -> {Oldest1, _Ref} =
                                                   gb_trees:smallest(Tree1),
                                               Oldest1
                                  end,
                              gen_server2:cast(
                                ?SERVER, {close, self(), Oldest}),
                              Tree1
                      end)
            end,
            case SoftOrHard of
                hard ->
                    #file { reader_count = RCount, has_writer = HasWriter } =
                        File = get({Path, fhc_file}),
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
                soft ->
                    {ok, Handle1 #handle { hdl = closed }}
            end;
        {Error, Handle1} ->
            put_handle(Ref, Handle1),
            Error
    end.

maybe_seek(NewOffset, Handle = #handle { hdl = Hdl, at_eof = AtEoF,
                                         offset = Offset }) ->
    {AtEoF1, NeedsSeek} = needs_seek(AtEoF, Offset, NewOffset),
    Result = case NeedsSeek of
                 true  -> file:position(Hdl, NewOffset);
                 false -> {ok, Offset}
             end,
    case Result of
        {ok, Offset1} ->
            {Result, Handle #handle { at_eof = AtEoF1, offset = Offset1 }};
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
            {ok, Handle #handle { offset = Offset1, write_buffer = [],
                                  write_buffer_size = 0, is_dirty = true }};
        {error, _} = Error ->
            {Error, Handle}
    end.

%%----------------------------------------------------------------------------
%% gen_server
%%----------------------------------------------------------------------------

init([]) ->
    Limit = case application:get_env(file_handles_high_watermark) of
                {ok, Watermark} when (is_integer(Watermark) andalso
                                      Watermark > 0) ->
                    Watermark;
                _ ->
                    ulimit()
            end,
    error_logger:info_msg("Limiting to approx ~p file handles~n", [Limit]),
    {ok, #fhc_state { elders = dict:new(), limit = Limit, count = 0}}.

handle_call(_Msg, _From, State) ->
    {reply, message_not_understood, State}.

handle_cast({open, Pid, EldestUnusedSince}, State =
            #fhc_state { elders = Elders, count = Count }) ->
    Elders1 = dict:store(Pid, EldestUnusedSince, Elders),
    {noreply, maybe_reduce(State #fhc_state { elders = Elders1,
                                              count = Count + 1 })};

handle_cast({update, Pid, EldestUnusedSince}, State =
            #fhc_state { elders = Elders }) ->
    Elders1 = dict:store(Pid, EldestUnusedSince, Elders),
    %% don't call maybe_reduce from here otherwise we can create a
    %% storm of messages
    {noreply, State #fhc_state { elders = Elders1 }};

handle_cast({close, Pid, EldestUnusedSince}, State =
            #fhc_state { elders = Elders, count = Count }) ->
    Elders1 = case EldestUnusedSince of
                  undefined -> dict:erase(Pid, Elders);
                  _         -> dict:store(Pid, EldestUnusedSince, Elders)
              end,
    {noreply, State #fhc_state { elders = Elders1, count = Count - 1 }};

handle_cast(increment, State = #fhc_state { count = Count }) ->
    {noreply, maybe_reduce(State #fhc_state { count = Count + 1 })};

handle_cast(decrement, State = #fhc_state { count = Count }) ->
    {noreply, State #fhc_state { count = Count - 1 }};

handle_cast(check_counts, State) ->
    {noreply, maybe_reduce(State)}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% server helpers
%%----------------------------------------------------------------------------

maybe_reduce(State = #fhc_state { limit = Limit, count = Count,
                                  elders = Elders })
  when Limit /= infinity andalso Count >= Limit ->
    Now = now(),
    {Pids, Sum, ClientCount} =
        dict:fold(fun (_Pid, undefined, Accs) ->
                          Accs;
                      (Pid, Eldest, {PidsAcc, SumAcc, CountAcc}) ->
                          {[Pid|PidsAcc], SumAcc + timer:now_diff(Now, Eldest),
                           CountAcc + 1}
                  end, {[], 0, 0}, Elders),
    case Pids of
        [] -> ok;
        _ ->
            %% ClientCount can't be 0 if we have some pids
            AverageAge = Sum / ClientCount,
            lists:foreach(
              fun (Pid) -> Pid ! {?MODULE, maximum_eldest_since_use, AverageAge}
              end, Pids)
    end,
    {ok, _TRef} = timer:apply_after(?FILE_HANDLES_CHECK_INTERVAL, gen_server2,
                                    cast, [?SERVER, check_counts]),
    State;
maybe_reduce(State) ->
    State.

%% Googling around suggests that Windows has a limit somewhere around
%% 16M, eg
%% http://blogs.technet.com/markrussinovich/archive/2009/09/29/3283844.aspx
%% For everything else, assume ulimit exists. Further googling
%% suggests that BSDs (incl OS X), solaris and linux all agree that
%% ulimit -n is file handles
ulimit() ->
    try
        %% under Linux, Solaris and FreeBSD, ulimit is a shell
        %% builtin, not a command. In OS X, it's a command, but it's
        %% still safe to call it this way:
        case rabbit_misc:cmd("sh -c \"ulimit -n\"") of
            "unlimited" -> infinity;
            String = [C|_] when $0 =< C andalso C =< $9 ->
                Num = list_to_integer(
                        lists:takewhile(fun (D) -> $0 =< D andalso D =< $9 end,
                                        String)) - ?RESERVED_FOR_OTHERS,
                lists:max([1, Num]);
            String ->
                error_logger:warning_msg(
                  "Unexpected result of \"ulimit -n\": ~p~n", [String]),
                throw({unexpected_result, String})
        end
    catch _ -> case os:type() of
                   {win32, _OsName} ->
                       ?FILE_HANDLES_LIMIT_WINDOWS;
                   _ ->
                       ?FILE_HANDLES_LIMIT_OTHER - ?RESERVED_FOR_OTHERS
               end
    end.
