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

-module(rabbit_msg_store).

-export([init/6, write/4, read/2, attrs/2, remove/2, release/2,
         needs_sync/2, sync/1, cleanup/1, cache_info/1, memory/1,
         ets_bpr/1, to_disk_only_mode/1, to_ram_disk_mode/1]).

%%----------------------------------------------------------------------------

-record(msstate,
        {operation_mode,         %% ram_disk | disk_only
         dir,                    %% store directory
         msg_location_dets,      %% where are messages?
         msg_location_ets,       %% as above, but for ets version
         file_summary,           %% what's in the files?
         current_file_num,       %% current file name as number
         current_file_name,      %% current file name
         current_file_handle,    %% current file handle
         current_offset,         %% current offset within current file
         current_dirty,          %% has the current file been written to
                                 %% since the last fsync?
         file_size_limit,        %% how big can our files get?
         read_file_handle_cache, %% file handle cache for reading
         last_sync_offset,       %% current_offset at the last time we sync'd
         message_cache,          %% ets message cache
         ets_bytes_per_record    %% bytes per record in msg_location_ets
         }).

-record(msg_location,
        {msg_id, ref_count, file, offset, total_size, attrs}).

-record(file_summary,
        {file, valid_total_size, contiguous_top, left, right}).

-define(MSG_LOC_NAME,          rabbit_disk_queue_msg_location).
-define(FILE_SUMMARY_ETS_NAME, rabbit_disk_queue_file_summary).
-define(FILE_EXTENSION,        ".rdq").
-define(FILE_EXTENSION_TMP,    ".rdt").
-define(FILE_EXTENSION_DETS,   ".dets").

-define(CACHE_ETS_NAME,        rabbit_disk_queue_cache).
-define(CACHE_MAX_SIZE,        10485760).

-define(BINARY_MODE, [raw, binary]).
-define(READ_MODE,   [read, read_ahead]).
-define(WRITE_MODE,  [write, delayed_write]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mode() :: 'ram_disk' | 'disk_only').
-type(dets_table() :: any()).
-type(ets_table() :: any()).
-type(msg_id() :: any()).
-type(msg() :: any()).
-type(msg_attrs() :: any()).
-type(file_path() :: any()).
-type(io_device() :: any()).

-type(msstate() :: #msstate {
               operation_mode         :: mode(),
               dir                    :: file_path(),
               msg_location_dets      :: dets_table(),
               msg_location_ets       :: ets_table(),
               file_summary           :: ets_table(),
               current_file_num       :: non_neg_integer(),
               current_file_name      :: file_path(),
               current_file_handle    :: io_device(),
               current_offset         :: non_neg_integer(),
               current_dirty          :: boolean(),
               file_size_limit        :: non_neg_integer(),
               read_file_handle_cache :: any(),
               last_sync_offset       :: non_neg_integer(),
               message_cache          :: ets_table(),
               ets_bytes_per_record   :: non_neg_integer()
               }).

-spec(init/6 :: ('ram_disk' | 'disk_only', file_path(),
                 non_neg_integer(), non_neg_integer(),
                 fun ((msg_id()) -> non_neg_integer()), non_neg_integer()) ->
             msstate()).
-spec(write/4 :: (msg_id(), msg(), msg_attrs(), msstate()) -> msstate()).
-spec(read/2 :: (msg_id(), msstate()) -> {msg(), msstate()} | 'not_found').
-spec(attrs/2 :: (msg_id(), msstate()) -> msg_attrs() | 'not_found').
-spec(remove/2 :: ([msg_id()], msstate()) -> msstate()).
-spec(release/2 :: ([msg_id()], msstate()) -> msstate()).
-spec(needs_sync/2 :: ([msg_id()], msstate()) -> boolean()).
-spec(sync/1 :: (msstate()) -> msstate()).
-spec(cleanup/1 :: (msstate()) -> msstate()).
-spec(cache_info/1 :: (msstate()) -> [{atom(), term()}]).
-spec(memory/1 :: (msstate()) -> non_neg_integer()).
-spec(ets_bpr/1 :: (msstate()) -> non_neg_integer()).
-spec(to_disk_only_mode/1 :: (msstate()) -> msstate()).
-spec(to_ram_disk_mode/1 :: (msstate()) -> msstate()).

-endif.

%%----------------------------------------------------------------------------

%% The components:
%%
%% MsgLocation: this is a (d)ets table which contains:
%%              {MsgId, RefCount, File, Offset, TotalSize, Attrs}
%% FileSummary: this is an ets table which contains:
%%              {File, ValidTotalSize, ContiguousTop, Left, Right}
%%
%% The basic idea is that messages are appended to the current file up
%% until that file becomes too big (> file_size_limit). At that point,
%% the file is closed and a new file is created on the _right_ of the
%% old file which is used for new messages. Files are named
%% numerically ascending, thus the file with the lowest name is the
%% eldest file.
%%
%% We need to keep track of which messages are in which files (this is
%% the MsgLocation table); how much useful data is in each file and
%% which files are on the left and right of each other. This is the
%% purpose of the FileSummary table.
%%
%% As messages are removed from files, holes appear in these
%% files. The field ValidTotalSize contains the total amount of useful
%% data left in the file, whilst ContiguousTop contains the amount of
%% valid data right at the start of each file. These are needed for
%% garbage collection.
%%
%% When we discover that either a file is now empty or that it can be
%% combined with the useful data in either its left or right file, we
%% compact the two files together. This keeps disk utilisation high
%% and aids performance.
%%
%% Given the compaction between two files, the left file is considered
%% the ultimate destination for the good data in the right file. If
%% necessary, the good data in the left file which is fragmented
%% throughout the file is written out to a temporary file, then read
%% back in to form a contiguous chunk of good data at the start of the
%% left file. Thus the left file is garbage collected and
%% compacted. Then the good data from the right file is copied onto
%% the end of the left file. MsgLocation and FileSummary tables are
%% updated.
%%
%% On startup, we scan the files we discover, dealing with the
%% possibilites of a crash have occured during a compaction (this
%% consists of tidyup - the compaction is deliberately designed such
%% that data is duplicated on disk rather than risking it being lost),
%% and rebuild the dets and ets tables (MsgLocation, FileSummary).
%%
%% MsgLocation is deliberately a dets table in order to ensure that we
%% are not RAM constrained. However, for performance reasons, it is
%% possible to call to_ram_disk_mode/0 which will convert MsgLocation
%% to an ets table. This results in a massive performance improvement,
%% at the expense of greater RAM usage. The idea is that when memory
%% gets tight, we switch to disk_only mode but otherwise try to run in
%% ram_disk mode.
%%
%% So, with this design, messages move to the left. Eventually, they
%% should end up in a contiguous block on the left and are then never
%% rewritten. But this isn't quite the case. If in a file there is one
%% message that is being ignored, for some reason, and messages in the
%% file to the right and in the current block are being read all the
%% time then it will repeatedly be the case that the good data from
%% both files can be combined and will be written out to a new
%% file. Whenever this happens, our shunned message will be rewritten.
%%
%% So, provided that we combine messages in the right order,
%% (i.e. left file, bottom to top, right file, bottom to top),
%% eventually our shunned message will end up at the bottom of the
%% left file. The compaction/combining algorithm is smart enough to
%% read in good data from the left file that is scattered throughout
%% (i.e. C and D in the below diagram), then truncate the file to just
%% above B (i.e. truncate to the limit of the good contiguous region
%% at the start of the file), then write C and D on top and then write
%% E, F and G from the right file on top. Thus contiguous blocks of
%% good data at the bottom of files are not rewritten (yes, this is
%% the data the size of which is tracked by the ContiguousTop
%% variable. Judicious use of a mirror is required).
%%
%% +-------+    +-------+         +-------+
%% |   X   |    |   G   |         |   G   |
%% +-------+    +-------+         +-------+
%% |   D   |    |   X   |         |   F   |
%% +-------+    +-------+         +-------+
%% |   X   |    |   X   |         |   E   |
%% +-------+    +-------+         +-------+
%% |   C   |    |   F   |   ===>  |   D   |
%% +-------+    +-------+         +-------+
%% |   X   |    |   X   |         |   C   |
%% +-------+    +-------+         +-------+
%% |   B   |    |   X   |         |   B   |
%% +-------+    +-------+         +-------+
%% |   A   |    |   E   |         |   A   |
%% +-------+    +-------+         +-------+
%%   left         right             left
%%
%% From this reasoning, we do have a bound on the number of times the
%% message is rewritten. From when it is inserted, there can be no
%% files inserted between it and the head of the queue, and the worst
%% case is that everytime it is rewritten, it moves one position lower
%% in the file (for it to stay at the same position requires that
%% there are no holes beneath it, which means truncate would be used
%% and so it would not be rewritten at all). Thus this seems to
%% suggest the limit is the number of messages ahead of it in the
%% queue, though it's likely that that's pessimistic, given the
%% requirements for compaction/combination of files.
%%
%% The other property is that we have is the bound on the lowest
%% utilisation, which should be 50% - worst case is that all files are
%% fractionally over half full and can't be combined (equivalent is
%% alternating full files and files with only one tiny message in
%% them).
%%
%% Messages are reference-counted. When a message with the same id is
%% written several times we only store it once, and only remove it
%% from the store when it has been removed the same number of times.
%%
%% The reference counts do not persist. Therefore the initialisation
%% function must be provided with a function that determines the
%% initial reference count of any (recovered) message.
%%
%% Read messages with a reference count greater than one are entered
%% into a message cache. The purpose of the cache is not especially
%% performance, though it can help there too, but prevention of memory
%% explosion. It ensures that as messages with a high reference count
%% are read from several processes they are read back as the same
%% binary object rather than multiples of identical binary
%% objects.

%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

init(Mode, Dir, FileSizeLimit, ReadFileHandlesLimit, RefCountFun,
     EtsBytesPerRecord) ->

    file:delete(msg_location_dets_file(Dir)),

    {ok, MsgLocationDets} =
        dets:open_file(?MSG_LOC_NAME,
                       [{file, msg_location_dets_file(Dir)},
                        {min_no_slots, 1024*1024},
                        %% man says this should be <= 32M. But it works...
                        {max_no_slots, 30*1024*1024},
                        {type, set},
                        {keypos, 2}
                       ]),

    %% it would be better to have this as private, but dets:from_ets/2
    %% seems to blow up if it is set private - see bug21489
    MsgLocationEts = ets:new(?MSG_LOC_NAME, [set, protected, {keypos, 2}]),

    InitName = "0" ++ ?FILE_EXTENSION,
    HandleCache = rabbit_file_handle_cache:init(ReadFileHandlesLimit,
                                                ?BINARY_MODE ++ [read]),
    State =
        #msstate { operation_mode         = Mode,
                   dir                    = Dir,
                   msg_location_dets      = MsgLocationDets,
                   msg_location_ets       = MsgLocationEts,
                   file_summary           = ets:new(
                                              ?FILE_SUMMARY_ETS_NAME,
                                              [set, private, {keypos, 2}]),
                   current_file_num       = 0,
                   current_file_name      = InitName,
                   current_file_handle    = undefined,
                   current_offset         = 0,
                   current_dirty          = false,
                   file_size_limit        = FileSizeLimit,
                   read_file_handle_cache = HandleCache,
                   last_sync_offset       = 0,
                   message_cache          = ets:new(?CACHE_ETS_NAME,
                                                    [set, private]),
                   ets_bytes_per_record   = EtsBytesPerRecord
                  },
    
    Files = 
        sort_file_names(filelib:wildcard("*" ++ ?FILE_EXTENSION, Dir)),
    TmpFiles =
        sort_file_names(filelib:wildcard("*" ++ ?FILE_EXTENSION_TMP, Dir)),
    ok = recover_crashed_compactions(RefCountFun, Dir, Files, TmpFiles),
    %% There should be no more tmp files now, so go ahead and load the
    %% whole lot
    State1 = #msstate { current_file_name = CurrentName,
                        current_offset = Offset }  =
        load_messages(RefCountFun, Files, State),

    %% read is only needed so that we can seek
    {ok, FileHdl} = open_file(Dir, CurrentName, ?WRITE_MODE ++ [read]),
    {ok, Offset} = file:position(FileHdl, Offset),

    State1 #msstate { current_file_handle = FileHdl }.

write(MsgId, Msg, Attrs,
      State = #msstate { current_file_handle = CurHdl,
                         current_file_name   = CurName,
                         current_offset      = CurOffset,
                         file_summary        = FileSummary }) ->
    case dets_ets_lookup(State, MsgId) of
        [] ->
            %% New message, lots to do
            {ok, TotalSize} = rabbit_msg_file:append(CurHdl, MsgId, Msg, Attrs),
            true = dets_ets_insert_new(
                     State, #msg_location {
                       msg_id = MsgId, ref_count = 1, file = CurName,
                       offset = CurOffset, total_size = TotalSize,
                       attrs = Attrs }),
            [FSEntry = #file_summary { valid_total_size = ValidTotalSize,
                                       contiguous_top = ContiguousTop,
                                       right = undefined }] =
                ets:lookup(FileSummary, CurName),
            ValidTotalSize1 = ValidTotalSize + TotalSize,
            ContiguousTop1 = if CurOffset =:= ContiguousTop ->
                                     %% can't be any holes in this file
                                     ValidTotalSize1;
                                true -> ContiguousTop
                             end,
            true = ets:insert(FileSummary, FSEntry #file_summary {
                                             valid_total_size = ValidTotalSize1,
                                             contiguous_top = ContiguousTop1 }),
            NextOffset = CurOffset + TotalSize,
            maybe_roll_to_new_file(
              NextOffset, State #msstate {current_offset = NextOffset,
                                          current_dirty = true});
        [StoreEntry =
         #msg_location { msg_id = MsgId, ref_count = RefCount }] ->
            %% We already know about it, just update counter
            ok = dets_ets_insert(State, StoreEntry #msg_location {
                                          ref_count = RefCount + 1 }),
            State
    end.

read(MsgId, State) ->
    Objs = dets_ets_lookup(State, MsgId),
    case Objs of
        [] ->
            not_found;
        [#msg_location { ref_count  = RefCount,
                         file       = File,
                         offset     = Offset,
                         total_size = TotalSize }] ->
            case fetch_and_increment_cache(MsgId, State) of
                not_found ->
                    {{ok, {MsgId, Msg, _Attrs}}, State1} =
                        with_read_handle_at(
                          File, Offset,
                          fun(Hdl) ->
                                  Res = case rabbit_msg_file:read(
                                               Hdl, TotalSize) of
                                            {ok, {MsgId, _, _}} = Obj -> Obj;
                                            {ok, Rest} ->
                                                throw({error,
                                                       {misread, 
                                                        [{old_state, State},
                                                         {file, File},
                                                         {offset, Offset},
                                                         {read, Rest}]}})
                                        end,
                                  {Offset + TotalSize, Res}
                          end, State),
                    ok = if RefCount > 1 ->
                                 insert_into_cache(MsgId, Msg, State1);
                            true -> ok
                                    %% it's not in the cache and we
                                    %% only have one reference to the
                                    %% message. So don't bother
                                    %% putting it in the cache.
                         end,
                    {Msg, State1};
                {Msg, _RefCount} ->
                    {Msg, State}
            end
    end.

attrs(MsgId, State) ->
    Objs = dets_ets_lookup(State, MsgId),
    case Objs of
        [] -> not_found;
        [#msg_location { msg_id = MsgId, attrs = Attrs }] -> Attrs
    end.

remove(MsgIds, State = #msstate { current_file_name = CurName }) ->
    compact(sets:to_list(
              lists:foldl(
                fun (MsgId, Files1) ->
                        case remove_message(MsgId, State) of
                            {compact, File} ->
                                if CurName =:= File -> Files1;
                                   true -> sets:add_element(File, Files1)
                                end;
                            no_compact -> Files1
                        end
                end, sets:new(), MsgIds)),
            State).

release(MsgIds, State) ->
    lists:foreach(fun (MsgId) -> decrement_cache(MsgId, State) end, MsgIds),
    State.

needs_sync(_MsgIds, #msstate { current_dirty = false }) ->
    false;    
needs_sync(MsgIds, State = #msstate { current_file_name = CurFile,
                                      last_sync_offset  = SyncOffset }) ->
    lists:any(fun (MsgId) ->
                      [#msg_location { msg_id = MsgId, file = File,
                                       offset = Offset }] =
                          dets_ets_lookup(State, MsgId),
                      File =:= CurFile andalso Offset >= SyncOffset
              end, MsgIds).

sync(State = #msstate { current_dirty = false }) ->
    State;
sync(State = #msstate { current_file_handle = CurHdl,
                        current_offset = CurOffset }) ->
    ok = file:sync(CurHdl),
    State #msstate { current_dirty = false, last_sync_offset = CurOffset }.

cleanup(State = #msstate { dir                    = Dir,
                           msg_location_dets      = MsgLocationDets,
                           msg_location_ets       = MsgLocationEts,
                           file_summary           = FileSummary,
                           current_file_handle    = FileHdl,
                           read_file_handle_cache = HC }) ->
    State1 = case FileHdl of
                 undefined -> State;
                 _ -> State2 = sync(State),
                      file:close(FileHdl),
                      State2
             end,
    HC1 = rabbit_file_handle_cache:close_all(HC),
    dets:close(MsgLocationDets),
    file:delete(msg_location_dets_file(Dir)),
    ets:delete(MsgLocationEts),
    ets:delete(FileSummary),
    State1 #msstate { msg_location_dets      = undefined,
                      msg_location_ets       = undefined,
                      file_summary           = undefined,
                      current_file_handle    = undefined,
                      current_dirty          = false,
                      read_file_handle_cache = HC1
                     }.

cache_info(#msstate { message_cache = Cache }) ->
    ets:info(Cache).

memory(#msstate { operation_mode   = ram_disk,
                  file_summary     = FileSummary,
                  msg_location_ets = MsgLocationEts,
                  message_cache    = Cache }) ->
    erlang:system_info(wordsize) *
        lists:sum([ets:info(Table, memory) ||
                      Table <- [FileSummary, MsgLocationEts, Cache]]);
memory(#msstate { operation_mode       = disk_only,
                  file_summary         = FileSummary,
                  msg_location_dets    = MsgLocationDets,
                  message_cache        = Cache,
                  ets_bytes_per_record = EtsBytesPerRecord }) ->
    erlang:system_info(wordsize) *
        lists:sum([ets:info(Table, memory) ||
                      Table <- [FileSummary, Cache]]) +
        rabbit_misc:ceil(dets:info(MsgLocationDets, size) * EtsBytesPerRecord).

ets_bpr(#msstate { operation_mode = disk_only,
                   ets_bytes_per_record = EtsBytesPerRecord }) ->
    EtsBytesPerRecord;
ets_bpr(#msstate { operation_mode = ram_disk,
                   msg_location_ets = MsgLocationEts }) ->
    erlang:system_info(wordsize) * ets:info(MsgLocationEts, memory) /
        lists:max([1, ets:info(MsgLocationEts, size)]).

to_disk_only_mode(State = #msstate { operation_mode = disk_only }) ->
    State;
to_disk_only_mode(State = #msstate { operation_mode = ram_disk,
                                     msg_location_dets = MsgLocationDets,
                                     msg_location_ets = MsgLocationEts }) ->
    ok = dets:from_ets(MsgLocationDets, MsgLocationEts),
    true = ets:delete_all_objects(MsgLocationEts),
    State #msstate { operation_mode       = disk_only,
                     ets_bytes_per_record = ets_bpr(State) }.

to_ram_disk_mode(State = #msstate { operation_mode = ram_disk }) ->
    State;
to_ram_disk_mode(State = #msstate { operation_mode = disk_only,
                                    msg_location_dets = MsgLocationDets,
                                    msg_location_ets = MsgLocationEts }) ->
    true = ets:from_dets(MsgLocationEts, MsgLocationDets),
    ok = dets:delete_all_objects(MsgLocationDets),
    State #msstate { operation_mode       = ram_disk,
                     ets_bytes_per_record = undefined }.

%%----------------------------------------------------------------------------
%% general helper functions
%%----------------------------------------------------------------------------

form_filename(Dir, Name) ->
    filename:join(Dir, Name).

msg_location_dets_file(Dir) ->
    form_filename(Dir, atom_to_list(?MSG_LOC_NAME) ++ ?FILE_EXTENSION_DETS).

open_file(Dir, File, Mode) ->
    file:open(form_filename(Dir, File), ?BINARY_MODE ++ Mode).

sort_file_names(Files) ->
    lists:sort(fun (A, B) ->
                       ANum = list_to_integer(filename:rootname(A)),
                       BNum = list_to_integer(filename:rootname(B)),
                       ANum < BNum
               end, Files).

preallocate(Hdl, FileSizeLimit, FinalPos) ->
    {ok, FileSizeLimit} = file:position(Hdl, FileSizeLimit),
    ok = file:truncate(Hdl),
    {ok, FinalPos} = file:position(Hdl, FinalPos),
    ok.

truncate_and_extend_file(FileHdl, Lowpoint, Highpoint) ->
    {ok, Lowpoint} = file:position(FileHdl, Lowpoint),
    ok = file:truncate(FileHdl),
    ok = preallocate(FileHdl, Highpoint, Lowpoint).

with_read_handle_at(File, Offset, Fun,
                    State = #msstate { dir                    = Dir,
                                       read_file_handle_cache = HC,
                                       current_file_name      = CurName,
                                       current_dirty          = IsDirty,
                                       last_sync_offset       = SyncOffset }) ->
    State1 = if CurName =:= File andalso IsDirty andalso Offset >= SyncOffset ->
                     sync(State);
                true -> State
             end,
    FilePath = form_filename(Dir, File),
    {Result, HC1} =
        rabbit_file_handle_cache:with_file_handle_at(FilePath, Offset, Fun, HC),
    {Result, State1 #msstate { read_file_handle_cache = HC1 }}.

remove_message(MsgId, State = #msstate { file_summary = FileSummary }) ->
    [StoreEntry =
     #msg_location { msg_id = MsgId, ref_count = RefCount, file = File,
                     offset = Offset, total_size = TotalSize }] =
        dets_ets_lookup(State, MsgId),
    case RefCount of
        1 ->
            ok = dets_ets_delete(State, MsgId),
            ok = remove_cache_entry(MsgId, State),
            [FSEntry = #file_summary { valid_total_size = ValidTotalSize,
                                       contiguous_top = ContiguousTop }] =
                ets:lookup(FileSummary, File),
            ContiguousTop1 = lists:min([ContiguousTop, Offset]),
            ValidTotalSize1 = ValidTotalSize - TotalSize,
            true = ets:insert(FileSummary, FSEntry #file_summary { 
                                             valid_total_size = ValidTotalSize1,
                                             contiguous_top = ContiguousTop1 }),
            {compact, File};
        _ when 1 < RefCount ->
            ok = decrement_cache(MsgId, State),
            ok = dets_ets_insert(State, StoreEntry #msg_location {
                                          ref_count = RefCount - 1 }),
            no_compact
    end.

%%----------------------------------------------------------------------------
%% message cache helper functions
%%----------------------------------------------------------------------------

remove_cache_entry(MsgId, #msstate { message_cache = Cache }) ->
    true = ets:delete(Cache, MsgId),
    ok.

fetch_and_increment_cache(MsgId, #msstate { message_cache = Cache }) ->
    case ets:lookup(Cache, MsgId) of
        [] ->
            not_found;
        [{MsgId, Msg, _RefCount}] ->
            NewRefCount = ets:update_counter(Cache, MsgId, {3, 1}),
            {Msg, NewRefCount}
    end.

decrement_cache(MsgId, #msstate { message_cache = Cache }) ->
    true = try case ets:update_counter(Cache, MsgId, {3, -1}) of
                   N when N =< 0 -> true = ets:delete(Cache, MsgId);
                   _N -> true
               end
           catch error:badarg ->
                   %% MsgId is not in there because although it's been
                   %% delivered, it's never actually been read (think:
                   %% persistent message in mixed queue)
                   true
           end,
    ok.

insert_into_cache(MsgId, Msg, #msstate { message_cache = Cache }) ->
    case cache_is_full(Cache) of
        true -> ok;
        false -> true = ets:insert_new(Cache, {MsgId, Msg, 1}),
                 ok
    end.

cache_is_full(Cache) ->
    ets:info(Cache, memory) > ?CACHE_MAX_SIZE.

%%----------------------------------------------------------------------------
%% dets/ets agnosticism
%%----------------------------------------------------------------------------

dets_ets_lookup(#msstate { msg_location_dets = MsgLocationDets,
                           operation_mode = disk_only }, Key) ->
    dets:lookup(MsgLocationDets, Key);
dets_ets_lookup(#msstate { msg_location_ets = MsgLocationEts,
                           operation_mode = ram_disk }, Key) ->
    ets:lookup(MsgLocationEts, Key).

dets_ets_delete(#msstate { msg_location_dets = MsgLocationDets,
                           operation_mode = disk_only }, Key) ->
    ok = dets:delete(MsgLocationDets, Key);
dets_ets_delete(#msstate { msg_location_ets = MsgLocationEts,
                           operation_mode = ram_disk }, Key) ->
    true = ets:delete(MsgLocationEts, Key),
    ok.

dets_ets_insert(#msstate { msg_location_dets = MsgLocationDets,
                           operation_mode = disk_only }, Obj) ->
    ok = dets:insert(MsgLocationDets, Obj);
dets_ets_insert(#msstate { msg_location_ets = MsgLocationEts,
                           operation_mode = ram_disk }, Obj) ->
    true = ets:insert(MsgLocationEts, Obj),
    ok.

dets_ets_insert_new(#msstate { msg_location_dets = MsgLocationDets,
                               operation_mode = disk_only }, Obj) ->
    true = dets:insert_new(MsgLocationDets, Obj);
dets_ets_insert_new(#msstate { msg_location_ets = MsgLocationEts,
                               operation_mode = ram_disk }, Obj) ->
    true = ets:insert_new(MsgLocationEts, Obj).

dets_ets_match_object(#msstate { msg_location_dets = MsgLocationDets,
                                 operation_mode = disk_only }, Obj) ->
    dets:match_object(MsgLocationDets, Obj);
dets_ets_match_object(#msstate { msg_location_ets = MsgLocationEts,
                                 operation_mode = ram_disk }, Obj) ->
    ets:match_object(MsgLocationEts, Obj).

%%----------------------------------------------------------------------------
%% recovery
%%----------------------------------------------------------------------------

recover_crashed_compactions(RefCountFun, Dir, Files, TmpFiles) ->
    lists:foreach(fun (TmpFile) ->
                          ok = recover_crashed_compactions1(
                                 RefCountFun, Dir, Files, TmpFile)
                  end,
                  TmpFiles),
    ok.

recover_crashed_compactions1(RefCountFun, Dir, Files, TmpFile) ->
    NonTmpRelatedFile = filename:rootname(TmpFile) ++ ?FILE_EXTENSION,
    true = lists:member(NonTmpRelatedFile, Files),
    {ok, UncorruptedMessagesTmp, MsgIdsTmp} =
        scan_file_for_valid_messages_msg_ids(Dir, TmpFile),
    %% all of these messages should be referenced
    %% otherwise they wouldn't have been copied out
    verify_messages_referenced(RefCountFun, MsgIdsTmp),
    {ok, UncorruptedMessages, MsgIds} =
        scan_file_for_valid_messages_msg_ids(Dir, NonTmpRelatedFile),
    %% 1) It's possible that everything in the tmp file is also in the
    %%    main file such that the main file is (prefix ++
    %%    tmpfile). This means that compaction failed immediately
    %%    prior to the final step of deleting the tmp file. Plan: just
    %%    delete the tmp file
    %% 2) It's possible that everything in the tmp file is also in the
    %%    main file but with holes throughout (or just somthing like
    %%    main = (prefix ++ hole ++ tmpfile)). This means that
    %%    compaction wrote out the tmp file successfully and then
    %%    failed. Plan: just delete the tmp file and allow the
    %%    compaction to eventually be triggered later
    %% 3) It's possible that everything in the tmp file is also in the
    %%    main file but such that the main file does not end with tmp
    %%    file (and there are valid messages in the suffix; main =
    %%    (prefix ++ tmpfile[with extra holes?] ++ suffix)). This
    %%    means that compaction failed as we were writing out the tmp
    %%    file. Plan: just delete the tmp file and allow the
    %%    compaction to eventually be triggered later
    %% 4) It's possible that there are messages in the tmp file which
    %%    are not in the main file. This means that writing out the
    %%    tmp file succeeded, but then we failed as we were copying
    %%    them back over to the main file, after truncating the main
    %%    file. As the main file has already been truncated, it should
    %%    consist only of valid messages. Plan: Truncate the main file
    %%    back to before any of the files in the tmp file and copy
    %%    them over again
    TmpPath = form_filename(Dir, TmpFile),
    case is_sublist(MsgIdsTmp, MsgIds) of
        true -> %% we're in case 1, 2 or 3 above. Just delete the tmp file
                %% note this also catches the case when the tmp file
                %% is empty
            ok = file:delete(TmpPath);
        false ->
            %% We're in case 4 above. We only care about the inital
            %% msgs in main file that are not in the tmp file. If
            %% there are no msgs in the tmp file then we would be in
            %% the 'true' branch of this case, so we know the
            %% lists:last call is safe.
            EldestTmpMsgId = lists:last(MsgIdsTmp),
            {MsgIds1, UncorruptedMessages1}
                = case lists:splitwith(
                         fun (MsgId) -> MsgId /= EldestTmpMsgId end, MsgIds) of
                      {_MsgIds, []} -> %% no msgs from tmp in main
                          {MsgIds, UncorruptedMessages};
                      {Dropped, [EldestTmpMsgId | Rest]} ->
                          %% Msgs in Dropped are in tmp, so forget them.
                          %% *cry*. Lists indexed from 1.
                          {Rest, lists:sublist(UncorruptedMessages,
                                               2 + length(Dropped),
                                               length(Rest))}
                  end,
            %% Check that everything in the main file prefix is referenced
            verify_messages_referenced(RefCountFun, MsgIds1),
            %% The main file prefix should be contiguous
            {Top, MsgIds1} = find_contiguous_block_prefix(
                               lists:reverse(UncorruptedMessages1)),
            %% we should have that none of the messages in the prefix
            %% are in the tmp file
            true = is_disjoint(MsgIds1, MsgIdsTmp),
            %% must open with read flag, otherwise will stomp over contents
            {ok, MainHdl} = open_file(Dir, NonTmpRelatedFile,
                                      ?WRITE_MODE ++ [read]),
            %% Wipe out any rubbish at the end of the file. Remember
            %% the head of the list will be the highest entry in the
            %% file.
            [{_, _, TmpTopTotalSize, TmpTopOffset}|_] = UncorruptedMessagesTmp,
            TmpSize = TmpTopOffset + TmpTopTotalSize,
            %% Extend the main file as big as necessary in a single
            %% move. If we run out of disk space, this truncate could
            %% fail, but we still aren't risking losing data
            ok = truncate_and_extend_file(MainHdl, Top, Top + TmpSize),
            {ok, TmpHdl} = open_file(Dir, TmpFile, ?READ_MODE),
            {ok, TmpSize} = file:copy(TmpHdl, MainHdl, TmpSize),
            ok = file:sync(MainHdl),
            ok = file:close(MainHdl),
            ok = file:close(TmpHdl),
            ok = file:delete(TmpPath),

            {ok, _MainMessages, MsgIdsMain} =
                scan_file_for_valid_messages_msg_ids(Dir, NonTmpRelatedFile),
            %% check that everything in MsgIds1 is in MsgIdsMain
            true = is_sublist(MsgIds1, MsgIdsMain),
            %% check that everything in MsgIdsTmp is in MsgIdsMain
            true = is_sublist(MsgIdsTmp, MsgIdsMain)
    end,
    ok.

is_sublist(SmallerL, BiggerL) ->
    lists:all(fun (Item) -> lists:member(Item, BiggerL) end, SmallerL).

is_disjoint(SmallerL, BiggerL) ->
    lists:all(fun (Item) -> not lists:member(Item, BiggerL) end, SmallerL).

verify_messages_referenced(RefCountFun, MsgIds) ->
    lists:foreach(fun (MsgId) -> false = RefCountFun(MsgId) == 0 end, MsgIds).

scan_file_for_valid_messages_msg_ids(Dir, File) ->
    {ok, Messages} = scan_file_for_valid_messages(Dir, File),
    {ok, Messages,
     [MsgId || {MsgId, _Attrs, _TotalSize, _FileOffset} <- Messages]}.

scan_file_for_valid_messages(Dir, File) ->
    case open_file(Dir, File, ?READ_MODE) of
        {ok, Hdl} ->
            Valid = rabbit_msg_file:scan(Hdl),
            %% if something really bad's happened, the close could fail,
            %% but ignore
            file:close(Hdl),
            Valid;
        {error, enoent} -> {ok, []};
        {error, Reason} -> throw({error, {unable_to_scan_file, File, Reason}})
    end.

%% Takes the list in *ascending* order (i.e. eldest message
%% first). This is the opposite of what scan_file_for_valid_messages
%% produces. The list of msgs that is produced is youngest first.
find_contiguous_block_prefix([]) -> {0, []};
find_contiguous_block_prefix(List) ->
    find_contiguous_block_prefix(List, 0, []).

find_contiguous_block_prefix([], ExpectedOffset, MsgIds) ->
    {ExpectedOffset, MsgIds};
find_contiguous_block_prefix([{MsgId, _Attrs, TotalSize, ExpectedOffset}
                             | Tail], ExpectedOffset, MsgIds) ->
    ExpectedOffset1 = ExpectedOffset + TotalSize,
    find_contiguous_block_prefix(Tail, ExpectedOffset1, [MsgId | MsgIds]);
find_contiguous_block_prefix([_MsgAfterGap | _Tail], ExpectedOffset, MsgIds) ->
    {ExpectedOffset, MsgIds}.

load_messages(RefCountFun, [], State) ->
    CurrentFile = State #msstate.current_file_name,
    load_messages(RefCountFun, undefined, [CurrentFile], State);
load_messages(RefCountFun, Files, State) ->
    load_messages(RefCountFun, undefined, Files, State).

load_messages(_RefCountFun, Left, [], State) ->
    Num = list_to_integer(filename:rootname(Left)),
    Offset =
        case dets_ets_match_object(State, #msg_location {
                                     file = Left, _ = '_' }) of
            [] -> 0;
            L ->
                [ #msg_location { file = Left,
                                  offset = MaxOffset,
                                  total_size = TotalSize} | _ ] =
                    sort_msg_locations_by_offset(desc, L),
                MaxOffset + TotalSize
             end,
    State #msstate { current_file_num = Num, current_file_name = Left,
                     current_offset = Offset };
load_messages(RefCountFun, Left, [File|Files],
              State = #msstate { dir = Dir, file_summary = FileSummary }) ->
    {ok, Messages} = scan_file_for_valid_messages(Dir, File),
    {ValidMessages, ValidTotalSize} = lists:foldl(
        fun (Obj = {MsgId, Attrs, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
                case RefCountFun(MsgId) of
                    0 -> {VMAcc, VTSAcc};
                    RefCount ->
                        true = dets_ets_insert_new(
                                 State, #msg_location {
                                   msg_id = MsgId, ref_count = RefCount,
                                   file = File, offset = Offset,
                                   total_size = TotalSize,
                                   attrs = Attrs }),
                        {[Obj | VMAcc], VTSAcc + TotalSize}
                end
        end, {[], 0}, Messages),
    %% foldl reverses lists, find_contiguous_block_prefix needs
    %% msgs eldest first, so, ValidMessages is the right way round
    {ContiguousTop, _} = find_contiguous_block_prefix(ValidMessages),
    Right = case Files of
                [] -> undefined;
                [F|_] -> F
            end,
    true = ets:insert_new(FileSummary, #file_summary {
                            file = File, valid_total_size = ValidTotalSize,
                            contiguous_top = ContiguousTop,
                            left = Left, right = Right }),
    load_messages(RefCountFun, File, Files, State).

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation
%%----------------------------------------------------------------------------

maybe_roll_to_new_file(Offset,
                       State = #msstate { dir                 = Dir,
                                          file_size_limit     = FileSizeLimit,
                                          current_file_name   = CurName,
                                          current_file_handle = CurHdl,
                                          current_file_num    = CurNum,
                                          file_summary        = FileSummary
                                        }
                      ) when Offset >= FileSizeLimit ->
    State1 = sync(State),
    ok = file:close(CurHdl),
    NextNum = CurNum + 1,
    NextName = integer_to_list(NextNum) ++ ?FILE_EXTENSION,
    {ok, NextHdl} = open_file(Dir, NextName, ?WRITE_MODE),
    true = ets:update_element(FileSummary, CurName,
                              {#file_summary.right, NextName}),
    true = ets:insert_new(
             FileSummary, #file_summary {
               file = NextName, valid_total_size = 0, contiguous_top = 0,
               left = CurName, right = undefined }),
    State2 = State1 #msstate { current_file_name = NextName,
                               current_file_handle = NextHdl,
                               current_file_num = NextNum,
                               current_offset = 0,
                               last_sync_offset = 0
                              },
    compact([CurName], State2);
maybe_roll_to_new_file(_, State) ->
    State.

compact(Files, State) ->
    %% smallest number, hence eldest, hence left-most, first
    SortedFiles = sort_file_names(Files),
    %% foldl reverses, so now youngest/right-most first
    RemainingFiles =
        lists:foldl(fun (File, Acc) ->
                            case delete_file_if_empty(File, State) of
                                true  -> Acc;
                                false -> [File | Acc]
                            end
                    end, [], SortedFiles),
    lists:foldl(fun combine_file/2, State, lists:reverse(RemainingFiles)).

%% At this stage, we simply know that the file has had msgs removed
%% from it. However, we don't know if we need to merge it left (which
%% is what we would prefer), or merge it right. If we merge left, then
%% this file is the source, and the left file is the destination. If
%% we merge right then this file is the destination and the right file
%% is the source.
combine_file(File, State = #msstate { file_summary = FileSummary,
                                      current_file_name = CurName }) ->
    %% the file we're looking at may no longer exist as it may have
    %% been deleted within the current GC run
    case ets:lookup(FileSummary, File) of
        [] -> State;
        [FSEntry = #file_summary { left = Left, right = Right }] ->
            GoRight =
                fun() ->
                        case Right of
                            undefined -> State;
                            _ when not (CurName == Right) ->
                                [FSRight] = ets:lookup(FileSummary, Right),
                                {_, State1} = adjust_meta_and_combine(
                                                FSEntry, FSRight, State),
                                State1;
                            _ -> State
                        end
                end,
            case Left of
                undefined ->
                    GoRight();
                _ -> [FSLeft] = ets:lookup(FileSummary, Left),
                     case adjust_meta_and_combine(FSLeft, FSEntry, State) of
                         {true, State1} -> State1;
                         {false, State} -> GoRight()
                     end
            end
    end.

adjust_meta_and_combine(
  LeftObj = #file_summary {
    file = LeftFile, valid_total_size = LeftValidData, right = RightFile },
  RightObj = #file_summary { 
    file = RightFile, valid_total_size = RightValidData, left = LeftFile,
    right = RightRight },
  State = #msstate { file_size_limit = FileSizeLimit,
                     file_summary = FileSummary }) ->
    TotalValidData = LeftValidData + RightValidData,
    if FileSizeLimit >= TotalValidData ->
            State1 = combine_files(RightObj, LeftObj, State),
            %% this could fail if RightRight is undefined
            ets:update_element(FileSummary, RightRight,
                               {#file_summary.left, LeftFile}),
            true = ets:insert(FileSummary, LeftObj #file_summary {
                                             valid_total_size = TotalValidData,
                                             contiguous_top = TotalValidData,
                                             right = RightRight }),
            true = ets:delete(FileSummary, RightFile),
            {true, State1};
       true -> {false, State}
    end.

sort_msg_locations_by_offset(Dir, List) ->
    Comp = case Dir of
               asc  -> fun erlang:'<'/2;
               desc -> fun erlang:'>'/2
           end,
    lists:sort(fun (#msg_location { offset = OffA },
                    #msg_location { offset = OffB }) ->
                       Comp(OffA, OffB)
               end, List).

combine_files(#file_summary { file = Source,
                              valid_total_size = SourceValid,
                              left = Destination },
              #file_summary { file = Destination,
                              valid_total_size = DestinationValid,
                              contiguous_top = DestinationContiguousTop,
                              right = Source },
              State = #msstate { dir = Dir }) ->
    State1 = close_file(Source, close_file(Destination, State)),
    {ok, SourceHdl} = open_file(Dir, Source, ?READ_MODE),
    {ok, DestinationHdl} = open_file(Dir, Destination,
                                     ?READ_MODE ++ ?WRITE_MODE),
    ExpectedSize = SourceValid + DestinationValid,
    %% if DestinationValid =:= DestinationContiguousTop then we don't
    %% need a tmp file
    %% if they're not equal, then we need to write out everything past
    %%   the DestinationContiguousTop to a tmp file then truncate,
    %%   copy back in, and then copy over from Source
    %% otherwise we just truncate straight away and copy over from Source
    if DestinationContiguousTop =:= DestinationValid ->
            ok = truncate_and_extend_file(DestinationHdl,
                                          DestinationValid, ExpectedSize);
       true ->
            Tmp = filename:rootname(Destination) ++ ?FILE_EXTENSION_TMP,
            {ok, TmpHdl} = open_file(Dir, Tmp, ?READ_MODE ++ ?WRITE_MODE),
            Worklist =
                lists:dropwhile(
                  fun (#msg_location { offset = Offset })
                      when Offset /= DestinationContiguousTop ->
                          %% it cannot be that Offset ==
                          %% DestinationContiguousTop because if it
                          %% was then DestinationContiguousTop would
                          %% have been extended by TotalSize
                          Offset < DestinationContiguousTop
                          %% Given expected access patterns, I suspect
                          %% that the list should be naturally sorted
                          %% as we require, however, we need to
                          %% enforce it anyway
                  end, sort_msg_locations_by_offset(
                         asc, dets_ets_match_object(
                                 State1, #msg_location {
                                   file = Destination, _ = '_' }))),
            ok = copy_messages(
                   Worklist, DestinationContiguousTop, DestinationValid,
                   DestinationHdl, TmpHdl, Destination, State1),
            TmpSize = DestinationValid - DestinationContiguousTop,
            %% so now Tmp contains everything we need to salvage from
            %% Destination, and MsgLocationDets has been updated to
            %% reflect compaction of Destination so truncate
            %% Destination and copy from Tmp back to the end
            {ok, 0} = file:position(TmpHdl, 0),
            ok = truncate_and_extend_file(
                   DestinationHdl, DestinationContiguousTop, ExpectedSize),
            {ok, TmpSize} = file:copy(TmpHdl, DestinationHdl, TmpSize),
            %% position in DestinationHdl should now be DestinationValid
            ok = file:sync(DestinationHdl),
            ok = file:close(TmpHdl),
            ok = file:delete(form_filename(Dir, Tmp))
    end,
    SourceWorkList =
        sort_msg_locations_by_offset(
          asc, dets_ets_match_object(State1, #msg_location {
                                       file = Source, _ = '_' })),
    ok = copy_messages(SourceWorkList, DestinationValid, ExpectedSize,
                       SourceHdl, DestinationHdl, Destination, State1),
    %% tidy up
    ok = file:close(SourceHdl),
    ok = file:close(DestinationHdl),
    ok = file:delete(form_filename(Dir, Source)),
    State1.

copy_messages(WorkList, InitOffset, FinalOffset, SourceHdl, DestinationHdl,
              Destination, State) ->
    {FinalOffset, BlockStart1, BlockEnd1} =
        lists:foldl(
          fun (StoreEntry = #msg_location { offset = Offset,
                                            total_size = TotalSize },
               {CurOffset, BlockStart, BlockEnd}) ->
                  %% CurOffset is in the DestinationFile.
                  %% Offset, BlockStart and BlockEnd are in the SourceFile
                  %% update MsgLocationDets to reflect change of file and offset
                  ok = dets_ets_insert(State, StoreEntry #msg_location {
                                                file = Destination,
                                                offset = CurOffset }),
                  NextOffset = CurOffset + TotalSize,
                  if BlockStart =:= undefined ->
                          %% base case, called only for the first list elem
                          {NextOffset, Offset, Offset + TotalSize};
                     Offset =:= BlockEnd ->
                          %% extend the current block because the next
                          %% msg follows straight on
                          {NextOffset, BlockStart, BlockEnd + TotalSize};
                     true ->
                          %% found a gap, so actually do the work for
                          %% the previous block
                          BSize = BlockEnd - BlockStart,
                          {ok, BlockStart} =
                              file:position(SourceHdl, BlockStart),
                          {ok, BSize} =
                              file:copy(SourceHdl, DestinationHdl, BSize),
                          {NextOffset, Offset, Offset + TotalSize}
                  end
          end, {InitOffset, undefined, undefined}, WorkList),
    %% do the last remaining block
    BSize1 = BlockEnd1 - BlockStart1,
    {ok, BlockStart1} = file:position(SourceHdl, BlockStart1),
    {ok, BSize1} = file:copy(SourceHdl, DestinationHdl, BSize1),
    ok = file:sync(DestinationHdl),
    ok.

close_file(File, State = #msstate { dir = Dir, read_file_handle_cache = HC }) ->
    HC1 = rabbit_file_handle_cache:close_file(form_filename(Dir, File), HC),
    State #msstate { read_file_handle_cache = HC1 }.

delete_file_if_empty(File,
                     #msstate { dir = Dir, file_summary = FileSummary }) ->
    [#file_summary { valid_total_size = ValidData,
                     left = Left, right = Right }] =
        ets:lookup(FileSummary, File),
    case ValidData of
        %% we should NEVER find the current file in here hence right
        %% should always be a file, not undefined
        0 -> case {Left, Right} of
                 {undefined, _} when not is_atom(Right) ->
                     %% the eldest file is empty.
                     true = ets:update_element(
                              FileSummary, Right,
                              {#file_summary.left, undefined});
                 {_, _} when not (is_atom(Right)) ->
                     true = ets:update_element(FileSummary, Right,
                                               {#file_summary.left, Left}),
                     true =
                         ets:update_element(FileSummary, Left,
                                            {#file_summary.right, Right})
             end,
             true = ets:delete(FileSummary, File),
             ok = file:delete(form_filename(Dir, File)),
             true;
        _ -> false
    end.
