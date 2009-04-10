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

-module(rabbit_disk_queue).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([publish/3, deliver/2, ack/2, tx_publish/2, tx_commit/2, tx_cancel/1]).

-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-define(WRITE_OK_SIZE_BITS, 8).
-define(WRITE_OK, 255).
-define(INTEGER_SIZE_BYTES, 8).
-define(INTEGER_SIZE_BITS, 8 * ?INTEGER_SIZE_BYTES).
-define(MSG_LOC_ETS_NAME, rabbit_disk_queue_msg_location).
-define(FILE_EXTENSION, ".rdq").
-define(FILE_EXTENSION_TMP, ".rdt").

-define(SERVER, ?MODULE).

-record(dqfile, {valid_data, contiguous_prefix, left, right, detail}).

-record(dqstate, {msg_location,
		  file_summary,
		  current_file_num,
		  current_file_name,
		  current_file_handle,
		  file_size_limit,
		  read_file_handles,
		  read_file_handles_limit
		 }).

%% ---- PUBLIC API ----

start_link(FileSizeLimit, ReadFileHandlesLimit) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [FileSizeLimit, ReadFileHandlesLimit], []).

publish(Q, MsgId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {publish, Q, MsgId, Msg}).

deliver(Q, MsgId) ->
    gen_server:call(?SERVER, {deliver, Q, MsgId}, infinity).

ack(Q, MsgIds) when is_list(MsgIds) ->
    gen_server:cast(?SERVER, {ack, Q, MsgIds}).

tx_publish(MsgId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {tx_publish, MsgId, Msg}).

tx_commit(Q, MsgIds) when is_list(MsgIds) ->
    gen_server:call(?SERVER, {tx_commit, Q, MsgIds}, infinity).

tx_cancel(MsgIds) when is_list(MsgIds) ->
    gen_server:cast(?SERVER, {tx_cancel, MsgIds}).

%% ---- GEN-SERVER INTERNAL API ----

init([FileSizeLimit, ReadFileHandlesLimit]) ->
    process_flag(trap_exit, true),
    InitName = "0" ++ (?FILE_EXTENSION),
    State = #dqstate { msg_location = ets:new((?MSG_LOC_ETS_NAME), [set, private]),
		       file_summary = dict:store(InitName, (#dqfile { valid_data = 0,
								      contiguous_prefix = 0,
								      left = undefined,
								      right = undefined,
								      detail = dict:new()}),
						 dict:new()),
		       current_file_num = 0,
		       current_file_name = InitName,
		       current_file_handle = undefined,
		       file_size_limit = FileSizeLimit,
		       read_file_handles = {dict:new(), gb_trees:empty()},
		       read_file_handles_limit = ReadFileHandlesLimit
		     },
    {ok, State1 = #dqstate { current_file_name = CurrentName } } = load_from_disk(State),
    Path = form_filename(CurrentName),
    ok = filelib:ensure_dir(Path),
    {ok, FileHdl} = file:open(Path, [append, raw, binary]),
    {ok, State1 # dqstate { current_file_handle = FileHdl }}.

handle_call({deliver, Q, MsgId}, _From, State) ->
    {ok, {MsgBody, BodySize, Delivered}, State1} = internal_deliver(Q, MsgId, State),
    {reply, {MsgBody, BodySize, Delivered}, State1};
handle_call({tx_commit, Q, MsgIds}, _From, State) ->
    {ok, State1} = internal_tx_commit(Q, MsgIds, State),
    {reply, ok, State1}.

handle_cast({publish, Q, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_publish(Q, MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({ack, Q, MsgIds}, State) ->
    {ok, State1} = lists:foldl(fun (MsgId, {ok, State2}) ->
				       internal_ack(Q, MsgId, State2)
			       end, {ok, State}, MsgIds),
    {noreply, State1};
handle_cast({tx_publish, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({tx_cancel, MsgIds}, State) ->
    {ok, State1} = internal_tx_cancel(MsgIds, State),
    {noreply, State1}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #dqstate { current_file_handle = FileHdl,
			      read_file_handles = {ReadHdls, _ReadHdlsAge}
			    }) ->
    io:format("DYING~n", []),
    ok = file:sync(FileHdl),
    ok = file:close(FileHdl),
    dict:fold(fun (_File, Hdl, _Acc) ->
		     file:close(Hdl)
	      end, ok, ReadHdls).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---- UTILITY FUNCTIONS ----

form_filename(Name) ->
    filename:join(base_directory(), Name).

base_directory() ->
    filename:join(mnesia:system_info(directory), "rabbit_disk_queue/").

file_packing_adjustment_bytes() ->
    1 + (2* (?INTEGER_SIZE_BYTES)).

%% ---- INTERNAL RAW FUNCTIONS ----

internal_deliver(Q, MsgId, State = #dqstate { msg_location = MsgLocation,
					      %current_file_handle = CurHdl,
					      %current_file_name = CurName,
					      read_file_handles_limit = ReadFileHandlesLimit,
					      read_file_handles = {ReadHdls, ReadHdlsAge}
					     }) ->
    [{MsgId, _RefCount, File, Offset, _TotalSize}] = ets:lookup(MsgLocation, MsgId),
    %if CurName =:= File -> ok = file:sync(CurHdl); % don't think this is necessary. Within a process you should always have a consistent view of a file
    %   true -> ok
    %end,
    % so this next bit implements an LRU for file handles. But it's a bit insane, and smells
    % of premature optimisation. So I might remove it and dump it overboard
    {FileHdl, ReadHdls1, ReadHdlsAge1}
	= case dict:find(File, ReadHdls) of
	      error ->
		  {ok, Hdl} = file:open(form_filename(File), [read, raw, binary]),
		  Now = now(),
		  case dict:size(ReadHdls) < ReadFileHandlesLimit of
		      true ->
			  {Hdl, dict:store(File, {Hdl, Now}, ReadHdls), gb_trees:enter(Now, File, ReadHdlsAge)};
		      _False ->
			  {_Then, OldFile, ReadHdlsAge2} = gb_trees:take_smallest(ReadHdlsAge),
			  {ok, {OldHdl, _Then}} = dict:find(OldFile, ReadHdls),
			  ok = file:close(OldHdl),
			  ReadHdls2 = dict:erase(OldFile, ReadHdls),
			  {Hdl, dict:store(File, {Hdl, Now}, ReadHdls2), gb_trees:enter(Now, File, ReadHdlsAge2)}
		  end;
	      {ok, {Hdl, Then}} ->
		  Now = now(),
		  {Hdl, dict:store(File, {Hdl, Now}, ReadHdls), gb_trees:enter(Now, File, gb_trees:delete(Then, ReadHdlsAge))}
	  end,
    % read the message
    {ok, {MsgBody, BodySize, _TotalSize}} = read_message_at_offset(FileHdl, Offset),
    [#dq_msg_loc {queue_and_msg_id = {MsgId, Q}, is_delivered = Delivered}] = mnesia:dirty_read(rabbit_disk_queue, {MsgId, Q}),
    ok = mnesia:dirty_write(rabbit_disk_queue, #dq_msg_loc {queue_and_msg_id = {MsgId, Q}, is_delivered = true}),
    {ok, {MsgBody, BodySize, Delivered},
     State # dqstate { read_file_handles = {ReadHdls1, ReadHdlsAge1} }}.

internal_ack(Q, MsgId, State = #dqstate { msg_location = MsgLocation,
					  file_summary = FileSummary
					}) ->
    [{MsgId, RefCount, File, Offset, TotalSize}] = ets:lookup(MsgLocation, MsgId),
    % is this the last time we need the message, in which case tidy up
    FileSummary1 =
	if 1 =:= RefCount ->
		true = ets:delete(MsgLocation, MsgId),
		{ok, FileSum = #dqfile { valid_data = ValidTotalSize,
					 contiguous_prefix = ContiguousTop,
					 detail = FileDetail }}
		    = dict:find(File, FileSummary),
		FileDetail1 = dict:erase(Offset, FileDetail),
		ContiguousTop1 = lists:min([ContiguousTop, Offset]),
		FileSummary2 = dict:store(File, FileSum #dqfile { valid_data = (ValidTotalSize - TotalSize - file_packing_adjustment_bytes()),
								  contiguous_prefix = ContiguousTop1,
								  detail = FileDetail1
								}, FileSummary),
		ok = mnesia:dirty_delete({rabbit_disk_queue, {MsgId, Q}}),
		FileSummary2;
	   1 < RefCount ->
		ets:insert(MsgLocation, {MsgId, RefCount - 1, File, Offset, TotalSize}),
		FileSummary
	end,
    State1 = compact(File, State # dqstate { file_summary = FileSummary1 } ),
    {ok, State1}.

internal_tx_publish(MsgId, MsgBody, State = #dqstate { msg_location = MsgLocation,
						       current_file_handle = CurHdl,
						       current_file_name = CurName,
						       file_summary = FileSummary
						     }) ->
    case ets:lookup(MsgLocation, MsgId) of
	[] ->
	    % New message, lots to do
	    {ok, Offset} = file:position(CurHdl, cur),
	    {ok, TotalSize} = append_message(CurHdl, MsgId, MsgBody),
	    true = ets:insert_new(MsgLocation, {MsgId, 1, CurName, Offset, TotalSize}),
	    {ok, FileSum = #dqfile { valid_data = ValidTotalSize,
				     contiguous_prefix = ContiguousTop,
				     right = undefined,
				     detail = FileDetail }}
		= dict:find(CurName, FileSummary),
	    FileDetail1 = dict:store(Offset, TotalSize, FileDetail),
	    ValidTotalSize1 = ValidTotalSize + TotalSize + file_packing_adjustment_bytes(),
	    ContiguousTop1 = if Offset =:= ContiguousTop ->
				     ValidTotalSize; % can't be any holes in this file
				true -> ContiguousTop
			     end,
	    FileSummary1 = dict:store(CurName, FileSum #dqfile { valid_data = ValidTotalSize1,
								 contiguous_prefix = ContiguousTop1,
								 detail = FileDetail1},
				      FileSummary),
	    maybe_roll_to_new_file(Offset + TotalSize + file_packing_adjustment_bytes(),
				   State # dqstate { file_summary = FileSummary1 });
	[{MsgId, RefCount, File, Offset, TotalSize}] ->
	    % We already know about it, just update counter
	    true = ets:insert(MsgLocation, {MsgId, RefCount + 1, File, Offset, TotalSize}),
	    {ok, State}
    end.

internal_tx_commit(Q, MsgIds, State = #dqstate { msg_location = MsgLocation,
						 current_file_handle = CurHdl,
						 current_file_name = CurName
					       }) ->
    {atomic, Sync}
	= mnesia:transaction(
	    fun() -> lists:foldl(fun (MsgId, Acc) ->
					 [{MsgId, _RefCount, File, _Offset, _TotalSize}] =
					     ets:lookup(MsgLocation, MsgId),
					 ok = mnesia:write(rabbit_disk_queue, #dq_msg_loc { queue_and_msg_id = {MsgId, Q}, is_delivered = false}, write),
					 Acc or (CurName =:= File)
				 end, false, MsgIds)
	    end),
    if Sync -> ok = file:sync(CurHdl);
       true -> ok
    end,
    {ok, State}.

internal_publish(Q, MsgId, MsgBody, State) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    ok = mnesia:dirty_write(rabbit_disk_queue, #dq_msg_loc { queue_and_msg_id = {MsgId, Q}, is_delivered = false}),
    {ok, State1}.


internal_tx_cancel(MsgIds, State = #dqstate { msg_location = MsgLocation,
					      file_summary = FileSummary
					    }) ->
    FileSummary1 =
	lists:foldl(fun (MsgId, FileSummary2) ->
			    [{MsgId, RefCount, File, Offset, TotalSize}]
				= ets:lookup(MsgLocation, MsgId),
			    if 1 =:= RefCount ->
				    true = ets:delete(MsgLocation, MsgId),
				    {ok, FileSum = #dqfile { valid_data = ValidTotalSize,
							     contiguous_prefix = ContiguousTop,
							     detail = FileDetail }}
					= dict:find(File, FileSummary2),
				    FileDetail1 = dict:erase(Offset, FileDetail),
				    ContiguousTop1 = lists:min([ContiguousTop, Offset]),
				    dict:store(File, FileSum #dqfile { valid_data = (ValidTotalSize - TotalSize - file_packing_adjustment_bytes()),
								       contiguous_prefix = ContiguousTop1,
								       detail = FileDetail1
								     }, FileSummary2);
			       1 < RefCount ->
				    ets:insert(MsgLocation, {MsgId, RefCount - 1, File, Offset, TotalSize}),
				    FileSummary2
			    end
		    end, FileSummary, MsgIds),
    {ok, State #dqstate { file_summary = FileSummary1 }}.

%% ---- ROLLING OVER THE APPEND FILE ----

maybe_roll_to_new_file(Offset, State = #dqstate { file_size_limit = FileSizeLimit,
						  current_file_name = CurName,
						  current_file_handle = CurHdl,
						  current_file_num = CurNum,
						  file_summary = FileSummary
						}
		      ) when Offset >= FileSizeLimit ->
    ok = file:sync(CurHdl),
    ok = file:close(CurHdl),
    NextNum = CurNum + 1,
    NextName = integer_to_list(NextNum) ++ (?FILE_EXTENSION),
    {ok, NextHdl} = file:open(form_filename(NextName), [write, raw, binary]),
    {ok, FileSum = #dqfile {right = undefined}} = dict:find(CurName, FileSummary),
    FileSummary1 = dict:store(CurName, FileSum #dqfile {right = NextName}, FileSummary),
    {ok, State # dqstate { current_file_name = NextName,
			   current_file_handle = NextHdl,
			   current_file_num = NextNum,
			   file_summary = dict:store(NextName, #dqfile { valid_data = 0,
									 contiguous_prefix = 0,
									 left = CurName,
									 right = undefined,
									 detail = dict:new()},
						     FileSummary1)
			 }
    };
maybe_roll_to_new_file(_, State) ->
    {ok, State}.

%% ---- GARBAGE COLLECTION / COMPACTION / AGGREGATION ----

compact(File, State) ->
    State.

%% ---- DISK RECOVERY ----

load_from_disk(State) ->
    % sorted so that smallest number is first. which also means eldest file (left-most) first
    {Files, TmpFiles} = get_disk_queue_files(),
    ok = recover_crashed_compactions(Files, TmpFiles),
    % There should be no more tmp files now, so go ahead and load the whole lot
    (State1 = #dqstate{ msg_location = MsgLocation }) = load_messages(undefined, Files, State),
    % Finally, check there is nothing in mnesia which we haven't loaded
    true = lists:all(fun ({MsgId, _Q}) -> 1 =:= length(ets:lookup(MsgLocation, MsgId)) end,
		     mnesia:async_dirty(fun() -> mnesia:all_keys(rabbit_disk_queue) end)),
    {ok, State1}.

load_messages(undefined, [], State) ->
    State;
load_messages(Left, [], State) ->
    Num = list_to_integer(filename:rootname(Left)),
    State # dqstate { current_file_num = Num, current_file_name = Left };
load_messages(Left, [File|Files],
	      State = #dqstate { msg_location = MsgLocation,
				 file_summary = FileSummary
			       }) ->
    % [{MsgId, TotalSize, FileOffset}]
    {ok, Messages} = scan_file_for_valid_messages(form_filename(File)),
    {ValidMessagesRev, ValidTotalSize, FileDetail} = lists:foldl(
	fun ({MsgId, TotalSize, Offset}, {VMAcc, VTSAcc, FileDetail1}) ->
		case length(mnesia:dirty_match_object(rabbit_disk_queue, {dq_msg_loc, {MsgId, '_'}, '_'})) of
		    0 -> {VMAcc, VTSAcc, FileDetail1};
		    RefCount ->
			true = ets:insert_new(MsgLocation, {MsgId, RefCount, File, Offset, TotalSize}),
			{[{MsgId, TotalSize, Offset}|VMAcc],
			 VTSAcc + TotalSize + file_packing_adjustment_bytes(),
			 dict:store(Offset, TotalSize, FileDetail1)
			}
		end
	end, {[], 0, dict:new()}, Messages),
    % foldl reverses lists and find_contiguous_block_prefix needs elems in the same order
    % as from scan_file_for_valid_messages
    {ContiguousTop, _} = find_contiguous_block_prefix(lists:reverse(ValidMessagesRev)),
    Right = case Files of
		[] -> undefined;
		[F|_] -> F
	    end,
    State1 = State # dqstate { file_summary =
			       dict:store(File, #dqfile { valid_data = ValidTotalSize,
							  contiguous_prefix = ContiguousTop,
							  left = Left,
							  right = Right,
							  detail = FileDetail
							 },
					  FileSummary)
			     },
    load_messages(File, Files, State1).

%% ---- DISK RECOVERY OF FAILED COMPACTION ----

recover_crashed_compactions(_Files, []) ->
    ok;
recover_crashed_compactions(Files, [TmpFile|TmpFiles]) ->
    GrabMsgId = fun ({MsgId, _TotalSize, _FileOffset}) -> MsgId end,
    NonTmpRelatedFile = filename:rootname(TmpFile) ++ (?FILE_EXTENSION),
    true = lists:member(NonTmpRelatedFile, Files),
    % [{MsgId, TotalSize, FileOffset}]
    {ok, UncorruptedMessagesTmp} = scan_file_for_valid_messages(form_filename(TmpFile)),
    MsgIdsTmp = lists:map(GrabMsgId, UncorruptedMessagesTmp),
    % all of these messages should appear in the mnesia table, otherwise they wouldn't have been copied out
    lists:foreach(fun (MsgId) ->
			  true = 0 < length(mnesia:dirty_match_object(rabbit_disk_queue, {dq_msg_loc, {MsgId, '_'}, '_'}))
		  end, MsgIdsTmp),
    {ok, UncorruptedMessages} = scan_file_for_valid_messages(form_filename(NonTmpRelatedFile)),
    MsgIds = lists:map(GrabMsgId, UncorruptedMessages),
    %% 1) It's possible that everything in the tmp file is also in the main file
    %%    such that the main file is (prefix ++ tmpfile). This means that compaction
    %%    failed immediately prior to the final step of deleting the tmp file.
    %%    Plan: just delete the tmp file
    %% 2) It's possible that everything in the tmp file is also in the main file
    %%    but with holes throughout (or just somthing like main = (prefix ++ hole ++ tmpfile)).
    %%    This means that compaction wrote out the tmp file successfully and then failed.
    %%    Plan: just delete the tmp file and allow the compaction to eventually be triggered later
    %% 3) It's possible that everything in the tmp file is also in the main file
    %%    but such that the main file does not end with tmp file (and there are valid messages
    %%    in the suffix; main = (prefix ++ tmpfile[with extra holes?] ++ suffix)).
    %%    This means that compaction failed as we were writing out the tmp file.
    %%    Plan: just delete the tmp file and allow the compaction to eventually be triggered later
    %% 4) It's possible that there are messages in the tmp file which are not in the main file.
    %%    This means that writing out the tmp file succeeded, but then we failed as we
    %%    were copying them back over to the main file, after truncating the main file.
    %%    As the main file has already been truncated, it should consist only of valid messages
    %%    Plan: Truncate the main file back to before any of the files in the tmp file and copy
    %%    them over again
    case lists:all(fun (MsgId) -> lists:member(MsgId, MsgIds) end, MsgIdsTmp) of
	true -> % we're in case 1, 2 or 3 above. Just delete the tmp file
	        % note this also catches the case when the tmp file is empty
	    ok = file:delete(TmpFile);
	_False ->
	    % we're in case 4 above.
	    % check that everything in the main file is a valid message in mnesia
	    lists:foreach(fun (MsgId) ->
				  true = 0 < length(mnesia:dirty_match_object(rabbit_disk_queue, {dq_msg_loc, {MsgId, '_'}, '_'}))
			  end, MsgIds),
	    % The main file should be contiguous
	    {Top, MsgIds} = find_contiguous_block_prefix(UncorruptedMessages),
	    % we should have that none of the messages in the prefix are in the tmp file
	    true = lists:all(fun (MsgId) -> not(lists:member(MsgId, MsgIdsTmp)) end, MsgIds),

	    {ok, MainHdl} = file:open(form_filename(NonTmpRelatedFile), [write, raw, binary]),
	    {ok, Top} = file:position(MainHdl, Top),
	    ok = file:truncate(MainHdl), % wipe out any rubbish at the end of the file
	    % there really could be rubbish at the end of the file - we could have failed after the
	    % extending truncate.
	    % Remember the head of the list will be the highest entry in the file
	    [{_, TmpTopTotalSize, TmpTopOffset}|_] = UncorruptedMessagesTmp,
	    TmpSize = TmpTopOffset + TmpTopTotalSize + file_packing_adjustment_bytes(),
	    ExpectedAbsPos = Top + TmpSize,
	    {ok, ExpectedAbsPos} = file:position(MainHdl, {cur, TmpSize}),
	    ok = file:truncate(MainHdl), % and now extend the main file as big as necessary in a single move
					 % if we run out of disk space, this truncate could fail, but we still
					 % aren't risking losing data
	    {ok, TmpHdl} = file:open(form_filename(TmpFile), [read, raw, binary]),
	    {ok, TmpSize} = file:copy(TmpHdl, MainHdl, TmpSize),
	    ok = file:close(MainHdl),
	    ok = file:close(TmpHdl),
	    ok = file:delete(TmpFile),

	    {ok, MainMessages} = scan_file_for_valid_messages(form_filename(NonTmpRelatedFile)),
	    MsgIdsMain = lists:map(GrabMsgId, MainMessages),
	    % check that everything in MsgIds is in MsgIdsMain
	    true = lists:all(fun (MsgId) -> lists:member(MsgId, MsgIdsMain) end, MsgIds),
	    % check that everything in MsgIdsTmp is in MsgIdsMain
	    true = lists:all(fun (MsgId) -> lists:member(MsgId, MsgIdsMain) end, MsgIdsTmp)
    end,
    recover_crashed_compactions(Files, TmpFiles).

% this assumes that the messages are ordered such that the highest address is at
% the head of the list.
% this matches what scan_file_for_valid_messages produces
find_contiguous_block_prefix([]) -> {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail]) ->
    case find_contiguous_block_prefix(Tail, Offset, [MsgId]) of
	{ok, Acc} -> {Offset + TotalSize + file_packing_adjustment_bytes(), lists:reverse(Acc)};
	Res -> Res
    end.
find_contiguous_block_prefix([], 0, Acc) ->
    {ok, Acc};
find_contiguous_block_prefix([], _N, _Acc) ->
    {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail], ExpectedOffset, Acc)
  when ExpectedOffset =:= Offset + TotalSize + 1 + (2* (?INTEGER_SIZE_BYTES)) -> %% Can't use file_packing_adjustment_bytes()
    find_contiguous_block_prefix(Tail, Offset, [MsgId|Acc]);
find_contiguous_block_prefix(List, _ExpectedOffset, _Acc) ->
    find_contiguous_block_prefix(List).
    
file_name_sort(A, B) ->
    ANum = list_to_integer(filename:rootname(A)),
    BNum = list_to_integer(filename:rootname(B)),
    ANum < BNum.

get_disk_queue_files() ->
    DQFiles = filelib:wildcard("*" ++ (?FILE_EXTENSION), base_directory()),
    DQFilesSorted = lists:sort(fun file_name_sort/2, DQFiles),
    DQTFiles = filelib:wildcard("*" ++ (?FILE_EXTENSION_TMP), base_directory()),
    DQTFilesSorted = lists:sort(fun file_name_sort/2, DQTFiles),
    {DQFilesSorted, DQTFilesSorted}.

%% ---- RAW READING AND WRITING OF FILES ----

append_message(FileHdl, MsgId, MsgBody) when is_binary(MsgBody) ->
    BodySize = size(MsgBody),
    MsgIdBin = term_to_binary(MsgId),
    MsgIdBinSize = size(MsgIdBin),
    TotalSize = BodySize + MsgIdBinSize,
    case file:write(FileHdl, <<TotalSize:(?INTEGER_SIZE_BITS),
			       MsgIdBinSize:(?INTEGER_SIZE_BITS),
			       MsgIdBin:MsgIdBinSize/binary, MsgBody:BodySize/binary>>) of
	ok -> ok = file:write(FileHdl, <<(?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>),
	      {ok, TotalSize};
	KO -> KO
    end.

read_message_at_offset(FileHdl, Offset) ->
    case file:position(FileHdl, {bof, Offset}) of
	{ok, Offset} ->
	    case file:read(FileHdl, 2 * (?INTEGER_SIZE_BYTES)) of
		{ok, <<TotalSize:(?INTEGER_SIZE_BITS), MsgIdBinSize:(?INTEGER_SIZE_BITS)>>} ->
		    ExpectedAbsPos = Offset + (2 * (?INTEGER_SIZE_BYTES)) + MsgIdBinSize,
		    case file:position(FileHdl, {cur, MsgIdBinSize}) of
			{ok, ExpectedAbsPos} ->
			    BodySize = TotalSize - MsgIdBinSize,
			    case file:read(FileHdl, 1 + BodySize) of
				{ok, <<MsgBody:BodySize/binary, (?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>} ->
				    {ok, {MsgBody, BodySize, TotalSize}};
				KO -> KO
			    end;
			KO -> KO
		    end;
		KO -> KO
	    end;
	KO -> KO
    end.

scan_file_for_valid_messages(File) ->
    {ok, Hdl} = file:open(File, [raw, binary, read]),
    Valid = scan_file_for_valid_messages(Hdl, 0, []),
    _ = file:close(Hdl), % if something really bad's happened, the close could fail, but ignore
    Valid.

scan_file_for_valid_messages(FileHdl, Offset, Acc) ->
    case read_next_file_entry(FileHdl, Offset) of
	{ok, eof} -> {ok, Acc};
	{ok, {corrupted, NextOffset}} ->
	    scan_file_for_valid_messages(FileHdl, NextOffset, Acc);
	{ok, {ok, MsgId, TotalSize, NextOffset}} ->
	    scan_file_for_valid_messages(FileHdl, NextOffset, [{MsgId, TotalSize, Offset}|Acc]);
	_KO -> {ok, Acc} %% bad message, but we may still have recovered some valid messages
    end.
	    

read_next_file_entry(FileHdl, Offset) ->
    TwoIntegers = 2 * (?INTEGER_SIZE_BYTES),
    case file:read(FileHdl, TwoIntegers) of
	{ok, <<TotalSize:(?INTEGER_SIZE_BITS), MsgIdBinSize:(?INTEGER_SIZE_BITS)>>} ->
	    case {TotalSize =:= 0, MsgIdBinSize =:= 0} of
		{true, _} -> {ok, eof}; %% Nothing we can do other than stop
		{false, true} -> %% current message corrupted, try skipping past it
		    ExpectedAbsPos = Offset + file_packing_adjustment_bytes() + TotalSize,
		    case file:position(FileHdl, {cur, TotalSize + 1}) of
			{ok, ExpectedAbsPos} -> {ok, {corrupted, ExpectedAbsPos}};
			{ok, _SomeOtherPos} -> {ok, eof}; %% seek failed, so give up
			KO -> KO
		    end;
		{false, false} -> %% all good, let's continue
		    case file:read(FileHdl, MsgIdBinSize) of
			{ok, <<MsgId:MsgIdBinSize/binary>>} ->
			    ExpectedAbsPos = Offset + TwoIntegers + TotalSize,
			    case file:position(FileHdl, {cur, TotalSize - MsgIdBinSize}) of
				{ok, ExpectedAbsPos} ->
				    case file:read(FileHdl, 1) of
					{ok, <<(?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>} ->
					    {ok, {ok, binary_to_term(MsgId), TotalSize,
						  Offset + file_packing_adjustment_bytes() + TotalSize}};
					{ok, _SomeOtherData} ->
					    {ok, {corrupted, Offset + file_packing_adjustment_bytes() + TotalSize}};
					KO -> KO
				    end;
				{ok, _SomeOtherPos} -> {ok, eof}; %% seek failed, so give up
				KO -> KO
			    end;
			eof -> {ok, eof};
			KO -> KO
		    end
	    end;
	eof -> {ok, eof};
	KO -> KO
    end.
