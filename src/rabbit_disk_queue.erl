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

-export([stop/0, clean_stop/0]).

-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-define(WRITE_OK_SIZE_BITS, 8).
-define(WRITE_OK, 255).
-define(INTEGER_SIZE_BYTES, 8).
-define(INTEGER_SIZE_BITS, 8 * ?INTEGER_SIZE_BYTES).
-define(MSG_LOC_ETS_NAME, rabbit_disk_queue_msg_location).
-define(FILE_DETAIL_ETS_NAME, rabbit_disk_queue_file_detail).
-define(FILE_SUMMARY_ETS_NAME, rabbit_disk_queue_file_summary).
-define(FILE_EXTENSION, ".rdq").
-define(FILE_EXTENSION_TMP, ".rdt").
-define(FILE_PACKING_ADJUSTMENT, 1 + (2* (?INTEGER_SIZE_BYTES))).

-define(SERVER, ?MODULE).

-record(dqfile, {valid_data, contiguous_prefix, left, right}).

-record(dqstate, {msg_location,
		  file_summary,
		  file_detail,
		  current_file_num,
		  current_file_name,
		  current_file_handle,
		  current_offset,
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

stop() ->
    gen_server:call(?SERVER, stop).

clean_stop() ->
    gen_server:call(?SERVER, clean_stop).

%% ---- GEN-SERVER INTERNAL API ----

init([FileSizeLimit, ReadFileHandlesLimit]) ->
    process_flag(trap_exit, true),
    InitName = "0" ++ (?FILE_EXTENSION),
    FileSummary = ets:new((?FILE_SUMMARY_ETS_NAME), [set, private]),
    State = #dqstate { msg_location = ets:new((?MSG_LOC_ETS_NAME), [set, private]),
		       file_summary = FileSummary,
		       file_detail = ets:new((?FILE_DETAIL_ETS_NAME), [ordered_set, private]),
		       current_file_num = 0,
		       current_file_name = InitName,
		       current_file_handle = undefined,
		       current_offset = 0,
		       file_size_limit = FileSizeLimit,
		       read_file_handles = {dict:new(), gb_trees:empty()},
		       read_file_handles_limit = ReadFileHandlesLimit
		     },
    {ok, State1 = #dqstate { current_file_name = CurrentName,
			     current_offset = Offset } } = load_from_disk(State),
    Path = form_filename(CurrentName),
    ok = filelib:ensure_dir(Path),
    {ok, FileHdl} = file:open(Path, [read, write, raw, binary, delayed_write]), %% read only needed so that we can seek
    {ok, Offset} = file:position(FileHdl, {bof, Offset}),
    {ok, State1 # dqstate { current_file_handle = FileHdl }}.

handle_call({deliver, Q, MsgId}, _From, State) ->
    {ok, {MsgBody, BodySize, Delivered}, State1} = internal_deliver(Q, MsgId, State),
    {reply, {MsgBody, BodySize, Delivered}, State1};
handle_call({tx_commit, Q, MsgIds}, _From, State) ->
    {ok, State1} = internal_tx_commit(Q, MsgIds, State),
    {reply, ok, State1};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}; %% gen_server now calls terminate
handle_call(clean_stop, _From, State) ->
    State1 = #dqstate { msg_location = MsgLocation,
			file_summary = FileSummary,
			file_detail = FileDetail }
	= shutdown(State), %% tidy up file handles early
    {atomic, ok} = mnesia:clear_table(rabbit_disk_queue),
    true = ets:delete(MsgLocation),
    true = ets:delete(FileSummary),
    true = ets:delete(FileDetail),
    lists:foreach(fun file:delete/1, filelib:wildcard(form_filename("*"))),
    {stop, normal, ok, State1 # dqstate { current_file_handle = undefined,
					  read_file_handles = {dict:new(), gb_trees:empty()}}}.
    %% gen_server now calls terminate, which then calls shutdown

handle_cast({publish, Q, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_publish(Q, MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({ack, Q, MsgIds}, State) ->
    {ok, State1} = internal_ack(Q, MsgIds, State),
    {noreply, State1};
handle_cast({tx_publish, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({tx_cancel, MsgIds}, State) ->
    {ok, State1} = internal_tx_cancel(MsgIds, State),
    {noreply, State1}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    shutdown(State).

shutdown(State = #dqstate { current_file_handle = FileHdl,
			    read_file_handles = {ReadHdls, _ReadHdlsAge}
			  }) ->
    if FileHdl =:= undefined -> ok;
       true -> file:sync(FileHdl),
	       file:close(FileHdl)
    end,
    dict:fold(fun (_File, Hdl, _Acc) ->
		     file:close(Hdl)
	      end, ok, ReadHdls),
    State # dqstate { current_file_handle = undefined,
		      read_file_handles = {dict:new(), gb_trees:empty()}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---- UTILITY FUNCTIONS ----

form_filename(Name) ->
    filename:join(base_directory(), Name).

base_directory() ->
    filename:join(mnesia:system_info(directory), "rabbit_disk_queue/").

%% ---- INTERNAL RAW FUNCTIONS ----

internal_deliver(Q, MsgId, State = #dqstate { msg_location = MsgLocation,
					      read_file_handles_limit = ReadFileHandlesLimit,
					      read_file_handles = {ReadHdls, ReadHdlsAge}
					     }) ->
    [{MsgId, _RefCount, File, Offset, TotalSize}] = ets:lookup(MsgLocation, MsgId),
    % so this next bit implements an LRU for file handles. But it's a bit insane, and smells
    % of premature optimisation. So I might remove it and dump it overboard
    {FileHdl, ReadHdls1, ReadHdlsAge1}
	= case dict:find(File, ReadHdls) of
	      error ->
		  {ok, Hdl} = file:open(form_filename(File), [read, raw, binary, read_ahead]),
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
		  {Hdl, dict:store(File, {Hdl, Now}, ReadHdls),
		   gb_trees:enter(Now, File, gb_trees:delete(Then, ReadHdlsAge))}
	  end,
    % read the message
    {ok, {MsgBody, BodySize}} = read_message_at_offset(FileHdl, Offset, TotalSize),
    [Obj = #dq_msg_loc {is_delivered = Delivered}]
	= mnesia:dirty_read(rabbit_disk_queue, {MsgId, Q}),
    if Delivered -> ok;
       true ->  ok = mnesia:dirty_write(rabbit_disk_queue, Obj #dq_msg_loc {is_delivered = true})
    end,
    {ok, {MsgBody, BodySize, Delivered},
     State # dqstate { read_file_handles = {ReadHdls1, ReadHdlsAge1} }}.

internal_ack(Q, MsgIds, State) ->
    remove_messages(Q, MsgIds, true, State).

%% Q is only needed if MnesiaDelete = true
remove_messages(Q, MsgIds, MnesiaDelete, State = # dqstate { msg_location = MsgLocation,
							     file_summary = FileSummary,
							     file_detail = FileDetail
							   }) ->
    Files
	= lists:foldl(fun (MsgId, Files2) ->
			      [{MsgId, RefCount, File, Offset, TotalSize}]
				  = ets:lookup(MsgLocation, MsgId),
			      if 1 =:= RefCount ->
				      true = ets:delete(MsgLocation, MsgId),
				      [{File, FileSum = #dqfile { valid_data = ValidTotalSize,
								  contiguous_prefix = ContiguousTop }}]
					  = ets:lookup(FileSummary, File),
				      true = ets:delete(FileDetail, {File, Offset}),
				      ContiguousTop1 = lists:min([ContiguousTop, Offset]),
				      true = ets:insert(FileSummary,
							{File, FileSum #dqfile { valid_data = (ValidTotalSize -
											       TotalSize - (?FILE_PACKING_ADJUSTMENT)),
										 contiguous_prefix = ContiguousTop1}}),
				      if MnesiaDelete ->
					      ok = mnesia:dirty_delete(rabbit_disk_queue, {MsgId, Q});
					 true ->
					      ok
				      end,
				      sets:add_element(File, Files2);
				 1 < RefCount ->
				      ets:insert(MsgLocation, {MsgId, RefCount - 1, File, Offset, TotalSize}),
				      Files2
			      end
		      end, sets:new(), MsgIds),
    State2 = compact(Files, State),
    {ok, State2}.

internal_tx_publish(MsgId, MsgBody, State = #dqstate { msg_location = MsgLocation,
						       current_file_handle = CurHdl,
						       current_file_name = CurName,
						       current_offset = CurOffset,
						       file_summary = FileSummary,
						       file_detail = FileDetail
						     }) ->
    case ets:lookup(MsgLocation, MsgId) of
	[] ->
	    % New message, lots to do
	    {ok, TotalSize} = append_message(CurHdl, MsgId, MsgBody),
	    true = ets:insert_new(MsgLocation, {MsgId, 1, CurName, CurOffset, TotalSize}),
	    [{CurName, FileSum = #dqfile { valid_data = ValidTotalSize,
					   contiguous_prefix = ContiguousTop,
					   right = undefined }}]
		= ets:lookup(FileSummary, CurName),
	    true = ets:insert_new(FileDetail, {{CurName, CurOffset}, TotalSize}),
	    ValidTotalSize1 = ValidTotalSize + TotalSize + (?FILE_PACKING_ADJUSTMENT),
	    ContiguousTop1 = if CurOffset =:= ContiguousTop ->
				     ValidTotalSize; % can't be any holes in this file
				true -> ContiguousTop
			     end,
	    true = ets:insert(FileSummary, {CurName, FileSum #dqfile { valid_data = ValidTotalSize1,
								       contiguous_prefix = ContiguousTop1 }}),
	    maybe_roll_to_new_file(CurOffset + TotalSize + (?FILE_PACKING_ADJUSTMENT),
				   State # dqstate {current_offset = CurOffset + TotalSize + (?FILE_PACKING_ADJUSTMENT)});
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
	    fun() -> ok = mnesia:write_lock_table(rabbit_disk_queue),
		     lists:foldl(fun (MsgId, Acc) ->
					 [{MsgId, _RefCount, File, _Offset, _TotalSize}] =
					     ets:lookup(MsgLocation, MsgId),
					 ok = mnesia:write(rabbit_disk_queue,
							   #dq_msg_loc { msg_id_and_queue = {MsgId, Q},
									 is_delivered = false}, write),
					 Acc or (CurName =:= File)
				 end, false, MsgIds)
	    end),
    if Sync -> ok = file:sync(CurHdl);
       true -> ok
    end,
    {ok, State}.

internal_publish(Q, MsgId, MsgBody, State) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    ok = mnesia:dirty_write(rabbit_disk_queue, #dq_msg_loc { msg_id_and_queue = {MsgId, Q},
							     is_delivered = false}),
    {ok, State1}.

internal_tx_cancel(MsgIds, State) ->
    remove_messages(undefined, MsgIds, false, State).

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
    {ok, NextHdl} = file:open(form_filename(NextName), [write, raw, binary, delayed_write]),
    [{CurName, FileSum = #dqfile {right = undefined}}] = ets:lookup(FileSummary, CurName),
    true = ets:insert(FileSummary, {CurName, FileSum #dqfile {right = NextName}}),
    true = ets:insert_new(FileSummary, {NextName, #dqfile { valid_data = 0,
							    contiguous_prefix = 0,
							    left = CurName,
							    right = undefined }}),
    {ok, State # dqstate { current_file_name = NextName,
			   current_file_handle = NextHdl,
			   current_file_num = NextNum,
			   current_offset = 0
			 }};
maybe_roll_to_new_file(_, State) ->
    {ok, State}.

%% ---- GARBAGE COLLECTION / COMPACTION / AGGREGATION ----

compact(_FilesSet, State) ->
    State.

%% ---- DISK RECOVERY ----

load_from_disk(State) ->
    % sorted so that smallest number is first. which also means eldest file (left-most) first
    {Files, TmpFiles} = get_disk_queue_files(),
    ok = recover_crashed_compactions(Files, TmpFiles),
    % There should be no more tmp files now, so go ahead and load the whole lot
    (State1 = #dqstate{ msg_location = MsgLocation }) = load_messages(undefined, Files, State),
    % Finally, check there is nothing in mnesia which we haven't loaded
    true = lists:foldl(fun ({MsgId, _Q}, true) -> 1 =:= length(ets:lookup(MsgLocation, MsgId)) end,
		       true, mnesia:dirty_all_keys(rabbit_disk_queue)),
    {ok, State1}.

load_messages(undefined, [], State = #dqstate { file_summary = FileSummary,
						current_file_name = CurName }) ->
    true = ets:insert_new(FileSummary, {CurName, #dqfile { valid_data = 0,
							   contiguous_prefix = 0,
							   left = undefined,
							   right = undefined}}),
    State;
load_messages(Left, [], State = #dqstate { file_detail = FileDetail }) ->
    Num = list_to_integer(filename:rootname(Left)),
    Offset = case ets:match_object(FileDetail, {{Left, '_'}, '_'}) of
		 [] -> 0;
		 L -> {{Left, Offset1}, TotalSize} = lists:last(L),
		      Offset1 + TotalSize + (?FILE_PACKING_ADJUSTMENT)
	     end,
    State # dqstate { current_file_num = Num, current_file_name = Left,
		      current_offset = Offset };
load_messages(Left, [File|Files],
	      State = #dqstate { msg_location = MsgLocation,
				 file_summary = FileSummary,
				 file_detail = FileDetail
			       }) ->
    % [{MsgId, TotalSize, FileOffset}]
    {ok, Messages} = scan_file_for_valid_messages(form_filename(File)),
    {ValidMessagesRev, ValidTotalSize} = lists:foldl(
	fun ({MsgId, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
		case length(mnesia:dirty_match_object(rabbit_disk_queue,
						      #dq_msg_loc { msg_id_and_queue = {MsgId, '_'},
								    is_delivered = '_'})) of
		    0 -> {VMAcc, VTSAcc};
		    RefCount ->
			true = ets:insert_new(MsgLocation, {MsgId, RefCount, File, Offset, TotalSize}),
			true = ets:insert_new(FileDetail, {{File, Offset}, TotalSize}),
			{[{MsgId, TotalSize, Offset}|VMAcc],
			 VTSAcc + TotalSize + (?FILE_PACKING_ADJUSTMENT)
			}
		end
	end, {[], 0}, Messages),
    % foldl reverses lists and find_contiguous_block_prefix needs elems in the same order
    % as from scan_file_for_valid_messages
    {ContiguousTop, _} = find_contiguous_block_prefix(lists:reverse(ValidMessagesRev)),
    Right = case Files of
		[] -> undefined;
		[F|_] -> F
	    end,
    true = ets:insert_new(FileSummary, {File, #dqfile { valid_data = ValidTotalSize,
							contiguous_prefix = ContiguousTop,
							left = Left,
							right = Right
						       }}),
    load_messages(File, Files, State).

%% ---- DISK RECOVERY OF FAILED COMPACTION ----

recover_crashed_compactions(Files, TmpFiles) ->
    lists:foreach(fun (TmpFile) -> ok = recover_crashed_compactions1(Files, TmpFile) end,
		  TmpFiles),
    ok.

recover_crashed_compactions1(Files, TmpFile) ->
    GrabMsgId = fun ({MsgId, _TotalSize, _FileOffset}) -> MsgId end,
    NonTmpRelatedFile = filename:rootname(TmpFile) ++ (?FILE_EXTENSION),
    true = lists:member(NonTmpRelatedFile, Files),
    % [{MsgId, TotalSize, FileOffset}]
    {ok, UncorruptedMessagesTmp} = scan_file_for_valid_messages(form_filename(TmpFile)),
    MsgIdsTmp = lists:map(GrabMsgId, UncorruptedMessagesTmp),
    % all of these messages should appear in the mnesia table, otherwise they wouldn't have been copied out
    lists:foreach(fun (MsgId) ->
			  true = 0 < length(mnesia:dirty_match_object(rabbit_disk_queue,
								      #dq_msg_loc { msg_id_and_queue = {MsgId, '_'},
										    is_delivered = '_'}))
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
				  true = 0 <
				      length(mnesia:dirty_match_object(rabbit_disk_queue,
								       #dq_msg_loc { msg_id_and_queue = {MsgId, '_'},
										     is_delivered = '_'}))
			  end, MsgIds),
	    % The main file should be contiguous
	    {Top, MsgIds} = find_contiguous_block_prefix(UncorruptedMessages),
	    % we should have that none of the messages in the prefix are in the tmp file
	    true = lists:all(fun (MsgId) -> not(lists:member(MsgId, MsgIdsTmp)) end, MsgIds),

	    {ok, MainHdl} = file:open(form_filename(NonTmpRelatedFile), [write, raw, binary, delayed_write]),
	    {ok, Top} = file:position(MainHdl, Top),
	    ok = file:truncate(MainHdl), % wipe out any rubbish at the end of the file
	    % there really could be rubbish at the end of the file - we could have failed after the
	    % extending truncate.
	    % Remember the head of the list will be the highest entry in the file
	    [{_, TmpTopTotalSize, TmpTopOffset}|_] = UncorruptedMessagesTmp,
	    TmpSize = TmpTopOffset + TmpTopTotalSize + (?FILE_PACKING_ADJUSTMENT),
	    ExpectedAbsPos = Top + TmpSize,
	    {ok, ExpectedAbsPos} = file:position(MainHdl, {cur, TmpSize}),
	    ok = file:truncate(MainHdl), % and now extend the main file as big as necessary in a single move
					 % if we run out of disk space, this truncate could fail, but we still
					 % aren't risking losing data
	    {ok, TmpHdl} = file:open(form_filename(TmpFile), [read, raw, binary, read_ahead]),
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
    ok.

% this assumes that the messages are ordered such that the highest address is at
% the head of the list.
% this matches what scan_file_for_valid_messages produces
find_contiguous_block_prefix([]) -> {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail]) ->
    case find_contiguous_block_prefix(Tail, Offset, [MsgId]) of
	{ok, Acc} -> {Offset + TotalSize + (?FILE_PACKING_ADJUSTMENT), lists:reverse(Acc)};
	Res -> Res
    end.
find_contiguous_block_prefix([], 0, Acc) ->
    {ok, Acc};
find_contiguous_block_prefix([], _N, _Acc) ->
    {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail], ExpectedOffset, Acc)
  when ExpectedOffset =:= Offset + TotalSize + (?FILE_PACKING_ADJUSTMENT) ->
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
			       MsgIdBin:MsgIdBinSize/binary, MsgBody:BodySize/binary,
			       (?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>) of
	ok -> {ok, TotalSize};
	KO -> KO
    end.

read_message_at_offset(FileHdl, Offset, TotalSize) ->
    TotalSizeWriteOkBytes = TotalSize + 1,
    case file:position(FileHdl, {bof, Offset}) of
	{ok, Offset} ->
	    case file:read(FileHdl, TotalSize + (?FILE_PACKING_ADJUSTMENT)) of
		{ok, <<TotalSize:(?INTEGER_SIZE_BITS), MsgIdBinSize:(?INTEGER_SIZE_BITS), Rest:TotalSizeWriteOkBytes/binary>>} ->
		    BodySize = TotalSize - MsgIdBinSize,
		    <<_MsgId:MsgIdBinSize/binary, MsgBody:BodySize/binary, (?WRITE_OK):(?WRITE_OK_SIZE_BITS)>> = Rest,
		    {ok, {MsgBody, BodySize}};
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
		    ExpectedAbsPos = Offset + (?FILE_PACKING_ADJUSTMENT) + TotalSize,
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
						  Offset + (?FILE_PACKING_ADJUSTMENT) + TotalSize}};
					{ok, _SomeOtherData} ->
					    {ok, {corrupted, Offset + (?FILE_PACKING_ADJUSTMENT) + TotalSize}};
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
