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

-compile(export_all). %% CHANGE ME

-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-define(WRITE_OK_SIZE_BITS, 8).
-define(WRITE_OK, 255).
-define(INTEGER_SIZE_BYTES, 8).
-define(INTEGER_SIZE_BITS, 8 * ?INTEGER_SIZE_BYTES).
-define(MSG_LOC_ETS_NAME, rabbit_disk_queue_msg_location).
-define(FILE_DETAIL_ETS_NAME, rabbit_disk_queue_file_detail).
-define(FILE_EXTENSION, ".rdq").
-define(FILE_EXTENSION_TMP, ".rdt").

-record(dqstate, {msg_location, file_summary, file_detail, next_file_name}).

init(_Args) ->
    process_flag(trap_exit, true),
    Dir = base_directory(),
    ok = filelib:ensure_dir(Dir),
    State = #dqstate { msg_location = ets:new((?MSG_LOC_ETS_NAME), [set, private]),
		       file_summary = dict:new(),
		       file_detail = ets:new((?FILE_DETAIL_ETS_NAME), [bag, private]),
		       next_file_name = 0
		     },
    {ok, State1} = load_from_disk(State),
    {ok, State1}.
    

base_directory() ->
    filename:join(mnesia:system_info(directory), "/rabbit_disk_queue/").

%% ---- DISK RECOVERY ----

load_from_disk(State = #dqstate{ msg_location = MsgLoc,
				 file_summary = FileSum,
				 file_detail = FileDetail
			       }
	      ) ->
    {Files, TmpFiles} = get_disk_queue_files(),
    ok = recover_crashed_compactions(Files, TmpFiles),
    {ok, State}.

recover_crashed_compactions(Files, []) ->
    ok;
recover_crashed_compactions(Files, [TmpFile|TmpFiles]) ->
    NonTmpRelatedFile = filename:rootname(TmpFile) ++ (?FILE_EXTENSION),
    true = lists:member(NonTmpRelatedFile, Files),
    % [{MsgId, TotalSize, FileOffset}]
    {ok, UncorruptedMessagesTmp} = scan_file_for_valid_messages(filename:join(base_directory(), TmpFile)),
    % all of these messages should appear in the mnesia table, otherwise they wouldn't have been copied out
    lists:foreach(fun ({MsgId, _TotalSize, _FileOffset}) ->
			  0 < length(mnesia:match_object(rabbit_disk_queue, {dq_msg_loc, {'_', MsgId}, '_'}))
		  end, UncorruptedMessagesTmp),
    {ok, UncorruptedMessages} = scan_file_for_valid_messages(filename:join(base_directory(), NonTmpRelatedFile)),
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
    GrabMsgId = fun ({MsgId, _TotalSize, _FileOffset}) -> MsgId end,
    MsgIdsTmp = lists:map(GrabMsgId, UncorruptedMessagesTmp),
    MsgIds = lists:map(GrabMsgId, UncorruptedMessages),
    case lists:all(fun (MsgId) -> lists:member(MsgId, MsgIds) end, MsgIdsTmp) of
	true -> % we're in case 1, 2 or 3 above. Just delete the tmp file
	        % note this also catches the case when the tmp file is empty
	    ok = file:delete(TmpFile);
	_False ->
	    % we're in case 4 above.
	    % check that everything in the main file is a valid message in mnesia
	    lists:foreach(fun (MsgId) ->
				  0 < length(mnesia:match_object(rabbit_disk_queue, {dq_msg_loc, {'_', MsgId}, '_'}))
			  end, MsgIds),
	    % The main file should be contiguous
	    {Top, MsgIds} = find_contiguous_block_prefix(UncorruptedMessages),
	    % we should have that none of the messages in the prefix are in the tmp file
	    true = lists:all(fun (MsgId) -> not(lists:member(MsgId, MsgIdsTmp)) end, MsgIds),

	    {ok, MainHdl} = file:open(filename:join(base_directory(), NonTmpRelatedFile), [write, raw, binary]),
	    {ok, Top} = file:position(MainHdl, Top),
	    ok = file:truncate(MainHdl), % wipe out any rubbish at the end of the file
	    % there really could be rubbish at the end of the file - we could have failed after the
	    % extending truncate.
	    % Remember the head of the list will be the highest entry in the file
	    [{_, TmpTopTotalSize, TmpTopOffset}|_] = UncorruptedMessagesTmp,
	    TmpSize = TmpTopOffset + TmpTopTotalSize + 1 + (2* (?INTEGER_SIZE_BYTES)),
	    ExpectedAbsPos = Top + TmpSize,
	    {ok, ExpectedAbsPos} = file:position(MainHdl, {cur, TmpSize}),
	    ok = file:truncate(MainHdl), % and now extend the main file as big as necessary in a single move
					 % if we run out of disk space, this truncate could fail, but we still
					 % aren't risking losing data
	    {ok, TmpHdl} = file:open(filename:join(base_directory(), TmpFile), [read, raw, binary]),
	    {ok, TmpSize} = file:copy(TmpHdl, MainHdl, TmpSize),
	    ok = file:close(MainHdl),
	    ok = file:close(TmpHdl),
	    ok = file:delete(TmpFile)
    end,
    recover_crashed_compactions(Files, TmpFiles).

% this assumes that the messages are ordered such that the highest address is at
% the head of the list.
% this matches what scan_file_for_valid_messages produces
find_contiguous_block_prefix([]) -> {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail]) ->
    case find_contiguous_block_prefix(Tail, Offset, [MsgId]) of
	{ok, Acc} -> {Offset + TotalSize + 1 + (2* (?INTEGER_SIZE_BYTES)), lists:reverse(Acc)};
	Res -> Res
    end.
find_contiguous_block_prefix([], 0, Acc) ->
    {ok, Acc};
find_contiguous_block_prefix([], _N, _Acc) ->
    {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail], ExpectedOffset, Acc)
  when ExpectedOffset =:= Offset + TotalSize + 1 + (2* (?INTEGER_SIZE_BYTES)) ->
    find_contiguous_block_prefix(Tail, Offset, [MsgId|Acc]);
find_contiguous_block_prefix(List, _ExpectedOffset, Acc) ->
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
	ok -> file:write(FileHdl, <<(?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>);
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
				    {ok, MsgBody, BodySize};
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
    file:close(Hdl),
    Valid.

scan_file_for_valid_messages(FileHdl, Offset, Acc) ->
    case read_next_file_entry(FileHdl, Offset) of
	{ok, eof} -> {ok, Acc};
	{ok, {corrupted, NextOffset}} ->
	    scan_file_for_valid_messages(FileHdl, NextOffset, Acc);
	{ok, {ok, MsgId, TotalSize, NextOffset}} ->
	    scan_file_for_valid_messages(FileHdl, NextOffset, [{MsgId, TotalSize, Offset}|Acc]);
	KO -> {ok, Acc} %% bad message, but we may still have recovered some valid messages
    end.
	    

read_next_file_entry(FileHdl, Offset) ->
    case file:read(FileHdl, 2 * (?INTEGER_SIZE_BYTES)) of
	{ok, <<TotalSize:(?INTEGER_SIZE_BITS), MsgIdBinSize:(?INTEGER_SIZE_BITS)>>} ->
	    case {TotalSize =:= 0, MsgIdBinSize =:= 0} of
		{true, _} -> {ok, eof}; %% Nothing we can do other than stop
		{false, true} -> %% current message corrupted, try skipping past it
		    ExpectedAbsPos = Offset + (2* (?INTEGER_SIZE_BYTES)) + TotalSize + 1,
		    case file:position(FileHdl, {cur, TotalSize + 1}) of
			{ok, ExpectedAbsPos} -> {ok, {corrupted, ExpectedAbsPos}};
			{ok, _SomeOtherPos} -> {ok, eof}; %% seek failed, so give up
			KO -> KO
		    end;
		{false, false} -> %% all good, let's continue
		    case file:read(FileHdl, MsgIdBinSize) of
			{ok, <<MsgId:MsgIdBinSize/binary>>} ->
			    ExpectedAbsPos = Offset + (2 * (?INTEGER_SIZE_BYTES)) + TotalSize,
			    case file:position(FileHdl, {cur, TotalSize - MsgIdBinSize}) of
				{ok, ExpectedAbsPos} ->
				    case file:read(FileHdl, 1) of
					{ok, <<(?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>} ->
					    {ok, {ok, binary_to_term(MsgId), TotalSize,
						  Offset + (2* (?INTEGER_SIZE_BYTES)) + TotalSize + 1}};
					{ok, _SomeOtherData} ->
					    {ok, {corrupted, Offset + (2* (?INTEGER_SIZE_BYTES)) + TotalSize + 1}};
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
