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

-module(rabbit_msg_store_gc).

-behaviour(gen_server2).

-export([start_link/4, gc/3, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(gcstate,
        {dir,
         index_state,
         index_module,
         parent,
         file_summary_ets
        }).

-include("rabbit_msg_store.hrl").

%%----------------------------------------------------------------------------

start_link(Dir, IndexState, IndexModule, FileSummaryEts) ->
    gen_server2:start_link(
      ?MODULE, [self(), Dir, IndexState, IndexModule, FileSummaryEts],
      [{timeout, infinity}]).

gc(Server, Source, Destination) ->
    gen_server2:cast(Server, {gc, Source, Destination}).

stop(Server) ->
    gen_server2:call(Server, stop).

%%----------------------------------------------------------------------------

init([Parent, Dir, IndexState, IndexModule, FileSummaryEts]) ->
    {ok, #gcstate { dir = Dir, index_state = IndexState,
                    index_module = IndexModule, parent = Parent,
                    file_summary_ets = FileSummaryEts},
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({gc, Source, Destination}, State = #gcstate { parent = Parent }) ->
    Reclaimed = adjust_meta_and_combine(Source, Destination,
                                        State),
    ok = rabbit_msg_store:gc_done(Parent, Reclaimed, Source, Destination),
    {noreply, State, hibernate}.

handle_info({file_handle_cache, maximum_eldest_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

adjust_meta_and_combine(SourceFile, DestFile, State =
                            #gcstate { file_summary_ets = FileSummaryEts }) ->

    [SourceObj = #file_summary {
       readers = SourceReaders,
       valid_total_size = SourceValidData, left = DestFile,
       file_size = SourceFileSize, locked = true }] =
        ets:lookup(FileSummaryEts, SourceFile),
    [DestObj = #file_summary {
       readers = DestReaders,
       valid_total_size = DestValidData, right = SourceFile,
       file_size = DestFileSize, locked = true }] =
        ets:lookup(FileSummaryEts, DestFile),

    case SourceReaders =:= 0 andalso DestReaders =:= 0 of
        true ->
            TotalValidData = DestValidData + SourceValidData,
            ok = combine_files(SourceObj, DestObj, State),
            %% don't update dest.right, because it could be changing
            %% at the same time
            true = ets:update_element(
                     FileSummaryEts, DestFile,
                     [{#file_summary.valid_total_size, TotalValidData},
                      {#file_summary.contiguous_top,   TotalValidData},
                      {#file_summary.file_size,        TotalValidData}]),
            SourceFileSize + DestFileSize - TotalValidData;
        false ->
            timer:sleep(100),
            adjust_meta_and_combine(SourceFile, DestFile, State)
    end.

combine_files(#file_summary { file = Source,
                              valid_total_size = SourceValid,
                              left = Destination },
              #file_summary { file = Destination,
                              valid_total_size = DestinationValid,
                              contiguous_top = DestinationContiguousTop,
                              right = Source },
              State = #gcstate { dir = Dir }) ->
    SourceName = rabbit_msg_store_misc:filenum_to_name(Source),
    DestinationName = rabbit_msg_store_misc:filenum_to_name(Destination),
    {ok, SourceHdl} =
        rabbit_msg_store_misc:open_file(Dir, SourceName, ?READ_AHEAD_MODE),
    {ok, DestinationHdl} =
        rabbit_msg_store_misc:open_file(Dir, DestinationName,
                                        ?READ_AHEAD_MODE ++ ?WRITE_MODE),
    ExpectedSize = SourceValid + DestinationValid,
    %% if DestinationValid =:= DestinationContiguousTop then we don't
    %% need a tmp file
    %% if they're not equal, then we need to write out everything past
    %%   the DestinationContiguousTop to a tmp file then truncate,
    %%   copy back in, and then copy over from Source
    %% otherwise we just truncate straight away and copy over from Source
    if DestinationContiguousTop =:= DestinationValid ->
            ok = rabbit_msg_store_misc:truncate_and_extend_file(
                   DestinationHdl, DestinationValid, ExpectedSize);
       true ->
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
                  end,
                  find_unremoved_messages_in_file(Destination, State)),
            Tmp = filename:rootname(DestinationName) ++ ?FILE_EXTENSION_TMP,
            {ok, TmpHdl} = rabbit_msg_store_misc:open_file(
                             Dir, Tmp, ?READ_AHEAD_MODE ++ ?WRITE_MODE),
            ok = copy_messages(
                   Worklist, DestinationContiguousTop, DestinationValid,
                   DestinationHdl, TmpHdl, Destination, State),
            TmpSize = DestinationValid - DestinationContiguousTop,
            %% so now Tmp contains everything we need to salvage from
            %% Destination, and index_state has been updated to
            %% reflect the compaction of Destination so truncate
            %% Destination and copy from Tmp back to the end
            {ok, 0} = file_handle_cache:position(TmpHdl, 0),
            ok = rabbit_msg_store_misc:truncate_and_extend_file(
                   DestinationHdl, DestinationContiguousTop, ExpectedSize),
            {ok, TmpSize} =
                file_handle_cache:copy(TmpHdl, DestinationHdl, TmpSize),
            %% position in DestinationHdl should now be DestinationValid
            ok = file_handle_cache:sync(DestinationHdl),
            ok = file_handle_cache:close(TmpHdl),
            ok = file:delete(rabbit_msg_store_misc:form_filename(Dir, Tmp))
    end,
    SourceWorkList = find_unremoved_messages_in_file(Source, State),
    ok = copy_messages(SourceWorkList, DestinationValid, ExpectedSize,
                       SourceHdl, DestinationHdl, Destination, State),
    %% tidy up
    ok = file_handle_cache:close(SourceHdl),
    ok = file_handle_cache:close(DestinationHdl),
    ok = file:delete(rabbit_msg_store_misc:form_filename(Dir, SourceName)),
    ok.

find_unremoved_messages_in_file(File, #gcstate { dir = Dir,
                                                 index_state = IndexState,
                                                 index_module = Index }) ->
    %% Msgs here will be end-of-file at start-of-list
    {ok, Messages, _FileSize} =
        rabbit_msg_store_misc:scan_file_for_valid_messages(
          Dir, rabbit_msg_store_misc:filenum_to_name(File)),
    %% foldl will reverse so will end up with msgs in ascending offset order
    lists:foldl(
      fun ({MsgId, _TotalSize, _Offset}, Acc) ->
              case Index:lookup(MsgId, IndexState) of
                  Entry = #msg_location { file = File } -> [ Entry | Acc ];
                  _                                     -> Acc
              end
      end, [], Messages).

copy_messages(WorkList, InitOffset, FinalOffset, SourceHdl, DestinationHdl,
              Destination, #gcstate { index_module = Index,
                                      index_state = IndexState }) ->
    {FinalOffset, BlockStart1, BlockEnd1} =
        lists:foldl(
          fun (#msg_location { msg_id = MsgId, offset = Offset,
                               total_size = TotalSize },
               {CurOffset, BlockStart, BlockEnd}) ->
                  %% CurOffset is in the DestinationFile.
                  %% Offset, BlockStart and BlockEnd are in the SourceFile
                  %% update MsgLocation to reflect change of file and offset
                  ok = Index:update_fields(MsgId,
                                           [{#msg_location.file, Destination},
                                            {#msg_location.offset, CurOffset}],
                                           IndexState),
                  {BlockStart2, BlockEnd2} =
                      if BlockStart =:= undefined ->
                              %% base case, called only for the first list elem
                              {Offset, Offset + TotalSize};
                         Offset =:= BlockEnd ->
                              %% extend the current block because the
                              %% next msg follows straight on
                              {BlockStart, BlockEnd + TotalSize};
                         true ->
                              %% found a gap, so actually do the work
                              %% for the previous block
                              BSize = BlockEnd - BlockStart,
                              {ok, BlockStart} =
                                  file_handle_cache:position(SourceHdl,
                                                             BlockStart),
                              {ok, BSize} = file_handle_cache:copy(
                                              SourceHdl, DestinationHdl, BSize),
                              {Offset, Offset + TotalSize}
                      end,
                  {CurOffset + TotalSize, BlockStart2, BlockEnd2}
          end, {InitOffset, undefined, undefined}, WorkList),
    case WorkList of
        [] ->
            ok;
        _ ->
            %% do the last remaining block
            BSize1 = BlockEnd1 - BlockStart1,
            {ok, BlockStart1} =
                file_handle_cache:position(SourceHdl, BlockStart1),
            {ok, BSize1} =
                file_handle_cache:copy(SourceHdl, DestinationHdl, BSize1),
            ok = file_handle_cache:sync(DestinationHdl)
    end,
    ok.
