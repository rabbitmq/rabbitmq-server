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

-module(rabbit_mixed_queue).

-include("rabbit.hrl").

-export([start_link/3]).

-export([publish/2, publish_delivered/2, deliver/1, ack/2,
         tx_publish/2, tx_commit/3, tx_cancel/2, requeue/2, purge/1,
         length/1, is_empty/1, delete_queue/1]).

-export([to_disk_only_mode/1, to_mixed_mode/1]).

-record(mqstate, { mode,
                   msg_buf,
                   next_write_seq,
                   queue,
                   is_durable
                 }
       ).

start_link(Queue, IsDurable, disk) ->
    purge_non_persistent_messages(
      #mqstate { mode = disk, msg_buf = queue:new(), queue = Queue,
        	 next_write_seq = 0, is_durable = IsDurable });
start_link(Queue, IsDurable, mixed) ->
    {ok, State} = start_link(Queue, IsDurable, disk),
    to_mixed_mode(State).

to_disk_only_mode(State = #mqstate { mode = disk }) ->
    {ok, State};
to_disk_only_mode(State = #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf,
                                     next_write_seq = NextSeq }) ->
    rabbit_log:info("Converting queue to disk only mode: ~p~n", [Q]),
    %% We enqueue _everything_ here. This means that should a message
    %% already be in the disk queue we must remove it and add it back
    %% in. Fortunately, by using requeue, we avoid rewriting the
    %% message on disk.
    %% Note we also batch together messages on disk so that we minimise
    %% the calls to requeue.
    Msgs = queue:to_list(MsgBuf),
    {NextSeq1, Requeue} =
        lists:foldl(
          fun ({_Seq, Msg = #basic_message { guid = MsgId },
                IsDelivered, OnDisk}, {NSeq, RQueueAcc}) ->
                  if OnDisk ->
                          {MsgId, IsDelivered, AckTag, _PersistRemaining} =
                              rabbit_disk_queue:phantom_deliver(Q),
                          {NSeq + 1,
                           [ {AckTag, {NSeq, IsDelivered}} | RQueueAcc ]};
                     true ->
                          ok = if [] == RQueueAcc -> ok;
                                  true ->
                                       rabbit_disk_queue:requeue_with_seqs(
                                         Q, lists:reverse(RQueueAcc))
                               end,
                          ok = rabbit_disk_queue:publish_with_seq(
                                 Q, MsgId, NSeq, msg_to_bin(Msg), false),
                          {NSeq + 1, []}
                  end
          end, {NextSeq, []}, Msgs),
    ok = if [] == Requeue -> ok;
            true ->
                 rabbit_disk_queue:requeue_with_seqs(Q, lists:reverse(Requeue))
         end,
    {ok, State #mqstate { mode = disk, msg_buf = queue:new(),
                          next_write_seq = NextSeq1 }}.

to_mixed_mode(State = #mqstate { mode = mixed }) ->
    {ok, State};
to_mixed_mode(State = #mqstate { mode = disk, queue = Q }) ->
    rabbit_log:info("Converting queue to mixed mode: ~p~n", [Q]),
    %% load up a new queue with everything that's on disk.
    %% don't remove non-persistent messages that happen to be on disk
    QList = rabbit_disk_queue:dump_queue(Q),
    {MsgBuf1, NextSeq1} =
        lists:foldl(
          fun ({MsgId, MsgBin, _Size, IsDelivered, _AckTag, SeqId}, {Buf, NSeq})
              when SeqId >= NSeq ->
                  Msg = #basic_message { guid = MsgId } = bin_to_msg(MsgBin),
                  {queue:in({SeqId, Msg, IsDelivered, true}, Buf), SeqId + 1}
          end, {queue:new(), 0}, QList),
    State1 = State #mqstate { mode = mixed, msg_buf = MsgBuf1,
			  next_write_seq = NextSeq1 },
    rabbit_log:info("Queue length: ~p ~w ~w~n",
                    [Q, rabbit_mixed_queue:length(State),
                     rabbit_mixed_queue:length(State1)]),
    {ok, State1}.

purge_non_persistent_messages(State = #mqstate { mode = disk, queue = Q,
						 is_durable = IsDurable }) ->
    %% iterate through the content on disk, ack anything which isn't
    %% persistent, accumulate everything else that is persistent and
    %% requeue it
    NextSeq = rabbit_disk_queue:next_write_seq(Q),
    {Acks, Requeue, NextSeq2} =
	deliver_all_messages(Q, IsDurable, [], [], NextSeq),
    ok = if Requeue == [] -> ok;
            true -> rabbit_disk_queue:requeue_with_seqs(Q, lists:reverse(Requeue))
         end,
    ok = if Acks == [] -> ok;
            true -> rabbit_disk_queue:ack(Q, lists:reverse(Acks))
         end,
    {ok, State #mqstate { next_write_seq = NextSeq2 }}.

deliver_all_messages(Q, IsDurable, Acks, Requeue, NextSeq) ->
    case rabbit_disk_queue:deliver(Q) of
	empty -> {Acks, Requeue, NextSeq};
	{MsgId, MsgBin, _Size, IsDelivered, AckTag, _Remaining} ->
	    #basic_message { guid = MsgId, is_persistent = IsPersistent } =
		bin_to_msg(MsgBin),
	    OnDisk = IsPersistent andalso IsDurable,
	    {Acks2, Requeue2, NextSeq2} =
		if OnDisk -> {Acks,
			      [{AckTag, {NextSeq, IsDelivered}} | Requeue],
			      NextSeq + 1
			     };
		   true -> {[AckTag | Acks], Requeue, NextSeq}
		end,
	    deliver_all_messages(Q, IsDurable, Acks2, Requeue2, NextSeq2)
    end.

msg_to_bin(Msg = #basic_message { content = Content }) ->
    ClearedContent = rabbit_binary_parser:clear_decoded_content(Content),
    term_to_binary(Msg #basic_message { content = ClearedContent }).

bin_to_msg(MsgBin) ->
    binary_to_term(MsgBin).

publish(Msg = #basic_message { guid = MsgId },
        State = #mqstate { mode = disk, queue = Q }) ->
    ok = rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg), false),
    {ok, State};
publish(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
        State = #mqstate { queue = Q, mode = mixed, is_durable = IsDurable,
                           next_write_seq = NextSeq, msg_buf = MsgBuf }) ->
    OnDisk = IsDurable andalso IsPersistent,
    ok = if OnDisk ->
                 rabbit_disk_queue:publish_with_seq(Q, MsgId, NextSeq,
						    msg_to_bin(Msg), false);
            true -> ok
         end,
    {ok, State #mqstate { next_write_seq = NextSeq + 1,
                          msg_buf = queue:in({NextSeq, Msg, false, OnDisk},
					     MsgBuf)
			}}.

%% Assumption here is that the queue is empty already (only called via
%% attempt_immediate_delivery).  Also note that the seq id assigned by
%% the disk queue could well not be the same as the NextSeq (true =
%% NextSeq >= disk_queue_write_seq_for_queue(Q)) , but this doesn't
%% matter because the AckTag will still be correct (AckTags for
%% non-persistent messages don't exist). (next_write_seq is actually
%% only used to calculate how many messages are in the queue).
publish_delivered(Msg =
		  #basic_message { guid = MsgId, is_persistent = IsPersistent},
                  State = #mqstate { mode = Mode, is_durable = IsDurable,
                                     next_write_seq = NextSeq, queue = Q })
  when Mode =:= disk orelse (IsDurable andalso IsPersistent) ->
    true = rabbit_disk_queue:is_empty(Q),
    rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg), false),
    %% must call phantom_deliver otherwise the msg remains at the head
    %% of the queue
    {MsgId, false, AckTag, 0} = rabbit_disk_queue:phantom_deliver(Q),
    State2 =
	if Mode =:= mixed -> State #mqstate { next_write_seq = NextSeq + 1 };
	   true -> State
	end,
    {ok, AckTag, State2};
publish_delivered(_Msg, State = #mqstate { mode = mixed, msg_buf = MsgBuf }) ->
    true = queue:is_empty(MsgBuf),
    {ok, noack, State}.

deliver(State = #mqstate { mode = disk, queue = Q, is_durable = IsDurable }) ->
    case rabbit_disk_queue:deliver(Q) of
	empty -> {empty, State};
	{MsgId, MsgBin, _Size, IsDelivered, AckTag, Remaining} ->
	    #basic_message { guid = MsgId, is_persistent = IsPersistent } =
		Msg = bin_to_msg(MsgBin),
	    AckTag2 = if IsPersistent andalso IsDurable -> AckTag;
			 true -> ok = rabbit_disk_queue:ack(Q, [AckTag]),
				 noack
		      end,
	    {{Msg, IsDelivered, AckTag2, Remaining}, State}
    end;
       
deliver(State = #mqstate { mode = mixed, queue = Q, is_durable = IsDurable,
                           next_write_seq = NextWrite, msg_buf = MsgBuf }) ->
    {Result, MsgBuf2} = queue:out(MsgBuf),
    case Result of
        empty ->
            {empty, State};
        {value, {Seq, Msg = #basic_message { guid = MsgId,
					     is_persistent = IsPersistent },
		 IsDelivered, OnDisk}} ->
            AckTag =
                if OnDisk ->
			if IsPersistent andalso IsDurable -> 
                                {MsgId, IsDelivered, AckTag2, _PersistRem} =
                                    rabbit_disk_queue:phantom_deliver(Q),
                                AckTag2;
			   true ->
                                ok = rabbit_disk_queue:auto_ack_next_message(Q),
                                noack
			end;
                   true -> noack
                end,
            {{Msg, IsDelivered, AckTag, (NextWrite - 1 - Seq)},
             State #mqstate { msg_buf = MsgBuf2 }}
    end.

remove_noacks(Acks) ->
    lists:filter(fun (A) -> A /= noack end, Acks).

ack(Acks, State = #mqstate { queue = Q }) ->
    case remove_noacks(Acks) of
        [] -> {ok, State};
        AckTags -> ok = rabbit_disk_queue:ack(Q, AckTags),
                   {ok, State}
    end.
                                                   
tx_publish(Msg = #basic_message { guid = MsgId },
	   State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_publish(MsgId, msg_to_bin(Msg)),
    {ok, State};
tx_publish(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
           State = #mqstate { mode = mixed, is_durable = IsDurable })
  when IsDurable andalso IsPersistent ->
    ok = rabbit_disk_queue:tx_publish(MsgId, msg_to_bin(Msg)),
    {ok, State};
tx_publish(_Msg, State = #mqstate { mode = mixed }) ->
    %% this message will reappear in the tx_commit, so ignore for now
    {ok, State}.

only_msg_ids(Pubs) ->
    lists:map(fun (Msg) -> Msg #basic_message.guid end, Pubs).

tx_commit(Publishes, Acks, State = #mqstate { mode = disk, queue = Q }) ->
    RealAcks = remove_noacks(Acks),
    ok = if ([] == Publishes) andalso ([] == RealAcks) -> ok;
	    true -> rabbit_disk_queue:tx_commit(Q, only_msg_ids(Publishes),
						RealAcks)
	 end,
    {ok, State};
tx_commit(Publishes, Acks, State = #mqstate { mode = mixed, queue = Q,
                                              msg_buf = MsgBuf,
                                              next_write_seq = NextSeq,
                                              is_durable = IsDurable
                                            }) ->
    {PersistentPubs, MsgBuf2, NextSeq2} =
        lists:foldl(fun (Msg = #basic_message { is_persistent = IsPersistent },
                         {Acc, MsgBuf3, NextSeq3}) ->
			    OnDisk = IsPersistent andalso IsDurable,
                            Acc2 =
                                if OnDisk ->
                                        [{Msg #basic_message.guid, NextSeq3}
					 | Acc];
                                   true -> Acc
                                end,
                            MsgBuf4 = queue:in({NextSeq3, Msg, false, OnDisk},
					       MsgBuf3),
                            {Acc2, MsgBuf4, NextSeq3 + 1}
                    end, {[], MsgBuf, NextSeq}, Publishes),
    %% foldl reverses, so re-reverse PersistentPubs to match
    %% requirements of rabbit_disk_queue (ascending SeqIds)
    RealAcks = remove_noacks(Acks),
    ok = if ([] == PersistentPubs) andalso ([] == RealAcks) -> ok;
	    true ->
		 rabbit_disk_queue:tx_commit_with_seqs(
		   Q, lists:reverse(PersistentPubs), RealAcks)
	 end,
    {ok, State #mqstate { msg_buf = MsgBuf2, next_write_seq = NextSeq2 }}.

only_persistent_msg_ids(Pubs) ->
    lists:reverse(
      lists:foldl(
	fun (Msg = #basic_message { is_persistent = IsPersistent }, Acc) ->
		if IsPersistent -> [Msg #basic_message.guid | Acc];
		   true -> Acc
		end
	end, [], Pubs)).

tx_cancel(Publishes, State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_cancel(only_msg_ids(Publishes)),
    {ok, State};
tx_cancel(Publishes,
	  State = #mqstate { mode = mixed, is_durable = IsDurable }) ->
    ok =
	if IsDurable ->
		rabbit_disk_queue:tx_cancel(only_persistent_msg_ids(Publishes));
	   true -> ok
	end,
    {ok, State}.

%% [{Msg, AckTag}]
requeue(MessagesWithAckTags, State = #mqstate { mode = disk, queue = Q,
                                                is_durable = IsDurable }) ->
    %% here, we may have messages with no ack tags, because of the
    %% fact they are not persistent, but nevertheless we want to
    %% requeue them. This means publishing them delivered.
    Requeue
        = lists:foldl(
            fun ({#basic_message { is_persistent = IsPersistent }, AckTag}, RQ)
                when IsPersistent andalso IsDurable ->
                    [AckTag | RQ];
                ({Msg = #basic_message { guid = MsgId }, _AckTag}, RQ) ->
                    ok = if RQ == [] -> ok;
                            true -> rabbit_disk_queue:requeue(
                                      Q, lists:reverse(RQ))
                         end,
                    _AckTag2 = rabbit_disk_queue:publish(
                                 Q, MsgId, msg_to_bin(Msg), true),
                    []
            end, [], MessagesWithAckTags),
    ok = rabbit_disk_queue:requeue(Q, lists:reverse(Requeue)),
    {ok, State};
requeue(MessagesWithAckTags, State = #mqstate { mode = mixed, queue = Q,
                                                msg_buf = MsgBuf,
                                                next_write_seq = NextSeq,
                                                is_durable = IsDurable
                                              }) ->
    {PersistentPubs, MsgBuf2, NextSeq2} =
        lists:foldl(
	  fun ({Msg = #basic_message { is_persistent = IsPersistent }, AckTag},
	       {Acc, MsgBuf3, NextSeq3}) ->
		  OnDisk = IsDurable andalso IsPersistent,
		  Acc2 =
		      if OnDisk -> [{AckTag, {NextSeq3, true}} | Acc];
			 true -> Acc
		      end,
		  MsgBuf4 = queue:in({NextSeq3, Msg, true, OnDisk}, MsgBuf3),
		  {Acc2, MsgBuf4, NextSeq3 + 1}
	  end, {[], MsgBuf, NextSeq}, MessagesWithAckTags),
    ok = if [] == PersistentPubs -> ok;
            true -> rabbit_disk_queue:requeue_with_seqs(
                      Q, lists:reverse(PersistentPubs))
         end,
    {ok, State #mqstate { msg_buf = MsgBuf2, next_write_seq = NextSeq2 }}.

purge(State = #mqstate { queue = Q, mode = disk }) ->
    Count = rabbit_disk_queue:purge(Q),
    {Count, State};
purge(State = #mqstate { queue = Q, msg_buf = MsgBuf, mode = mixed }) ->
    rabbit_disk_queue:purge(Q),
    Count = queue:len(MsgBuf),
    {Count, State #mqstate { msg_buf = queue:new() }}.

delete_queue(State = #mqstate { queue = Q, mode = disk }) ->
    rabbit_disk_queue:delete_queue(Q),
    {ok, State};
delete_queue(State = #mqstate { queue = Q, mode = mixed }) ->
    rabbit_disk_queue:delete_queue(Q),
    {ok, State #mqstate { msg_buf = queue:new() }}.

length(#mqstate { queue = Q, mode = disk }) ->
    rabbit_disk_queue:length(Q);
length(#mqstate { mode = mixed, msg_buf = MsgBuf }) ->
    queue:len(MsgBuf).

is_empty(State) ->
    0 == rabbit_mixed_queue:length(State).
