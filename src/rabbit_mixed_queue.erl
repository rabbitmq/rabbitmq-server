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

-export([publish/4, deliver/1, ack/2,
	 tx_publish/4, tx_commit/3, tx_cancel/2,
	 requeue/2, purge/1]).

-record(mqstate, { mode,
		   msg_buf,
		   next_write_seq,
		   queue
		 }
       ).

publish(MsgId, Msg, _IsPersistent, State = #mqstate { mode = disk, queue = Q }) ->
    ok = rabbit_disk_queue:publish(Q, MsgId, Msg),
    {ok, State};
publish(MsgId, Msg, IsPersistent,
	State = #mqstate { queue = Q, mode = mixed,
			   next_write_seq = NextSeq, msg_buf = MsgBuf }) ->
    if IsPersistent ->
	    ok = rabbit_disk_queue:publish_with_seq(Q, MsgId, NextSeq, Msg);
       true -> ok
    end,
    {ok, State #mqstate { next_write_seq = NextSeq + 1,
			  msg_buf = queue:in({NextSeq, {MsgId, Msg, IsPersistent}},
					     MsgBuf)
			}}.

deliver(State = #mqstate { mode = disk, queue = Q }) ->
    {rabbit_disk_queue:deliver(Q), State};
deliver(State = #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf }) ->
    {Result, MsgBuf2} = queue:out(MsgBuf),
    case Result of
	empty ->
	    {empty, State};
	{value, {_Seq, {MsgId, Msg, IsPersistent}}} ->
	    {IsDelivered, Ack} =
		if IsPersistent ->
			{MsgId, IsDelivered2, Ack2} = rabbit_disk_queue:phantom_deliver(Q),
			{IsDelivered2, Ack2};
		   true -> {false, noack}
		end,
	    {{MsgId, Msg, size(Msg), IsDelivered, Ack},
	     State #mqstate { msg_buf = MsgBuf2 }}
    end.

remove_noacks(Acks) ->
    lists:filter(fun (A) -> A /= noack end, Acks).

ack(Acks, State = #mqstate { queue = Q }) ->	     
    ok = rabbit_disk_queue:ack(Q, remove_noacks(Acks)),
    {ok, State}.
						   
tx_publish(MsgId, Msg, _IsPersistent, State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_publish(MsgId, Msg),
    {ok, State};
tx_publish(MsgId, Msg, true, State = #mqstate { mode = mixed }) ->
    ok = rabbit_disk_queue:tx_publish(MsgId, Msg),
    {ok, State};
tx_publish(_MsgId, _Msg, false, State = #mqstate { mode = mixed }) ->
    {ok, State}.

only_msg_ids(Pubs) ->
    lists:map(fun (P) -> element(1, P) end, Pubs).

tx_commit(Publishes, Acks, State = #mqstate { mode = disk, queue = Q }) ->
    ok = rabbit_disk_queue:tx_commit(Q, only_msg_ids(Publishes), Acks),
    {ok, State};
tx_commit(Publishes, Acks, State = #mqstate { mode = mixed, queue = Q,
					      msg_buf = MsgBuf,
					      next_write_seq = NextSeq
					     }) ->
    {PersistentPubs, MsgBuf2, NextSeq2} =
	lists:foldl(fun ({MsgId, Msg, IsPersistent}, {Acc, MsgBuf3, NextSeq3}) ->
			    Acc2 =
				if IsPersistent ->
					[{MsgId, NextSeq3} | Acc];
				   true -> Acc
				end,
			    MsgBuf4 = queue:in({NextSeq3, {MsgId, Msg, IsPersistent}},
					       MsgBuf3),
			    {Acc2, MsgBuf4, NextSeq3 + 1}
		    end, {[], MsgBuf, NextSeq}, Publishes),
    %% foldl reverses, so re-reverse PersistentPubs to match
    %% requirements of rabbit_disk_queue (ascending SeqIds)
    ok = rabbit_disk_queue:tx_commit_with_seqs(Q, lists:reverse(PersistentPubs),
					       remove_noacks(Acks)),
    {ok, State #mqstate { msg_buf = MsgBuf2, next_write_seq = NextSeq2 }}.

only_persistent_msg_ids(Pubs) ->
    lists:reverse(lists:foldl(fun ({MsgId, _, IsPersistent}, Acc) ->
				      if IsPersistent -> [MsgId | Acc];
					 true -> Acc
				      end
			      end, [], Pubs)).

tx_cancel(Publishes, State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_cancel(only_msg_ids(Publishes)),
    {ok, State};
tx_cancel(Publishes, State = #mqstate { mode = mixed }) ->
    ok = rabbit_disk_queue:tx_cancel(only_persistent_msg_ids(Publishes)),
    {ok, State}.

only_ack_tags(MsgWithAcks) ->
    lists:map(fun (P) -> element(2, P) end, MsgWithAcks).

requeue(MessagesWithAckTags, State = #mqstate { mode = disk, queue = Q }) ->
    rabbit_disk_queue:requeue(Q, only_ack_tags(MessagesWithAckTags)),
    {ok, State};
requeue(MessagesWithAckTags, State = #mqstate { mode = mixed, queue = Q,
						msg_buf = MsgBuf,
						next_write_seq = NextSeq
					      }) ->
    {PersistentPubs, MsgBuf2, NextSeq2} =
	lists:foldl(fun ({{MsgId, Msg, IsPersistent}, AckTag}, {Acc, MsgBuf3, NextSeq3}) ->
			    Acc2 =
				if IsPersistent ->
					{MsgId, _OldSeqId} = AckTag,
					[{AckTag, NextSeq3} | Acc];
				   true -> Acc
				end,
			    MsgBuf4 = queue:in({NextSeq3, {MsgId, Msg, IsPersistent}},
					       MsgBuf3),
			    {Acc2, MsgBuf4, NextSeq3 + 1}
		    end, {[], MsgBuf, NextSeq}, MessagesWithAckTags),
    ok = rabbit_disk_queue:requeue_with_seqs(Q, lists:reverse(PersistentPubs)),
    {ok, State #mqstate { msg_buf = MsgBuf2, next_write_seq = NextSeq2 }}.

purge(State = #mqstate { queue = Q, mode = disk }) ->
    Count = rabbit_disk_queue:purge(Q),
    {Count, State};
purge(State = #mqstate { queue = Q, msg_buf = MsgBuf, mode = mixed }) ->
    rabbit_disk_queue:purge(Q),
    Count = queue:len(MsgBuf),
    {Count, State #mqstate { msg_buf = queue:new() }}.
