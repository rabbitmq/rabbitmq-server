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

-record(mqstate, { mode,
                   msg_buf,
                   next_write_seq,
                   queue,
                   is_durable
                 }
       ).

start_link(Queue, IsDurable, Mode) when Mode =:= disk orelse Mode =:= mixed ->
    {ok, #mqstate { mode = Mode, msg_buf = queue:new(), next_write_seq = 1,
                    queue = Queue, is_durable = IsDurable }}.

msg_to_bin(Msg = #basic_message { content = Content }) ->
    ClearedContent = rabbit_binary_parser:clear_decoded_content(Content),
    term_to_binary(Msg #basic_message { content = ClearedContent }).

bin_to_msg(MsgBin) ->
    binary_to_term(MsgBin).

publish(Msg = #basic_message { guid = MsgId },
        State = #mqstate { mode = disk, queue = Q }) ->
    ok = rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg)),
    {ok, State};
publish(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
        State = #mqstate { queue = Q, mode = mixed, is_durable = IsDurable,
                           next_write_seq = NextSeq, msg_buf = MsgBuf }) ->
    ok = if IsDurable andalso IsPersistent ->
                 rabbit_disk_queue:publish_with_seq(Q, MsgId, NextSeq, msg_to_bin(Msg));
            true -> ok
         end,
    {ok, State #mqstate { next_write_seq = NextSeq + 1,
                          msg_buf = queue:in({NextSeq, Msg, false}, MsgBuf)
                        }}.

%% assumption here is that the queue is empty already (only called via attempt_immediate_delivery)
publish_delivered(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent},
                  State = #mqstate { mode = Mode, queue = Q, is_durable = IsDurable,
                                     next_write_seq = NextSeq })
  when Mode =:= disk orelse (IsDurable andalso IsPersistent) ->
    ok = rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg)),
    {MsgId, false, AckTag, 0} = rabbit_disk_queue:phantom_deliver(Q),
    State2 = if Mode =:= mixed -> State #mqstate { next_write_seq = NextSeq + 1 };
                true -> State
             end,
    {ok, AckTag, State2};
publish_delivered(_Msg, State = #mqstate { mode = mixed }) ->
    {ok, noack, State}.

deliver(State = #mqstate { mode = disk, queue = Q }) ->
    {MsgId, MsgBin, _Size, IsDelivered, AckTag, Remaining} = rabbit_disk_queue:deliver(Q),
    Msg = #basic_message { guid = MsgId } = bin_to_msg(MsgBin),
    {{Msg, IsDelivered, AckTag, Remaining}, State};
deliver(State = #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf,
                           next_write_seq = NextWrite, is_durable = IsDurable }) ->
    {Result, MsgBuf2} = queue:out(MsgBuf),
    case Result of
        empty ->
            {empty, State};
        {value, {Seq, Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent }, IsDelivered}} ->
            AckTag =
                if IsDurable andalso IsPersistent ->
                        {MsgId, IsDelivered, AckTag2, _PersistRemaining} = rabbit_disk_queue:phantom_deliver(Q),
                        AckTag2;
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
                                                   
tx_publish(Msg = #basic_message { guid = MsgId }, State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_publish(MsgId, msg_to_bin(Msg)),
    {ok, State};
tx_publish(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
           State = #mqstate { mode = mixed, is_durable = IsDurable })
  when IsDurable andalso IsPersistent ->
    ok = rabbit_disk_queue:tx_publish(MsgId, msg_to_bin(Msg)),
    {ok, State};
tx_publish(_Msg, State = #mqstate { mode = mixed }) ->
    {ok, State}.

only_msg_ids(Pubs) ->
    lists:map(fun (Msg) -> Msg #basic_message.guid end, Pubs).

tx_commit(Publishes, Acks, State = #mqstate { mode = disk, queue = Q }) ->
    ok = rabbit_disk_queue:tx_commit(Q, only_msg_ids(Publishes), Acks),
    {ok, State};
tx_commit(Publishes, Acks, State = #mqstate { mode = mixed, queue = Q,
                                              msg_buf = MsgBuf,
                                              next_write_seq = NextSeq,
                                              is_durable = IsDurable
                                            }) ->
    {PersistentPubs, MsgBuf2, NextSeq2} =
        lists:foldl(fun (Msg = #basic_message { is_persistent = IsPersistent },
                         {Acc, MsgBuf3, NextSeq3}) ->
                            Acc2 =
                                if IsPersistent ->
                                        [{Msg #basic_message.guid, NextSeq3} | Acc];
                                   true -> Acc
                                end,
                            MsgBuf4 = queue:in({NextSeq3, Msg, false}, MsgBuf3),
                            {Acc2, MsgBuf4, NextSeq3 + 1}
                    end, {[], MsgBuf, NextSeq}, Publishes),
    %% foldl reverses, so re-reverse PersistentPubs to match
    %% requirements of rabbit_disk_queue (ascending SeqIds)
    PersistentPubs2 = if IsDurable -> lists:reverse(PersistentPubs);
                         true -> []
                      end,
    ok = rabbit_disk_queue:tx_commit_with_seqs(Q, PersistentPubs2,
                                               remove_noacks(Acks)),
    {ok, State #mqstate { msg_buf = MsgBuf2, next_write_seq = NextSeq2 }}.

only_persistent_msg_ids(Pubs) ->
    lists:reverse(lists:foldl(fun (Msg = #basic_message { is_persistent = IsPersistent },
                                   Acc) ->
                                      if IsPersistent -> [Msg #basic_message.guid | Acc];
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

%% [{Msg, AckTag}]
requeue(MessagesWithAckTags, State = #mqstate { mode = disk, queue = Q }) ->
    rabbit_disk_queue:requeue(Q, only_ack_tags(MessagesWithAckTags)),
    {ok, State};
requeue(MessagesWithAckTags, State = #mqstate { mode = mixed, queue = Q,
                                                msg_buf = MsgBuf,
                                                next_write_seq = NextSeq,
                                                is_durable = IsDurable
                                              }) ->
    {PersistentPubs, MsgBuf2, NextSeq2} =
        lists:foldl(fun ({Msg = #basic_message { is_persistent = IsPersistent, guid = MsgId }, AckTag},
                         {Acc, MsgBuf3, NextSeq3}) ->
                            Acc2 =
                                if IsDurable andalso IsPersistent ->
                                        {MsgId, _OldSeqId} = AckTag,
                                        [{AckTag, NextSeq3} | Acc];
                                   true -> Acc
                                end,
                            MsgBuf4 = queue:in({NextSeq3, Msg, true}, MsgBuf3),
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
