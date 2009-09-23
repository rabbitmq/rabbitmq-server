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

-export([init/2]).

-export([publish/2, publish_delivered/2, fetch/1, ack/2,
         tx_publish/2, tx_commit/3, tx_rollback/2, requeue/2, purge/1,
         len/1, is_empty/1, delete_queue/1, maybe_prefetch/1]).

-export([set_storage_mode/3, storage_mode/1,
         estimate_queue_memory/1]).

-record(mqstate, { mode,
                   msg_buf,
                   queue,
                   is_durable,
                   length,
                   memory_size,
                   prefetcher
                 }
       ).

-define(TO_DISK_MAX_FLUSH_SIZE, 100000).
-define(MAGIC_MARKER, <<"$magic_marker">>).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mode() :: ( 'disk' | 'mixed' )).
-type(mqstate() :: #mqstate { mode :: mode(),
                              msg_buf :: queue(),
                              queue :: queue_name(),
                              is_durable :: boolean(),
                              length :: non_neg_integer(),
                              memory_size :: (non_neg_integer() | 'undefined'),
                              prefetcher :: (pid() | 'undefined')
                            }).
-type(msg_id() :: guid()).
-type(seq_id() :: non_neg_integer()).
-type(ack_tag() :: ( 'not_on_disk' | {msg_id(), seq_id()} )).
-type(okmqs() :: {'ok', mqstate()}).

-spec(init/2 :: (queue_name(), boolean()) -> okmqs()).
-spec(publish/2 :: (message(), mqstate()) -> okmqs()).
-spec(publish_delivered/2 :: (message(), mqstate()) ->
             {'ok', ack_tag(), mqstate()}).
-spec(fetch/1 :: (mqstate()) ->
             {('empty' | {message(), boolean(), ack_tag(), non_neg_integer()}),
              mqstate()}).
-spec(ack/2 :: ([{message(), ack_tag()}], mqstate()) -> okmqs()).
-spec(tx_publish/2 :: (message(), mqstate()) -> okmqs()).
-spec(tx_commit/3 :: ([message()], [ack_tag()], mqstate()) -> okmqs()).
-spec(tx_rollback/2 :: ([message()], mqstate()) -> okmqs()).
-spec(requeue/2 :: ([{message(), ack_tag()}], mqstate()) -> okmqs()).
-spec(purge/1 :: (mqstate()) -> okmqs()).
-spec(delete_queue/1 :: (mqstate()) -> {'ok', mqstate()}).
-spec(len/1 :: (mqstate()) -> non_neg_integer()).
-spec(is_empty/1 :: (mqstate()) -> boolean()).

-spec(set_storage_mode/3 :: (mode(), [message()], mqstate()) -> okmqs()).
-spec(estimate_queue_memory/1 :: (mqstate()) ->
             {mqstate(), non_neg_integer()}).
-spec(storage_mode/1 :: (mqstate()) -> mode()).

-endif.

%%----------------------------------------------------------------------------

init(Queue, IsDurable) ->
    Len = rabbit_disk_queue:len(Queue),
    {Size, MarkerFound, MarkerPreludeCount} =
        rabbit_disk_queue:foldl(
          fun (Msg = #basic_message { is_persistent = true },
               _AckTag, _IsDelivered, {SizeAcc, MFound, MPCount}) ->
                  SizeAcc1 = SizeAcc + size_of_message(Msg),
                  case {MFound, is_magic_marker_message(Msg)} of
                      {false, false} -> {SizeAcc1, false, MPCount + 1};
                      {false, true}  -> {SizeAcc,  true,  MPCount};
                      {true, false}  -> {SizeAcc1, true,  MPCount}
                  end
          end, {0, false, 0}, Queue),
    Len1 = case MarkerFound of
               false -> Len;
               true ->
                   ok = rabbit_disk_queue:requeue_next_n(Queue,
                                                         MarkerPreludeCount),
                   Len2 = Len - 1,
                   {ok, Len2} = fetch_ack_magic_marker_message(Queue),
                   Len2
           end,
    MsgBuf = inc_queue_length(queue:new(), Len1),
    {ok, #mqstate { mode = disk, msg_buf = MsgBuf, queue = Queue,
                    is_durable = IsDurable, length = Len1,
                    memory_size = Size, prefetcher = undefined }}.

publish(Msg = #basic_message { is_persistent = IsPersistent }, State = 
        #mqstate { queue = Q, mode = Mode, is_durable = IsDurable,
                   msg_buf = MsgBuf, length = Length }) ->
    Msg1 = ensure_binary_properties(Msg),
    ok = case on_disk(Mode, IsDurable, IsPersistent) of
             true  -> rabbit_disk_queue:publish(Q, Msg1, false);
             false -> ok
         end,
    MsgBuf1 = case Mode of
                  disk  -> inc_queue_length(MsgBuf, 1);
                  mixed -> queue:in({Msg1, false}, MsgBuf)
              end,
    {ok, gain_memory(size_of_message(Msg1),
                     State #mqstate { msg_buf = MsgBuf1,
                                      length = Length + 1 })}.

%% Assumption here is that the queue is empty already (only called via
%% attempt_immediate_delivery).
publish_delivered(Msg = #basic_message { guid = MsgId,
                                         is_persistent = IsPersistent},
                  State = #mqstate { is_durable = IsDurable, queue = Q,
                                     length = 0 })
  when IsDurable andalso IsPersistent ->
    Msg1 = ensure_binary_properties(Msg),
    ok = rabbit_disk_queue:publish(Q, Msg1, true),
    State1 = gain_memory(size_of_message(Msg1), State),
    %% must call phantom_fetch otherwise the msg remains at the head
    %% of the queue. This is synchronous, but unavoidable as we need
    %% the AckTag
    {MsgId, true, AckTag, 0} = rabbit_disk_queue:phantom_fetch(Q),
    {ok, AckTag, State1};
publish_delivered(Msg, State = #mqstate { length = 0 }) ->
    Msg1 = ensure_binary_properties(Msg),
    {ok, not_on_disk, gain_memory(size_of_message(Msg1), State)}.

fetch(State = #mqstate { length = 0 }) ->
    {empty, State};
fetch(State = #mqstate { msg_buf = MsgBuf, queue = Q,
                         is_durable = IsDurable, length = Length,
                         prefetcher = Prefetcher }) ->
    {{value, Value}, MsgBuf1} = queue:out(MsgBuf),
    Rem = Length - 1,
    State1 = State #mqstate { length = Rem },
    case Value of
        {Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
         IsDelivered} ->
            AckTag =
                case IsDurable andalso IsPersistent of
                    true ->
                        {MsgId, IsDelivered, AckTag1, _PRem}
                            = rabbit_disk_queue:phantom_fetch(Q),
                        AckTag1;
                    false ->
                        not_on_disk
                end,
            {{Msg, IsDelivered, AckTag, Rem},
             State1 #mqstate { msg_buf = MsgBuf1 }};
        {Msg = #basic_message { is_persistent = IsPersistent },
         IsDelivered, AckTag} ->
            %% message has come via the prefetcher, thus it's been
            %% marked delivered. If it's not persistent+durable, we
            %% should ack it now
            AckTag1 = maybe_ack(Q, IsDurable, IsPersistent, AckTag),
            {{Msg, IsDelivered, AckTag1, Rem},
             State1 #mqstate { msg_buf = MsgBuf1 }};
        _ when Prefetcher == undefined ->
            MsgBuf2 = dec_queue_length(MsgBuf, 1),
            {Msg = #basic_message { is_persistent = IsPersistent },
             IsDelivered, AckTag, _PersistRem}
                = rabbit_disk_queue:fetch(Q),
            AckTag1 = maybe_ack(Q, IsDurable, IsPersistent, AckTag),
            {{Msg, IsDelivered, AckTag1, Rem},
             State1 #mqstate { msg_buf = MsgBuf2 }};
        _ ->
            %% use State, not State1 as we've not dec'd length
            fetch(case rabbit_queue_prefetcher:drain(Prefetcher) of
                      empty -> State #mqstate { prefetcher = undefined };
                      {Fetched, Status} ->
                          MsgBuf2 = dec_queue_length(MsgBuf, queue:len(Fetched)),
                          State #mqstate
                            { msg_buf = queue:join(Fetched, MsgBuf2),
                              prefetcher = case Status of
                                               finished -> undefined;
                                               continuing -> Prefetcher
                                           end }
                  end)
    end.

ack(MsgsWithAcks, State = #mqstate { queue = Q }) ->
    {AckTags, ASize} = remove_diskless(MsgsWithAcks),
    ok = case AckTags of
             [] -> ok;
             _ -> rabbit_disk_queue:ack(Q, AckTags)
         end,
    {ok, lose_memory(ASize, State)}.
                                                   
tx_publish(Msg = #basic_message { is_persistent = IsPersistent },
           State = #mqstate { mode = Mode, is_durable = IsDurable }) ->
    Msg1 = ensure_binary_properties(Msg),
    ok = case on_disk(Mode, IsDurable, IsPersistent) of
             true  -> rabbit_disk_queue:tx_publish(Msg1);
             false -> ok
         end,
    {ok, gain_memory(size_of_message(Msg1), State)}.

tx_commit(Publishes, MsgsWithAcks,
          State = #mqstate { mode = Mode, queue = Q, msg_buf = MsgBuf,
                             is_durable = IsDurable, length = Length }) ->
    PersistentPubs =
        [{MsgId, false, IsPersistent} ||
            #basic_message { guid = MsgId,
                             is_persistent = IsPersistent } <- Publishes,
            on_disk(Mode, IsDurable, IsPersistent)],
    {RealAcks, ASize} = remove_diskless(MsgsWithAcks),
    ok = case {PersistentPubs, RealAcks} of
             {[], []} -> ok;
             _        -> rabbit_disk_queue:tx_commit(
                           Q, PersistentPubs, RealAcks)
         end,
    Len = length(Publishes),
    MsgBuf1 = case Mode of
                  disk  -> inc_queue_length(MsgBuf, Len);
                  mixed -> ToAdd = [{Msg, false} || Msg <- Publishes],
                           queue:join(MsgBuf, queue:from_list(ToAdd))
              end,
    {ok, lose_memory(ASize, State #mqstate { msg_buf = MsgBuf1,
                                             length = Length + Len })}.

tx_rollback(Publishes,
            State = #mqstate { mode = Mode, is_durable = IsDurable }) ->
    {PersistentPubs, CSize} =
        lists:foldl(
          fun (Msg = #basic_message { is_persistent = IsPersistent,
                                      guid = MsgId }, {Acc, CSizeAcc}) ->
                  Msg1 = ensure_binary_properties(Msg),
                  CSizeAcc1 = CSizeAcc + size_of_message(Msg1),
                  {case on_disk(Mode, IsDurable, IsPersistent) of
                       true -> [MsgId | Acc];
                       _    -> Acc
                   end, CSizeAcc1}
          end, {[], 0}, Publishes),
    ok = case PersistentPubs of
             [] -> ok;
             _  -> rabbit_disk_queue:tx_rollback(PersistentPubs)
         end,
    {ok, lose_memory(CSize, State)}.

%% [{Msg, AckTag}]
requeue(MsgsWithAckTags,
        State = #mqstate { mode = Mode, queue = Q, msg_buf = MsgBuf,
                           is_durable = IsDurable, length = Length }) ->
    RQ = lists:foldl(
           fun ({Msg = #basic_message { is_persistent = IsPersistent }, AckTag},
                RQAcc) ->
                   case IsDurable andalso IsPersistent of
                       true ->
                           [{AckTag, true} | RQAcc];
                       false ->
                           case Mode of
                               mixed ->
                                   RQAcc;
                               disk when not_on_disk =:= AckTag ->
                                   ok = case RQAcc of
                                            [] -> ok;
                                            _  -> rabbit_disk_queue:requeue
                                                    (Q, lists:reverse(RQAcc))
                                        end,
                                   ok = rabbit_disk_queue:publish(Q, Msg, true),
                                   []
                           end
                   end
           end, [], MsgsWithAckTags),
    ok = case RQ of
             [] -> ok;
             _  -> rabbit_disk_queue:requeue(Q, lists:reverse(RQ))
         end,
    Len = length(MsgsWithAckTags),
    MsgBuf1 = case Mode of
                  mixed -> ToAdd = [{Msg, true} || {Msg, _} <- MsgsWithAckTags],
                           queue:join(MsgBuf, queue:from_list(ToAdd));
                  disk  -> inc_queue_length(MsgBuf, Len)
              end,
    {ok, State #mqstate { msg_buf = MsgBuf1, length = Length + Len }}.

purge(State = #mqstate { queue = Q, mode = Mode, length = Count,
                         prefetcher = Prefetcher, memory_size = QSize }) ->
    PurgedFromDisk = rabbit_disk_queue:purge(Q),
    Count = case Mode of
                disk ->
                    PurgedFromDisk;
                mixed ->
                    ok = case Prefetcher of
                             undefined -> ok;
                             _ -> rabbit_queue_prefetcher:stop(Prefetcher)
                         end,
                    Count
            end,
    {Count, lose_memory(QSize, State #mqstate { msg_buf = queue:new(),
                                                length = 0,
                                                prefetcher = undefined })}.

delete_queue(State = #mqstate { queue = Q, memory_size = QSize,
                                prefetcher = Prefetcher
                              }) ->
    ok = case Prefetcher of
             undefined -> ok;
             _ -> rabbit_queue_prefetcher:stop(Prefetcher)
         end,
    ok = rabbit_disk_queue:delete_queue(Q),
    {ok, lose_memory(QSize, State #mqstate { length = 0, msg_buf = queue:new(),
                                             prefetcher = undefined })}.

len(#mqstate { length = Length }) ->
    Length.

is_empty(#mqstate { length = Length }) ->
    0 == Length.

%%----------------------------------------------------------------------------
%% storage mode management
%%----------------------------------------------------------------------------

set_storage_mode(Mode, _TxnMessages, State = #mqstate { mode = Mode }) ->
    {ok, State};
set_storage_mode(disk, TxnMessages, State =
         #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf, length = Length,
                    is_durable = IsDurable, prefetcher = Prefetcher }) ->
    State1 = State #mqstate { mode = disk },
    MsgBuf1 =
        case Prefetcher of
            undefined -> MsgBuf;
            _ ->
                case rabbit_queue_prefetcher:drain_and_stop(Prefetcher) of
                    empty -> MsgBuf;
                    Fetched ->
                        MsgBuf2 = dec_queue_length(MsgBuf, queue:len(Fetched)),
                        queue:join(Fetched, MsgBuf2)
                end
        end,
    {ok, MsgBuf3} =
        send_messages_to_disk(IsDurable, Q, MsgBuf1, Length),
    %% tx_publish txn messages. Some of these will have been already
    %% published if they really are durable and persistent which is
    %% why we can't just use our own tx_publish/2 function (would end
    %% up publishing twice, so refcount would go wrong in disk_queue).
    %% The order of msgs within a txn is determined only at tx_commit
    %% time, so it doesn't matter if we're publishing msgs to the disk
    %% queue in a different order from that which we received them in.
    lists:foreach(
      fun (Msg = #basic_message { is_persistent = IsPersistent }) ->
              ok = case IsDurable andalso IsPersistent of
                       true -> ok;
                       _    -> rabbit_disk_queue:tx_publish(Msg)
                   end
      end, TxnMessages),
    garbage_collect(),
    {ok, State1 #mqstate { msg_buf = MsgBuf3, prefetcher = undefined }};
set_storage_mode(mixed, TxnMessages, State =
                 #mqstate { mode = disk, is_durable = IsDurable }) ->
    %% The queue has a token just saying how many msgs are on disk
    %% (this is already built for us when in disk mode).
    %% Don't actually do anything to the disk
    %% Don't start prefetcher just yet because the queue maybe busy -
    %% wait for hibernate timeout in the amqqueue_process.
    
    %% Remove txn messages from disk which are not (persistent and
    %% durable). This is necessary to avoid leaks. This is also pretty
    %% much the inverse behaviour of our own tx_rollback/2 which is
    %% why we're not using that.
    Cancel = [ MsgId || #basic_message { is_persistent = IsPersistent,
                                         guid = MsgId } <- TxnMessages,
                   not (IsDurable andalso IsPersistent) ],
    ok = case Cancel of
             [] -> ok;
             _  -> rabbit_disk_queue:tx_rollback(Cancel)
         end,
    garbage_collect(),
    {ok, State #mqstate { mode = mixed }}.

send_messages_to_disk(_IsDurable, _Q, MsgBuf, 0) ->
    {ok, MsgBuf};
send_messages_to_disk(IsDurable, Q, MsgBuf, Length) ->
    case scan_for_disk_after_ram(IsDurable, MsgBuf) of
        disk_only ->
            %% Everything on disk already, we don't need to do
            %% anything
            {ok, inc_queue_length(queue:new(), Length)};
        {not_found, PrefixLen, MsgBufRAMSuffix} ->
            %% No disk msgs follow RAM msgs and the queue has a RAM
            %% suffix, so we can just publish those. If we crash at
            %% this point, we may lose some messages, but everything
            %% will remain in the right order, so no need for the
            %% marker messages.
            MsgBuf1 = inc_queue_length(queue:new(), PrefixLen),
            send_messages_to_disk(IsDurable, Q, MsgBufRAMSuffix, 0, 0, [], [],
                                  MsgBuf1);
        found ->
            %% There are disk msgs *after* ram msgs in the queue. We
            %% need to reenqueue everything. Note that due to batching
            %% going on (see comments above send_messages_to_disk/8),
            %% if we crash during this transition, we could have
            %% messages in the wrong order on disk. Thus we publish a
            %% magic_marker_message which, when this transition is
            %% complete, will be back at the head of the queue. Should
            %% we die, on startup, during the foldl over the queue, we
            %% detect the marker message and requeue all the messages
            %% in front of it, to the back of the queue, thus
            %% correcting the order.  The result is that everything
            %% ends up back in the same order, but will have new
            %% sequence IDs.
            ok = publish_magic_marker_message(Q),
            {ok, MsgBuf1} =
                send_messages_to_disk(IsDurable, Q, MsgBuf, 0, 0, [], [],
                                      queue:new()),
            {ok, Length} = fetch_ack_magic_marker_message(Q),
            {ok, MsgBuf1}
    end.

scan_for_disk_after_ram(IsDurable, MsgBuf) ->
    scan_for_disk_after_ram(IsDurable, MsgBuf, {disk, 0}).

%% We return 'disk_only' if everything is alread on disk; 'found' if
%% we find a disk message after finding RAM messages; and
%% {'not_found', Count, MsgBuf} otherwise, where Count is the length
%% of the disk prefix, and MsgBuf is the RAM suffix of the MsgBuf
%% argument. Note msgs via the prefetcher are counted as RAM msgs on
%% the grounds that they have to be republished.
scan_for_disk_after_ram(IsDurable, MsgBuf, Mode) ->
    case queue:out(MsgBuf) of
        {empty, _MsgBuf} ->
            case Mode of
                {ram, N, MsgBuf1} -> {not_found, N, MsgBuf1};
                {disk, _N}        -> disk_only
            end;
        {{value, {on_disk, Count}}, MsgBuf1} ->
            case Mode of
                {ram, _, _} -> found; %% found disk after RAM, bad
                {disk, N} -> scan_for_disk_after_ram(IsDurable, MsgBuf1,
                                                     {disk, N + Count})
            end;
        {{value, {_Msg, _IsDelivered, _AckTag}}, MsgBuf1} ->
            %% found a msg from the prefetcher. Ensure RAM mode
            scan_for_disk_after_ram(IsDurable, MsgBuf1,
                                    ensure_ram(Mode, MsgBuf));
        {{value,
          {#basic_message { is_persistent = IsPersistent }, _IsDelivered}},
          MsgBuf1} ->
            %% normal message
            case IsDurable andalso IsPersistent of
                true ->
                    case Mode of
                        {ram, _, _} -> found; %% found disk after RAM, bad
                        {disk, N} -> scan_for_disk_after_ram(IsDurable, MsgBuf1,
                                                             {disk, N + 1})
                    end;
                false -> scan_for_disk_after_ram(IsDurable, MsgBuf1,
                                                 ensure_ram(Mode, MsgBuf))
            end
    end.

ensure_ram(Obj = {ram, _N, _MsgBuf}, _MsgBuf1) -> Obj;
ensure_ram({disk, N}, MsgBuf)                  -> {ram, N, MsgBuf}.

%% (Re)enqueue _everything_ here. Messages which are not on disk will
%% be tx_published, messages that are on disk will be requeued to the
%% end of the queue. This is done in batches, where a batch consists
%% of a number a tx_publishes, a tx_commit and then a call to
%% requeue_next_n. We do not want to fetch messages off disk only to
%% republish them later. Note in the tx_commit, we ack messages which
%% are being _re_published. These are messages that have been fetched
%% by the prefetcher.
%% Batches are limited in size to make sure that the resultant mnesia
%% transaction on tx_commit does not get too big, memory wise.
send_messages_to_disk(IsDurable, Q, Queue, PublishCount, RequeueCount,
                      Commit, Ack, MsgBuf) ->
    case queue:out(Queue) of
        {empty, _Queue} ->
            ok = flush_messages_to_disk_queue(Q, Commit, Ack),
            {[], []} = flush_requeue_to_disk_queue(Q, RequeueCount, [], []),
            {ok, MsgBuf};
        {{value, {Msg = #basic_message { is_persistent = IsPersistent },
                  IsDelivered}}, Queue1} ->
            case IsDurable andalso IsPersistent of
                true -> %% it's already in the Q
                    send_messages_to_disk(
                      IsDurable, Q, Queue1, PublishCount, RequeueCount + 1,
                      Commit, Ack, inc_queue_length(MsgBuf, 1));
                false ->
                    republish_message_to_disk_queue(
                      IsDurable, Q, Queue1, PublishCount, RequeueCount, Commit,
                      Ack, MsgBuf, Msg, IsDelivered)
            end;
        {{value, {Msg, IsDelivered, AckTag}}, Queue1} ->
            %% These have come via the prefetcher, so are no longer in
            %% the disk queue (yes, they've not been ack'd yet, but
            %% the head of the queue has passed these messages). We
            %% need to requeue them, which we sneakily achieve by
            %% tx_publishing them, and then in the tx_commit, ack the
            %% old copy.
            republish_message_to_disk_queue(
              IsDurable, Q, Queue1, PublishCount, RequeueCount, Commit,
              [AckTag | Ack], MsgBuf, Msg, IsDelivered);
        {{value, {on_disk, Count}}, Queue1} ->
            send_messages_to_disk(
              IsDurable, Q, Queue1, PublishCount, RequeueCount + Count,
              Commit, Ack, inc_queue_length(MsgBuf, Count))
    end.

republish_message_to_disk_queue(
  IsDurable, Q, Queue, PublishCount, RequeueCount, Commit, Ack, MsgBuf,
  Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
  IsDelivered) ->
    {Commit1, Ack1} = flush_requeue_to_disk_queue(Q, RequeueCount, Commit, Ack),
    ok = rabbit_disk_queue:tx_publish(Msg),
    Commit2 = [{MsgId, IsDelivered, IsPersistent} | Commit1],
    {PublishCount1, Commit3, Ack2} =
        case PublishCount == ?TO_DISK_MAX_FLUSH_SIZE of
            true  -> ok = flush_messages_to_disk_queue(Q, Commit2, Ack1),
                     {0, [], []};
            false -> {PublishCount + 1, Commit2, Ack1}
        end,
    send_messages_to_disk(IsDurable, Q, Queue, PublishCount1, 0,
                          Commit3, Ack2, inc_queue_length(MsgBuf, 1)).

flush_messages_to_disk_queue(_Q, [], []) ->
    ok;
flush_messages_to_disk_queue(Q, Commit, Ack) ->
    rabbit_disk_queue:tx_commit(Q, lists:reverse(Commit), Ack).

flush_requeue_to_disk_queue(_Q, 0, Commit, Ack) ->
    {Commit, Ack};
flush_requeue_to_disk_queue(Q, RequeueCount, Commit, Ack) ->
    ok = flush_messages_to_disk_queue(Q, Commit, Ack),
    ok = rabbit_disk_queue:requeue_next_n(Q, RequeueCount),
    {[], []}.

%% Scaling this by 4 is a magic number. Found by trial and error to
%% work ok. We are deliberately over reporting so that we run out of
%% memory sooner rather than later, because the transition to disk
%% only modes transiently can take quite a lot of memory.
estimate_queue_memory(State = #mqstate { memory_size = Size }) ->
    {State, 4 * Size}.

storage_mode(#mqstate { mode = Mode }) ->
    Mode.

%%----------------------------------------------------------------------------
%% helpers
%%----------------------------------------------------------------------------

size_of_message(
  #basic_message { content = #content { payload_fragments_rev = Payload,
                                        properties_bin = PropsBin }})
  when is_binary(PropsBin) ->
    size(PropsBin) + lists:foldl(fun (Frag, SumAcc) ->
                                         SumAcc + size(Frag)
                                 end, 0, Payload).

ensure_binary_properties(Msg = #basic_message { content = Content }) ->
    Msg #basic_message {
      content = rabbit_binary_generator:ensure_content_encoded(Content) }.

gain_memory(Inc, State = #mqstate { memory_size = QSize }) ->
    State #mqstate { memory_size = QSize + Inc }.

lose_memory(Dec, State = #mqstate { memory_size = QSize }) ->
    State #mqstate { memory_size = QSize - Dec }.

inc_queue_length(MsgBuf, 0) ->
    MsgBuf;
inc_queue_length(MsgBuf, Count) ->
    {NewCount, MsgBufTail} =
        case queue:out_r(MsgBuf) of
            {empty, MsgBuf1}                   -> {Count, MsgBuf1};
            {{value, {on_disk, Len}}, MsgBuf1} -> {Len + Count, MsgBuf1};
            {{value, _}, _MsgBuf1}             -> {Count, MsgBuf}
        end,
    queue:in({on_disk, NewCount}, MsgBufTail).

dec_queue_length(MsgBuf, Count) ->
    case queue:out(MsgBuf) of
        {{value, {on_disk, Len}}, MsgBuf1} ->
            case Len of
                Count ->
                    MsgBuf1;
                _ when Len > Count ->
                    queue:in_r({on_disk, Len-Count}, MsgBuf1)
            end;
        _ -> MsgBuf
    end.

maybe_prefetch(State = #mqstate { prefetcher = undefined,
                                  mode = mixed,
                                  msg_buf = MsgBuf,
                                  queue = Q }) ->
    case queue:peek(MsgBuf) of
        {value, {on_disk, Count}} ->
            %% only prefetch for the next contiguous block on
            %% disk. Beyond there, we either hit the end of the queue,
            %% or the next msg is already in RAM, held by us, the
            %% mixed queue
            {ok, Prefetcher} = rabbit_queue_prefetcher:start_link(Q, Count),
            State #mqstate { prefetcher = Prefetcher };
        _ -> State
    end;
maybe_prefetch(State) ->
    State.

maybe_ack(_Q, true, true, AckTag) ->
    AckTag;
maybe_ack(Q, _, _, AckTag) ->
    ok = rabbit_disk_queue:ack(Q, [AckTag]),
    not_on_disk.

remove_diskless(MsgsWithAcks) ->
    lists:foldl(
      fun ({Msg, AckTag}, {AccAckTags, AccSize}) ->
              Msg1 = ensure_binary_properties(Msg),
              {case AckTag of
                   not_on_disk -> AccAckTags;
                   _ -> [AckTag | AccAckTags]
               end, size_of_message(Msg1) + AccSize}
      end, {[], 0}, MsgsWithAcks).

on_disk(disk, _IsDurable, _IsPersistent)  -> true;
on_disk(mixed, true, true)                -> true;
on_disk(mixed, _IsDurable, _IsPersistent) -> false.

publish_magic_marker_message(Q) ->
    Msg = rabbit_basic:message(
            rabbit_misc:r(<<"/">>, exchange, <<>>), ?MAGIC_MARKER,
            [], <<>>, <<>>, true),
    ok = rabbit_disk_queue:publish(Q, ensure_binary_properties(Msg), false).

fetch_ack_magic_marker_message(Q) ->
    {Msg, false, AckTag, Length} = rabbit_disk_queue:fetch(Q),
    true = is_magic_marker_message(Msg),
    ok = rabbit_disk_queue:ack(Q, [AckTag]),
    {ok, Length}.

is_magic_marker_message(#basic_message { routing_key = ?MAGIC_MARKER,
                                         is_persistent = true, guid = <<>> }) ->
    true;
is_magic_marker_message(_) ->
    false.
