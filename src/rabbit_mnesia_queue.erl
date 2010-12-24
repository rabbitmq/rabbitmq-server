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

-module(rabbit_mnesia_queue).

-export([start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
         publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4,
	 tx_ack/3, tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1,
	 dropwhile/2, set_ram_duration_target/2, ram_duration/1,
	 needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1,
	 status/1]).

%%----------------------------------------------------------------------------
%% This is Take Three of a simple initial Mnesia implementation of the
%% rabbit_backing_queue behavior. This version was created by starting
%% with rabbit_variable_queue.erl, and removing everything unneeded.
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------

%% In the queue we keep track of both messages that are pending
%% delivery and messages that are pending acks. This ensures that
%% purging (deleting the former) and deletion (deleting the former and
%% the latter) are both cheap and do require any scanning through q
%% segments.
%%
%% Pending acks are recorded in memory as the message itself.
%%
%% All queues are durable in this version, no matter how they are
%% requested. (We may need to remember the requested type in the
%% future, to catch accidental redeclares.) All messages are transient
%% (non-persistent) in this interim version, in order to rip aout all
%% of the old backing code before insetring the new backing
%% code. (This breaks some tests, since all messages are temporarily
%% dropped on restart.)
%%
%% May need to add code to throw away transient messages upon
%% initialization, depending on storage strategy.
%%
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(state,
	{ q,
	  next_seq_id,
	  pending_ack,
	  ack_index,
	  on_sync,
	  len,
	  out_counter,
	  in_counter,
	  unconfirmed,
	  ack_out_counter,
	  ack_in_counter
	}).

-record(msg_status,
	{ seq_id,
	  msg,
	  is_delivered,
	  msg_props
	}).

-record(tx, { pending_messages, pending_acks }).

-record(sync, { acks, pubs, funs }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id() | 'blank_ack').

-type(sync() :: #sync { acks :: [[seq_id()]],
			pubs :: [{message_properties_transformer(),
				  [rabbit_types:basic_message()]}],
			funs :: [fun (() -> any())] }).

-type(state() :: #state { q :: queue(),
			  next_seq_id :: seq_id(),
			  pending_ack :: dict(),
			  ack_index :: gb_tree(),
			  on_sync :: sync(),
			  len :: non_neg_integer(),
			  out_counter :: non_neg_integer(),
			  in_counter :: non_neg_integer(),
			  unconfirmed :: gb_set(),
			  ack_out_counter :: non_neg_integer(),
			  ack_in_counter :: non_neg_integer() }).

-include("rabbit_backing_queue_spec.hrl").

-endif.

-define(BLANK_SYNC, #sync { acks = [],
			    pubs = [],
			    funs = [] }).

%%----------------------------------------------------------------------------
%% Public API
%%
%% Specs are in rabbit_backing_queue_spec.hrl but are repeated here.

%%----------------------------------------------------------------------------
%% start/1 is called on startup with a list of (durable) queue
%% names. The queues aren't being started at this point, but this call
%% allows the backing queue to perform any checking necessary for the
%% consistency of those queues, or initialise any other shared
%% resources.

%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(_DurableQueues) -> ok.

%%----------------------------------------------------------------------------
%% stop/0 is called to tear down any state/resources. NB:
%% Implementations should not depend on this function being called on
%% shutdown and instead should hook into the rabbit supervision
%% hierarchy.

%% -spec(stop/0 :: () -> 'ok').

stop() -> ok.

%%----------------------------------------------------------------------------
%% init/3 initializes one backing queue and its state.

%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).

%% Should do quite a bit more upon recovery....

init(_QueueName, _IsDurable, _Recover) ->
    a(#state { q = queue:new(),
	       next_seq_id = 0,
	       pending_ack = dict:new(),
	       ack_index = gb_trees:empty(),
	       on_sync = ?BLANK_SYNC,
	       len = 0,
	       out_counter = 0,
	       in_counter = 0,
	       unconfirmed = gb_sets:new(),
	       ack_out_counter = 0,
	       ack_in_counter = 0 }).

%%----------------------------------------------------------------------------
%% terminate/1 is called on queue shutdown when the queue isn't being
%% deleted.

%% -spec(terminate/1 :: (state()) -> state()).

terminate(State) -> a(remove_pending_ack(tx_commit_index(State))).

%%----------------------------------------------------------------------------
%% delete_and_terminate/1 is called when the queue is terminating and
%% needs to delete all its content. The only difference between purge
%% and delete is that delete also needs to delete everything that's
%% been delivered and not ack'd.

%% -spec(delete_and_terminate/1 :: (state()) -> state()).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.

delete_and_terminate(State) ->
    {_PurgeCount, State1} = purge(State),
    a(remove_pending_ack(State1)).

%%----------------------------------------------------------------------------
%% purge/1 removes all messages in the queue, but not messages which
%% have been fetched and are pending acks.

%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State = #state { len = Len }) ->
    {Len, a(State #state { q = queue:new(), len = 0 })}.

%%----------------------------------------------------------------------------
%% publish/3 publishes a message.

%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, MsgProps, State) ->
    {_SeqId, State1} = publish(Msg, MsgProps, false, false, State),
    a(State1).

%%----------------------------------------------------------------------------
%% publish_delivered/4 is called for messages which have already been
%% passed straight out to a client. The queue will be empty for these
%% calls (i.e. saves the round trip through the backing queue).

%% -spec(publish_delivered/4 ::
%%         (ack_required(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> {ack(), state()}).

publish_delivered(false, _Msg, _MsgProps, State = #state { len = 0 }) ->
    {blank_ack, a(State)};
publish_delivered(true, Msg = #basic_message { guid = Guid },
		  MsgProps = #message_properties {
		    needs_confirming = NeedsConfirming },
		  State = #state { len = 0,
				   next_seq_id = SeqId,
				   out_counter = OutCount,
				   in_counter = InCount,
				   unconfirmed = Unconfirmed }) ->
    MsgStatus = (msg_status(SeqId, Msg, MsgProps))
	#msg_status { is_delivered = true },
    State1 = record_pending_ack(m(MsgStatus), State),
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, a(State1 #state { next_seq_id = SeqId + 1,
			      out_counter = OutCount + 1,
			      in_counter = InCount + 1,
			      unconfirmed = Unconfirmed1 })}.

%%----------------------------------------------------------------------------
%% dropwhile/2 drops messages from the head of the queue while the
%% supplied predicate returns true.

%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, State) ->
    {_OkOrEmpty, State1} = dropwhile1(Pred, State),
    State1.

dropwhile1(Pred, State) ->
    internal_queue_out(
      fun(MsgStatus = #msg_status { msg_props = MsgProps }, State1) ->
	      case Pred(MsgProps) of
		  true ->
		      {_, State2} = internal_fetch(false, MsgStatus, State1),
		      dropwhile1(Pred, State2);
		  false ->
		      %% message needs to go back into Q
		      #state { q = Q } = State1,
		      {ok, State1 #state {q = queue:in_r(MsgStatus, Q) }}
	      end
      end, State).

%%----------------------------------------------------------------------------
%% fetch/2 produces the next message.

%% -spec(fetch/2 :: (ack_required(), state()) -> {fetch_result(), state()}).

fetch(AckRequired, State) ->
    internal_queue_out(
      fun(MsgStatus, State1) -> internal_fetch(AckRequired, MsgStatus, State1)
      end, State).

internal_queue_out(Fun, State = #state { q = Q }) ->
    case queue:out(Q) of
	{empty, _Q} -> {empty, a(State)};
	{{value, MsgStatus}, Qa} -> Fun(MsgStatus, State #state { q = Qa })
    end.

internal_fetch(AckRequired,
	       MsgStatus = #msg_status { seq_id = SeqId,
					 msg = Msg,
					 is_delivered = IsDelivered },
	       State = #state {out_counter = OutCount, len = Len }) ->
    {AckTag, State1} =
	case AckRequired of
	    true ->
		StateN =
		    record_pending_ack(
		      MsgStatus #msg_status { is_delivered = true },
		      State),
		{SeqId, StateN};
	    false -> {blank_ack, State}
	end,
    Len1 = Len - 1,
    {{Msg, IsDelivered, AckTag, Len1},
     a(State1 #state { out_counter = OutCount + 1,
		       len = Len1 })}.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges messages. Acktags supplied are for messages
%% which can now be forgotten about. Must return 1 guid per Ack, in
%% the same order as Acks.

%% -spec(ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

ack(AckTags, State) ->
    {Guids, State1} =
	ack(fun (#msg_status{msg = #basic_message { guid = Guid }},
		 State1) ->
		    remove_confirms(gb_sets:singleton(Guid), State1)
	    end,
	    AckTags, State),
    {Guids, a(State1)}.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish, but in the context of a transaction.

%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, MsgProps, State) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, MsgProps} | Pubs] }),
    a(State).

%%----------------------------------------------------------------------------
%% tx_ack/3 acks, but in the context of a transaction.

%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, AckTags, State) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }),
    State.

%%----------------------------------------------------------------------------
%% tx_rollback/2 undoes anything which has been done in the context of
%% the specified transaction.

%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, State) ->
    #tx { pending_acks = AckTags } = lookup_tx(Txn),
    erase_tx(Txn),
    {lists:append(AckTags), a(State)}.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits a transaction. The Fun passed in must be called
%% once the messages have really been commited. This CPS permits the
%% possibility of commit coalescing.

%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, Fun, MsgPropsFun, State) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    {AckTags1, a(tx_commit_post_msg_store(Pubs, AckTags1, Fun, MsgPropsFun,
					  State))}.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts messages into the queue which have already been
%% delivered and were pending acknowledgement.

%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(AckTags, MsgPropsFun, State) ->
    {_Guids, State1} =
	ack(fun (#msg_status { msg = Msg, msg_props = MsgProps }, State1) ->
		    {_SeqId, State2} =
			publish(
			  Msg, MsgPropsFun(MsgProps), true, false, State1),
		    State2
	    end,
	    AckTags, State),
    a(State1).

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.

%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#state { len = Len }) -> Len.

%%----------------------------------------------------------------------------
%% is_empty/1 returns 'true' if the queue is empty, and 'false'
%% otherwise.

%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(State) -> 0 == len(State).

%%----------------------------------------------------------------------------
%% For the next two functions, the assumption is that you're
%% monitoring something like the ingress and egress rates of the
%% queue. The RAM duration is thus the length of time represented by
%% the messages held in RAM given the current rates. If you want to
%% ignore all of this stuff, then do so, and return 0 in
%% ram_duration/1.

%% set_ram_duration_target states that the target is to have no more
%% messages in RAM than indicated by the duration and the current
%% queue rates.

%% -spec(set_ram_duration_target/2 ::
%%         (('undefined' | 'infinity' | number()), state())
%%         -> state()).

set_ram_duration_target(_DurationTarget, State) -> State.

%%----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the messages in RAM represent given the current rates of
%% the queue.

%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(State) -> {0, State}.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns 'true' if 'idle_timeout' should be
%% called as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and 'false' otherwise.

%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(_State = #state { on_sync = ?BLANK_SYNC }) ->
    false;
needs_idle_timeout(_State) ->
    true.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns 'true' if 'idle_timeout' should be
%% called as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and 'false' otherwise.

%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

idle_timeout(State) -> a(tx_commit_index(State)).

%%----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates.

%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(State) -> State.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging purposes, to be able to expose state
%% via rabbitmqctl list_queues backing_queue_status

%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#state { q = Q,
		len = Len,
		pending_ack = PA,
		ack_index = AI,
		on_sync = #sync { funs = From },
		next_seq_id = NextSeqId }) ->
    [{q, queue:len(Q)},
     {len, Len},
     {pending_acks, dict:size(PA)},
     {outstanding_txns, length(From)},
     {ack_count, gb_trees:size(AI)},
     {next_seq_id, NextSeqId}].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

a(State = #state { q = Q, len = Len }) ->
    E4 = queue:is_empty(Q),
    LZ = Len == 0,
    true = LZ == E4,
    true = Len >= 0,
    State.

m(MsgStatus) -> MsgStatus.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
%% when requeueing, we re-add a guid to the unconfirmed set
gb_sets_maybe_insert(true, Val, Set) -> gb_sets:add(Val, Set).

msg_status(SeqId, Msg, MsgProps) ->
    #msg_status { seq_id = SeqId, msg = Msg, is_delivered = false,
		  msg_props = MsgProps }.

lookup_tx(Txn) -> case get({txn, Txn}) of
		      undefined ->
			  #tx { pending_messages = [], pending_acks = [] };
		      V -> V
		  end.

store_tx(Txn, Tx) -> put({txn, Txn}, Tx).

erase_tx(Txn) -> erase({txn, Txn}).

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

tx_commit_post_msg_store(Pubs, AckTags, Fun, MsgPropsFun,
			 State = #state { on_sync = OnSync }) ->
    State1 = tx_commit_index(
	       State #state { on_sync = #sync { acks = [AckTags],
						pubs = [{MsgPropsFun, Pubs}],
						funs = [Fun] } }),
    State1 #state { on_sync = OnSync }.

tx_commit_index(State = #state { on_sync = ?BLANK_SYNC }) -> State;
tx_commit_index(State = #state { on_sync = #sync { acks = SAcks,
						   pubs = SPubs,
						   funs = SFuns } }) ->
    Acks = lists:append(SAcks),
    {_Guids, NewState} = ack(Acks, State),
    Pubs = [{Msg, Fun(MsgProps)} ||
	       {Fun, PubsN} <- lists:reverse(SPubs),
	       {Msg, MsgProps} <- lists:reverse(PubsN)],
    {_SeqIds, State1} =
	lists:foldl(
	  fun ({Msg, MsgProps}, {SeqIdsAcc, State2}) ->
		  {_SeqId, State3} =
		      publish(Msg, MsgProps, false, false, State2),
		  {SeqIdsAcc, State3}
	  end,
	  {[], NewState},
	  Pubs),
    [ Fun() || Fun <- lists:reverse(SFuns) ],
    State1 #state { on_sync = ?BLANK_SYNC }.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

publish(Msg = #basic_message { guid = Guid },
	MsgProps = #message_properties { needs_confirming = NeedsConfirming },
	IsDelivered, _MsgOnDisk,
	State = #state { q = Q,
			 next_seq_id = SeqId,
			 len = Len,
			 in_counter = InCount,
			 unconfirmed = Unconfirmed }) ->
    MsgStatus = (msg_status(SeqId, Msg, MsgProps))
	#msg_status { is_delivered = IsDelivered },
    State1 = State #state { q = queue:in(m(MsgStatus), Q) },
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, State1 #state { next_seq_id = SeqId + 1,
			    len = Len + 1,
			    in_counter = InCount + 1,
			    unconfirmed = Unconfirmed1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(MsgStatus = #msg_status { seq_id = SeqId,
					     msg = #basic_message {
					       guid = Guid }},
		   State = #state { pending_ack = PA,
				    ack_index = AI,
				    ack_in_counter = AckInCount}) ->
    {AckEntry, AI1} = {MsgStatus, gb_trees:insert(SeqId, Guid, AI)},
    PA1 = dict:store(SeqId, AckEntry, PA),
    State #state { pending_ack = PA1,
		   ack_index = AI1,
		   ack_in_counter = AckInCount + 1}.

remove_pending_ack(State = #state { pending_ack = PA }) ->
    _ = dict:fold(fun accumulate_ack/3, accumulate_ack_init(), PA),
    a(State #state { pending_ack = dict:new(),
		     ack_index = gb_trees:empty() }).

ack(_Fun, [], State) -> {[], State};
ack(Fun, AckTags, State) ->
    {{_PersistentSeqIds, _GuidsByStore, AllGuids},
     State1 = #state { ack_out_counter = AckOutCount }} =
	lists:foldl(
	  fun (SeqId,
	       {Acc,
		State2 = #state { pending_ack = PA, ack_index = AI }}) ->
		  AckEntry = dict:fetch(SeqId, PA),
		  {accumulate_ack(SeqId, AckEntry, Acc),
		   Fun(AckEntry,
		       State2 #state {
			 pending_ack = dict:erase(SeqId, PA),
			 ack_index = gb_trees:delete_any(SeqId, AI)})}
	  end,
	  {accumulate_ack_init(), State},
	  AckTags),
    {lists:reverse(AllGuids),
     State1 #state { ack_out_counter = AckOutCount + length(AckTags) }}.

accumulate_ack_init() -> {[], orddict:new(), []}.

accumulate_ack(_SeqId, #msg_status { msg = #basic_message { guid = Guid }},
	       {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {PersistentSeqIdsAcc, GuidsByStore, [Guid | AllGuids]}.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

remove_confirms(GuidSet, State = #state { unconfirmed = UC }) ->
    State #state { unconfirmed = gb_sets:difference(UC, GuidSet) }.

