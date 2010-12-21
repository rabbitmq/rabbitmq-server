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
	 publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
	 tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
	 set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
	 idle_timeout/1, handle_pre_hibernate/1, status/1]).

%% exported for testing only
-export([start_msg_store/2, stop_msg_store/0, init/4]).

%%----------------------------------------------------------------------------
%% This is Take Three of a simple initial Mnesia implementation of the
%% rabbit_backing_queue behavior. This version was created by starting
%% with rabbit_variable_queue.erl, and removing everything unneeded.
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% Definitions:

%% alpha: this is a message where both the message itself, and its
%% position within the queue are held in RAM
%%
%% beta: this is a message where the message itself is only held on
%% disk, but its position within the queue is held in RAM.
%%
%% gamma: this is a message where the message itself is only held on
%% disk, but its position is both in RAM and on disk.
%%
%% delta: this is a collection of messages, represented by a single
%% term, where the messages and their position are only held on
%% disk.
%%
%% Note that for persistent messages, the message and its position
%% within the queue are always held on disk, *in addition* to being in
%% one of the above classifications.
%%
%% Also note that within this code, the term gamma never
%% appears. Instead, gammas are defined by betas who have had their
%% queue position recorded on disk.
%%
%% In general, messages move q1 -> delta -> q3 -> q4, though many of
%% these steps are frequently skipped. q1 and q4 only hold alphas, q3
%% holds both betas and gammas (as queues of queues, using the bpqueue
%% module where the block prefix determines whether they're betas or
%% gammas). When a message arrives, its classification is
%% determined. It is then added to the rightmost appropriate queue.
%%
%% If a new message is determined to be a beta or gamma, q1 is
%% empty. If a new message is determined to be a delta, q1 is empty
%% (and actually q4 too).
%%
%% When removing messages from a queue, if q4 is empty then q3 is read
%% directly. If q3 becomes empty then the next segment's worth of
%% messages from delta are read into q3, reducing the size of
%% delta. If the queue is non empty, either q4 or q3 contain
%% entries. It is never permitted for delta to hold all the messages
%% in the queue.
%%
%% Whilst messages are pushed to disk and forgotten from RAM as soon
%% as requested by a new setting of the queue RAM duration, the
%% inverse is not true: we only load messages back into RAM as
%% demanded as the queue is read from. Thus only publishes to the
%% queue will take up available spare capacity.
%%
%% If a queue is full of transient messages, then the transition from
%% betas to deltas will be potentially very expensive as millions of
%% entries must be written to disk by the queue_index module. This can
%% badly stall the queue. In order to avoid this, the proportion of
%% gammas / (betas+gammas) must not be lower than (betas+gammas) /
%% (alphas+betas+gammas). As the queue grows or available memory
%% shrinks, the latter ratio increases, requiring the conversion of
%% more gammas to betas in order to maintain the invariant. At the
%% point at which betas and gammas must be converted to deltas, there
%% should be very few betas remaining, thus the transition is fast (no
%% work needs to be done for the gamma -> delta transition).
%%
%% The conversion of betas to gammas is done in batches of exactly
%% ?IO_BATCH_SIZE. This value should not be too small, otherwise the
%% frequent operations on q3 will not be effectively amortised
%% (switching the direction of queue access defeats amortisation), nor
%% should it be too big, otherwise converting a batch stalls the queue
%% for too long. Therefore, it must be just right. ram_index_count is
%% used here and is the number of betas.
%%
%% The conversion from alphas to betas is also chunked, but only to
%% ensure no more than ?IO_BATCH_SIZE alphas are converted to betas at
%% any one time. This further ensures the queue remains responsive
%% even when there is a large amount of IO work to do. The
%% idle_timeout callback is utilised to ensure that conversions are
%% done as promptly as possible whilst ensuring the queue remains
%% responsive.
%%
%% In the queue we keep track of both messages that are pending
%% delivery and messages that are pending acks. This ensures that
%% purging (deleting the former) and deletion (deleting the former and
%% the latter) are both cheap and do require any scanning through qi
%% segments.
%%
%% Pending acks are recorded in memory either as the tuple {SeqId,
%% Guid, MsgProps} (tuple-form) or as the message itself (message-
%% form). Acks for persistent messages are always stored in the tuple-
%% form. Acks for transient messages are also stored in tuple-form if
%% the message has been sent to disk as part of the memory reduction
%% process. For transient messages that haven't already been written
%% to disk, acks are stored in message-form.
%%
%% During memory reduction, acks stored in message-form are converted
%% to tuple-form, and the corresponding messages are pushed out to
%% disk.
%%
%% Notes on Clean Shutdown
%% (This documents behaviour in variable_queue, queue_index and
%% msg_store.)
%%
%% In order to try to achieve as fast a start-up as possible, if a
%% clean shutdown occurs, we try to save out state to disk to reduce
%% work on startup. In the msg_store this takes the form of the
%% index_module's state, plus the file_summary ets table, and client
%% refs. In the VQ, this takes the form of the count of persistent
%% messages in the queue and references into the msg_stores. The
%% queue_index adds to these terms the details of its segments and
%% stores the terms in the queue directory.
%%
%% All queues are durable in this version.
%%
%% Two message stores are used. One is created for persistent messages
%% that must survive restarts, and the other is used for all other
%% messages that just happen to need to be written to disk. On start
%% up we can therefore nuke the transient message store, and be sure
%% that the messages in the persistent store are all that we need.
%%
%% The references to the msg_stores are there so that the msg_store
%% knows to only trust its saved state if all of the queues it was
%% previously talking to come up cleanly. Likewise, the queues
%% themselves (esp queue_index) skips work in init if all the queues
%% and msg_store were shutdown cleanly. This gives both good speed
%% improvements and also robustness so that if anything possibly went
%% wrong in shutdown (or there was subsequent manual tampering), all
%% messages and queues that can be recovered are recovered, safely.
%%
%% May need to add code to throw away transient messages upon
%% initialization, depending on storage strategy.
%%
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(mqstate,
	{ q1,
	  delta,
	  q3,
	  q4,
	  next_seq_id,
	  pending_ack,
	  pending_ack_index,
	  ram_ack_index,
	  index_state,
	  msg_store_clients,
	  on_sync,

	  len,
	  persistent_count,

	  ram_msg_count,
	  ram_msg_count_prev,
	  ram_ack_count_prev,
	  ram_index_count,
	  out_counter,
	  in_counter,
	  msgs_on_disk,
	  msg_indices_on_disk,
	  unconfirmed,
	  ack_out_counter,
	  ack_in_counter
	}).

-record(msg_status,
	{ seq_id,
	  guid,
	  msg,
	  is_persistent,
	  is_delivered,
	  msg_on_disk,
	  index_on_disk,
	  msg_props
	}).

-record(delta,
	{ start_seq_id, %% start_seq_id is inclusive
	  count,
	  end_seq_id %% end_seq_id is exclusive
	}).

-record(tx, { pending_messages, pending_acks }).

-record(sync, { acks_persistent, acks_all, pubs, funs }).

%% When we discover, on publish, that we should write some indices to
%% disk for some betas, the IO_BATCH_SIZE sets the number of betas
%% that we must be due to write indices for before we do any work at
%% all. This is both a minimum and a maximum - we don't write fewer
%% than RAM_INDEX_BATCH_SIZE indices out in one go, and we don't write
%% more - we can always come back on the next publish to do more.
-define(IO_BATCH_SIZE, 64).
-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE, msg_store_transient).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id() | 'blank_ack').

-type(delta() :: #delta { start_seq_id :: non_neg_integer(),
			  count :: non_neg_integer(),
			  end_seq_id :: non_neg_integer() }).

-type(sync() :: #sync { acks_persistent :: [[seq_id()]],
			acks_all :: [[seq_id()]],
			pubs :: [{message_properties_transformer(),
				  [rabbit_types:basic_message()]}],
			funs :: [fun (() -> any())] }).

-type(state() :: #mqstate {
	     q1 :: queue(),
	     delta :: delta(),
	     q3 :: bpqueue:bpqueue(),
	     q4 :: queue(),
	     next_seq_id :: seq_id(),
	     pending_ack :: dict(),
	     ram_ack_index :: gb_tree(),
	     index_state :: any(),
	     msg_store_clients :: 'undefined' | {{any(), binary()},
						 {any(), binary()}},
	     on_sync :: sync(),

	     len :: non_neg_integer(),
	     persistent_count :: non_neg_integer(),

	     ram_msg_count :: non_neg_integer(),
	     ram_msg_count_prev :: non_neg_integer(),
	     ram_index_count :: non_neg_integer(),
	     out_counter :: non_neg_integer(),
	     in_counter :: non_neg_integer(),
	     msgs_on_disk :: gb_set(),
	     msg_indices_on_disk :: gb_set(),
	     unconfirmed :: gb_set(),
	     ack_out_counter :: non_neg_integer(),
	     ack_in_counter :: non_neg_integer() }).

-include("rabbit_backing_queue_spec.hrl").

-endif.

-define(BLANK_DELTA, #delta { start_seq_id = undefined,
			      count = 0,
			      end_seq_id = undefined }).
-define(BLANK_DELTA_PATTERN(Z), #delta { start_seq_id = Z,
					 count = 0,
					 end_seq_id = Z }).

-define(BLANK_SYNC, #sync { acks_persistent = [],
			    acks_all = [],
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

start(DurableQueues) ->
    {AllTerms, StartFunState} = rabbit_queue_index:recover(DurableQueues),
    start_msg_store(
      [Ref || Terms <- AllTerms,
	      begin
		  Ref = proplists:get_value(persistent_ref, Terms),
		  Ref =/= undefined
	      end],
      StartFunState).

%%----------------------------------------------------------------------------
%% stop/0 is called to tear down any state/resources. NB:
%% Implementations should not depend on this function being called on
%% shutdown and instead should hook into the rabbit supervision
%% hierarchy.

%% -spec(stop/0 :: () -> 'ok').

stop() -> stop_msg_store().

start_msg_store(Refs, StartFunState) ->
    ok = rabbit_sup:start_child(?TRANSIENT_MSG_STORE, rabbit_msg_store,
				[?TRANSIENT_MSG_STORE, rabbit_mnesia:dir(),
				 undefined, {fun (ok) -> finished end, ok}]),
    ok = rabbit_sup:start_child(?PERSISTENT_MSG_STORE, rabbit_msg_store,
				[?PERSISTENT_MSG_STORE, rabbit_mnesia:dir(),
				 Refs, StartFunState]).

stop_msg_store() ->
    ok = rabbit_sup:stop_child(?PERSISTENT_MSG_STORE),
    ok = rabbit_sup:stop_child(?TRANSIENT_MSG_STORE).

%%----------------------------------------------------------------------------
%% init/3 initializes one backing queue and its state.

%% -spec(init/3 :: (rabbit_amqqueue:name(), is_durable(), attempt_recovery()) ->
%% state()).

init(QueueName, _IsDurable, Recover) ->
    Self = self(),
    init(QueueName, Recover,
	 fun (Guids) -> msgs_written_to_disk(Self, Guids) end,
	 fun (Guids) -> msg_indices_written_to_disk(Self, Guids) end).

init(QueueName, false, MsgOnDiskFun, MsgIdxOnDiskFun) ->
    IndexState = rabbit_queue_index:init(QueueName, MsgIdxOnDiskFun),
    init(IndexState, 0, [],
	 msg_store_client_init(?PERSISTENT_MSG_STORE, MsgOnDiskFun),
	 msg_store_client_init(?TRANSIENT_MSG_STORE, undefined));

init(QueueName, true, MsgOnDiskFun, MsgIdxOnDiskFun) ->
    Terms = rabbit_queue_index:shutdown_terms(QueueName),
    {PRef, TRef, Terms1} =
	case [persistent_ref, transient_ref] -- proplists:get_keys(Terms) of
	    [] -> {proplists:get_value(persistent_ref, Terms),
		   proplists:get_value(transient_ref, Terms),
		   Terms};
	    _ -> {rabbit_guid:guid(), rabbit_guid:guid(), []}
	end,
    PersistentClient = rabbit_msg_store:client_init(?PERSISTENT_MSG_STORE,
						    PRef, MsgOnDiskFun),
    TransientClient = rabbit_msg_store:client_init(?TRANSIENT_MSG_STORE,
						   TRef, undefined),
    {DeltaCount, IndexState} =
	rabbit_queue_index:recover(
	  QueueName, Terms1,
	  rabbit_msg_store:successfully_recovered_state(?PERSISTENT_MSG_STORE),
	  fun (Guid) ->
		  rabbit_msg_store:contains(Guid, PersistentClient)
	  end,
	  MsgIdxOnDiskFun),
    init(IndexState, DeltaCount, Terms1, PersistentClient, TransientClient).

%%----------------------------------------------------------------------------
%% terminate/1 is called on queue shutdown when the queue isn't being
%% deleted.

%% -spec(terminate/1 :: (state()) -> state()).

terminate(State) ->
    State1 = #mqstate { persistent_count = PCount,
			index_state = IndexState,
			msg_store_clients = {MSCStateP, MSCStateT} } =
	remove_pending_ack(true, tx_commit_index(State)),
    PRef = case MSCStateP of
	       undefined -> undefined;
	       _ -> ok = rabbit_msg_store:client_terminate(MSCStateP),
		    rabbit_msg_store:client_ref(MSCStateP)
	   end,
    ok = rabbit_msg_store:client_terminate(MSCStateT),
    TRef = rabbit_msg_store:client_ref(MSCStateT),
    Terms = [{persistent_ref, PRef},
	     {transient_ref, TRef},
	     {persistent_count, PCount}],
    a(State1 #mqstate { index_state = rabbit_queue_index:terminate(
					Terms, IndexState),
			msg_store_clients = undefined }).

%%----------------------------------------------------------------------------
%% delete_and_terminate/1 is called when the queue is terminating and
%% needs to delete all its content. The only difference between purge
%% and delete is that delete also needs to delete everything that's
%% been delivered and not ack'd.

%% -spec(delete_and_terminate/1 :: (state()) -> state()).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.

delete_and_terminate(State) ->
    %% TODO: there is no need to interact with qi at all - which we do
    %% as part of 'purge' and 'remove_pending_ack', other than
    %% deleting it.
    {_PurgeCount, State1} = purge(State),
    State2 = #mqstate { index_state = IndexState,
			msg_store_clients = {MSCStateP, MSCStateT} } =
	remove_pending_ack(false, State1),
    IndexState1 = rabbit_queue_index:delete_and_terminate(IndexState),
    case MSCStateP of
	undefined -> ok;
	_ -> rabbit_msg_store:client_delete_and_terminate(MSCStateP)
    end,
    rabbit_msg_store:client_delete_and_terminate(MSCStateT),
    a(State2 #mqstate { index_state = IndexState1,
			msg_store_clients = undefined }).

%%----------------------------------------------------------------------------
%% purge/1 removes all messages in the queue, but not messages which
%% have been fetched and are pending acks.

%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State = #mqstate { q4 = Q4,
			 index_state = IndexState,
			 msg_store_clients = MSCState,
			 len = Len,
			 persistent_count = PCount }) ->
    %% TODO: when there are no pending acks, which is a common case,
    %% we could simply wipe the qi instead of issuing delivers and
    %% acks for all the messages.
    {LensByStore, IndexState1} = remove_queue_entries(
				   fun rabbit_misc:queue_fold/3, Q4,
				   orddict:new(), IndexState, MSCState),
    {LensByStore1, State1 = #mqstate { q1 = Q1,
				       index_state = IndexState2,
				       msg_store_clients = MSCState1 }} =
	purge_betas_and_deltas(LensByStore,
			       State #mqstate { q4 = queue:new(),
						index_state = IndexState1 }),
    {LensByStore2, IndexState3} = remove_queue_entries(
				    fun rabbit_misc:queue_fold/3, Q1,
				    LensByStore1, IndexState2, MSCState1),
    PCount1 = PCount - find_persistent_count(LensByStore2),
    {Len, a(State1 #mqstate { q1 = queue:new(),
			      index_state = IndexState3,
			      len = 0,
			      ram_msg_count = 0,
			      ram_index_count = 0,
			      persistent_count = PCount1 })}.

%%----------------------------------------------------------------------------
%% publish/3 publishes a message.

%% -spec(publish/3 :: (rabbit_types:basic_message(),
%% rabbit_types:message_properties(), state()) -> state()).

publish(Msg, MsgProps, State) ->
    {_SeqId, State1} = publish(Msg, MsgProps, false, false, State),
    a(State1).

%%----------------------------------------------------------------------------
%% publish_delivered/4 is called for messages which have already been
%% passed straight out to a client. The queue will be empty for these
%% calls (i.e. saves the round trip through the backing queue).

%% -spec(publish_delivered/4 :: (ack_required(), rabbit_types:basic_message(),
%% rabbit_types:message_properties(), state())
%% -> {ack(), state()}).

publish_delivered(false, _Msg, _MsgProps, State = #mqstate { len = 0 }) ->
    {blank_ack, a(State)};
publish_delivered(true, Msg = #basic_message { is_persistent = IsPersistent,
					       guid = Guid },
		  MsgProps = #message_properties {
		    needs_confirming = NeedsConfirming },
		  State = #mqstate { len = 0,
				     next_seq_id = SeqId,
				     out_counter = OutCount,
				     in_counter = InCount,
				     persistent_count = PCount,
				     unconfirmed = Unconfirmed }) ->
    MsgStatus = (msg_status(IsPersistent, SeqId, Msg, MsgProps))
	#msg_status { is_delivered = true },
    {MsgStatus1, State1} = maybe_write_to_disk(false, false, MsgStatus, State),
    State2 = record_pending_ack(m(MsgStatus1), State1),
    PCount1 = PCount + one_if(IsPersistent),
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, a(State2 #mqstate { next_seq_id = SeqId + 1,
				out_counter = OutCount + 1,
				in_counter = InCount + 1,
				persistent_count = PCount1,
				unconfirmed = Unconfirmed1 })}.

%%----------------------------------------------------------------------------
%% dropwhile/2 drops messages from the head of the queue while the
%% supplied predicate returns true.

%% -spec(dropwhile/2 ::
%% (fun ((rabbit_types:message_properties()) -> boolean()), state())
%% -> state()).

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
		      %% message needs to go back into Q4 (or maybe go
		      %% in for the first time if it was loaded from
		      %% Q3). Also the msg contents might not be in
		      %% RAM, so read them in now
		      {MsgStatus1, State2 = #mqstate { q4 = Q4 }} =
			  read_msg(MsgStatus, State1),
		      {ok, State2 #mqstate {q4 = queue:in_r(MsgStatus1, Q4) }}
	      end
      end, State).

%%----------------------------------------------------------------------------
%% fetch/2 produces the next message.

%% -spec(fetch/2 :: (ack_required(), state()) -> {fetch_result(), state()}).

fetch(AckRequired, State) ->
    internal_queue_out(
      fun(MsgStatus, State1) ->
	      %% it's possible that the message wasn't read from disk
	      %% at this point, so read it in.
	      {MsgStatus1, State2} = read_msg(MsgStatus, State1),
	      internal_fetch(AckRequired, MsgStatus1, State2)
      end, State).

internal_queue_out(Fun, State = #mqstate { q4 = Q4 }) ->
    case queue:out(Q4) of
	{empty, _Q4} ->
	    case fetch_from_q3(State) of
		{empty, State1} = Result -> a(State1), Result;
		{loaded, {MsgStatus, State1}} -> Fun(MsgStatus, State1)
	    end;
	{{value, MsgStatus}, Q4a} ->
	    Fun(MsgStatus, State #mqstate { q4 = Q4a })
    end.

read_msg(MsgStatus = #msg_status { msg = undefined,
				   guid = Guid,
				   is_persistent = IsPersistent },
	 State = #mqstate { ram_msg_count = RamMsgCount,
			    msg_store_clients = MSCState}) ->
    {{ok, Msg = #basic_message {}}, MSCState1} =
	msg_store_read(MSCState, IsPersistent, Guid),
    {MsgStatus #msg_status { msg = Msg },
     State #mqstate { ram_msg_count = RamMsgCount + 1,
		      msg_store_clients = MSCState1 }};
read_msg(MsgStatus, State) ->
    {MsgStatus, State}.

internal_fetch(AckRequired, MsgStatus = #msg_status {
			      seq_id = SeqId,
			      guid = Guid,
			      msg = Msg,
			      is_persistent = IsPersistent,
			      is_delivered = IsDelivered,
			      msg_on_disk = MsgOnDisk,
			      index_on_disk = IndexOnDisk },
	       State = #mqstate {ram_msg_count = RamMsgCount,
				 out_counter = OutCount,
				 index_state = IndexState,
				 msg_store_clients = MSCState,
				 len = Len,
				 persistent_count = PCount }) ->
    %% 1. Mark it delivered if necessary
    IndexState1 = maybe_write_delivered(
		    IndexOnDisk andalso not IsDelivered,
		    SeqId, IndexState),

    %% 2. Remove from msg_store and queue index, if necessary
    Rem = fun () ->
		  ok = msg_store_remove(MSCState, IsPersistent, [Guid])
	  end,
    Ack = fun () -> rabbit_queue_index:ack([SeqId], IndexState1) end,
    IndexState2 =
	case {AckRequired, MsgOnDisk, IndexOnDisk, IsPersistent} of
	    {false, true, false, _} -> Rem(), IndexState1;
	    {false, true, true, _} -> Rem(), Ack();
	    { true, true, true, false} -> Ack();
	    _ -> IndexState1
	end,

    %% 3. If an ack is required, add something sensible to PA
    {AckTag, State1} = case AckRequired of
			   true -> StateN = record_pending_ack(
					      MsgStatus #msg_status {
						is_delivered = true }, State),
				   {SeqId, StateN};
			   false -> {blank_ack, State}
		       end,

    PCount1 = PCount - one_if(IsPersistent andalso not AckRequired),
    Len1 = Len - 1,
    RamMsgCount1 = RamMsgCount - one_if(Msg =/= undefined),

    {{Msg, IsDelivered, AckTag, Len1},
     a(State1 #mqstate { ram_msg_count = RamMsgCount1,
			 out_counter = OutCount + 1,
			 index_state = IndexState2,
			 len = Len1,
			 persistent_count = PCount1 })}.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges messages. Acktags supplied are for messages
%% which can now be forgotten about. Must return 1 guid per Ack, in
%% the same order as Acks.

%% -spec(ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

ack(AckTags, State) ->
    {Guids, State1} =
	ack(fun msg_store_remove/3,
	    fun ({_IsPersistent, Guid, _MsgProps}, State1) ->
		    remove_confirms(gb_sets:singleton(Guid), State1);
		(#msg_status{msg = #basic_message { guid = Guid }}, State1) ->
		    remove_confirms(gb_sets:singleton(Guid), State1)
	    end,
	    AckTags, State),
    {Guids, a(State1)}.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish, but in the context of a transaction.

%% -spec(tx_publish/4 :: (rabbit_types:txn(), rabbit_types:basic_message(),
%% rabbit_types:message_properties(), state())
%% -> state()).

tx_publish(Txn, Msg = #basic_message { is_persistent = IsPersistent }, MsgProps,
	   State = #mqstate { msg_store_clients = MSCState }) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, MsgProps} | Pubs] }),
    case IsPersistent of
	true -> MsgStatus = msg_status(true, undefined, Msg, MsgProps),
		#msg_status { msg_on_disk = true } =
		    maybe_write_msg_to_disk(false, MsgStatus, MSCState);
	false -> ok
    end,
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

tx_rollback(Txn, State = #mqstate { msg_store_clients = MSCState }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    ok = msg_store_remove(MSCState, true, persistent_guids(Pubs)),
    {lists:append(AckTags), a(State)}.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits a transaction. The Fun passed in must be called
%% once the messages have really been commited. This CPS permits the
%% possibility of commit coalescing.

%% -spec(tx_commit/4 ::
%% (rabbit_types:txn(), fun (() -> any()),
%% message_properties_transformer(), state()) -> {[ack()], state()}).

tx_commit(Txn, Fun, MsgPropsFun,
	  State = #mqstate { msg_store_clients = MSCState }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    PersistentGuids = persistent_guids(Pubs),
    HasPersistentPubs = PersistentGuids =/= [],
    {AckTags1,
     a(case HasPersistentPubs of
	   true -> ok = msg_store_sync(
			  MSCState, true, PersistentGuids,
			  msg_store_callback(PersistentGuids, Pubs, AckTags1,
					     Fun, MsgPropsFun)),
		   State;
	   false -> tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags1,
					     Fun, MsgPropsFun, State)
       end)}.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts messages into the queue which have already been
%% delivered and were pending acknowledgement.

%% -spec(requeue/3 :: ([ack()], message_properties_transformer(), state())
%% -> state()).

requeue(AckTags, MsgPropsFun, State) ->
    {_Guids, State1} =
	ack(fun msg_store_release/3,
	    fun (#msg_status { msg = Msg, msg_props = MsgProps }, State1) ->
		    {_SeqId, State2} = publish(Msg, MsgPropsFun(MsgProps),
					       true, false, State1),
		    State2;
		({IsPersistent, Guid, MsgProps}, State1) ->
		    #mqstate { msg_store_clients = MSCState } = State1,
		    {{ok, Msg = #basic_message{}}, MSCState1} =
			msg_store_read(MSCState, IsPersistent, Guid),
		    State2 = State1 #mqstate { msg_store_clients = MSCState1 },
		    {_SeqId, State3} = publish(Msg, MsgPropsFun(MsgProps),
					       true, true, State2),
		    State3
	    end,
	    AckTags, State),
    a(State1).

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.

%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#mqstate { len = Len }) -> Len.

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
%% (('undefined' | 'infinity' | number()), state()) -> state()).

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

needs_idle_timeout(_State = #mqstate { on_sync = ?BLANK_SYNC }) ->
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

handle_pre_hibernate(State = #mqstate { index_state = IndexState }) ->
    State #mqstate { index_state = rabbit_queue_index:flush(IndexState) }.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging purposes, to be able to expose state
%% via rabbitmqctl list_queues backing_queue_status

%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#mqstate {
	  q1 = Q1, delta = Delta, q3 = Q3, q4 = Q4,
	  len = Len,
	  pending_ack = PA,
	  ram_ack_index = RAI,
	  on_sync = #sync { funs = From },
	  ram_msg_count = RamMsgCount,
	  ram_index_count = RamIndexCount,
	  next_seq_id = NextSeqId,
	  persistent_count = PersistentCount }) ->
    [ {q1 , queue:len(Q1)},
      {delta , Delta},
      {q3 , bpqueue:len(Q3)},
      {q4 , queue:len(Q4)},
      {len , Len},
      {pending_acks , dict:size(PA)},
      {outstanding_txns , length(From)},
      {ram_msg_count , RamMsgCount},
      {ram_ack_count , gb_trees:size(RAI)},
      {ram_index_count , RamIndexCount},
      {next_seq_id , NextSeqId},
      {persistent_count , PersistentCount} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

a(State = #mqstate { q1 = Q1, delta = Delta, q3 = Q3, q4 = Q4,
		     len = Len,
		     persistent_count = PersistentCount,
		     ram_msg_count = RamMsgCount,
		     ram_index_count = RamIndexCount }) ->
    E1 = queue:is_empty(Q1),
    ED = Delta#delta.count == 0,
    E3 = bpqueue:is_empty(Q3),
    E4 = queue:is_empty(Q4),
    LZ = Len == 0,

    true = E1 or not E3,
    true = ED or not E3,
    true = LZ == (E3 and E4),

    true = Len >= 0,
    true = PersistentCount >= 0,
    true = RamMsgCount >= 0,
    true = RamIndexCount >= 0,

    State.

m(MsgStatus = #msg_status { msg = Msg,
			    is_persistent = IsPersistent,
			    msg_on_disk = MsgOnDisk,
			    index_on_disk = IndexOnDisk }) ->
    true = (not IsPersistent) or IndexOnDisk,
    true = (not IndexOnDisk) or MsgOnDisk,
    true = (Msg =/= undefined) or MsgOnDisk,

    MsgStatus.

one_if(true ) -> 1;
one_if(false) -> 0.

cons_if(true, E, L) -> [E | L];
cons_if(false, _E, L) -> L.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
%% when requeueing, we re-add a guid to the unconfirmed set
gb_sets_maybe_insert(true, Val, Set) -> gb_sets:add(Val, Set).

msg_status(IsPersistent, SeqId, Msg = #basic_message { guid = Guid },
	   MsgProps) ->
    #msg_status { seq_id = SeqId, guid = Guid, msg = Msg,
		  is_persistent = IsPersistent, is_delivered = false,
		  msg_on_disk = false, index_on_disk = false,
		  msg_props = MsgProps }.

with_msg_store_state({MSCStateP, MSCStateT}, true, Fun) ->
    {Result, MSCStateP1} = Fun(MSCStateP),
    {Result, {MSCStateP1, MSCStateT}};
with_msg_store_state({MSCStateP, MSCStateT}, false, Fun) ->
    {Result, MSCStateT1} = Fun(MSCStateT),
    {Result, {MSCStateP, MSCStateT1}}.

with_immutable_msg_store_state(MSCState, IsPersistent, Fun) ->
    {Res, MSCState} = with_msg_store_state(MSCState, IsPersistent,
					   fun (MSCState1) ->
						   {Fun(MSCState1), MSCState1}
					   end),
    Res.

msg_store_client_init(MsgStore, MsgOnDiskFun) ->
    rabbit_msg_store:client_init(MsgStore, rabbit_guid:guid(), MsgOnDiskFun).

msg_store_write(MSCState, IsPersistent, Guid, Msg) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:write(Guid, Msg, MSCState1) end).

msg_store_read(MSCState, IsPersistent, Guid) ->
    with_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:read(Guid, MSCState1) end).

msg_store_remove(MSCState, IsPersistent, Guids) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MCSState1) -> rabbit_msg_store:remove(Guids, MCSState1) end).

msg_store_release(MSCState, IsPersistent, Guids) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MCSState1) -> rabbit_msg_store:release(Guids, MCSState1) end).

msg_store_sync(MSCState, IsPersistent, Guids, Callback) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:sync(Guids, Callback, MSCState1) end).

maybe_write_delivered(false, _SeqId, IndexState) ->
    IndexState;
maybe_write_delivered(true, SeqId, IndexState) ->
    rabbit_queue_index:deliver([SeqId], IndexState).

lookup_tx(Txn) -> case get({txn, Txn}) of
		      undefined -> #tx { pending_messages = [],
					 pending_acks = [] };
		      V -> V
		  end.

store_tx(Txn, Tx) -> put({txn, Txn}, Tx).

erase_tx(Txn) -> erase({txn, Txn}).

persistent_guids(Pubs) ->
    [Guid || {#basic_message { guid = Guid,
			       is_persistent = true }, _MsgProps} <- Pubs].

betas_from_index_entries(List, IndexState) ->
    {Filtered, Delivers, Acks} =
	lists:foldr(
	  fun ({Guid, SeqId, MsgProps, IsPersistent, IsDelivered},
	       {Filtered1, Delivers1, Acks1}) ->
		  case not IsPersistent of
		      true -> {Filtered1,
			       cons_if(not IsDelivered, SeqId, Delivers1),
			       [SeqId | Acks1]};
		      false -> {[m(#msg_status { msg = undefined,
						 guid = Guid,
						 seq_id = SeqId,
						 is_persistent = IsPersistent,
						 is_delivered = IsDelivered,
						 msg_on_disk = true,
						 index_on_disk = true,
						 msg_props = MsgProps
					       }) | Filtered1],
				Delivers1,
				Acks1}
		  end
	  end, {[], [], []}, List),
    {bpqueue:from_list([{true, Filtered}]),
     rabbit_queue_index:ack(Acks,
			    rabbit_queue_index:deliver(Delivers, IndexState))}.

beta_fold(Fun, Init, Q) ->
    bpqueue:foldr(fun (_Prefix, Value, Acc) -> Fun(Value, Acc) end, Init, Q).

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

init(IndexState, DeltaCount, Terms, PersistentClient, TransientClient) ->
    {LowSeqId, NextSeqId, IndexState1} = rabbit_queue_index:bounds(IndexState),

    DeltaCount1 = proplists:get_value(persistent_count, Terms, DeltaCount),
    Delta = case DeltaCount1 == 0 andalso DeltaCount /= undefined of
		true -> ?BLANK_DELTA;
		false -> #delta { start_seq_id = LowSeqId,
				  count = DeltaCount1,
				  end_seq_id = NextSeqId }
	    end,
    State = #mqstate {
      q1 = queue:new(),
      delta = Delta,
      q3 = bpqueue:new(),
      q4 = queue:new(),
      next_seq_id = NextSeqId,
      pending_ack = dict:new(),
      ram_ack_index = gb_trees:empty(),
      index_state = IndexState1,
      msg_store_clients = {PersistentClient, TransientClient},
      on_sync = ?BLANK_SYNC,

      len = DeltaCount1,
      persistent_count = DeltaCount1,

      ram_msg_count = 0,
      ram_msg_count_prev = 0,
      ram_ack_count_prev = 0,
      ram_index_count = 0,
      out_counter = 0,
      in_counter = 0,
      msgs_on_disk = gb_sets:new(),
      msg_indices_on_disk = gb_sets:new(),
      unconfirmed = gb_sets:new(),
      ack_out_counter = 0,
      ack_in_counter = 0 },
    a(maybe_deltas_to_betas(State)).

msg_store_callback(PersistentGuids, Pubs, AckTags, Fun, MsgPropsFun) ->
    Self = self(),
    F = fun () -> rabbit_amqqueue:maybe_run_queue_via_backing_queue(
		    Self, fun (StateN) -> {[], tx_commit_post_msg_store(
						 true, Pubs, AckTags,
						 Fun, MsgPropsFun, StateN)}
			  end)
	end,
    fun () -> spawn(fun () -> ok = rabbit_misc:with_exit_handler(
				     fun () -> remove_persistent_messages(
						 PersistentGuids)
				     end, F)
		    end)
    end.

remove_persistent_messages(Guids) ->
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, undefined),
    ok = rabbit_msg_store:remove(Guids, PersistentClient),
    rabbit_msg_store:client_delete_and_terminate(PersistentClient).

tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags, Fun, MsgPropsFun,
			 State = #mqstate {
			   on_sync = OnSync = #sync {
				       acks_persistent = SPAcks,
				       acks_all = SAcks,
				       pubs = SPubs,
				       funs = SFuns },
			   pending_ack = PA }) ->
    PersistentAcks =
	[AckTag || AckTag <- AckTags,
		   case dict:fetch(AckTag, PA) of
		       #msg_status {} -> false;
		       {IsPersistent, _Guid, _MsgProps} -> IsPersistent
		   end],
    case (HasPersistentPubs orelse PersistentAcks =/= []) of
	true -> State #mqstate {
		  on_sync = #sync {
		    acks_persistent = [PersistentAcks | SPAcks],
		    acks_all = [AckTags | SAcks],
		    pubs = [{MsgPropsFun, Pubs} | SPubs],
		    funs = [Fun | SFuns] }};
	false -> State1 = tx_commit_index(
			    State #mqstate {
			      on_sync = #sync {
				acks_persistent = [],
				acks_all = [AckTags],
				pubs = [{MsgPropsFun, Pubs}],
				funs = [Fun] } }),
		 State1 #mqstate { on_sync = OnSync }
    end.

tx_commit_index(State = #mqstate { on_sync = ?BLANK_SYNC }) ->
    State;
tx_commit_index(State = #mqstate { on_sync = #sync {
				     acks_persistent = SPAcks,
				     acks_all = SAcks,
				     pubs = SPubs,
				     funs = SFuns } }) ->
    PAcks = lists:append(SPAcks),
    Acks = lists:append(SAcks),
    {_Guids, NewState} = ack(Acks, State),
    Pubs = [{Msg, Fun(MsgProps)} || {Fun, PubsN} <- lists:reverse(SPubs),
				    {Msg, MsgProps} <- lists:reverse(PubsN)],
    {SeqIds, State1 = #mqstate { index_state = IndexState }} =
	lists:foldl(
	  fun ({Msg = #basic_message { is_persistent = IsPersistent },
		MsgProps},
	       {SeqIdsAcc, State2}) ->
		  {SeqId, State3} =
		      publish(Msg, MsgProps, false, IsPersistent, State2),
		  {cons_if(IsPersistent, SeqId, SeqIdsAcc), State3}
	  end, {PAcks, NewState}, Pubs),
    IndexState1 = rabbit_queue_index:sync(SeqIds, IndexState),
    [ Fun() || Fun <- lists:reverse(SFuns) ],
    State1 #mqstate { index_state = IndexState1, on_sync = ?BLANK_SYNC }.

purge_betas_and_deltas(LensByStore,
		       State = #mqstate { q3 = Q3,
					  index_state = IndexState,
					  msg_store_clients = MSCState }) ->
    case bpqueue:is_empty(Q3) of
	true -> {LensByStore, State};
	false -> {LensByStore1, IndexState1} =
		     remove_queue_entries(fun beta_fold/3, Q3,
					  LensByStore, IndexState, MSCState),
		 purge_betas_and_deltas(LensByStore1,
					maybe_deltas_to_betas(
					  State #mqstate {
					    q3 = bpqueue:new(),
					    index_state = IndexState1 }))
    end.

remove_queue_entries(Fold, Q, LensByStore, IndexState, MSCState) ->
    {GuidsByStore, Delivers, Acks} =
	Fold(fun remove_queue_entries1/2, {orddict:new(), [], []}, Q),
    ok = orddict:fold(fun (IsPersistent, Guids, ok) ->
			      msg_store_remove(MSCState, IsPersistent, Guids)
		      end, ok, GuidsByStore),
    {sum_guids_by_store_to_len(LensByStore, GuidsByStore),
     rabbit_queue_index:ack(Acks,
			    rabbit_queue_index:deliver(Delivers, IndexState))}.

remove_queue_entries1(
  #msg_status { guid = Guid, seq_id = SeqId,
		is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
		index_on_disk = IndexOnDisk, is_persistent = IsPersistent },
  {GuidsByStore, Delivers, Acks}) ->
    {case MsgOnDisk of
	 true -> rabbit_misc:orddict_cons(IsPersistent, Guid, GuidsByStore);
	 false -> GuidsByStore
     end,
     cons_if(IndexOnDisk andalso not IsDelivered, SeqId, Delivers),
     cons_if(IndexOnDisk, SeqId, Acks)}.

sum_guids_by_store_to_len(LensByStore, GuidsByStore) ->
    orddict:fold(
      fun (IsPersistent, Guids, LensByStore1) ->
	      orddict:update_counter(IsPersistent, length(Guids), LensByStore1)
      end, LensByStore, GuidsByStore).

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

publish(Msg = #basic_message { is_persistent = IsPersistent, guid = Guid },
	MsgProps = #message_properties { needs_confirming = NeedsConfirming },
	IsDelivered, MsgOnDisk,
	State = #mqstate { q1 = Q1, q3 = Q3, q4 = Q4,
			   next_seq_id = SeqId,
			   len = Len,
			   in_counter = InCount,
			   persistent_count = PCount,
			   ram_msg_count = RamMsgCount,
			   unconfirmed = Unconfirmed }) ->
    MsgStatus = (msg_status(IsPersistent, SeqId, Msg, MsgProps))
	#msg_status { is_delivered = IsDelivered, msg_on_disk = MsgOnDisk},
    {MsgStatus1, State1} = maybe_write_to_disk(false, false, MsgStatus, State),
    State2 = case bpqueue:is_empty(Q3) of
		 false -> State1 #mqstate { q1 = queue:in(m(MsgStatus1), Q1) };
		 true -> State1 #mqstate { q4 = queue:in(m(MsgStatus1), Q4) }
	     end,
    PCount1 = PCount + one_if(IsPersistent),
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, State2 #mqstate { next_seq_id = SeqId + 1,
			      len = Len + 1,
			      in_counter = InCount + 1,
			      persistent_count = PCount1,
			      ram_msg_count = RamMsgCount + 1,
			      unconfirmed = Unconfirmed1 }}.

maybe_write_msg_to_disk(_Force, MsgStatus = #msg_status {
				  msg_on_disk = true }, _MSCState) ->
    MsgStatus;
maybe_write_msg_to_disk(Force, MsgStatus = #msg_status {
				 msg = Msg, guid = Guid,
				 is_persistent = IsPersistent }, MSCState)
  when Force orelse IsPersistent ->
    Msg1 = Msg #basic_message {
	     %% don't persist any recoverable decoded properties
	     content = rabbit_binary_parser:clear_decoded_content(
			 Msg #basic_message.content)},
    ok = msg_store_write(MSCState, IsPersistent, Guid, Msg1),
    MsgStatus #msg_status { msg_on_disk = true };
maybe_write_msg_to_disk(_Force, MsgStatus, _MSCState) ->
    MsgStatus.

maybe_write_index_to_disk(_Force, MsgStatus = #msg_status {
				    index_on_disk = true }, IndexState) ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    {MsgStatus, IndexState};
maybe_write_index_to_disk(Force, MsgStatus = #msg_status {
				   guid = Guid,
				   seq_id = SeqId,
				   is_persistent = IsPersistent,
				   is_delivered = IsDelivered,
				   msg_props = MsgProps}, IndexState)
  when Force orelse IsPersistent ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    IndexState1 = rabbit_queue_index:publish(
		    Guid, SeqId, MsgProps, IsPersistent, IndexState),
    {MsgStatus #msg_status { index_on_disk = true },
     maybe_write_delivered(IsDelivered, SeqId, IndexState1)};
maybe_write_index_to_disk(_Force, MsgStatus, IndexState) ->
    {MsgStatus, IndexState}.

maybe_write_to_disk(ForceMsg, ForceIndex, MsgStatus,
		    State = #mqstate { index_state = IndexState,
				       msg_store_clients = MSCState }) ->
    MsgStatus1 = maybe_write_msg_to_disk(ForceMsg, MsgStatus, MSCState),
    {MsgStatus2, IndexState1} =
	maybe_write_index_to_disk(ForceIndex, MsgStatus1, IndexState),
    {MsgStatus2, State #mqstate { index_state = IndexState1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#msg_status { seq_id = SeqId,
				 guid = Guid,
				 is_persistent = IsPersistent,
				 msg_on_disk = MsgOnDisk,
				 msg_props = MsgProps } = MsgStatus,
		   State = #mqstate { pending_ack = PA,
				      ram_ack_index = RAI,
				      ack_in_counter = AckInCount}) ->
    {AckEntry, RAI1} =
	case MsgOnDisk of
	    true -> {{IsPersistent, Guid, MsgProps}, RAI};
	    false -> {MsgStatus, gb_trees:insert(SeqId, Guid, RAI)}
	end,
    PA1 = dict:store(SeqId, AckEntry, PA),
    State #mqstate { pending_ack = PA1,
		     ram_ack_index = RAI1,
		     ack_in_counter = AckInCount + 1}.

remove_pending_ack(KeepPersistent,
		   State = #mqstate { pending_ack = PA,
				      index_state = IndexState,
				      msg_store_clients = MSCState }) ->
    {PersistentSeqIds, GuidsByStore, _AllGuids} =
	dict:fold(fun accumulate_ack/3, accumulate_ack_init(), PA),
    State1 = State #mqstate { pending_ack = dict:new(),
			      ram_ack_index = gb_trees:empty() },
    case KeepPersistent of
	true -> case orddict:find(false, GuidsByStore) of
		    error -> State1;
		    {ok, Guids} -> ok = msg_store_remove(MSCState, false,
							 Guids),
				   State1
		end;
	false -> IndexState1 =
		     rabbit_queue_index:ack(PersistentSeqIds, IndexState),
		 [ok = msg_store_remove(MSCState, IsPersistent, Guids)
		  || {IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
		 State1 #mqstate { index_state = IndexState1 }
    end.

ack(_MsgStoreFun, _Fun, [], State) ->
    {[], State};
ack(MsgStoreFun, Fun, AckTags, State) ->
    {{PersistentSeqIds, GuidsByStore, AllGuids},
     State1 = #mqstate { index_state = IndexState,
			 msg_store_clients = MSCState,
			 persistent_count = PCount,
			 ack_out_counter = AckOutCount }} =
	lists:foldl(
	  fun (SeqId, {Acc, State2 = #mqstate { pending_ack = PA,
						ram_ack_index = RAI }}) ->
		  AckEntry = dict:fetch(SeqId, PA),
		  {accumulate_ack(SeqId, AckEntry, Acc),
		   Fun(AckEntry, State2 #mqstate {
				   pending_ack = dict:erase(SeqId, PA),
				   ram_ack_index =
				       gb_trees:delete_any(SeqId, RAI)})}
	  end, {accumulate_ack_init(), State}, AckTags),
    IndexState1 = rabbit_queue_index:ack(PersistentSeqIds, IndexState),
    [ok = MsgStoreFun(MSCState, IsPersistent, Guids)
     || {IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
    PCount1 = PCount - find_persistent_count(sum_guids_by_store_to_len(
					       orddict:new(), GuidsByStore)),
    {lists:reverse(AllGuids),
     State1 #mqstate { index_state = IndexState1,
		       persistent_count = PCount1,
		       ack_out_counter = AckOutCount + length(AckTags) }}.

accumulate_ack_init() -> {[], orddict:new(), []}.

accumulate_ack(_SeqId, #msg_status { is_persistent = false, %% ASSERTIONS
				     msg_on_disk = false,
				     index_on_disk = false,
				     guid = Guid },
	       {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {PersistentSeqIdsAcc, GuidsByStore, [Guid | AllGuids]};
accumulate_ack(SeqId, {IsPersistent, Guid, _MsgProps},
	       {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {cons_if(IsPersistent, SeqId, PersistentSeqIdsAcc),
     rabbit_misc:orddict_cons(IsPersistent, Guid, GuidsByStore),
     [Guid | AllGuids]}.

find_persistent_count(LensByStore) ->
    case orddict:find(true, LensByStore) of
	error -> 0;
	{ok, Len} -> Len
    end.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

remove_confirms(GuidSet, State = #mqstate { msgs_on_disk = MOD,
					    msg_indices_on_disk = MIOD,
					    unconfirmed = UC }) ->
    State #mqstate { msgs_on_disk = gb_sets:difference(MOD, GuidSet),
		     msg_indices_on_disk = gb_sets:difference(MIOD, GuidSet),
		     unconfirmed = gb_sets:difference(UC, GuidSet) }.

msgs_confirmed(GuidSet, State) ->
    {gb_sets:to_list(GuidSet), remove_confirms(GuidSet, State)}.

msgs_written_to_disk(QPid, GuidSet) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (State = #mqstate { msgs_on_disk = MOD,
				    msg_indices_on_disk = MIOD,
				    unconfirmed = UC }) ->
		    msgs_confirmed(gb_sets:intersection(GuidSet, MIOD),
				   State #mqstate {
				     msgs_on_disk =
					 gb_sets:intersection(
					   gb_sets:union(MOD, GuidSet), UC) })
	    end).

msg_indices_written_to_disk(QPid, GuidSet) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (State = #mqstate { msgs_on_disk = MOD,
				    msg_indices_on_disk = MIOD,
				    unconfirmed = UC }) ->
		    msgs_confirmed(gb_sets:intersection(GuidSet, MOD),
				   State #mqstate {
				     msg_indices_on_disk =
					 gb_sets:intersection(
					   gb_sets:union(MIOD, GuidSet), UC) })
	    end).

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

fetch_from_q3(State = #mqstate {
		q1 = Q1,
		delta = #delta { count = DeltaCount },
		q3 = Q3,
		q4 = Q4,
		ram_index_count = RamIndexCount}) ->
    case bpqueue:out(Q3) of
	{empty, _Q3} ->
	    {empty, State};
	{{value, IndexOnDisk, MsgStatus}, Q3a} ->
	    RamIndexCount1 = RamIndexCount - one_if(not IndexOnDisk),
	    true = RamIndexCount1 >= 0, %% ASSERTION
	    State1 = State #mqstate { q3 = Q3a,
				      ram_index_count = RamIndexCount1 },
	    State2 =
		case {bpqueue:is_empty(Q3a), 0 == DeltaCount} of
		    {true, true} ->
			%% q3 is now empty, it wasn't before; delta is
			%% still empty. We know q4 is empty otherwise
			%% we wouldn't be loading from q3. As such, we
			%% can just set q4 to Q1.
			true = queue:is_empty(Q4), %% ASSERTION
			State1 #mqstate { q1 = queue:new(),
					  q4 = Q1 };
		    {true, false} ->
			maybe_deltas_to_betas(State1);
		    {false, _} ->
			%% q3 still isn't empty, we've not touched
			%% delta, so the invariants between q1, delta
			%% and q3 are maintained
			State1
		end,
	    {loaded, {MsgStatus, State2}}
    end.

maybe_deltas_to_betas(State = #mqstate { delta = ?BLANK_DELTA_PATTERN(X) }) ->
    State;
maybe_deltas_to_betas(State = #mqstate {
			delta = Delta,
			q3 = Q3,
			index_state = IndexState }) ->
    #delta { start_seq_id = DeltaSeqId,
	     count = DeltaCount,
	     end_seq_id = DeltaSeqIdEnd } = Delta,
    DeltaSeqId1 =
	lists:min([rabbit_queue_index:next_segment_boundary(DeltaSeqId),
		   DeltaSeqIdEnd]),
    {List, IndexState1} =
	rabbit_queue_index:read(DeltaSeqId, DeltaSeqId1, IndexState),
    {Q3a, IndexState2} =
	betas_from_index_entries(List, IndexState1),
    State1 = State #mqstate { index_state = IndexState2 },
    case bpqueue:len(Q3a) of
	0 ->
	    %% we ignored every message in the segment due to it being
	    %% transient and below the threshold
	    maybe_deltas_to_betas(
	      State1 #mqstate {
		delta = Delta #delta { start_seq_id = DeltaSeqId1 }});
	Q3aLen ->
	    Q3b = bpqueue:join(Q3, Q3a),
	    case DeltaCount - Q3aLen of
		0 ->
		    State1 #mqstate { delta = ?BLANK_DELTA,
				      q3 = Q3b };
		N when N > 0 ->
		    Delta1 = #delta { start_seq_id = DeltaSeqId1,
				      count = N,
				      end_seq_id = DeltaSeqIdEnd },
		    State1 #mqstate { delta = Delta1,
				      q3 = Q3b }
	    end
    end.

