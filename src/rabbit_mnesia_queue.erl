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

%% In the queue we keep track of both messages that are pending
%% delivery and messages that are pending acks. This ensures that
%% purging (deleting the former) and deletion (deleting the former and
%% the latter) are both cheap and do require any scanning through q
%% segments.
%%
%% Pending acks are recorded in memory either as the tuple {SeqId,
%% Guid, MsgProps} (tuple-form) or as the message itself (message-
%% form). Acks for messages are stored in tuple-form if the message
%% has been sent to disk as part of the memory reduction process. For
%% messages that haven't already been written to disk, acks are stored
%% in message-form.
%%
%% During memory reduction, acks stored in message-form are converted
%% to tuple-form, and the corresponding messages are pushed out to
%% disk.
%%
%% All queues are durable in this version, no matter how they are
%% requested. (We may need to remember the requested type in the
%% future, to catch accidental redeclares.) All messages are transient
%% (non-persistent) in this interim version, in order to rip aout all
%% of the old backing code before insetring the new backing
%% code. (This breaks some tests, since all messages are temporarily
%% dropped on restart.)
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
	{ q,
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
	  is_delivered,
	  msg_on_disk,
	  index_on_disk,
	  msg_props
	}).

-record(tx, { pending_messages, pending_acks }).

-record(sync, { acks_persistent, acks_all, pubs, funs }).

-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE, msg_store_transient).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id() | 'blank_ack').

-type(sync() :: #sync { acks_persistent :: [[seq_id()]],
			acks_all :: [[seq_id()]],
			pubs :: [{message_properties_transformer(),
				  [rabbit_types:basic_message()]}],
			funs :: [fun (() -> any())] }).

-type(state() :: #mqstate {
	     q :: queue(),
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

%% Need declaration for msg_status and tx.

-include("rabbit_backing_queue_spec.hrl").

-endif.

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
    %% TODO: there is no need to interact with q at all - which we do
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

purge(State = #mqstate { q = Q,
			 index_state = IndexState,
			 msg_store_clients = MSCState,
			 len = Len,
			 persistent_count = PCount }) ->
    %% TODO: when there are no pending acks, which is a common case,
    %% we could simply wipe the q instead of issuing delivers and
    %% acks for all the messages.
    {LensByStore, IndexState1} = remove_queue_entries(
				   fun rabbit_misc:queue_fold/3, Q,
				   orddict:new(), IndexState, MSCState),
    {LensByStore1, State1 = #mqstate { index_state = IndexState2,
				       msg_store_clients = MSCState1 }} =
	purge_betas(LensByStore,
		    State #mqstate { q = queue:new(),
				     index_state = IndexState1 }),
    {LensByStore2, IndexState3} = remove_queue_entries(
				    fun rabbit_misc:queue_fold/3, queue:new(),
				    LensByStore1, IndexState2, MSCState1),
    PCount1 = PCount - find_persistent_count(LensByStore2),
    {Len, a(State1 #mqstate { index_state = IndexState3,
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
publish_delivered(true, Msg = #basic_message { guid = Guid },
		  MsgProps = #message_properties {
		    needs_confirming = NeedsConfirming },
		  State = #mqstate { len = 0,
				     next_seq_id = SeqId,
				     out_counter = OutCount,
				     in_counter = InCount,
				     unconfirmed = Unconfirmed }) ->
    MsgStatus = (msg_status(SeqId, Msg, MsgProps))
	#msg_status { is_delivered = true },
    State1 = record_pending_ack(m(MsgStatus), State),
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, a(State1 #mqstate { next_seq_id = SeqId + 1,
				out_counter = OutCount + 1,
				in_counter = InCount + 1,
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
		      %% message needs to go back into Q. Also the
		      %% msg contents might not be in RAM, so read
		      %% them in now
		      {MsgStatus1, State2 = #mqstate { q = Q }} =
			  read_msg(MsgStatus, State1),
		      {ok, State2 #mqstate {q = queue:in_r(MsgStatus1, Q) }}
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

internal_queue_out(Fun, State = #mqstate { q = Q }) ->
    case queue:out(Q) of
	{empty, _Q} -> a(State),
			{empty, State};
	{{value, MsgStatus}, Qa} ->
	    Fun(MsgStatus, State #mqstate { q = Qa })
    end.

read_msg(MsgStatus = #msg_status { msg = undefined,
				   guid = Guid },
	 State = #mqstate { ram_msg_count = RamMsgCount,
			    msg_store_clients = MSCState}) ->
    {{ok, Msg = #basic_message {}}, MSCState1} =
	msg_store_read(MSCState, false, Guid),
    {MsgStatus #msg_status { msg = Msg },
     State #mqstate { ram_msg_count = RamMsgCount + 1,
		      msg_store_clients = MSCState1 }};
read_msg(MsgStatus, State) ->
    {MsgStatus, State}.

internal_fetch(AckRequired, MsgStatus = #msg_status {
			      seq_id = SeqId,
			      guid = Guid,
			      msg = Msg,
			      is_delivered = IsDelivered,
			      msg_on_disk = MsgOnDisk,
			      index_on_disk = IndexOnDisk },
	       State = #mqstate {ram_msg_count = RamMsgCount,
				 out_counter = OutCount,
				 index_state = IndexState,
				 msg_store_clients = MSCState,
				 len = Len }) ->
    %% 1. Mark it delivered if necessary
    IndexState1 = maybe_write_delivered(
		    IndexOnDisk andalso not IsDelivered,
		    SeqId, IndexState),

    %% 2. Remove from msg_store and queue index, if necessary
    Rem = fun () ->
		  ok = msg_store_remove(MSCState, false, [Guid])
	  end,
    Ack = fun () -> rabbit_queue_index:ack([SeqId], IndexState1) end,
    IndexState2 =
	case {AckRequired, MsgOnDisk, IndexOnDisk} of
	    {false, true, false} -> Rem(), IndexState1;
	    {false, true, true} -> Rem(), Ack();
	    {true, true, true} -> Ack();
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

    Len1 = Len - 1,
    RamMsgCount1 = RamMsgCount - one_if(Msg =/= undefined),

    {{Msg, IsDelivered, AckTag, Len1},
     a(State1 #mqstate { ram_msg_count = RamMsgCount1,
			 out_counter = OutCount + 1,
			 index_state = IndexState2,
			 len = Len1 })}.

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

tx_rollback(Txn, State = #mqstate { msg_store_clients = MSCState }) ->
    #tx { pending_acks = AckTags } = lookup_tx(Txn),
    erase_tx(Txn),
    ok = msg_store_remove(MSCState, true, []),
    {lists:append(AckTags), a(State)}.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits a transaction. The Fun passed in must be called
%% once the messages have really been commited. This CPS permits the
%% possibility of commit coalescing.

%% -spec(tx_commit/4 ::
%% (rabbit_types:txn(), fun (() -> any()),
%% message_properties_transformer(), state()) -> {[ack()], state()}).

tx_commit(Txn, Fun, MsgPropsFun, State) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    {AckTags1,
     a(tx_commit_post_msg_store(false, Pubs, AckTags1, Fun, MsgPropsFun, State))}.

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
		({_IsPersistent, Guid, MsgProps}, State1) ->
		    #mqstate { msg_store_clients = MSCState } = State1,
		    {{ok, Msg = #basic_message{}}, MSCState1} =
			msg_store_read(MSCState, false, Guid),
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
	  q = Q,
	  len = Len,
	  pending_ack = PA,
	  ram_ack_index = RAI,
	  on_sync = #sync { funs = From },
	  ram_msg_count = RamMsgCount,
	  ram_index_count = RamIndexCount,
	  next_seq_id = NextSeqId,
	  persistent_count = PersistentCount }) ->
    [ {q, queue:len(Q)},
      {len, Len},
      {pending_acks, dict:size(PA)},
      {outstanding_txns, length(From)},
      {ram_msg_count, RamMsgCount},
      {ram_ack_count, gb_trees:size(RAI)},
      {ram_index_count, RamIndexCount},
      {next_seq_id, NextSeqId},
      {persistent_count, PersistentCount} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

a(State = #mqstate { q = Q,
		     len = Len,
		     persistent_count = PersistentCount,
		     ram_msg_count = RamMsgCount,
		     ram_index_count = RamIndexCount }) ->
    E4 = queue:is_empty(Q),
    LZ = Len == 0,

    true = LZ == E4,

    true = Len >= 0,
    true = PersistentCount >= 0,
    true = RamMsgCount >= 0,
    true = RamIndexCount >= 0,

    State.

m(MsgStatus = #msg_status { msg = Msg,
			    msg_on_disk = MsgOnDisk,
			    index_on_disk = IndexOnDisk }) ->
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

msg_status(SeqId, Msg = #basic_message { guid = Guid }, MsgProps) ->
    #msg_status { seq_id = SeqId, guid = Guid, msg = Msg, is_delivered = false,
		  msg_on_disk = false, index_on_disk = false,
		  msg_props = MsgProps }.

with_msg_store_state({MSCStateP, MSCStateT}, true, Fun) ->
    {Result, MSCStateP1} = Fun(MSCStateP),
    {Result, {MSCStateP1, MSCStateT}};
with_msg_store_state({MSCStateP, MSCStateT}, false, Fun) ->
    {Result, MSCStateT1} = Fun(MSCStateT),
    {Result, {MSCStateP, MSCStateT1}}.

with_immutable_msg_store_state(MSCState, Fun) ->
    {Res, MSCState} = with_msg_store_state(MSCState, false,
					   fun (MSCState1) ->
						   {Fun(MSCState1), MSCState1}
					   end),
    Res.

msg_store_client_init(MsgStore, MsgOnDiskFun) ->
    rabbit_msg_store:client_init(MsgStore, rabbit_guid:guid(), MsgOnDiskFun).

msg_store_read(MSCState, _IsPersistent, Guid) ->
    with_msg_store_state(
      MSCState, false,
      fun (MSCState1) -> rabbit_msg_store:read(Guid, MSCState1) end).

msg_store_remove(MSCState, _IsPersistent, Guids) ->
    with_immutable_msg_store_state(
      MSCState,
      fun (MCSState1) -> rabbit_msg_store:remove(Guids, MCSState1) end).

msg_store_release(MSCState, _IsPersistent, Guids) ->
    with_immutable_msg_store_state(
      MSCState,
      fun (MCSState1) -> rabbit_msg_store:release(Guids, MCSState1) end).

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

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

init(IndexState, DeltaCount, Terms, PersistentClient, TransientClient) ->
    {_LowSeqId, NextSeqId, IndexState1} = rabbit_queue_index:bounds(IndexState),

    DeltaCount = 0,       % Sure hope so!
    DeltaCount1 = proplists:get_value(persistent_count, Terms, DeltaCount),
    DeltaCount1 = 0,      % Sure hope so!
    State = #mqstate {
      q = queue:new(),
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
    a(State).

tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags, Fun, MsgPropsFun,
			 State = #mqstate {
			   on_sync = OnSync = #sync {
				       acks_persistent = SPAcks,
				       acks_all = SAcks,
				       pubs = SPubs,
				       funs = SFuns } }) ->
    case HasPersistentPubs of
	true -> State #mqstate {
		  on_sync = #sync {
		    acks_persistent = [[] | SPAcks],
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
	  fun ({Msg, MsgProps}, {SeqIdsAcc, State2}) ->
		  {_SeqId, State3} =
		      publish(Msg, MsgProps, false, false, State2),
		  {SeqIdsAcc, State3}
	  end, {PAcks, NewState}, Pubs),
    IndexState1 = rabbit_queue_index:sync(SeqIds, IndexState),
    [ Fun() || Fun <- lists:reverse(SFuns) ],
    State1 #mqstate { index_state = IndexState1, on_sync = ?BLANK_SYNC }.

purge_betas(LensByStore, State) -> {LensByStore, State}.

remove_queue_entries(Fold, Q, LensByStore, IndexState, MSCState) ->
    {GuidsByStore, Delivers, Acks} =
	Fold(fun remove_queue_entries1/2, {orddict:new(), [], []}, Q),
    ok = orddict:fold(fun (_IsPersistent, Guids, ok) ->
			      msg_store_remove(MSCState, false, Guids)
		      end, ok, GuidsByStore),
    {sum_guids_by_store_to_len(LensByStore, GuidsByStore),
     rabbit_queue_index:ack(Acks,
			    rabbit_queue_index:deliver(Delivers, IndexState))}.

remove_queue_entries1(
  #msg_status { guid = Guid, seq_id = SeqId,
		is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
		index_on_disk = IndexOnDisk },
  {GuidsByStore, Delivers, Acks}) ->
    {case MsgOnDisk of
	 true -> rabbit_misc:orddict_cons(false, Guid, GuidsByStore);
	 false -> GuidsByStore
     end,
     cons_if(IndexOnDisk andalso not IsDelivered, SeqId, Delivers),
     cons_if(IndexOnDisk, SeqId, Acks)}.

sum_guids_by_store_to_len(LensByStore, GuidsByStore) ->
    orddict:fold(
      fun (_IsPersistent, Guids, LensByStore1) ->
	      orddict:update_counter(false, length(Guids), LensByStore1)
      end, LensByStore, GuidsByStore).

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

publish(Msg = #basic_message { guid = Guid },
	MsgProps = #message_properties { needs_confirming = NeedsConfirming },
	IsDelivered, MsgOnDisk,
	State = #mqstate { q = Q,
			   next_seq_id = SeqId,
			   len = Len,
			   in_counter = InCount,
			   ram_msg_count = RamMsgCount,
			   unconfirmed = Unconfirmed }) ->
    MsgStatus = (msg_status(SeqId, Msg, MsgProps))
	#msg_status { is_delivered = IsDelivered, msg_on_disk = MsgOnDisk},
    State1 = State #mqstate { q = queue:in(m(MsgStatus), Q) },
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, State1 #mqstate { next_seq_id = SeqId + 1,
			      len = Len + 1,
			      in_counter = InCount + 1,
			      ram_msg_count = RamMsgCount + 1,
			      unconfirmed = Unconfirmed1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#msg_status { seq_id = SeqId,
				 guid = Guid,
				 msg_on_disk = MsgOnDisk,
				 msg_props = MsgProps } = MsgStatus,
		   State = #mqstate { pending_ack = PA,
				      ram_ack_index = RAI,
				      ack_in_counter = AckInCount}) ->
    {AckEntry, RAI1} =
	case MsgOnDisk of
	    true -> {{false, Guid, MsgProps}, RAI};
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
		 [ok = msg_store_remove(MSCState, false, Guids)
		  || {_IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
		 State1 #mqstate { index_state = IndexState1 }
    end.

ack(_MsgStoreFun, _Fun, [], State) -> {[], State};
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
    [ok = MsgStoreFun(MSCState, false, Guids)
     || {_IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
    PCount1 = PCount - find_persistent_count(sum_guids_by_store_to_len(
					       orddict:new(), GuidsByStore)),
    {lists:reverse(AllGuids),
     State1 #mqstate { index_state = IndexState1,
		       persistent_count = PCount1,
		       ack_out_counter = AckOutCount + length(AckTags) }}.

accumulate_ack_init() -> {[], orddict:new(), []}.

accumulate_ack(_SeqId, #msg_status { msg_on_disk = false,
				     index_on_disk = false,
				     guid = Guid },
	       {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {PersistentSeqIdsAcc, GuidsByStore, [Guid | AllGuids]};
accumulate_ack(_SeqId, {_IsPersistent, Guid, _MsgProps},
	       {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {PersistentSeqIdsAcc, rabbit_misc:orddict_cons(false, Guid, GuidsByStore),
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
