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

-export(
   [init/3, terminate/1, delete_and_terminate/1, purge/1, publish/3,
    publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3, tx_rollback/2,
    tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

-export([start/1, stop/0]).

-behaviour(rabbit_backing_queue).

-record(state,
        { q,
          next_seq_id,
          pending_ack,
          ram_ack_index,
          index_state,
          msg_store_clients,
          on_sync,
          is_durable,

          len,

          msgs_on_disk,
          msg_indices_on_disk,
          unconfirmed
        }).

-record(m,
        { seq_id,
          guid,
          msg,
          is_persistent,
          is_delivered,
          msg_on_disk,
          index_on_disk,
          props
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

-type(state() :: #state {
             q :: queue(),
             next_seq_id :: seq_id(),
             pending_ack :: dict(),
             ram_ack_index :: gb_tree(),
             index_state :: any(),
             msg_store_clients :: 'undefined' | {{any(), binary()},
                                                 {any(), binary()}},
             on_sync :: sync(),
             is_durable :: boolean(),

             len :: non_neg_integer(),

             msgs_on_disk :: gb_set(),
             msg_indices_on_disk :: gb_set(),
             unconfirmed :: gb_set() }).

-include("rabbit_backing_queue_spec.hrl").

-endif.

-define(BLANK_SYNC, #sync { acks_persistent = [],
                            acks_all = [],
                            pubs = [],
                            funs = [] }).

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

init(QueueName, _IsDurable, _Recover) ->
    Self = self(),
    IsDurable = false,
    Recover = false,
    init5(QueueName, IsDurable, Recover,
          fun (Guids) -> msgs_written_to_disk(Self, Guids) end,
	  fun (Guids) -> msg_indices_written_to_disk(Self, Guids) end).

init5(QueueName, IsDurable, false, MsgOnDiskFun, MsgIdxOnDiskFun) ->
    IndexState = rabbit_queue_index:init(QueueName, MsgIdxOnDiskFun),
    init(IsDurable, IndexState, 0, [],
         case IsDurable of
             true -> msg_store_client_init(?PERSISTENT_MSG_STORE,
                                           MsgOnDiskFun);
             false -> undefined
         end,
         msg_store_client_init(?TRANSIENT_MSG_STORE, undefined));

init5(QueueName, true, true, MsgOnDiskFun, MsgIdxOnDiskFun) ->
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
    DeltaCount = 0, %ASSERTION
    init(true, IndexState, DeltaCount, Terms1,
         PersistentClient, TransientClient).

terminate(State) ->
    State1 = #state { index_state = IndexState,
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
             {transient_ref, TRef}],
    State1 #state { index_state = rabbit_queue_index:terminate(
                                    Terms, IndexState),
                    msg_store_clients = undefined }.

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
delete_and_terminate(State) ->
    %% TODO: there is no need to interact with qi at all - which we do
    %% as part of 'purge' and 'remove_pending_ack', other than
    %% deleting it.
    {_PurgeCount, State1} = purge(State),
    State2 = #state { index_state = IndexState,
                      msg_store_clients = {MSCStateP, MSCStateT} } =
        remove_pending_ack(false, State1),
    IndexState1 = rabbit_queue_index:delete_and_terminate(IndexState),
    case MSCStateP of
        undefined -> ok;
        _ -> rabbit_msg_store:client_delete_and_terminate(MSCStateP)
    end,
    rabbit_msg_store:client_delete_and_terminate(MSCStateT),
    State2 #state { index_state = IndexState1,
                    msg_store_clients = undefined }.

purge(State = #state { q = Q,
                       index_state = IndexState,
                       msg_store_clients = MSCState,
                       len = Len }) ->
    %% TODO: when there are no pending acks, which is a common case,
    %% we could simply wipe the qi instead of issuing delivers and
    %% acks for all the messages.
    {LensByStore, IndexState1} = remove_queue_entries(
                                   fun rabbit_misc:queue_fold/3, Q,
                                   orddict:new(), IndexState, MSCState),
    {LensByStore1, State1 = #state { index_state = IndexState2,
                                     msg_store_clients = MSCState1 }} =
        purge_betas_and_deltas(LensByStore,
                               State #state { q = queue:new(),
                                              index_state = IndexState1 }),
    {_, IndexState3} = remove_queue_entries(
			 fun rabbit_misc:queue_fold/3, queue:new(),
			 LensByStore1, IndexState2, MSCState1),
    {Len, State1 #state { index_state = IndexState3, len = 0 }}.

publish(Msg, Props, State) ->
    {_SeqId, State1} = publish(Msg, Props, false, false, State),
    State1.

publish_delivered(false, _Msg, _Props, State = #state { len = 0 }) ->
    {blank_ack, State};
publish_delivered(true, Msg = #basic_message { is_persistent = IsPersistent,
                                               guid = Guid },
                  Props = #message_properties {
                    needs_confirming = NeedsConfirming },
                  State = #state { len = 0,
                                   next_seq_id = SeqId,
                                   is_durable = IsDurable,
                                   unconfirmed = Unconfirmed }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    M = (m(IsPersistent1, SeqId, Msg, Props)) #m { is_delivered = true },
    {M1, State1} = maybe_write_to_disk(false, false, M, State),
    State2 = record_pending_ack(M1, State1),
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, State2 #state { next_seq_id = SeqId + 1,
                            unconfirmed = Unconfirmed1 }}.

dropwhile(Pred, State) ->
    {_OkOrEmpty, State1} = dropwhile1(Pred, State),
    State1.

dropwhile1(Pred, State) ->
    internal_queue_out(
      fun(M = #m { props = Props }, State1) ->
              case Pred(Props) of
                  true ->
                      {_, State2} = internal_fetch(false, M, State1),
                      dropwhile1(Pred, State2);
                  false ->
                      {M1, State2 = #state { q = Q }} =
                          read_msg(M, State1),
                      {ok, State2 #state {q = queue:in_r(M1, Q) }}
              end
      end, State).

fetch(AckRequired, State) ->
    internal_queue_out(
      fun(M, State1) ->
              %% it's possible that the message wasn't read from disk
              %% at this point, so read it in.
              {M1, State2} = read_msg(M, State1),
              internal_fetch(AckRequired, M1, State2)
      end, State).

internal_queue_out(Fun, State = #state { q = Q }) ->
    case queue:out(Q) of
        {empty, _Q} ->
            case fetch_from_q3(State) of
                {empty, State1} = Result -> _ = State1, Result
            end;
        {{value, M}, Qa} ->
            Fun(M, State #state { q = Qa })
    end.

read_msg(M = #m { msg = undefined,
                  guid = Guid,
                  is_persistent = IsPersistent },
         State = #state { msg_store_clients = MSCState}) ->
    {{ok, Msg = #basic_message {}}, MSCState1} =
        msg_store_read(MSCState, IsPersistent, Guid),
    {M #m { msg = Msg },
     State #state { msg_store_clients = MSCState1 }};
read_msg(M, State) ->
    {M, State}.

internal_fetch(AckRequired,
               M = #m {
                 seq_id = SeqId,
                 guid = Guid,
                 msg = Msg,
                 is_persistent = IsPersistent,
                 is_delivered = IsDelivered,
                 msg_on_disk = MsgOnDisk,
                 index_on_disk = IndexOnDisk },
               State = #state { index_state = IndexState,
                                msg_store_clients = MSCState,
                                len = Len }) ->
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
                                              M #m {
                                                is_delivered = true }, State),
                                   {SeqId, StateN};
                           false -> {blank_ack, State}
                       end,

    Len1 = Len - 1,

    {{Msg, IsDelivered, AckTag, Len1},
     State1 #state { index_state = IndexState2,
                     len = Len1 }}.

ack(AckTags, State) ->
    {Guids, State1} =
        ack(fun msg_store_remove/3,
            fun ({_IsPersistent, Guid, _Props}, State1) ->
                    remove_confirms(gb_sets:singleton(Guid), State1);
                (#m{msg = #basic_message { guid = Guid }}, State1) ->
                    remove_confirms(gb_sets:singleton(Guid), State1)
            end,
            AckTags, State),
    {Guids, State1}.

tx_publish(Txn, Msg = #basic_message { is_persistent = IsPersistent }, Props,
           State = #state { is_durable = IsDurable,
                            msg_store_clients = MSCState }) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, Props} | Pubs] }),
    _ = case IsPersistent andalso IsDurable of
            true -> M = m(true, undefined, Msg, Props),
                    #m { msg_on_disk = true } =
                        maybe_write_msg_to_disk(false, M, MSCState);
            false -> ok
        end,
    State.

tx_ack(Txn, AckTags, State) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }),
    State.

tx_rollback(Txn, State = #state { is_durable = IsDurable,
                                  msg_store_clients = MSCState }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    ok = case IsDurable of
             true -> msg_store_remove(MSCState, true, persistent_guids(Pubs));
             false -> ok
         end,
    {lists:append(AckTags), State}.

tx_commit(Txn, Fun, PropsFun,
          State = #state { is_durable = IsDurable,
                           msg_store_clients = MSCState }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    PersistentGuids = persistent_guids(Pubs),
    HasPersistentPubs = PersistentGuids =/= [],
    {AckTags1,
     case IsDurable andalso HasPersistentPubs of
         true -> ok = msg_store_sync(
                        MSCState, true, PersistentGuids,
                        msg_store_callback(PersistentGuids, Pubs, AckTags1,
                                           Fun, PropsFun)),
                 State;
         false -> tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags1,
                                           Fun, PropsFun, State)
     end}.

requeue(AckTags, PropsFun, State) ->
    {_Guids, State1} =
        ack(fun msg_store_release/3,
            fun (#m { msg = Msg, props = Props }, State1) ->
                    {_SeqId, State2} = publish(Msg, PropsFun(Props),
                                               true, false, State1),
                    State2;
                ({IsPersistent, Guid, Props}, State1) ->
                    #state { msg_store_clients = MSCState } = State1,
                    {{ok, Msg = #basic_message{}}, MSCState1} =
                        msg_store_read(MSCState, IsPersistent, Guid),
                    State2 = State1 #state { msg_store_clients = MSCState1 },
                    {_SeqId, State3} = publish(Msg, PropsFun(Props),
                                               true, true, State2),
                    State3
            end,
            AckTags, State),
    State1.

len(#state { len = Len }) -> Len.

is_empty(State) -> 0 == len(State).

set_ram_duration_target(_DurationTarget, State) -> State.

ram_duration(State) -> {0, State}.

needs_idle_timeout(#state { on_sync = ?BLANK_SYNC }) -> false;
needs_idle_timeout(_State) -> true.

idle_timeout(State) -> tx_commit_index(State).

handle_pre_hibernate(State = #state { index_state = IndexState }) ->
    State #state { index_state = rabbit_queue_index:flush(IndexState) }.

status(#state {
          q = Q,
          len = Len,
          pending_ack = PA,
          ram_ack_index = RAI,
          on_sync = #sync { funs = From },
          next_seq_id = NextSeqId }) ->
    [ {q, queue:len(Q)},
      {len, Len},
      {pending_acks, dict:size(PA)},
      {outstanding_txns, length(From)},
      {ram_ack_count, gb_trees:size(RAI)},
      {next_seq_id, NextSeqId} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

cons_if(true, E, L) -> [E | L];
cons_if(false, _E, L) -> L.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
gb_sets_maybe_insert(true, Val, Set) -> gb_sets:add(Val, Set).

m(IsPersistent, SeqId, Msg = #basic_message { guid = Guid },
           Props) ->
    #m { seq_id = SeqId,
         guid = Guid,
         msg = Msg,
         is_persistent = IsPersistent,
         is_delivered = false,
         msg_on_disk = false,
         index_on_disk = false,
         props = Props }.

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
                               is_persistent = true }, _Props} <- Pubs].

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

init(IsDurable, IndexState, _, _, PersistentClient, TransientClient) ->
    {_, NextSeqId, IndexState1} = rabbit_queue_index:bounds(IndexState),
    State = #state {
      q = queue:new(),
      next_seq_id = NextSeqId,
      pending_ack = dict:new(),
      ram_ack_index = gb_trees:empty(),
      index_state = IndexState1,
      msg_store_clients = {PersistentClient, TransientClient},
      on_sync = ?BLANK_SYNC,
      is_durable = IsDurable,
      len = 0,
      msgs_on_disk = gb_sets:new(),
      msg_indices_on_disk = gb_sets:new(),
      unconfirmed = gb_sets:new() },
    maybe_deltas_to_betas(State).

msg_store_callback(PersistentGuids, Pubs, AckTags, Fun, PropsFun) ->
    Self = self(),
    F = fun () -> rabbit_amqqueue:maybe_run_queue_via_backing_queue(
                    Self, fun (StateN) -> {[], tx_commit_post_msg_store(
                                                 true, Pubs, AckTags,
                                                 Fun, PropsFun, StateN)}
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

tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags, Fun, PropsFun,
                         State = #state {
                           on_sync = OnSync = #sync {
                                       acks_persistent = SPAcks,
                                       acks_all = SAcks,
                                       pubs = SPubs,
                                       funs = SFuns },
                           pending_ack = PA,
                           is_durable = IsDurable }) ->
    PersistentAcks =
        case IsDurable of
            true -> [AckTag || AckTag <- AckTags,
                               case dict:fetch(AckTag, PA) of
                                   #m {} -> false;
                                   {IsPersistent, _Guid, _Props} ->
                                       IsPersistent
                               end];
            false -> []
        end,
    case IsDurable andalso (HasPersistentPubs orelse PersistentAcks =/= []) of
        true -> State #state {
                  on_sync = #sync {
                    acks_persistent = [PersistentAcks | SPAcks],
                    acks_all = [AckTags | SAcks],
                    pubs = [{PropsFun, Pubs} | SPubs],
                    funs = [Fun | SFuns] }};
        false -> State1 = tx_commit_index(
                            State #state {
                              on_sync = #sync {
                                acks_persistent = [],
                                acks_all = [AckTags],
                                pubs = [{PropsFun, Pubs}],
                                funs = [Fun] } }),
                 State1 #state { on_sync = OnSync }
    end.

tx_commit_index(State = #state { on_sync = ?BLANK_SYNC }) ->
    State;
tx_commit_index(State = #state { on_sync = #sync {
                                   acks_persistent = SPAcks,
                                   acks_all = SAcks,
                                   pubs = SPubs,
                                   funs = SFuns },
                                 is_durable = IsDurable }) ->
    PAcks = lists:append(SPAcks),
    Acks = lists:append(SAcks),
    {_Guids, NewState} = ack(Acks, State),
    Pubs = [{Msg, Fun(Props)} || {Fun, PubsN} <- lists:reverse(SPubs),
                                    {Msg, Props} <- lists:reverse(PubsN)],
    {SeqIds, State1 = #state { index_state = IndexState }} =
        lists:foldl(
          fun ({Msg = #basic_message { is_persistent = IsPersistent },
                Props},
               {SeqIdsAcc, State2}) ->
                  IsPersistent1 = IsDurable andalso IsPersistent,
                  {SeqId, State3} =
                      publish(Msg, Props, false, IsPersistent1, State2),
                  {cons_if(IsPersistent1, SeqId, SeqIdsAcc), State3}
          end, {PAcks, NewState}, Pubs),
    IndexState1 = rabbit_queue_index:sync(SeqIds, IndexState),
    _ = [ Fun() || Fun <- lists:reverse(SFuns) ],
    State1 #state { index_state = IndexState1, on_sync = ?BLANK_SYNC }.

purge_betas_and_deltas(LensByStore, State) -> {LensByStore, State}.

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
  #m { guid = Guid,
       seq_id = SeqId,
       is_delivered = IsDelivered,
       msg_on_disk = MsgOnDisk,
       index_on_disk = IndexOnDisk,
       is_persistent = IsPersistent },
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
        Props = #message_properties { needs_confirming = NeedsConfirming },
        IsDelivered, MsgOnDisk,
        State = #state { q = Q,
                         next_seq_id = SeqId,
                         len = Len,
                         is_durable = IsDurable,
                         unconfirmed = Unconfirmed }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    M = (m(IsPersistent1, SeqId, Msg, Props))
        #m { is_delivered = IsDelivered, msg_on_disk = MsgOnDisk},
    {M1, State1} = maybe_write_to_disk(false, false, M, State),
    State2 = State1 #state { q = queue:in(M1, Q) },
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, State2 #state { next_seq_id = SeqId + 1,
                            len = Len + 1,
                            unconfirmed = Unconfirmed1 }}.

maybe_write_msg_to_disk(_Force, M = #m { msg_on_disk = true }, _MSCState) ->
    M;
maybe_write_msg_to_disk(Force, M = #m {
                                 msg = Msg, guid = Guid,
                                 is_persistent = IsPersistent }, MSCState)
  when Force orelse IsPersistent ->
    Msg1 = Msg #basic_message {
             %% don't persist any recoverable decoded properties
             content = rabbit_binary_parser:clear_decoded_content(
                         Msg #basic_message.content)},
    ok = msg_store_write(MSCState, IsPersistent, Guid, Msg1),
    M #m { msg_on_disk = true };
maybe_write_msg_to_disk(_Force, M, _MSCState) -> M.

maybe_write_index_to_disk(_Force,
                          M = #m { index_on_disk = true }, IndexState) ->
    true = M #m.msg_on_disk, %% ASSERTION
    {M, IndexState};
maybe_write_index_to_disk(Force,
                          M = #m {
                            guid = Guid,
                            seq_id = SeqId,
                            is_persistent = IsPersistent,
                            is_delivered = IsDelivered,
                            props = Props},
                          IndexState)
  when Force orelse IsPersistent ->
    true = M #m.msg_on_disk, %% ASSERTION
    IndexState1 = rabbit_queue_index:publish(
                    Guid, SeqId, Props, IsPersistent, IndexState),
    {M #m { index_on_disk = true },
     maybe_write_delivered(IsDelivered, SeqId, IndexState1)};
maybe_write_index_to_disk(_Force, M, IndexState) ->
    {M, IndexState}.

maybe_write_to_disk(ForceMsg, ForceIndex, M,
                    State = #state { index_state = IndexState,
                                     msg_store_clients = MSCState }) ->
    M1 = maybe_write_msg_to_disk(ForceMsg, M, MSCState),
    {M2, IndexState1} =
        maybe_write_index_to_disk(ForceIndex, M1, IndexState),
    {M2, State #state { index_state = IndexState1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#m { seq_id = SeqId,
                        guid = Guid,
                        is_persistent = IsPersistent,
                        msg_on_disk = MsgOnDisk,
                        props = Props } = M,
                   State = #state { pending_ack = PA, ram_ack_index = RAI}) ->
    {AckEntry, RAI1} =
        case MsgOnDisk of
            true -> {{IsPersistent, Guid, Props}, RAI};
            false -> {M, gb_trees:insert(SeqId, Guid, RAI)}
        end,
    PA1 = dict:store(SeqId, AckEntry, PA),
    State #state { pending_ack = PA1, ram_ack_index = RAI1 }.

remove_pending_ack(KeepPersistent,
                   State = #state { pending_ack = PA,
                                    index_state = IndexState,
                                    msg_store_clients = MSCState }) ->
    {PersistentSeqIds, GuidsByStore, _AllGuids} =
        dict:fold(fun accumulate_ack/3, accumulate_ack_init(), PA),
    State1 = State #state { pending_ack = dict:new(),
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
                 _ = [ok = msg_store_remove(MSCState, IsPersistent, Guids)
                      || {IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
                 State1 #state { index_state = IndexState1 }
    end.

ack(_MsgStoreFun, _Fun, [], State) ->
    {[], State};
ack(MsgStoreFun, Fun, AckTags, State) ->
    {{PersistentSeqIds, GuidsByStore, AllGuids},
     State1 = #state { index_state = IndexState,
                       msg_store_clients = MSCState }} =
        lists:foldl(
          fun (SeqId, {Acc, State2 = #state { pending_ack = PA,
                                              ram_ack_index = RAI }}) ->
                  AckEntry = dict:fetch(SeqId, PA),
                  {accumulate_ack(SeqId, AckEntry, Acc),
                   Fun(AckEntry, State2 #state {
                                   pending_ack = dict:erase(SeqId, PA),
                                   ram_ack_index =
                                       gb_trees:delete_any(SeqId, RAI)})}
          end, {accumulate_ack_init(), State}, AckTags),
    IndexState1 = rabbit_queue_index:ack(PersistentSeqIds, IndexState),
    _ = [ok = MsgStoreFun(MSCState, IsPersistent, Guids)
         || {IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
    {lists:reverse(AllGuids), State1 #state { index_state = IndexState1 }}.

accumulate_ack_init() -> {[], orddict:new(), []}.

accumulate_ack(_SeqId, #m { is_persistent = false, %% ASSERTIONS
                            msg_on_disk = false,
                            index_on_disk = false,
                            guid = Guid },
               {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {PersistentSeqIdsAcc, GuidsByStore, [Guid | AllGuids]};
accumulate_ack(SeqId, {IsPersistent, Guid, _Props},
               {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {cons_if(IsPersistent, SeqId, PersistentSeqIdsAcc),
     rabbit_misc:orddict_cons(IsPersistent, Guid, GuidsByStore),
     [Guid | AllGuids]}.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

remove_confirms(GuidSet, State = #state { msgs_on_disk = MOD,
                                          msg_indices_on_disk = MIOD,
                                          unconfirmed = UC }) ->
    State #state { msgs_on_disk = gb_sets:difference(MOD, GuidSet),
                     msg_indices_on_disk = gb_sets:difference(MIOD, GuidSet),
                     unconfirmed = gb_sets:difference(UC, GuidSet) }.

msgs_confirmed(GuidSet, State) ->
    {gb_sets:to_list(GuidSet), remove_confirms(GuidSet, State)}.

msgs_written_to_disk(QPid, GuidSet) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (State = #state { msgs_on_disk = MOD,
                                  msg_indices_on_disk = MIOD,
                                  unconfirmed = UC }) ->
                    msgs_confirmed(gb_sets:intersection(GuidSet, MIOD),
                                   State #state {
                                     msgs_on_disk =
                                         gb_sets:intersection(
                                           gb_sets:union(MOD, GuidSet), UC) })
            end).

msg_indices_written_to_disk(QPid, GuidSet) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (State = #state { msgs_on_disk = MOD,
                                  msg_indices_on_disk = MIOD,
                                  unconfirmed = UC }) ->
                    msgs_confirmed(gb_sets:intersection(GuidSet, MOD),
                                   State #state {
                                     msg_indices_on_disk =
                                         gb_sets:intersection(
                                           gb_sets:union(MIOD, GuidSet), UC) })
            end).

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

fetch_from_q3(State) -> {empty, State}.

maybe_deltas_to_betas(State) -> State.
