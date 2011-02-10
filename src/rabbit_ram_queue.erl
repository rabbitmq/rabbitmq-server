%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc. All rights reserved.
%%

-module(rabbit_ram_queue).

-export([init/3, terminate/1, delete_and_terminate/1,
         purge/1, publish/3, publish_delivered/4, fetch/2, ack/2,
         tx_publish/4, tx_ack/3, tx_rollback/2, tx_commit/4,
         requeue/3, len/1, is_empty/1, dropwhile/2,
         set_ram_duration_target/2, ram_duration/1,
         needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1,
         status/1]).

-export([start/1, stop/0]).

-export([start_msg_store/2, stop_msg_store/0, init/5]).

-behaviour(rabbit_backing_queue).

-record(s,
        { q1,
          q2,
          q3,
          q4,
          next_seq_id,
          pending_ack,
          pending_ack_index,
          ram_ack_index,
          index_s,
          msg_store_clients,
          on_sync,
          durable,
          transient_threshold,
        
          len,
          persistent_count,
        
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
          msg_props
        }).

-record(tx, { pending_messages, pending_acks }).

-record(sync, { acks_persistent, acks_all, pubs, funs }).

-define(IO_BATCH_SIZE, 64).
-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE, msg_store_transient).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(sync() :: #sync { acks_persistent :: [[seq_id()]],
                        acks_all :: [[seq_id()]],
                        pubs :: [{message_properties_transformer(),
                                  [rabbit_types:basic_message()]}],
                        funs :: [fun (() -> any())] }).

-type(s() :: #s {
         q1 :: queue(),
         q2 :: bpqueue:bpqueue(),
         q3 :: bpqueue:bpqueue(),
         q4 :: queue(),
         next_seq_id :: seq_id(),
         pending_ack :: dict(),
         ram_ack_index :: gb_tree(),
         index_s :: any(),
         msg_store_clients :: 'undefined' | {{any(), binary()},
                                             {any(), binary()}},
         on_sync :: sync(),
         durable :: boolean(),
        
         len :: non_neg_integer(),
         persistent_count :: non_neg_integer(),
        
         transient_threshold :: non_neg_integer(),
         msgs_on_disk :: gb_set(),
         msg_indices_on_disk :: gb_set(),
         unconfirmed :: gb_set() }).
-type(state() :: s()).

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
    {AllTerms, StartFS} = rabbit_queue_index:recover(DurableQueues),
    start_msg_store(
      [Ref || Terms <- AllTerms,
              begin
                  Ref = proplists:get_value(persistent_ref, Terms),
                  Ref =/= undefined
              end],
      StartFS).

stop() -> stop_msg_store().

start_msg_store(Refs, StartFS) ->
    ok = rabbit_sup:start_child(?TRANSIENT_MSG_STORE, rabbit_msg_store,
                                [?TRANSIENT_MSG_STORE, rabbit_mnesia:dir(),
                                 undefined, {fun (ok) -> finished end, ok}]),
    ok = rabbit_sup:start_child(?PERSISTENT_MSG_STORE, rabbit_msg_store,
                                [?PERSISTENT_MSG_STORE, rabbit_mnesia:dir(),
                                 Refs, StartFS]).

stop_msg_store() ->
    ok = rabbit_sup:stop_child(?PERSISTENT_MSG_STORE),
    ok = rabbit_sup:stop_child(?TRANSIENT_MSG_STORE).

init(QueueName, IsDurable, Recover) ->
    Self = self(),
    init(QueueName, IsDurable, Recover,
         fun (Guids, ActionTaken) ->
                 msgs_written_to_disk(Self, Guids, ActionTaken)
         end,
         fun (Guids) -> msg_indices_written_to_disk(Self, Guids) end).

init(QueueName, IsDurable, false, MsgOnDiskF, MsgIdxOnDiskF) ->
    IndexS = rabbit_queue_index:init(QueueName, MsgIdxOnDiskF),
    init(IsDurable, IndexS, 0, [],
         case IsDurable of
             true -> msg_store_client_init(?PERSISTENT_MSG_STORE,
                                           MsgOnDiskF);
             false -> undefined
         end,
         msg_store_client_init(?TRANSIENT_MSG_STORE, undefined));

init(QueueName, true, true, MsgOnDiskF, MsgIdxOnDiskF) ->
    Terms = rabbit_queue_index:shutdown_terms(QueueName),
    {PRef, TRef, Terms1} =
        case [persistent_ref, transient_ref] -- proplists:get_keys(Terms) of
            [] -> {proplists:get_value(persistent_ref, Terms),
                   proplists:get_value(transient_ref, Terms),
                   Terms};
            _ -> {rabbit_guid:guid(), rabbit_guid:guid(), []}
        end,
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef,
                                             MsgOnDiskF),
    TransientClient = msg_store_client_init(?TRANSIENT_MSG_STORE, TRef,
                                            undefined),
    {DeltaCount, IndexS} =
        rabbit_queue_index:recover(
          QueueName, Terms1,
          rabbit_msg_store:successfully_recovered_state(?PERSISTENT_MSG_STORE),
          fun (Guid) ->
                  rabbit_msg_store:contains(Guid, PersistentClient)
          end,
          MsgIdxOnDiskF),
    init(true, IndexS, DeltaCount, Terms1,
         PersistentClient, TransientClient).

terminate(S) ->
    S1 = #s { persistent_count = PCount,
              index_s = IndexS,
              msg_store_clients = {MSCSP, MSCST} } =
        remove_pending_ack(true, tx_commit_index(S)),
    PRef = case MSCSP of
               undefined -> undefined;
               _ -> ok = rabbit_msg_store:client_terminate(MSCSP),
                    rabbit_msg_store:client_ref(MSCSP)
           end,
    ok = rabbit_msg_store:client_terminate(MSCST),
    TRef = rabbit_msg_store:client_ref(MSCST),
    Terms = [{persistent_ref, PRef},
             {transient_ref, TRef},
             {persistent_count, PCount}],
    a(S1 #s { index_s = rabbit_queue_index:terminate(
                          Terms, IndexS),
              msg_store_clients = undefined }).

delete_and_terminate(S) ->
    {_PurgeCount, S1} = purge(S),
    S2 = #s { index_s = IndexS,
              msg_store_clients = {MSCSP, MSCST} } =
        remove_pending_ack(false, S1),
    IndexS1 = rabbit_queue_index:delete_and_terminate(IndexS),
    case MSCSP of
        undefined -> ok;
        _ -> rabbit_msg_store:client_delete_and_terminate(MSCSP)
    end,
    rabbit_msg_store:client_delete_and_terminate(MSCST),
    a(S2 #s { index_s = IndexS1,
              msg_store_clients = undefined }).

purge(S = #s { q4 = Q4,
               index_s = IndexS,
               msg_store_clients = MSCS,
               len = Len,
               persistent_count = PCount }) ->
    {LensByStore, IndexS1} = remove_queue_entries(
                               fun rabbit_misc:queue_fold/3, Q4,
                               orddict:new(), IndexS, MSCS),
    {LensByStore1, S1 = #s { q1 = Q1,
                             index_s = IndexS2,
                             msg_store_clients = MSCS1 }} =
        purge_betas_and_deltas(LensByStore,
                               S #s { q4 = queue:new(),
                                      index_s = IndexS1 }),
    {LensByStore2, IndexS3} = remove_queue_entries(
                                fun rabbit_misc:queue_fold/3, Q1,
                                LensByStore1, IndexS2, MSCS1),
    PCount1 = PCount - find_persistent_count(LensByStore2),
    {Len, a(S1 #s { q1 = queue:new(),
                    index_s = IndexS3,
                    len = 0,
                    persistent_count = PCount1 })}.

publish(Msg, MsgProps, S) ->
    {_SeqId, S1} = publish(Msg, MsgProps, false, false, S),
    a(S1).

publish_delivered(false, #basic_message { guid = Guid },
                  _MsgProps, S = #s { len = 0 }) ->
    blind_confirm(self(), gb_sets:singleton(Guid)),
    {undefined, a(S)};
publish_delivered(true, Msg = #basic_message { is_persistent = IsPersistent,
                                               guid = Guid },
                  MsgProps = #message_properties {
                    needs_confirming = NeedsConfirming },
                  S = #s { len = 0,
                           next_seq_id = SeqId,
                           persistent_count = PCount,
                           durable = IsDurable,
                           unconfirmed = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    M = (m(IsPersistent1, SeqId, Msg, MsgProps))
        #m { is_delivered = true },
    {M1, S1} = maybe_write_to_disk(false, false, M, S),
    S2 = record_pending_ack(m(M1), S1),
    PCount1 = PCount + one_if(IsPersistent1),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, Guid, UC),
    {SeqId, a(S2 #s { next_seq_id = SeqId + 1,
		      persistent_count = PCount1,
		      unconfirmed = UC1 })}.

dropwhile(Pred, S) ->
    {_OkOrEmpty, S1} = dropwhile1(Pred, S),
    S1.

dropwhile1(Pred, S) ->
    internal_queue_out(
      fun(M = #m { msg_props = MsgProps }, S1) ->
              case Pred(MsgProps) of
                  true ->
                      {_, S2} = internal_fetch(false, M, S1),
                      dropwhile1(Pred, S2);
                  false ->
                      {M1, S2 = #s { q4 = Q4 }} =
                          read_msg(M, S1),
                      {ok, S2 #s {q4 = queue:in_r(M1, Q4) }}
              end
      end, S).

fetch(AckRequired, S) ->
    internal_queue_out(
      fun(M, S1) ->
              {M1, S2} = read_msg(M, S1),
              internal_fetch(AckRequired, M1, S2)
      end, S).

internal_queue_out(F, S = #s { q4 = Q4 }) ->
    case queue:out(Q4) of
        {empty, _Q4} ->
            case fetch_from_q3(S) of
                {empty, S1} = Result -> a(S1), Result;
                {loaded, {M, S1}} -> F(M, S1)
            end;
        {{value, M}, Q4a} ->
            F(M, S #s { q4 = Q4a })
    end.

read_msg(M = #m { msg = undefined,
                  guid = Guid,
                  is_persistent = IsPersistent },
         S = #s { msg_store_clients = MSCS}) ->
    {{ok, Msg = #basic_message {}}, MSCS1} =
        msg_store_read(MSCS, IsPersistent, Guid),
    {M #m { msg = Msg },
     S #s { msg_store_clients = MSCS1 }};
read_msg(M, S) ->
    {M, S}.

internal_fetch(AckRequired, M = #m {
                              seq_id = SeqId,
                              guid = Guid,
                              msg = Msg,
                              is_persistent = IsPersistent,
                              is_delivered = IsDelivered,
                              msg_on_disk = MsgOnDisk,
                              index_on_disk = IndexOnDisk },
               S = #s {index_s = IndexS,
                       msg_store_clients = MSCS,
                       len = Len,
                       persistent_count = PCount }) ->
    IndexS1 = maybe_write_delivered(
                IndexOnDisk andalso not IsDelivered,
                SeqId, IndexS),

    Rem = fun () ->
                  ok = msg_store_remove(MSCS, IsPersistent, [Guid])
          end,
    Ack = fun () -> rabbit_queue_index:ack([SeqId], IndexS1) end,
    IndexS2 =
        case {AckRequired, MsgOnDisk, IndexOnDisk, IsPersistent} of
            {false, true, false, _} -> Rem(), IndexS1;
            {false, true, true, _} -> Rem(), Ack();
            { true, true, true, false} -> Ack();
            _ -> IndexS1
        end,

    {AckTag, S1} = case AckRequired of
                       true -> SN = record_pending_ack(
                                      M #m {
                                        is_delivered = true }, S),
                               {SeqId, SN};
                       false -> {undefined, S}
                   end,

    PCount1 = PCount - one_if(IsPersistent andalso not AckRequired),
    Len1 = Len - 1,

    {{Msg, IsDelivered, AckTag, Len1},
     a(S1 #s { index_s = IndexS2,
               len = Len1,
               persistent_count = PCount1 })}.

ack(AckTags, S) ->
    a(ack(fun msg_store_remove/3,
          fun (_, S0) -> S0 end,
          AckTags, S)).

tx_publish(Txn, Msg = #basic_message { is_persistent = IsPersistent }, MsgProps,
           S = #s { durable = IsDurable,
                    msg_store_clients = MSCS }) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, MsgProps} | Pubs] }),
    case IsPersistent andalso IsDurable of
        true -> M = m(true, undefined, Msg, MsgProps),
                #m { msg_on_disk = true } =
                    maybe_write_msg_to_disk(false, M, MSCS);
        false -> ok
    end,
    a(S).

tx_ack(Txn, AckTags, S) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }),
    S.

tx_rollback(Txn, S = #s { durable = IsDurable,
                          msg_store_clients = MSCS }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    ok = case IsDurable of
             true -> msg_store_remove(MSCS, true, persistent_guids(Pubs));
             false -> ok
         end,
    {lists:append(AckTags), a(S)}.

tx_commit(Txn, F, MsgPropsF,
          S = #s { durable = IsDurable,
                   msg_store_clients = MSCS }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    PersistentGuids = persistent_guids(Pubs),
    HasPersistentPubs = PersistentGuids =/= [],
    {AckTags1,
     a(case IsDurable andalso HasPersistentPubs of
           true -> ok = msg_store_sync(
                          MSCS, true, PersistentGuids,
                          msg_store_callback(PersistentGuids, Pubs, AckTags1,
                                             F, MsgPropsF)),
                   S;
           false -> tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags1,
                                             F, MsgPropsF, S)
       end)}.

requeue(AckTags, MsgPropsF, S) ->
    MsgPropsF1 = fun (MsgProps) ->
                           (MsgPropsF(MsgProps)) #message_properties {
                             needs_confirming = false }
                   end,
    a(ack(fun msg_store_release/3,
	  fun (#m { msg = Msg, msg_props = MsgProps }, S1) ->
		  {_SeqId, S2} = publish(Msg, MsgPropsF1(MsgProps),
					 true, false, S1),
		  S2;
	      ({IsPersistent, Guid, MsgProps}, S1) ->
		  #s { msg_store_clients = MSCS } = S1,
		  {{ok, Msg = #basic_message{}}, MSCS1} =
		      msg_store_read(MSCS, IsPersistent, Guid),
		  S2 = S1 #s { msg_store_clients = MSCS1 },
		  {_SeqId, S3} = publish(Msg, MsgPropsF1(MsgProps),
					 true, true, S2),
		  S3
	  end,
	  AckTags, S)).

len(#s { len = Len }) -> Len.

is_empty(S) -> 0 == len(S).

set_ram_duration_target(_, S) -> S.

ram_duration(S) -> {0, S}.

needs_idle_timeout(_) -> false.

idle_timeout(S) -> S.

handle_pre_hibernate(S = #s { index_s = IndexS }) ->
    S #s { index_s = rabbit_queue_index:flush(IndexS) }.

status(#s {
          q1 = Q1, q2 = Q2, q3 = Q3, q4 = Q4,
          len = Len,
          pending_ack = PA,
          ram_ack_index = RAI,
          on_sync = #sync { funs = From },
          next_seq_id = NextSeqId,
          persistent_count = PersistentCount } ) ->
    [ {q1 , queue:len(Q1)},
      {q2 , bpqueue:len(Q2)},
      {q3 , bpqueue:len(Q3)},
      {q4 , queue:len(Q4)},
      {len , Len},
      {pending_acks , dict:size(PA)},
      {outstanding_txns , length(From)},
      {ram_ack_count , gb_trees:size(RAI)},
      {next_seq_id , NextSeqId},
      {persistent_count , PersistentCount} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

a(S = #s { q1 = Q1, q2 = Q2, q3 = Q3, q4 = Q4,
           len = Len,
           persistent_count = PersistentCount }) ->
    E1 = queue:is_empty(Q1),
    E2 = bpqueue:is_empty(Q2),
    E3 = bpqueue:is_empty(Q3),
    E4 = queue:is_empty(Q4),
    LZ = Len == 0,

    true = E1 or not E3,
    true = E2,
    true = LZ == (E3 and E4),

    true = Len >= 0,
    true = PersistentCount >= 0,

    S.

m(M = #m { msg = Msg,
           is_persistent = IsPersistent,
           msg_on_disk = MsgOnDisk,
           index_on_disk = IndexOnDisk }) ->
    true = (not IsPersistent) or IndexOnDisk,
    true = (not IndexOnDisk) or MsgOnDisk,
    true = (Msg =/= undefined) or MsgOnDisk,

    M.

one_if(true ) -> 1;
one_if(false) -> 0.

cons_if(true, E, L) -> [E | L];
cons_if(false, _E, L) -> L.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
gb_sets_maybe_insert(true, Val, Set) -> gb_sets:add(Val, Set).

m(IsPersistent, SeqId, Msg = #basic_message { guid = Guid },
  MsgProps) ->
    #m { seq_id = SeqId, guid = Guid, msg = Msg,
         is_persistent = IsPersistent, is_delivered = false,
         msg_on_disk = false, index_on_disk = false,
         msg_props = MsgProps }.

with_msg_store_s({MSCSP, MSCST}, true, F) ->
    {Result, MSCSP1} = F(MSCSP),
    {Result, {MSCSP1, MSCST}};
with_msg_store_s({MSCSP, MSCST}, false, F) ->
    {Result, MSCST1} = F(MSCST),
    {Result, {MSCSP, MSCST1}}.

with_immutable_msg_store_s(MSCS, IsPersistent, F) ->
    {Res, MSCS} = with_msg_store_s(MSCS, IsPersistent,
                                   fun (MSCS1) ->
                                           {F(MSCS1), MSCS1}
                                   end),
    Res.

msg_store_client_init(MsgStore, MsgOnDiskF) ->
    msg_store_client_init(MsgStore, rabbit_guid:guid(), MsgOnDiskF).

msg_store_client_init(MsgStore, Ref, MsgOnDiskF) ->
    rabbit_msg_store:client_init(
      MsgStore, Ref, MsgOnDiskF,
      msg_store_close_fds_fun(MsgStore =:= ?PERSISTENT_MSG_STORE)).

msg_store_write(MSCS, IsPersistent, Guid, Msg) ->
    with_immutable_msg_store_s(
      MSCS, IsPersistent,
      fun (MSCS1) -> rabbit_msg_store:write(Guid, Msg, MSCS1) end).

msg_store_read(MSCS, IsPersistent, Guid) ->
    with_msg_store_s(
      MSCS, IsPersistent,
      fun (MSCS1) -> rabbit_msg_store:read(Guid, MSCS1) end).

msg_store_remove(MSCS, IsPersistent, Guids) ->
    with_immutable_msg_store_s(
      MSCS, IsPersistent,
      fun (MCSS1) -> rabbit_msg_store:remove(Guids, MCSS1) end).

msg_store_release(MSCS, IsPersistent, Guids) ->
    with_immutable_msg_store_s(
      MSCS, IsPersistent,
      fun (MCSS1) -> rabbit_msg_store:release(Guids, MCSS1) end).

msg_store_sync(MSCS, IsPersistent, Guids, Callback) ->
    with_immutable_msg_store_s(
      MSCS, IsPersistent,
      fun (MSCS1) -> rabbit_msg_store:sync(Guids, Callback, MSCS1) end).

msg_store_close_fds(MSCS, IsPersistent) ->
    with_msg_store_s(
      MSCS, IsPersistent,
      fun (MSCS1) -> rabbit_msg_store:close_all_indicated(MSCS1) end).

msg_store_close_fds_fun(IsPersistent) ->
    Self = self(),
    fun () ->
            rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
              Self,
              fun (S = #s { msg_store_clients = MSCS }) ->
                      {ok, MSCS1} =
                          msg_store_close_fds(MSCS, IsPersistent),
                      {[], S #s { msg_store_clients = MSCS1 }}
              end)
    end.

maybe_write_delivered(false, _SeqId, IndexS) ->
    IndexS;
maybe_write_delivered(true, SeqId, IndexS) ->
    rabbit_queue_index:deliver([SeqId], IndexS).

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

beta_fold(F, Init, Q) ->
    bpqueue:foldr(fun (_Prefix, Value, Acc) -> F(Value, Acc) end, Init, Q).

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

init(IsDurable, IndexS, DeltaCount, Terms,
     PersistentClient, TransientClient) ->
    {_, NextSeqId, IndexS1} = rabbit_queue_index:bounds(IndexS),

    DeltaCount1 = proplists:get_value(persistent_count, Terms, DeltaCount),
    S = #s {
      q1 = queue:new(),
      q2 = bpqueue:new(),
      q3 = bpqueue:new(),
      q4 = queue:new(),
      next_seq_id = NextSeqId,
      pending_ack = dict:new(),
      ram_ack_index = gb_trees:empty(),
      index_s = IndexS1,
      msg_store_clients = {PersistentClient, TransientClient},
      on_sync = ?BLANK_SYNC,
      durable = IsDurable,
      transient_threshold = NextSeqId,

      len = DeltaCount1,
      persistent_count = DeltaCount1,

      msgs_on_disk = gb_sets:new(),
      msg_indices_on_disk = gb_sets:new(),
      unconfirmed = gb_sets:new() },
    a(S).

msg_store_callback(PersistentGuids, Pubs, AckTags, F, MsgPropsF) ->
    Self = self(),
    F = fun () -> rabbit_amqqueue:maybe_run_queue_via_backing_queue(
                    Self, fun (SN) -> {[], tx_commit_post_msg_store(
                                             true, Pubs, AckTags,
                                             F, MsgPropsF, SN)}
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

tx_commit_post_msg_store(HasPersistentPubs, Pubs, AckTags, F, MsgPropsF,
                         S = #s {
                           on_sync = OnSync = #sync {
                                       acks_persistent = SPAcks,
                                       acks_all = SAcks,
                                       pubs = SPubs,
                                       funs = SFs },
                           pending_ack = PA,
                           durable = IsDurable }) ->
    PersistentAcks =
        case IsDurable of
            true -> [AckTag || AckTag <- AckTags,
                               case dict:fetch(AckTag, PA) of
                                   #m {} ->
                                       false;
                                   {IsPersistent, _Guid, _MsgProps} ->
                                       IsPersistent
                               end];
            false -> []
        end,
    case IsDurable andalso (HasPersistentPubs orelse PersistentAcks =/= []) of
        true -> S #s {
                  on_sync = #sync {
                    acks_persistent = [PersistentAcks | SPAcks],
                    acks_all = [AckTags | SAcks],
                    pubs = [{MsgPropsF, Pubs} | SPubs],
                    funs = [F | SFs] }};
        false -> S1 = tx_commit_index(
                        S #s {
                          on_sync = #sync {
                            acks_persistent = [],
                            acks_all = [AckTags],
                            pubs = [{MsgPropsF, Pubs}],
                            funs = [F] } }),
                 S1 #s { on_sync = OnSync }
    end.

tx_commit_index(S = #s { on_sync = ?BLANK_SYNC }) ->
    S;
tx_commit_index(S = #s { on_sync = #sync {
                           acks_persistent = SPAcks,
                           acks_all = SAcks,
                           pubs = SPubs,
                           funs = SFs },
                         durable = IsDurable }) ->
    PAcks = lists:append(SPAcks),
    Acks = lists:append(SAcks),
    Pubs = [{Msg, F(MsgProps)} || {F, PubsN} <- lists:reverse(SPubs),
                                    {Msg, MsgProps} <- lists:reverse(PubsN)],
    {SeqIds, S1 = #s { index_s = IndexS }} =
        lists:foldl(
          fun ({Msg = #basic_message { is_persistent = IsPersistent },
                MsgProps},
               {SeqIdsAcc, S2}) ->
                  IsPersistent1 = IsDurable andalso IsPersistent,
                  {SeqId, S3} =
                      publish(Msg, MsgProps, false, IsPersistent1, S2),
                  {cons_if(IsPersistent1, SeqId, SeqIdsAcc), S3}
          end, {PAcks, ack(Acks, S)}, Pubs),
    IndexS1 = rabbit_queue_index:sync(SeqIds, IndexS),
    [ F() || F <- lists:reverse(SFs) ],
    S1 #s { index_s = IndexS1, on_sync = ?BLANK_SYNC }.

purge_betas_and_deltas(LensByStore,
                       S = #s { q3 = Q3,
                                index_s = IndexS,
                                msg_store_clients = MSCS }) ->
    case bpqueue:is_empty(Q3) of
        true -> {LensByStore, S};
        false -> {LensByStore1, IndexS1} =
                     remove_queue_entries(fun beta_fold/3, Q3,
                                          LensByStore, IndexS, MSCS),
                 purge_betas_and_deltas(LensByStore1,
					S #s {
					  q3 = bpqueue:new(),
					  index_s = IndexS1 })
    end.

remove_queue_entries(Fold, Q, LensByStore, IndexS, MSCS) ->
    {GuidsByStore, Delivers, Acks} =
        Fold(fun remove_queue_entries1/2, {orddict:new(), [], []}, Q),
    ok = orddict:fold(fun (IsPersistent, Guids, ok) ->
                              msg_store_remove(MSCS, IsPersistent, Guids)
                      end, ok, GuidsByStore),
    {sum_guids_by_store_to_len(LensByStore, GuidsByStore),
     rabbit_queue_index:ack(Acks,
                            rabbit_queue_index:deliver(Delivers, IndexS))}.

remove_queue_entries1(
  #m { guid = Guid, seq_id = SeqId,
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
        S = #s { q1 = Q1, q3 = Q3, q4 = Q4,
                 next_seq_id = SeqId,
                 len = Len,
                 persistent_count = PCount,
                 durable = IsDurable,
                 unconfirmed = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    M = (m(IsPersistent1, SeqId, Msg, MsgProps))
        #m { is_delivered = IsDelivered, msg_on_disk = MsgOnDisk},
    {M1, S1} = maybe_write_to_disk(false, false, M, S),
    S2 = case bpqueue:is_empty(Q3) of
             false -> S1 #s { q1 = queue:in(m(M1), Q1) };
             true -> S1 #s { q4 = queue:in(m(M1), Q4) }
         end,
    PCount1 = PCount + one_if(IsPersistent1),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, Guid, UC),
    {SeqId, S2 #s { next_seq_id = SeqId + 1,
                    len = Len + 1,
                    persistent_count = PCount1,
                    unconfirmed = UC1 }}.

maybe_write_msg_to_disk(_Force, M = #m {
                                  msg_on_disk = true }, _MSCS) ->
    M;
maybe_write_msg_to_disk(Force, M = #m {
                                 msg = Msg, guid = Guid,
                                 is_persistent = IsPersistent }, MSCS)
  when Force orelse IsPersistent ->
    Msg1 = Msg #basic_message {
             content = rabbit_binary_parser:clear_decoded_content(
                         Msg #basic_message.content)},
    ok = msg_store_write(MSCS, IsPersistent, Guid, Msg1),
    M #m { msg_on_disk = true };
maybe_write_msg_to_disk(_Force, M, _MSCS) ->
    M.

maybe_write_index_to_disk(_Force, M = #m {
                                    index_on_disk = true }, IndexS) ->
    true = M #m.msg_on_disk, %% ASSERTION
    {M, IndexS};
maybe_write_index_to_disk(Force, M = #m {
                                   guid = Guid,
                                   seq_id = SeqId,
                                   is_persistent = IsPersistent,
                                   is_delivered = IsDelivered,
                                   msg_props = MsgProps}, IndexS)
  when Force orelse IsPersistent ->
    true = M #m.msg_on_disk, %% ASSERTION
    IndexS1 = rabbit_queue_index:publish(
                Guid, SeqId, MsgProps, IsPersistent, IndexS),
    {M #m { index_on_disk = true },
     maybe_write_delivered(IsDelivered, SeqId, IndexS1)};
maybe_write_index_to_disk(_Force, M, IndexS) ->
    {M, IndexS}.

maybe_write_to_disk(ForceMsg, ForceIndex, M,
                    S = #s { index_s = IndexS,
                             msg_store_clients = MSCS }) ->
    M1 = maybe_write_msg_to_disk(ForceMsg, M, MSCS),
    {M2, IndexS1} =
        maybe_write_index_to_disk(ForceIndex, M1, IndexS),
    {M2, S #s { index_s = IndexS1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#m { seq_id = SeqId,
                        guid = Guid,
                        is_persistent = IsPersistent,
                        msg_on_disk = MsgOnDisk,
                        msg_props = MsgProps } = M,
                   S = #s { pending_ack = PA,
                            ram_ack_index = RAI}) ->
    {AckEntry, RAI1} =
        case MsgOnDisk of
            true -> {{IsPersistent, Guid, MsgProps}, RAI};
            false -> {M, gb_trees:insert(SeqId, Guid, RAI)}
        end,
    PA1 = dict:store(SeqId, AckEntry, PA),
    S #s { pending_ack = PA1,
           ram_ack_index = RAI1 }.

remove_pending_ack(KeepPersistent,
                   S = #s { pending_ack = PA,
                            index_s = IndexS,
                            msg_store_clients = MSCS }) ->
    {PersistentSeqIds, GuidsByStore} =
        dict:fold(fun accumulate_ack/3, accumulate_ack_init(), PA),
    S1 = S #s { pending_ack = dict:new(),
                ram_ack_index = gb_trees:empty() },
    case KeepPersistent of
        true -> case orddict:find(false, GuidsByStore) of
                    error -> S1;
                    {ok, Guids} -> ok = msg_store_remove(MSCS, false,
                                                         Guids),
                                   S1
                end;
        false -> IndexS1 =
                     rabbit_queue_index:ack(PersistentSeqIds, IndexS),
                 [ok = msg_store_remove(MSCS, IsPersistent, Guids)
                  || {IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
                 S1 #s { index_s = IndexS1 }
    end.

ack(_MsgStoreF, _F, [], S) ->
    S;
ack(MsgStoreF, F, AckTags, S) ->
    {{PersistentSeqIds, GuidsByStore},
     S1 = #s { index_s = IndexS,
               msg_store_clients = MSCS,
               persistent_count = PCount }} =
        lists:foldl(
          fun (SeqId, {Acc, S2 = #s { pending_ack = PA,
                                      ram_ack_index = RAI }}) ->
                  AckEntry = dict:fetch(SeqId, PA),
                  {accumulate_ack(SeqId, AckEntry, Acc),
                   F(AckEntry, S2 #s {
                                   pending_ack = dict:erase(SeqId, PA),
                                   ram_ack_index =
                                       gb_trees:delete_any(SeqId, RAI)})}
          end, {accumulate_ack_init(), S}, AckTags),
    IndexS1 = rabbit_queue_index:ack(PersistentSeqIds, IndexS),
    [ok = MsgStoreF(MSCS, IsPersistent, Guids)
     || {IsPersistent, Guids} <- orddict:to_list(GuidsByStore)],
    PCount1 = PCount - find_persistent_count(sum_guids_by_store_to_len(
                                               orddict:new(), GuidsByStore)),
    S1 #s { index_s = IndexS1,
            persistent_count = PCount1 }.

accumulate_ack_init() -> {[], orddict:new()}.

accumulate_ack(_SeqId, #m { is_persistent = false, %% ASSERTIONS
                            msg_on_disk = false,
                            index_on_disk = false },
               {PersistentSeqIdsAcc, GuidsByStore}) ->
    {PersistentSeqIdsAcc, GuidsByStore};
accumulate_ack(SeqId, {IsPersistent, Guid, _MsgProps},
               {PersistentSeqIdsAcc, GuidsByStore}) ->
    {cons_if(IsPersistent, SeqId, PersistentSeqIdsAcc),
     rabbit_misc:orddict_cons(IsPersistent, Guid, GuidsByStore)}.

find_persistent_count(LensByStore) ->
    case orddict:find(true, LensByStore) of
        error -> 0;
        {ok, Len} -> Len
    end.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

remove_confirms(GuidSet, S = #s { msgs_on_disk = MOD,
                                  msg_indices_on_disk = MIOD,
                                  unconfirmed = UC }) ->
    S #s { msgs_on_disk = gb_sets:difference(MOD, GuidSet),
           msg_indices_on_disk = gb_sets:difference(MIOD, GuidSet),
           unconfirmed = gb_sets:difference(UC, GuidSet) }.

msgs_confirmed(GuidSet, S) ->
    {gb_sets:to_list(GuidSet), remove_confirms(GuidSet, S)}.

blind_confirm(QPid, GuidSet) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (S) -> msgs_confirmed(GuidSet, S) end).

msgs_written_to_disk(QPid, GuidSet, removed) ->
    blind_confirm(QPid, GuidSet);
msgs_written_to_disk(QPid, GuidSet, written) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (S = #s { msgs_on_disk = MOD,
                          msg_indices_on_disk = MIOD,
                          unconfirmed = UC }) ->
                    msgs_confirmed(gb_sets:intersection(GuidSet, MIOD),
                                   S #s {
                                     msgs_on_disk =
                                         gb_sets:intersection(
                                           gb_sets:union(MOD, GuidSet), UC) })
            end).

msg_indices_written_to_disk(QPid, GuidSet) ->
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      QPid, fun (S = #s { msgs_on_disk = MOD,
                          msg_indices_on_disk = MIOD,
                          unconfirmed = UC }) ->
                    msgs_confirmed(gb_sets:intersection(GuidSet, MOD),
                                   S #s {
                                     msg_indices_on_disk =
                                         gb_sets:intersection(
                                           gb_sets:union(MIOD, GuidSet), UC) })
            end).

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

fetch_from_q3(S = #s {
                q1 = Q1,
                q3 = Q3}) ->
    case bpqueue:out(Q3) of
        {empty, _Q3} ->
            {empty, S};
        {{value, _, M}, Q3a} ->
            S1 = S #s { q3 = Q3a },
            S2 =
                case bpqueue:is_empty(Q3a) of
                    true ->
                        S1 #s { q1 = queue:new(),
                                q4 = Q1 };
                     false ->
                        S1
                end,
            {loaded, {M, S2}}
    end.
