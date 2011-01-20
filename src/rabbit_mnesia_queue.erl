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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2011 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2011 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2011 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mnesia_queue).

-export(
   [start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
    tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

%%----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, with all msgs in Mnesia.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% This module wraps msgs into M records for internal use, including
%% additional information. Pending acks are also recorded in memory as
%% Ms.
%%
%% All queues are durable in this version, and all msgs are treated as
%% persistent. (This will no doubt break some tests for non-durable
%% queues.)
%% ----------------------------------------------------------------------------

%% BUG: Need to provide better back-pressure when queue is filling up.

-behaviour(rabbit_backing_queue).

-record(s,                        % The in-RAM queue state
        { mnesia_table_name,      % An atom holding the Mnesia table name
          q,                      % The in-Mnesia queue of Ms
          next_seq_id,            % The next seq_id to use to build an M
          pending_ack_dict,       % The Mnesia seq_id->M map, pending ack
          txn_dict,               % In-progress txn->tx map
          busy                    % Recursive calls not allowed
        }).

-record(m,                        % A wrapper aroung a msg
        { seq_id,                 % The seq_id for the msg
          msg,                    % The msg itself
          props,                  % The msg properties
          is_delivered            % Has the msg been delivered? (for reporting)
        }).

-record(tx,
        { to_pub,
          to_ack }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id() | 'blank_ack').

-type(s() :: #s { mnesia_table_name :: atom(),
                  q :: maybe(queue()),
                  next_seq_id :: seq_id(),
                  pending_ack_dict :: dict(),
                  txn_dict :: dict(),
                  busy :: boolean() }).
-type(state() :: s()).

-type(m() :: #m { msg :: rabbit_types:basic_message(),
                  seq_id :: seq_id(),
                  props :: rabbit_types:message_properties(),
                  is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [{rabbit_types:basic_message(),
                                rabbit_types:message_properties()}],
                    to_ack :: [seq_id()] }).

-include("rabbit_backing_queue_spec.hrl").

%% -endif.

%%----------------------------------------------------------------------------
%% Public API
%%
%% Specs are in rabbit_backing_queue_spec.hrl but are repeated here.

%%----------------------------------------------------------------------------
%% start/1 promises that a list of (durable) queue names will be
%% started in the near future. This lets us perform early checking
%% necessary for the consistency of those queues or initialise other
%% shared resources. This function creates an Mnesia transaction to
%% run in, and must not be called from inside another Mnesia
%% transaction.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(_DurableQueues) ->
    rabbit_log:info("start(_) ->"),
    rabbit_log:info(" -> ok"),
    ok.

%%----------------------------------------------------------------------------
%% stop/0 tears down all state/resources upon shutdown. It might not
%% be called. This function creates an Mnesia transaction to run in,
%% and must not be called from inside another Mnesia transaction.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() ->
    rabbit_log:info("stop(_) ->"),
    rabbit_log:info(" -> ok"),
    ok.

%%----------------------------------------------------------------------------
%% init/3 creates one backing queue, returning its state. Names are
%% local to the vhost, and must be unique. This function creates
%% Mnesia transactions to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).

init(QueueName, _IsDurable, _Recover) ->
    rabbit_log:info("init(~p, _, _) ->", [QueueName]),
    MnesiaTableName = mnesia_table_name(QueueName),
    %% It's unfortunate that tables cannot be created or deleted
    %% within an Mnesia transaction!
    Attributes = record_info(fields, s),
    case mnesia:create_table(
           MnesiaTableName, [{record_name, s}, {attributes, Attributes}])
    of
        {atomic, ok} -> ok;
        {aborted, {already_exists, MnesiaTableName}} ->
            s = mnesia:table_info(MnesiaTableName, record_name),
            Attributes = mnesia:table_info(MnesiaTableName, attributes),
            ok
    end,
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  #s { mnesia_table_name = MnesiaTableName,
                       q = {just, queue:new()},
                       next_seq_id = 0,
                       pending_ack_dict = dict:new(),
                       txn_dict = dict:new(),
                       busy = false}
          end),
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% terminate/1 is called on queue shutdown when the queue isn't being
%% deleted. This function creates an Mnesia transaction to run in, and
%% must not be called from inside another Mnesia transaction.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(S = #s { busy = false }) ->
    rabbit_log:info("terminate(~p) ->", [S]),
    S1 = S #s { busy = true },
    {atomic, S3} =
        mnesia:transaction(
          fun () ->
                  RS = S1 #s { pending_ack_dict = dict:new() },
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% delete_and_terminate/1 is called when the queue is terminating and
%% needs to delete all its content. The only difference between purge
%% and delete is that delete also needs to delete everything that's
%% been delivered and not ack'd. This function creates an Mnesia
%% transaction to run in, and must not be called from inside another
%% Mnesia transaction.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.

delete_and_terminate(S) ->
    rabbit_log:info("delete_and_terminate(~p) ->", [S]),
    S1 = S #s { busy = true },
    {atomic, S3} =
        mnesia:transaction(
          fun () ->
                  RS = S1 #s { q = {just, queue:new()},
                               pending_ack_dict = dict:new() },
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% purge/1 removes all msgs in the queue, but not msgs that have been
%% fetched and are pending acks. This function creates an Mnesia
%% transaction to run in, and must not be called from inside another
%% Mnesia transaction.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(S = #s { q = {just, Q}, busy = false }) ->
    rabbit_log:info("purge(~p) ->", [S #s { busy = true }]),
    S1 = S #s { busy = true },
    {atomic, {A, S3}} =
        mnesia:transaction(
          fun () ->
                  RS = S1 #s { q = {just, queue:new()} },
                  {queue:len(Q), RS}
          end),
    Result = {A, S3 #s { busy = false }},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% publish/3 publishes a msg. This function creates an Mnesia
%% transaction to run in, and must not be called from inside another
%% Mnesia transaction. All msgs are silently treated as persistent.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, Props, S = #s { busy = false }) ->
    rabbit_log:info("publish("),
    rabbit_log:info(" ~p,", [Msg]),
    rabbit_log:info(" ~p,", [Props]),
    rabbit_log:info(" ~p) ->", [S]),
    S1 = S #s { busy = true },
    {atomic, S3} =
        mnesia:transaction(
          fun () ->
                  RS = publish_state(Msg, Props, false, S1),
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% publish_delivered/4 is called for msgs that have already been
%% passed straight out to a client. The queue will be empty for these
%% calls (saving the round trip through the backing queue). All msgs
%% are silently treated as non-persistent. This function creates an
%% Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction. All msgs are silently treated as
%% persistent.
%%
%% -spec(publish_delivered/4 ::
%%         (ack_required(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> {ack(), state()}).

publish_delivered(false, _, _, S = #s { busy = false }) ->
    rabbit_log:info("publish_delivered(false, _, _,"),
    rabbit_log:info(" ~p) ->", [S]),
    Result = {blank_ack, S},
    rabbit_log:info(" -> ~p", [Result]),
    Result;
publish_delivered(true,
                  Msg,
                  Props,
                  S = #s { next_seq_id = SeqId, busy = false }) ->
    rabbit_log:info("publish_delivered(true, "),
    rabbit_log:info(" ~p,", [Msg]),
    rabbit_log:info(" ~p,", [Props]),
    rabbit_log:info(" ~p) ->", [S]),
    S1 = S #s { busy = true },
    {atomic, {A, B}} =
        mnesia:transaction(
          fun () ->
                  RS = (record_pending_ack_state(
                       (m(Msg, SeqId, Props)) #m { is_delivered = true }, S1))
                      #s { next_seq_id = SeqId + 1 },
                  {SeqId, RS}
          end),
    Result = {A, B #s { busy = false }},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% dropwhile/2 drops msgs from the head of the queue while the
%% supplied predicate returns true. This function creates an Mnesia
%% transaction to run in, and must not be called from inside another
%% Mnesia transaction.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, S = #s { busy = false }) ->
    rabbit_log:info("dropwhile(~p, ~p) ->", [Pred, S]),
    S1 = S #s { busy = true },
    {atomic, {_, S3}} =
        mnesia:transaction(
          fun () ->
                  RS = dropwhile_state(Pred, S1),
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% fetch/2 produces the next msg. This function creates an Mnesia
%% transaction to run in, and must not be called from inside another
%% Mnesia transaction.
%%
%% -spec(fetch/2 :: (ack_required(), state()) ->
%%                       {ok | fetch_result(), state()}).

fetch(AckRequired, S = #s { busy = false }) ->
    rabbit_log:info("fetch(~p, ~p) ->", [AckRequired, S]),
    S1 = S #s { busy = true },
    {atomic, {R, S3}} =
        mnesia:transaction(
          fun () ->
                  {DR, RS} =
                      internal_queue_out(
                        fun (M, Si) -> internal_fetch(AckRequired, M, Si) end,
                        S1),
                  {DR, RS}
          end),
    Result = {R, S3 #s { busy = false } },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges msgs names by SeqIds. Maps SeqIds to guids upon
%% return. This function creates an Mnesia transaction to run in, and
%% must not be called from inside another Mnesia transaction.
%%
%% The following spec is wrong, as a blank_ack cannot be passed back in.
%%
%% -spec(ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

ack(SeqIds, S = #s { busy = false }) ->
    rabbit_log:info("ack("),
    rabbit_log:info("~p,", [SeqIds]),
    rabbit_log:info(" ~p) ->", [S]),
    S1 = S #s { busy = true },
    {atomic, {Guids, S3}} =
        mnesia:transaction(
          fun () ->
                  {Guids, RS} = internal_ack(SeqIds, S1),
                  {Guids, RS}
          end),
    Result = {Guids, S3 # s { busy = false }},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish, but in the context of an AMQP
%% transaction. It stores the msg and its properties in the to_pub
%% field of the txn, waiting to be committed. This function creates an
%% Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, Props, S = #s { busy = false }) ->
    rabbit_log:info("tx_publish(~p, ~p, ~p, ~p) ->", [Txn, Msg, Props, S]),
    S1 = S #s { busy = true },
    {atomic, S3} =
        mnesia:transaction(
          fun () ->
                  Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, S1),
                  RS = store_tx(Txn,
                                Tx #tx { to_pub = [{Msg, Props} | Pubs] },
                                S1),
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_ack/3 acks, but in the context of an AMQP transaction. It stores
%% the seq_id in the acks field of the txn, waiting to be
%% committed. This function creates an Mnesia transaction to run in,
%% and must not be called from inside another Mnesia transaction.
%%
%% The following spec is wrong, as a blank_ack cannot be passed back in.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, SeqIds, S = #s { busy = false }) ->
    rabbit_log:info("tx_ack(~p, ~p, ~p) ->", [Txn, SeqIds, S]),
    S1 = S #s { busy = true },
    {atomic, S3} =
        mnesia:transaction(
          fun () ->
                  Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, S1),
                  RS =
                      store_tx(Txn,
                               Tx #tx {
                                 to_ack = lists:append(SeqIds, SeqIds0) },
                               S1),
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_rollback/2 undoes anything that has been done in the context of
%% the specified AMQP transaction. It returns the state with to_pub
%% and to_ack erased. This function creates an Mnesia transaction to
%% run in, and must not be called from inside another Mnesia
%% transaction.
%%
%% The following spec is wrong, as a blank_ack cannot be passed back in.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, S = #s { busy = false }) ->
    rabbit_log:info("tx_rollback(~p, ~p) ->", [Txn, S]),
    S1 = S #s { busy = true },
    {atomic, {A, B}} =
        mnesia:transaction(
          fun () ->
                  #tx { to_ack = SeqIds } = lookup_tx(Txn, S1),
                  RS = erase_tx(Txn, S),
                  {SeqIds, RS}
          end),
    Result = {A, B #s { busy = false }},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits an AMQP transaction. The F passed in must be
%% called once the msgs have really been commited. This CPS permits
%% the possibility of commit coalescing. This function creates an
%% Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% The following spec is wrong, blank_acks cannot be returned.
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, F, PropsF, S = #s { busy = false }) ->
    rabbit_log:info(
      "tx_commit(~p, ~p, ~p, ~p) ->", [Txn, F, PropsF, S]),
    S1 = S #s { busy = true },
    {atomic, {A, B}} =
        mnesia:transaction(
          fun () ->
                  #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, S1),
                  RS =
                      tx_commit_state(Pubs, SeqIds, PropsF, erase_tx(Txn, S1)),
                  {SeqIds, RS}
          end),
    F(),
    Result = {A, B #s { busy = false }},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts msgs into the queue that have already been
%% delivered and were pending acknowledgement. This function creates
%% an Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% The following spec is wrong, as blank_acks cannot be passed back in.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(SeqIds, PropsF, S = #s { busy = false }) ->
    rabbit_log:info("requeue(~p, ~p, ~p) ->", [SeqIds, PropsF, S]),
    S1 = S #s { busy = true },
    {atomic, S3} =
        mnesia:transaction(
          fun () ->
                  {_, RS} =
                      internal_ack3(
                        fun (#m { msg = Msg, props = Props }, Si) ->
                                publish_state(Msg, PropsF(Props), true, Si)
                        end,
                        SeqIds,
                        S1),
                  RS
          end),
    Result = S3 #s { busy = false },
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% len/1 returns the queue length. It should be Mnesia transactional
%% but it is not.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#s { q = {just, Q}, busy = false }) ->
    rabbit_log:info("len(~p) ->", [Q]),
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  queue:len(Q)
          end),
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% is_empty/1 returns true if the queue is empty, and false
%% otherwise. It should be Mnesia transactional but it is not.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(#s { q = {just, Q}, busy = false }) ->
    rabbit_log:info("is_empty(~p)", [Q]),
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  queue:is_empty(Q)
          end),
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% set_ram_duration_target states that the target is to have no more
%% msgs in RAM than indicated by the duration and the current queue
%% rates. It is ignored in this implementation. This function creates
%% an Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% -spec(set_ram_duration_target/2 ::
%%         (('undefined' | 'infinity' | number()), state())
%%         -> state()).

set_ram_duration_target(_, S = #s { busy = false }) -> S.

%%----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the msgs in RAM represent given the current rates of the
%% queue. It is a dummy in this implementation. This function creates
%% an Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(S = #s { busy = false }) -> {0, S}.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns true if idle_timeout should be called
%% as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and false otherwise. It always
%% returns false in this implementation. This function creates an
%% Mnesia transaction to run in, and must not be called from inside
%% another Mnesia transaction.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(#s { busy = false }) -> false.

%%----------------------------------------------------------------------------
%% idle_timeout/1 is called (eventually) after needs_idle_timeout
%% returns true. It is a dummy in this implementation. This function
%% creates an Mnesia transaction to run in, and must not be called
%% from inside another Mnesia transaction.
%%
%% -spec(idle_timeout/1 :: (state()) -> state()).

idle_timeout(S = #s { busy = false }) -> S.

%%----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates. It is a dummy in this implementation. This function
%% creates an Mnesia transaction to run in, and must not be called
%% from inside another Mnesia transaction.
%%
%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(S = #s { busy = false }) -> S.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging and operational purposes, to be able
%% to expose state via rabbitmqctl. This function should be Mnesia
%% transactional but it is not.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#s { q = {just, Q},
            next_seq_id = NextSeqId,
            pending_ack_dict = PAD,
            busy = false}) ->
    [{len, queue:len(Q)}, {next_seq_id, NextSeqId}, {acks, dict:size(PAD)}].

%%----------------------------------------------------------------------------
%% Various internal helper functions. All functions are pure below
%% this point, and all Ss are busy and have non-nothing qs.
%% ----------------------------------------------------------------------------

-spec internal_queue_out(fun ((m(), state()) -> T), state()) ->
                                {empty, state()} | T.

internal_queue_out(F, S = #s { q = {just, Q} }) ->
    case queue:out(Q) of
        {empty, _} -> {empty, S};
        {{value, M}, Qa} -> F(M, S #s { q = {just, Qa} })
    end.

-spec internal_fetch/3 :: (ack_required(), m(), s()) -> {fetch_result(), s()}.

internal_fetch(AckRequired,
               M = #m {
                 seq_id = SeqId,
                 msg = Msg,
                 is_delivered = IsDelivered },
               S = #s { q = {just, Q} }) ->
    {Ack, S1} =
        case AckRequired of
            true ->
                {SeqId,
                 record_pending_ack_state(
                   M #m { is_delivered = true }, S)};
            false -> {blank_ack, S}
        end,
    {{Msg, IsDelivered, Ack, queue:len(Q)}, S1}.

-spec(dropwhile_state/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), s())
        -> {empty | ok, s()}).

dropwhile_state(Pred, S) ->
    internal_queue_out(
      fun (M = #m { props = Props }, Si = #s { q = {just, Q} }) ->
              case Pred(Props) of
                  true ->
                      {_, Si1} = internal_fetch(false, M, Si),
                      dropwhile_state(Pred, Si1);
                  false -> {ok, Si #s {q = {just,queue:in_r(M, Q)} }}
              end
      end,
      S).

-spec(internal_ack/2 :: ([seq_id()], s()) -> {[rabbit_guid:guid()], s()}).

internal_ack(SeqIds, S) ->
    internal_ack3(fun (_, Si) -> Si end, SeqIds, S).

-spec tx_commit_state([rabbit_types:basic_message()],
                      [seq_id()],
                      message_properties_transformer(),
                      s()) ->
                             s().

tx_commit_state(Pubs, SeqIds, PropsF, S) ->
    {_, S1} = internal_ack(SeqIds, S),
    lists:foldl(
      fun ({Msg, Props}, Si) -> publish_state(Msg, Props, false, Si) end,
      S1,
      [{Msg, PropsF(Props)} || {Msg, Props} <- lists:reverse(Pubs)]).

-spec m(rabbit_types:basic_message(),
        seq_id(),
        rabbit_types:message_properties()) ->
               m().

m(Msg, SeqId, Props) ->
    #m { seq_id = SeqId, msg = Msg, props = Props, is_delivered = false }.

-spec lookup_tx(rabbit_types:txn(), s()) -> tx().

lookup_tx(Txn, #s { txn_dict = TxnDict }) ->
    case dict:find(Txn, TxnDict) of
        error -> #tx { to_pub = [], to_ack = [] };
        {ok, Tx} -> Tx
    end.

-spec store_tx(rabbit_types:txn(), tx(), s()) -> s().

store_tx(Txn, Tx, S = #s { txn_dict = TxnDict }) ->
    S #s { txn_dict = dict:store(Txn, Tx, TxnDict) }.

-spec erase_tx(rabbit_types:txn(), s()) -> s().

erase_tx(Txn, S = #s { txn_dict = TxnDict }) ->
    S #s { txn_dict = dict:erase(Txn, TxnDict) }.

-spec publish_state(rabbit_types:basic_message(),
                    rabbit_types:message_properties(),
                    boolean(),
                    s()) ->
                           s().

publish_state(Msg,
              Props,
              IsDelivered,
              S = #s { q = {just, Q}, next_seq_id = SeqId }) ->
    S #s {
      q = {just,
           queue:in(
               (m(Msg, SeqId, Props)) #m { is_delivered = IsDelivered }, Q)},
      next_seq_id = SeqId + 1 }.

-spec record_pending_ack_state(m(), s()) -> s().

record_pending_ack_state(M = #m { seq_id = SeqId },
                         S = #s { pending_ack_dict = PAD }) ->
    S #s { pending_ack_dict = dict:store(SeqId, M, PAD) }.

-spec internal_ack3(fun (([rabbit_guid:guid()], s()) -> s()),
                    [rabbit_guid:guid()],
                    s()) ->
                           {[rabbit_guid:guid()], s()}.

internal_ack3(_, [], S) -> {[], S};
internal_ack3(F, SeqIds, S) ->
    {AllGuids, S1} =
        lists:foldl(
          fun (SeqId, {Acc, Si = #s { pending_ack_dict = PAD }}) ->
                  M = dict:fetch(SeqId, PAD),
                  {[m_guid(M) | Acc],
                   F(M, Si #s { pending_ack_dict = dict:erase(SeqId, PAD)})}
          end,
          {[], S},
          SeqIds),
    {lists:reverse(AllGuids), S1}.

-spec m_guid(m()) -> rabbit_guid:guid().

m_guid(#m { msg = #basic_message { guid = Guid }}) -> Guid.

%% Convert a queue name (a record) into an Mnesia table name (an atom).

%% TODO: Import correct argument type.

-spec mnesia_table_name(_) -> atom().

mnesia_table_name(QueueName) ->
    list_to_atom(lists:flatten(io_lib:format("~p", [QueueName]))).

