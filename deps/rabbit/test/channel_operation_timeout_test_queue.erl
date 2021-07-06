%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @todo This module also needs to be updated when variable queue changes.
-module(channel_operation_timeout_test_queue).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/6, publish_delivered/5,
         batch_publish/4, batch_publish_delivered/4,
         discard/4, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         set_ram_duration_target/2, ram_duration/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2, set_queue_mode/2,
         start/2, stop/1, zip_msgs_and_acks/4, handle_info/2]).

%%----------------------------------------------------------------------------
%% This test backing queue follows the variable queue implementation, with
%% the exception that it will introduce infinite delays on some operations if
%% the test message has been published, and is awaiting acknowledgement in the
%% queue index. Test message is "timeout_test_msg!".
%%
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(vqstate,
        { q1,
          q2,
          delta,
          q3,
          q4,
          next_seq_id,
          ram_pending_ack,    %% msgs using store, still in RAM
          disk_pending_ack,   %% msgs in store, paged out
          qi_pending_ack,     %% msgs using qi, *can't* be paged out
          index_state,
          msg_store_clients,
          durable,
          transient_threshold,
          qi_embed_msgs_below,

          len,                %% w/o unacked
          bytes,              %% w/o unacked
          unacked_bytes,
          persistent_count,   %% w   unacked
          persistent_bytes,   %% w   unacked
          delta_transient_bytes,        %%

          target_ram_count,
          ram_msg_count,      %% w/o unacked
          ram_msg_count_prev,
          ram_ack_count_prev,
          ram_bytes,          %% w   unacked
          out_counter,
          in_counter,
          rates,
          msgs_on_disk,
          msg_indices_on_disk,
          unconfirmed,
          confirmed,
          ack_out_counter,
          ack_in_counter,
          %% Unlike the other counters these two do not feed into
          %% #rates{} and get reset
          disk_read_count,
          disk_write_count,

          io_batch_size,

          %% default queue or lazy queue
          mode,
          %% number of reduce_memory_usage executions, once it
          %% reaches a threshold the queue will manually trigger a runtime GC
            %% see: maybe_execute_gc/1
          memory_reduction_run_count,
          %% Queue data is grouped by VHost. We need to store it
          %% to work with queue index.
          virtual_host,
          waiting_bump = false
        }).

-record(rates, { in, out, ack_in, ack_out, timestamp }).

-record(msg_status,
        { seq_id,
          msg_id,
          msg,
          is_persistent,
          is_delivered,
          msg_in_store,
          index_on_disk,
          persist_to,
          msg_props
        }).

-record(delta,
        { start_seq_id, %% start_seq_id is inclusive
          count,
          transient,
          end_seq_id    %% end_seq_id is exclusive
        }).


-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(QUEUE, lqueue).
-define(TIMEOUT_TEST_MSG, <<"timeout_test_msg!">>).

%%----------------------------------------------------------------------------

-type seq_id()  :: non_neg_integer().

-type rates() :: #rates { in        :: float(),
                          out       :: float(),
                          ack_in    :: float(),
                          ack_out   :: float(),
                          timestamp :: rabbit_types:timestamp()}.

-type delta() :: #delta { start_seq_id :: non_neg_integer(),
                          count        :: non_neg_integer(),
                          end_seq_id   :: non_neg_integer() }.

%% The compiler (rightfully) complains that ack() and state() are
%% unused. For this reason we duplicate a -spec from
%% rabbit_backing_queue with the only intent being to remove
%% warnings. The problem here is that we can't parameterise the BQ
%% behaviour by these two types as we would like to. We still leave
%% these here for documentation purposes.
-type ack() :: seq_id().
-type state() :: #vqstate {
             q1                    :: ?QUEUE:?QUEUE(),
             q2                    :: ?QUEUE:?QUEUE(),
             delta                 :: delta(),
             q3                    :: ?QUEUE:?QUEUE(),
             q4                    :: ?QUEUE:?QUEUE(),
             next_seq_id           :: seq_id(),
             ram_pending_ack       :: gb_trees:tree(),
             disk_pending_ack      :: gb_trees:tree(),
             qi_pending_ack        :: gb_trees:tree(),
             index_state           :: any(),
             msg_store_clients     :: 'undefined' | {{any(), binary()},
                                                    {any(), binary()}},
             durable               :: boolean(),
             transient_threshold   :: non_neg_integer(),
             qi_embed_msgs_below   :: non_neg_integer(),

             len                   :: non_neg_integer(),
             bytes                 :: non_neg_integer(),
             unacked_bytes         :: non_neg_integer(),

             persistent_count      :: non_neg_integer(),
             persistent_bytes      :: non_neg_integer(),

             target_ram_count      :: non_neg_integer() | 'infinity',
             ram_msg_count         :: non_neg_integer(),
             ram_msg_count_prev    :: non_neg_integer(),
             ram_ack_count_prev    :: non_neg_integer(),
             ram_bytes             :: non_neg_integer(),
             out_counter           :: non_neg_integer(),
             in_counter            :: non_neg_integer(),
             rates                 :: rates(),
             msgs_on_disk          :: gb_sets:set(),
             msg_indices_on_disk   :: gb_sets:set(),
             unconfirmed           :: gb_sets:set(),
             confirmed             :: gb_sets:set(),
             ack_out_counter       :: non_neg_integer(),
             ack_in_counter        :: non_neg_integer(),
             disk_read_count       :: non_neg_integer(),
             disk_write_count      :: non_neg_integer(),

             io_batch_size         :: pos_integer(),
             mode                  :: 'default' | 'lazy',
             virtual_host          :: rabbit_types:vhost() }.
%% Duplicated from rabbit_backing_queue
-spec ack([ack()], state()) -> {[rabbit_guid:guid()], state()}.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(VHost, DurableQueues) ->
    rabbit_variable_queue:start(VHost, DurableQueues).

stop(VHost) ->
    rabbit_variable_queue:stop(VHost).

init(Queue, Recover, Callback) ->
    rabbit_variable_queue:init(Queue, Recover, Callback).

terminate(Reason, State) ->
    rabbit_variable_queue:terminate(Reason, State).

delete_and_terminate(Reason, State) ->
    rabbit_variable_queue:delete_and_terminate(Reason, State).

delete_crashed(Q) ->
    rabbit_variable_queue:delete_crashed(Q).

purge(State = #vqstate { qi_pending_ack= QPA }) ->
    maybe_delay(QPA),
    rabbit_variable_queue:purge(State).

purge_acks(State) ->
    rabbit_variable_queue:purge_acks(State).

publish(Msg, MsgProps, IsDelivered, ChPid, Flow, State) ->
    rabbit_variable_queue:publish(Msg, MsgProps, IsDelivered, ChPid, Flow, State).

batch_publish(Publishes, ChPid, Flow, State) ->
    rabbit_variable_queue:batch_publish(Publishes, ChPid, Flow, State).

publish_delivered(Msg, MsgProps, ChPid, Flow, State) ->
    rabbit_variable_queue:publish_delivered(Msg, MsgProps, ChPid, Flow, State).

batch_publish_delivered(Publishes, ChPid, Flow, State) ->
    rabbit_variable_queue:batch_publish_delivered(Publishes, ChPid, Flow, State).

discard(_MsgId, _ChPid, _Flow, State) -> State.

drain_confirmed(State) ->
    rabbit_variable_queue:drain_confirmed(State).

dropwhile(Pred, State) ->
    rabbit_variable_queue:dropwhile(Pred, State).

fetchwhile(Pred, Fun, Acc, State) ->
    rabbit_variable_queue:fetchwhile(Pred, Fun, Acc, State).

fetch(AckRequired, State) ->
    rabbit_variable_queue:fetch(AckRequired, State).

drop(AckRequired, State) ->
    rabbit_variable_queue:drop(AckRequired, State).

ack(List, State) ->
    rabbit_variable_queue:ack(List, State).

requeue(AckTags, #vqstate { qi_pending_ack = QPA } = State) ->
    maybe_delay(QPA),
    rabbit_variable_queue:requeue(AckTags, State).

ackfold(MsgFun, Acc, State, AckTags) ->
    rabbit_variable_queue:ackfold(MsgFun, Acc, State, AckTags).

fold(Fun, Acc, State) ->
    rabbit_variable_queue:fold(Fun, Acc, State).

len(#vqstate { qi_pending_ack = QPA } = State) ->
    maybe_delay(QPA),
    rabbit_variable_queue:len(State).

is_empty(State) -> 0 == len(State).

depth(State) ->
    rabbit_variable_queue:depth(State).

set_ram_duration_target(DurationTarget, State) ->
    rabbit_variable_queue:set_ram_duration_target(DurationTarget, State).

ram_duration(State) ->
    rabbit_variable_queue:ram_duration(State).

needs_timeout(State) ->
    rabbit_variable_queue:needs_timeout(State).

timeout(State) ->
    rabbit_variable_queue:timeout(State).

handle_pre_hibernate(State) ->
    rabbit_variable_queue:handle_pre_hibernate(State).

handle_info(Msg, State) ->
    rabbit_variable_queue:handle_info(Msg, State).

resume(State) -> rabbit_variable_queue:resume(State).

msg_rates(State) ->
    rabbit_variable_queue:msg_rates(State).

info(Info, State) ->
    rabbit_variable_queue:info(Info, State).

invoke(Module, Fun, State) -> rabbit_variable_queue:invoke(Module, Fun, State).

is_duplicate(Msg, State) -> rabbit_variable_queue:is_duplicate(Msg, State).

set_queue_mode(Mode, State) ->
    rabbit_variable_queue:set_queue_mode(Mode, State).

zip_msgs_and_acks(Msgs, AckTags, Accumulator, State) ->
    rabbit_variable_queue:zip_msgs_and_acks(Msgs, AckTags, Accumulator, State).

%% Delay
maybe_delay(QPA) ->
  case is_timeout_test(gb_trees:values(QPA)) of
    true -> receive
              %% The queue received an EXIT message, it's probably the
              %% node being stopped with "rabbitmqctl stop". Thus, abort
              %% the wait and requeue the EXIT message.
              {'EXIT', _, shutdown} = ExitMsg -> self() ! ExitMsg,
                                                 void
            after infinity -> void
            end;
    _ -> void
  end.

is_timeout_test([]) -> false;
is_timeout_test([#msg_status{
                    msg = #basic_message{
                             content = #content{
                                          payload_fragments_rev = PFR}}}|Rem]) ->
  case lists:member(?TIMEOUT_TEST_MSG, PFR) of
    T = true -> T;
    _        -> is_timeout_test(Rem)
  end;
is_timeout_test([_|Rem]) -> is_timeout_test(Rem).
