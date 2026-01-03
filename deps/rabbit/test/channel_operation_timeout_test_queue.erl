%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(channel_operation_timeout_test_queue).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/5, publish_delivered/4,
         discard/3, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/3,
         ackfold/5, len/1, is_empty/1, depth/1,
         update_rates/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2,
         start/2, stop/1, zip_msgs_and_acks/4, handle_info/2]).

%% Removed in 4.3. @todo Remove in next LTS.
-export([set_queue_mode/2, set_queue_version/2]).

%%----------------------------------------------------------------------------
%% This test backing queue follows the variable queue implementation, with
%% the exception that it will introduce infinite delays on some operations if
%% the test message has been published, and is awaiting acknowledgement in the
%% queue index. Test message is "timeout_test_msg!".
%%
%% Only access the #msg_status{} record needs to be updated if it changes.
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(msg_status,
        { seq_id,
          msg_id,
          msg,
          is_persistent,
          is_delivered,
          msg_location, %% ?IN_SHARED_STORE | ?IN_QUEUE_STORE | ?IN_QUEUE_INDEX | ?IN_MEMORY
          index_on_disk,
          persist_to,
          msg_props
        }).

-include_lib("rabbit_common/include/rabbit.hrl").
-define(TIMEOUT_TEST_MSG, <<"timeout_test_msg!">>).

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

purge(State) ->
    QPA = ram_pending_acks(State),
    maybe_delay(QPA),
    rabbit_variable_queue:purge(State).

purge_acks(State) ->
    rabbit_variable_queue:purge_acks(State).

publish(Msg, MsgProps, IsDelivered, ChPid, State) ->
    rabbit_variable_queue:publish(Msg, MsgProps, IsDelivered, ChPid, State).

publish_delivered(Msg, MsgProps, ChPid, State) ->
    rabbit_variable_queue:publish_delivered(Msg, MsgProps, ChPid, State).

discard(_MsgId, _ChPid, State) -> State.

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

requeue(AckTags, DelFailed, State) ->
    QPA = ram_pending_acks(State),
    maybe_delay(QPA),
    rabbit_variable_queue:requeue(AckTags, DelFailed, State).

ackfold(MsgFun, Acc, State, AckTags, DelFailed) ->
    rabbit_variable_queue:ackfold(MsgFun, Acc, State, AckTags, DelFailed).

len(State) ->
    QPA = ram_pending_acks(State),
    maybe_delay(QPA),
    rabbit_variable_queue:len(State).

is_empty(State) -> 0 == len(State).

depth(State) ->
    rabbit_variable_queue:depth(State).

update_rates(State) ->
    rabbit_variable_queue:update_rates(State).

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

zip_msgs_and_acks(Msgs, AckTags, Accumulator, State) ->
    rabbit_variable_queue:zip_msgs_and_acks(Msgs, AckTags, Accumulator, State).

set_queue_mode(_, State) ->
    State.

set_queue_version(_, State) ->
    State.

ram_pending_acks(State) ->
    case erlang:function_exported(rabbit_variable_queue, ram_pending_acks, 1) of
        true -> rabbit_variable_queue:ram_pending_acks(State);
        %% For v4.2.x because the state has changed.
        false -> element(9, State)
    end.

%% Delay
maybe_delay(QPA) ->
  %% The structure for ram_pending_acks has changed to maps in 3.12.
  Values = case is_map(QPA) of
    true -> maps:values(QPA);
    false -> gb_trees:values(QPA)
  end,
  case is_timeout_test(Values) of
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
