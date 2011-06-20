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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_backing_queue_qc).

-behaviour(proper_statem).

-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-export([prop_backing_queue_test/0]).


-record(state, {bqstate,
                messages,
                acks}).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("proper/include/proper.hrl").

initial_state() ->
    VQ = qc_variable_queue_init(qc_test_queue()),
    #state{bqstate=VQ, messages = [], acks = []}.

prop_backing_queue_test() ->
    ?FORALL(Cmds, commands(?MODULE, initial_state()),
        begin
            {_H, _S, Res} = run_commands(?MODULE, Cmds),
            ?WHENFAIL(
                 io:format("Result: ~p~n", [Res]),
                 aggregate(command_names(Cmds), Res =:= ok))
        end).

%% Commands

command(S) ->
    frequency([{5, qc_publish(S)},
               {3, qc_fetch(S)},
               {2, qc_ack(S)},
               {2, qc_requeue(S)},
               {1, qc_ram(S)}]).

qc_publish(#state{bqstate = VQ}) ->
    {call, rabbit_variable_queue, publish,
        [qc_message(), #message_properties{}, self(), VQ]}.

qc_fetch(#state{bqstate = VQ}) ->
    {call, rabbit_variable_queue, fetch, [true, VQ]}.

qc_ack(#state{bqstate = VQ, acks = Acks}) ->
    {call, rabbit_variable_queue, ack, [sublist(Acks), VQ]}.

qc_requeue(#state{bqstate = VQ, acks = Acks}) ->
    {call, rabbit_variable_queue, requeue,
        [sublist(Acks), fun(MsgOpts) -> MsgOpts end, VQ]}.

qc_ram(#state{bqstate = VQ}) ->
    {call, rabbit_variable_queue, set_ram_duration_target,
        [oneof([0, infinity]), VQ]}.

%% Preconditions

precondition(_S, {call, rabbit_variable_queue, Fun, _Arg})
    when Fun =:= publish; Fun =:= fetch; Fun =:= set_ram_duration_target ->
    true;
precondition(#state{acks = Acks}, {call, rabbit_variable_queue, Fun, _Arg})
    when Fun =:= ack; Fun =:= requeue ->
    length(Acks) > 0.

%% Next state

next_state(S, VQ, {call, rabbit_variable_queue, publish,
                      [Msg, _MsgProps, _Pid, _VQ]}) ->
    #state{messages = Messages} = S,
    S#state{bqstate=VQ, messages= [Msg | Messages]};

next_state(S, Res, {call, rabbit_variable_queue, fetch, [AckReq, _VQ]}) ->
    #state{messages = M, acks = Acks} = S,
    ResultDetails = {call, erlang, element, [1, Res]},
    AckTag = {call, erlang, element, [3, ResultDetails]},
    VQ1 = {call, erlang, element, [2, Res]},
    S1  = S#state{bqstate = VQ1},
    case M of
         []    -> S1;
         [_|_] -> Msg = lists:last(M),
                  case AckReq of
                      true  -> S1#state{messages = M    -- [Msg],
                                         acks     = Acks ++ [AckTag]};
                      false -> throw(non_ack_not_supported)
                  end
    end;

next_state(S, Res, {call, rabbit_variable_queue, ack, [AcksArg, _VQ]}) ->
    #state{acks = AcksState} = S,
    VQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate = VQ1,
            acks    = AcksState -- AcksArg};

next_state(S, Res, {call, rabbit_variable_queue, requeue, [AcksArg, _F, _V]}) ->
    #state{messages = Messages, acks = AcksState} = S,
    VQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate  = VQ1,
            messages = AcksArg   ++ Messages,
            acks     = AcksState -- AcksArg};

next_state(S, VQ, {call, rabbit_variable_queue, set_ram_duration_target, _A}) ->
    S#state{bqstate = VQ}.

%% Postconditions

postcondition(#state{bqstate  = VQ,
                     messages = Messages},
              {call, _Mod, _Fun, _Args}, _Res) ->
    rabbit_variable_queue:len(VQ) =:= length(Messages).

%% Helpers

qc_message_payload() ->
    binary().

qc_routing_key() ->
    noshrink(binary(10)).

qc_delivery_mode() ->
    oneof([1, 2]).

qc_message() ->
    qc_message(qc_delivery_mode()).

qc_message(DeliveryMode) ->
    {call, rabbit_basic, message, [
        qc_default_exchange(),
        qc_routing_key(),
        #'P_basic'{delivery_mode = DeliveryMode},
        qc_message_payload()]}.

qc_default_exchange() ->
    {call, rabbit_misc, r, [<<>>, exchange, <<>>]}.

qc_variable_queue_init(Q) ->
    {call, rabbit_variable_queue, init,
        [Q, false, nop(2), nop(2), nop(2), nop(1)]}.

qc_test_q() ->
    {call, rabbit_misc, r, [<<"/">>, queue, noshrink(binary(16))]}.

qc_test_queue() ->
    qc_test_queue(boolean()).

qc_test_queue(Durable) ->
    #amqqueue{name        = qc_test_q(),
              durable     = Durable,
              auto_delete = false,
              arguments   = [],
              pid         = self()}.

nop(N) -> function(N, ok).

sublist(List) ->
    case List of
        []  -> [];
        _   -> Item = lists:nth(random:uniform(length(List)), List),
               case random:uniform(3) of
                   1 -> [Item];
                   _ -> [Item | sublist(List -- [Item])]
               end
    end.
