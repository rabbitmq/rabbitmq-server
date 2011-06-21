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
-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("proper/include/proper.hrl").

-behaviour(proper_statem).

-define(BQMOD, rabbit_variable_queue).

-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-export([prop_backing_queue_test/0]).


-record(state, {bqstate,
                messages,
                acks}).


initial_state() ->
    VQ = qc_variable_queue_init(qc_test_queue()),
    #state{bqstate=VQ, messages = queue:new(), acks = []}.

prop_backing_queue_test() ->
    ?FORALL(Cmds, commands(?MODULE, initial_state()),
        begin
            {_H, #state{bqstate = VQ}, Res} = run_commands(?MODULE, Cmds),
            rabbit_variable_queue:delete_and_terminate(shutdown, VQ),
            ?WHENFAIL(
                io:format("Result: ~p~n", [Res]),
                aggregate(command_names(Cmds), Res =:= ok))
        end).

%% Commands

command(#state{bqstate = VQ} = S) ->
    ?SIZED(Size,
           frequency([{Size, qc_publish(S)},
                      {Size, qc_fetch(S)},
                      {Size, qc_ack(S)},
                      {Size, qc_requeue(S)},
                      {Size, qc_ram(S)},
                      {1,    {call, ?BQMOD, purge, [VQ]}}])).

qc_publish(#state{bqstate = VQ}) ->
    {call, ?BQMOD, publish,
      [qc_message(), #message_properties{}, self(), VQ]}.

qc_fetch(#state{bqstate = VQ}) ->
    {call, ?BQMOD, fetch, [boolean(), VQ]}.

qc_ack(#state{bqstate = VQ, acks = Acks}) ->
    {call, ?BQMOD, ack, [rand_choice(proplists:get_keys(Acks)), VQ]}.

qc_requeue(#state{bqstate = VQ, acks = Acks}) ->
    {call, ?BQMOD, requeue,
      [rand_choice(proplists:get_keys(Acks)), fun(MsgOpts) -> MsgOpts end, VQ]}.

qc_ram(#state{bqstate = VQ}) ->
    {call, ?BQMOD, set_ram_duration_target,
      [oneof([0, infinity]), VQ]}.

%% Preconditions

precondition(#state{acks = Acks}, {call, ?BQMOD, Fun, _Arg})
    when Fun =:= ack; Fun =:= requeue ->
    length(Acks) > 0;
precondition(_S, {call, ?BQMOD, _Fun, _Arg}) ->
    true.

%% Next state

next_state(S, VQ, {call, ?BQMOD, publish, [Msg, _MsgProps, _Pid, _VQ]}) ->
    #state{messages = Messages} = S,
    S#state{bqstate = VQ, messages = queue:in(Msg, Messages)};

next_state(S, Res, {call, ?BQMOD, fetch, [AckReq, _VQ]}) ->
    #state{messages = Messages, acks = Acks} = S,
    ResultInfo = {call, erlang, element, [1, Res]},
    VQ1        = {call, erlang, element, [2, Res]},
    AckTag     = {call, erlang, element, [3, ResultInfo]},
    S1         = S#state{bqstate = VQ1},
    case queue:out(Messages) of
        {empty, _M2}       ->
            S1;
        {{value, Msg}, M2} ->
            S2 = S1#state{messages = M2},
            case AckReq of
                true  -> S2#state{acks = Acks ++ [{AckTag, Msg}]};
                false -> S2
           end
    end;

next_state(S, Res, {call, ?BQMOD, ack, [AcksArg, _VQ]}) ->
    #state{acks = AcksState} = S,
    VQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate = VQ1,
            acks    = propvals_by_keys(AcksState, AcksArg)};

next_state(S, Res, {call, ?BQMOD, requeue, [AcksArg, _F, _V]}) ->
    #state{messages = Messages, acks = AcksState} = S,
    VQ1 = {call, erlang, element, [2, Res]},
    RequeueMsgs = [proplists:get_value(Key, AcksState) || Key <- AcksArg ],
    S#state{bqstate  = VQ1,
            messages = queue:join(Messages, queue:from_list(RequeueMsgs)),
            acks     = propvals_by_keys(AcksState, AcksArg)};

next_state(S, VQ, {call, ?BQMOD, set_ram_duration_target, _A}) ->
    S#state{bqstate = VQ};

next_state(S, Res, {call, ?BQMOD, purge, _A}) ->
    VQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate = VQ1, messages = queue:new()}.

%% Postconditions

postcondition(#state{messages = Messages}, {call, ?BQMOD, fetch, _Args}, Res) ->
    case Res of
        {{MsgFetched, _IsDelivered, _AckTag, _Remaining_Len}, _VQ} ->
            MsgFetched =:= queue:head(Messages);
        {empty, _VQ} ->
            queue:len(Messages) =:= 0
    end;

postcondition(#state{messages = Messages}, {call, ?BQMOD, purge, _Args}, Res) ->
    {PurgeCount, _VQ} = Res,
    queue:len(Messages) =:= PurgeCount;

postcondition(#state{bqstate  = VQ,
                     messages = Messages},
              {call, ?BQMOD, _Fun, _Args}, _Res) ->
    ?BQMOD:len(VQ) =:= queue:len(Messages).

%% Helpers

qc_message_payload() ->
    ?SIZED(Size, resize(Size * Size, binary())).

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
    {call, ?BQMOD, init,
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

propvals_by_keys(Props, Keys) ->
    lists:filter(fun ({Key, _Msg}) ->
                     not lists:member(Key, Keys)
                 end, Props).

rand_choice(List) ->
    case List of
        []  -> [];
        _   -> [lists:nth(random:uniform(length(List)), List)]
    end.
