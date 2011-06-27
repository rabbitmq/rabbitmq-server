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

-define(RECORD_INDEX(Key, Record),
    erlang:element(2, proplists:lookup(Key, lists:zip(
       record_info(fields, Record), lists:seq(2, record_info(size, Record)))))).

-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-export([prop_backing_queue_test/0]).

-record(state, {bqstate,
                messages,   %% queue of {msg_props, basic_msg}
                acks,       %% list of {acktag, {message_props, basic_msg}}
                confirms}). %% set of msgid

%% Initialise model

initial_state() ->
    #state{bqstate  = qc_variable_queue_init(qc_test_queue()),
           messages = queue:new(),
           acks     = [],
           confirms = gb_sets:new()}.

%% Property

prop_backing_queue_test() ->
    ?FORALL(Cmds, commands(?MODULE, initial_state()),
        begin
            {ok, FileSizeLimit} =
                application:get_env(rabbit, msg_store_file_size_limit),
            application:set_env(rabbit, msg_store_file_size_limit, 512,
                                infinity),
            {ok, MaxJournal} =
                application:get_env(rabbit, queue_index_max_journal_entries),
            application:set_env(rabbit, queue_index_max_journal_entries, 128,
                                infinity),

            {_H, #state{bqstate = BQ}, Res} = run_commands(?MODULE, Cmds),

            application:set_env(rabbit, msg_store_file_size_limit,
                                FileSizeLimit, infinity),
            application:set_env(rabbit, queue_index_max_journal_entries,
                                MaxJournal, infinity),

            rabbit_variable_queue:delete_and_terminate(shutdown, BQ),
            ?WHENFAIL(
                io:format("Result: ~p~n", [Res]),
                aggregate(command_names(Cmds), Res =:= ok))
        end).

%% Commands

command(S) ->
    ?SIZED(Size,
        frequency([{Size, qc_publish(S)},
                   {Size, qc_fetch(S)},
                   {Size, qc_ack(S)},
                   {Size, qc_requeue(S)},
                   {Size, qc_ram(S)},
                   {Size, qc_drain_confirmed(S)},
                   {Size, qc_dropwhile(S)},
                   {1,    qc_purge(S)}])).

qc_publish(#state{bqstate = BQ}) ->
    {call, ?BQMOD, publish,
      [qc_message(),
       #message_properties{needs_confirming = frequency([{1,  true},
                                                         {20, false}]),
                           expiry = choose(0, 10)},
       self(), BQ]}.

qc_fetch(#state{bqstate = BQ}) ->
    {call, ?BQMOD, fetch, [boolean(), BQ]}.

qc_ack(#state{bqstate = BQ, acks = Acks}) ->
    {call, ?BQMOD, ack, [rand_choice(proplists:get_keys(Acks)), BQ]}.

qc_requeue(#state{bqstate = BQ, acks = Acks}) ->
    {call, ?BQMOD, requeue,
      [rand_choice(proplists:get_keys(Acks)), fun(MsgOpts) -> MsgOpts end, BQ]}.

qc_ram(#state{bqstate = BQ}) ->
    {call, ?BQMOD, set_ram_duration_target, [oneof([0, infinity]), BQ]}.

qc_drain_confirmed(#state{bqstate = BQ}) ->
    {call, ?BQMOD, drain_confirmed, [BQ]}.

qc_dropwhile(#state{bqstate = BQ}) ->
    {call, ?BQMOD, dropwhile, [fun dropfun/1, BQ]}.

qc_purge(#state{bqstate = BQ}) ->
    {call, ?BQMOD, purge, [BQ]}.

%% Preconditions

precondition(#state{acks = Acks}, {call, ?BQMOD, Fun, _Arg})
    when Fun =:= ack; Fun =:= requeue ->
    length(Acks) > 0;
precondition(_S, {call, ?BQMOD, _Fun, _Arg}) ->
    true.

%% Model updates

next_state(S, BQ, {call, ?BQMOD, publish, [Msg, MsgProps, _Pid, _BQ]}) ->
    #state{messages = Messages, confirms = Confirms} = S,
    MsgId = {call, erlang, element, [?RECORD_INDEX(id, basic_message), Msg]},
    NeedsConfirm =
        {call, erlang, element,
            [?RECORD_INDEX(needs_confirming, message_properties), MsgProps]},
    Confirms1 = case eval(NeedsConfirm) of
                    true -> gb_sets:add(MsgId, Confirms);
                    _    -> Confirms
                end,
    S#state{bqstate  = BQ,
            messages = queue:in({MsgProps, Msg}, Messages),
            confirms = Confirms1};

next_state(S, Res, {call, ?BQMOD, fetch, [AckReq, _BQ]}) ->
    #state{messages = Messages, acks = Acks} = S,
    ResultInfo = {call, erlang, element, [1, Res]},
    BQ1        = {call, erlang, element, [2, Res]},
    AckTag     = {call, erlang, element, [3, ResultInfo]},
    S1         = S#state{bqstate = BQ1},
    case queue:out(Messages) of
        {empty, _M2}       ->
            S1;
        {{value, MsgProp_Msg}, M2} ->
            S2 = S1#state{messages = M2},
            case AckReq of
                true  -> S2#state{acks = Acks ++ [{AckTag, MsgProp_Msg}]};
                false -> S2
           end
    end;

next_state(S, Res, {call, ?BQMOD, ack, [AcksArg, _BQ]}) ->
    #state{acks = AcksState} = S,
    BQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate = BQ1,
            acks    = propvals_by_keys(AcksState, AcksArg)};

next_state(S, Res, {call, ?BQMOD, requeue, [AcksArg, _F, _V]}) ->
    #state{messages = Messages, acks = AcksState} = S,
    BQ1 = {call, erlang, element, [2, Res]},
    RequeueMsgs = [proplists:get_value(Key, AcksState) || Key <- AcksArg],
    S#state{bqstate  = BQ1,
            messages = queue:join(Messages, queue:from_list(RequeueMsgs)),
            acks     = propvals_by_keys(AcksState, AcksArg)};

next_state(S, BQ, {call, ?BQMOD, set_ram_duration_target, _Args}) ->
    S#state{bqstate = BQ};

next_state(S, Res, {call, ?BQMOD, drain_confirmed, _Args}) ->
    BQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate = BQ1};

next_state(S, BQ1, {call, ?BQMOD, dropwhile, _Args}) ->
    #state{messages = Messages} = S,
    S#state{bqstate = BQ1, messages = drop_messages(Messages)};

next_state(S, Res, {call, ?BQMOD, purge, _Args}) ->
    BQ1 = {call, erlang, element, [2, Res]},
    S#state{bqstate = BQ1, messages = queue:new()}.

%% Postconditions

postcondition(#state{messages = Messages}, {call, ?BQMOD, fetch, _Args}, Res) ->
    case Res of
        {{MsgFetched, _IsDelivered, _AckTag, _Remaining_Len}, _BQ} ->
            {_MsgProps, Msg} = queue:head(Messages),
            MsgFetched =:= Msg;
        {empty, _BQ} ->
            queue:len(Messages) =:= 0
    end;

postcondition(#state{messages = Messages}, {call, ?BQMOD, purge, _Args}, Res) ->
    {PurgeCount, _BQ} = Res,
    queue:len(Messages) =:= PurgeCount;

postcondition(S, {call, ?BQMOD, drain_confirmed, _Args}, Res) ->
    #state{confirms = Confirms} = S,
    {ReportedConfirmed, _BQ} = Res,
    lists:all(fun (M) -> lists:member(M, Confirms) end, ReportedConfirmed);

postcondition(#state{bqstate  = BQ,
                     messages = Messages},
              {call, ?BQMOD, _Fun, _Args}, _Res) ->
    ?BQMOD:len(BQ) =:= queue:len(Messages).

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

dropfun(Props) ->
    Expiry = eval({call, erlang, element,
                       [?RECORD_INDEX(expiry, message_properties), Props]}),
    Expiry =/= 0.

drop_messages(Messages) ->
    case queue:out(Messages) of
        {empty, _} ->
            Messages;
        {{value, MsgProps_Msg}, M2} ->
            MsgProps = {call, erlang, element, [1, MsgProps_Msg]},
            case dropfun(MsgProps) of
                true  -> drop_messages(M2);
                false -> Messages
            end
    end.
