%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

%% Unconfirmed messages tracking.

%% A message should be confirmed only when all queues confirm

%% Messages are published to multiple queues while each queue may be
%% represented by several processes (queue refs).

%% Queue refs return confirmations, rejections, may fail or disconnect.
%% If a queue ref fails - the messgae should be rejected.
%% If all queue refs for a queue disconnect (not fail) without confirmation -
%% the messge should be rejected.

%% For simplicity, disconnects do not return reject until all message refs
%% confirm or disconnect.


-module(unconfirmed_messages).


-export([new/0,
         insert/5,
         confirm_msg_ref/4,
         confirm_multiple_msg_ref/4,
         forget_ref/2,

         reject_msg/2,
         reject_all_for_queue/2,

         smallest/1,
         size/1,
         is_empty/1]).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).
-define(SET_VALUE, []).

-type queue_ref() :: term().
-type msg_id() :: term().
-type queue_name() :: rabbit_amqqueue:name().
-type exchange_name() :: rabbit_exchange:name().
-type map_set(Type) :: #{Type => ?SET_VALUE}.


%% refs is a set of refs waiting for confirm
%% queue_status shows which queues had at least one confirmation

-record(msg_status,
    {refs = #{}         :: map_set(queue_ref()),
     queue_status = #{} :: #{queue_name() => confirmed | rejected},
     exchange           :: exchange_name()}).

%% ordered set is needed to get unconfirmed cutoff
%% index contains message statuses of all message IDs
%% reverse index is needed to locate message IDs from queue refs
-record(unconfirmed,
    {ordered = gb_sets:new() :: gb_sets:set(msg_id()),
     index = #{}             :: #{msg_id() => #msg_status{}},
     reverse = #{}           :: #{queue_ref() => #{msg_id() => ?SET_VALUE}}}).

-opaque ?MODULE() :: #unconfirmed{}.

%%----------------------------------------------------------------------------

-spec new() -> ?MODULE().
new() -> #unconfirmed{}.

%% Insert and entry. Fails if there already is an entry with the given
%% message id.

-spec insert(msg_id(), [queue_name()], [queue_ref()], exchange_name(), ?MODULE()) -> ?MODULE().
insert(MsgId, QueueNames, QueueRefs, XName,
       #unconfirmed{ordered = Ordered,
                    index   = Index,
                    reverse = Reverse} = UC) ->

    case maps:get(MsgId, Index, none) of
        none ->
            UC#unconfirmed{
                ordered      = gb_sets:add(MsgId, Ordered),
                index        =
                    Index#{MsgId =>
                        #msg_status{
                            refs = maps:from_list([{QR, ?SET_VALUE} || QR <- QueueRefs]),
                            queue_status = maps:from_list([{QN, rejected} || QN <- QueueNames]),
                            exchange = XName}},
                reverse = lists:foldl(
                              fun
                                 (Ref, R) ->
                                    case R of
                                        #{Ref := MsgIdsSet} ->
                                            R#{Ref => MsgIdsSet#{MsgId => ?SET_VALUE}};
                                        _ ->
                                            R#{Ref => #{MsgId => ?SET_VALUE}}
                                    end
                              end,
                              Reverse, QueueRefs)
                };
        _ ->
            error({message_already_exists, MsgId, QueueNames, QueueRefs, XName, UC})
    end.

%% Standard confirmation.
%% Removes the queue ref from waiting, if it was the last one -
%% return confirmed and cleanup the message id state
%% If the ref was not the last one - update queues status for the queue
%% to confirmed

-spec confirm_msg_ref(msg_id(), queue_name(), queue_ref(), ?MODULE()) ->
    {{confirmed | rejected, {msg_id(), exchange_name()}} | not_confirmed, ?MODULE()}.
confirm_msg_ref(MsgId, QueueName, QueueRef,
                #unconfirmed{reverse = Reverse} = UC) ->
    remove_msg_ref(confirm, MsgId, QueueName, QueueRef,
                   UC#unconfirmed{reverse = remove_from_reverse(QueueRef, [MsgId], Reverse)}).

-spec confirm_multiple_msg_ref(msg_id(), queue_name(), queue_ref(), ?MODULE()) ->
    {{confirmed | rejected, {msg_id(), exchange_name()}} | not_confirmed, ?MODULE()}.
confirm_multiple_msg_ref(MsgIds, QueueName, QueueRef,
                         #unconfirmed{reverse = Reverse} = UC0) ->
    lists:foldl(
        fun(MsgId, {C, UC}) ->
            case remove_msg_ref(confirm, MsgId, QueueName, QueueRef, UC) of
                {{confirmed, V}, UC1} -> {[V | C], UC1};
                {not_confirmed, UC1}  -> {C, UC1}
            end
        end,
        {[], UC0#unconfirmed{reverse = remove_from_reverse(QueueRef, MsgIds, Reverse)}},
        MsgIds).

%% Remove queue ref from waiting for all messages based on reverse index.
%% If there are no more refs left for the message - return either
%% confirmed or rejected.
%% Confirmed is returned if all queues have queue status confirmed,
%% which means that each queue has at least one ref (process) confirmed.
%% Returns lists of confirmed and rejected messages.
-spec forget_ref(queue_ref(), ?MODULE()) ->
    {Confirmed :: [{msg_id(), exchange_name()}],
     Rejected :: [{msg_id(), exchange_name()}],
     ?MODULE()}.
forget_ref(QueueRef, #unconfirmed{reverse = Reverse0} = UC0) ->
    MsgIds = maps:keys(maps:get(QueueRef, Reverse0, #{})),
    lists:foldl(fun(MsgId, {C, R, UC}) ->
        case remove_msg_ref(no_confirm, MsgId, ignore, QueueRef, UC) of
            {not_confirmed, UC1}  -> {C, R, UC1};
            {{confirmed, V}, UC1} -> {[V | C], R, UC1};
            {{rejected, V}, UC1}  -> {C, [V | R], UC1}
        end
    end,
    {[], [], UC0#unconfirmed{reverse = maps:remove(QueueRef, Reverse0)}},
    MsgIds).

%% Cleanup message id
%% Return rejected if there was a message with
%% such ID.
-spec reject_msg(msg_id(), ?MODULE()) ->
    {{rejected, {msg_id(), exchange_name()}} | not_confirmed, ?MODULE()}.
reject_msg(MsgId, #unconfirmed{ordered = Ordered, index = Index, reverse = Reverse} = UC) ->
    case maps:get(MsgId, Index, none) of
        none ->
            {not_confirmed, UC};
        #msg_status{exchange = XName,
                    refs = Refs} ->
            {{rejected, {MsgId, XName}},
             UC#unconfirmed{ordered = gb_sets:del_element(MsgId, Ordered),
                            index   = maps:remove(MsgId, Index),
                            reverse = remove_multiple_from_reverse(maps:keys(Refs), [MsgId], Reverse)}}
    end.

%% Cleanup message ids for all messages referencing ref
%% Based on reverse index.
%% Returns a list of rejected messages
-spec reject_all_for_queue(queue_ref(), ?MODULE()) ->
    {Rejected :: [{msg_id(), exchange_name()}], ?MODULE()}.
reject_all_for_queue(QueueRef, #unconfirmed{reverse = Reverse0} = UC0) ->
    MsgIds = maps:keys(maps:get(QueueRef, Reverse0, #{})),
    lists:foldl(fun(MsgId, {R, UC}) ->
        case reject_msg(MsgId, UC) of
            {not_confirmed, UC1} -> {R, UC1};
            {{rejected, V}, UC1} -> {[V | R], UC1}
        end
    end,
    {[], UC0#unconfirmed{reverse = maps:remove(QueueRef, Reverse0)}},
    MsgIds).

%% Returns a smallest message id.
-spec smallest(?MODULE()) -> msg_id().
smallest(#unconfirmed{ordered = Ordered}) ->
    gb_sets:smallest(Ordered).

-spec size(?MODULE()) -> msg_id().
size(#unconfirmed{index = Index}) -> maps:size(Index).

-spec is_empty(?MODULE()) -> boolean().
is_empty(#unconfirmed{index = Index, reverse = Reverse, ordered = Ordered} = UC) ->
    case maps:size(Index) == 0 of
        true ->
            %% Assertion
            case maps:size(Reverse) == gb_sets:size(Ordered)
                andalso
                 maps:size(Reverse) == 0 of
                true  -> ok;
                false -> error({size_mismatch, UC})
            end,
            true;
        _ ->
            false
    end.

-spec remove_from_reverse(queue_ref(), [msg_id()],
                          #{queue_ref() => #{msg_id() => ?SET_VALUE}}) ->
    #{queue_ref() => #{msg_id() => ?SET_VALUE}}.
remove_from_reverse(QueueRef, MsgIds, Reverse) when is_list(MsgIds) ->
    case maps:get(QueueRef, Reverse, none) of
        none ->
            Reverse;
        MsgIdsSet ->
            NewMsgIdsSet = maps:without(MsgIds, MsgIdsSet),
            case maps:size(NewMsgIdsSet) > 0 of
                true  -> Reverse#{QueueRef => NewMsgIdsSet};
                false -> maps:remove(QueueRef, Reverse)
            end
    end.

-spec remove_multiple_from_reverse([queue_ref()], [msg_id()],
                                   #{queue_ref() => #{msg_id() => ?SET_VALUE}}) ->
    #{queue_ref() => #{msg_id() => ?SET_VALUE}}.
remove_multiple_from_reverse(Refs, MsgIds, Reverse0) ->
    lists:foldl(
        fun(Ref, Reverse) ->
            remove_from_reverse(Ref, MsgIds, Reverse)
        end,
        Reverse0,
        Refs).

-spec remove_msg_ref(confirm | no_confirm, msg_id(), queue_name(), queue_ref(), ?MODULE()) ->
    {{confirmed | rejected, {msg_id(), exchange_name()}} | not_confirmed,
     ?MODULE()}.
remove_msg_ref(Confirm, MsgId, QueueName, QueueRef,
               #unconfirmed{ordered = Ordered, index = Index} = UC) ->
    case maps:get(MsgId, Index, none) of
        none ->
            {not_confirmed, UC};
        #msg_status{refs = #{QueueRef := ?SET_VALUE} = Refs,
                    queue_status = QStatus,
                    exchange = XName} = MsgStatus ->
            QStatus1 = case {Confirm, QueueName} of
                            {no_confirm, _} -> QStatus;
                            {_, ignore}     -> QStatus;
                            {confirm, _}    -> QStatus#{QueueName => confirmed}
                        end,
            case maps:size(Refs) == 1 of
                true ->
                    {{confirm_status(QStatus1), {MsgId, XName}},
                     UC#unconfirmed{
                        ordered = gb_sets:del_element(MsgId, Ordered),
                        index = maps:remove(MsgId, Index)}};
                false ->
                    {not_confirmed,
                     UC#unconfirmed{
                        index = Index#{MsgId =>
                            MsgStatus#msg_status{
                                refs = maps:remove(QueueRef, Refs),
                                queue_status = QStatus1}}}}
            end;
        _ -> {not_confirmed, UC}
    end.

-spec confirm_status(#{queue_name() => confirmed | rejected}) -> confirmed | rejected.
confirm_status(QueueStatus) ->
    case lists:all(fun(confirmed) -> true; (_) -> false end,
                   maps:values(QueueStatus)) of
        true  -> confirmed;
        false -> rejected
    end.


