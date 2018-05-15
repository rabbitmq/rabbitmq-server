-module(rabbit_queue).

-export([
         stat/1,
         ack/5,
         reject/6,
         basic_get/6,
         declare/2,
         deliver/3,
         delete/4
         ]).

-type state() :: term().

-record(rq_state, {
          unconfirmed = dtree:new() :: dtree:dtree(),
          mandatory = dtree:new() :: dtree:dtree()
                  }).

%% state management functions
%% confirm
%% 

-opaque rq_state() :: #rq_state{}.

-export_type([
              state/0,
              rq_state/0
              ]).

-include_lib("rabbit_common/include/rabbit.hrl").

% -type queue_state() :: term().
-type amqqueue() :: #amqqueue{}.

-type maybe_state() :: undefined | state().

-type queue_states() :: #{atom() => term()}.

-type rabbit_queue_modules() :: rabbit_classic_queue |
                                rabbit_quorum_queue.

-type pid_or_id() :: pid() | {atom(), node()}.

-type qlen() :: rabbit_types:ok(non_neg_integer()).

-type msg_id() :: non_neg_integer().

-callback init_state(pid_or_id(), rabbit_types:r(queue)) ->
    state().

-callback ack(pid_or_id(), rabbit_types:ctag(), [msg_id()], queue_states()) ->
    queue_states().

-callback basic_get(amqqueue(), ChPid :: pid(), NoAck :: boolean(),
                    LimiterPid :: pid(), rabbit_types:ctag(), maybe_state()) ->
          {'ok', non_neg_integer(), rabbit_amqqueue:qmsg(), maybe_state()} |
          {'empty', maybe_state()}.

-callback declare(amqqueue(), node()) ->
    {new, amqqueue()} |
    {existing | absent | ownder_died, amqqueue()}.

-callback deliver([amqqueue()], rabbit_types:delivery(), queue_states()) ->
    {[pid () | {atom(), node()}], queue_states()}.

-callback delivery_target(amqqueue(), Acc :: #{rabbit_queue_modules() => term()}) ->
    {pid_or_id(), queue_states()}.

-callback delete(amqqueue(), boolean(), boolean(), rabbit_types:username()) ->
    qlen() |
    rabbit_types:error('in_use') |
    rabbit_types:error('not_empty').

%%TODO
%%
%% basic_consume
%%
%% basic_cancel

-callback stat(amqqueue()) ->
    {ok, non_neg_integer(), non_neg_integer()}.


-spec stat(amqqueue()) -> {ok, non_neg_integer(), non_neg_integer()}.
stat(#amqqueue{type = {Mod, _}} = Q) ->
    Mod:stat(Q).

-spec ack({module(), atom()}, pid_or_id(), rabbit_types:ctag(), [msg_id()],
          queue_states()) -> queue_states().
ack({Mod, Key}, Pid, CTag, MsgIds, QStates) ->
    QS0 = maps:get(Key, QStates, undefined),
    {ok, Qs} = Mod:ack(Pid, CTag, MsgIds, QS0),
    maps:put(Key, Qs, QStates).

-spec reject({module(), atom()}, pid_or_id(), rabbit_types:ctag(),
             boolean(), [msg_id()], queue_states()) -> queue_states().
reject({Mod, Key}, Pid, CTag, Requeue, MsgIds, QStates) ->
    QS0 = maps:get(Key, QStates, undefined),
    {ok, Qs} = Mod:reject(Pid, Requeue, CTag, MsgIds, QS0),
    maps:put(Key, Qs, QStates).

-spec basic_get(amqqueue(), ChPid :: pid(), NoAck :: boolean(),
                LimiterPid :: pid(), rabbit_types:ctag(), maybe_state()) ->
    {'ok', non_neg_integer(), rabbit_amqqueue:qmsg(), queue_states()} |
    {'empty', queue_states()}.
basic_get(#amqqueue{type = {Mod, Key} = Id,
                    pid = PidOrId,
                    name = QName} = Q, ChPid, NoAck, LimiterPid, CTag,
          QStates) ->
    QState0 = get_queue_state(Id, PidOrId, QName, QStates),
    case Mod:basic_get(Q, ChPid, NoAck, LimiterPid, CTag, QState0) of
        {empty, QState} ->
            {empty, update_queue_state(Key, QState, QStates)};
        {ok, Count, Msg, QState} ->
            {ok, Count, Msg, update_queue_state(Key, QState, QStates)}
    end.

-spec declare(amqqueue(), node()) ->
    {'new', rabbit_types:amqqueue(), state()}.
declare(#amqqueue{type = {Mod, _}} = Q, Node) ->
    Mod:declare(Q, Node).

-spec delete(amqqueue(), boolean(), boolean(), rabbit_types:user()) ->
    {'new', rabbit_types:amqqueue(), state()}.
delete(#amqqueue{type = {Mod, _}} = Q, IfUnused, IfEmpty, ActingUser) ->
    Mod:delete(Q, IfUnused, IfEmpty, ActingUser).

%% TODO: refine types
-spec deliver([amqqueue()], rabbit_types:delivery(), queue_states()) ->
    {[pid () | {atom(), node()}], queue_states()}.
deliver(Qs, Delivery, QueueStates) ->
    %% TODO only fold once
    Delivered = lists:foldl(fun (#amqqueue{pid = Pid}, Acc0) ->
                                    [Pid | Acc0]
                            end, [], Qs),
    Targets = lists:foldl(fun (#amqqueue{type = {Mod, _}} = Q, Acc0) ->
                                  Mod:delivery_target(Q, Acc0)
                          end, #{}, Qs),
    {Delivered,
     maps:fold(fun (Mod, Target, QStates) ->
                       Mod:deliver(Target, Delivery, QStates)
               end, QueueStates, Targets)}.

update_queue_state(_, undefined, QStates) ->
    QStates;
update_queue_state(Name, QState, QStates) ->
    maps:put(Name, QState, QStates).

get_queue_state({Mod, Name}, PidOrId, QName, QStates) ->
    case maps:get(Name, QStates, undefined) of
        undefined ->
            %% TODO: optimise in case of classic queue
            Mod:init_state(PidOrId, QName);
        S ->
            S
    end.
