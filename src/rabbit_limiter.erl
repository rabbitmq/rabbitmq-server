%% TODO Decide what to do with the license statement now that Cohesive have
%% bailed.
-module(rabbit_limiter).


% I'm starting out with a gen_server because of the synchronous query
% that the queue process makes
-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([start_link/1]).
-export([set_prefetch_count/2, can_send/2, decrement_capacity/2]).

-record(lim, {prefetch_count = 0,
              ch_pid,
              in_use = dict:new()}).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

% Kicks this pig
start_link(ChPid) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [ChPid], []),
    Pid.

set_prefetch_count(LimiterPid, PrefetchCount) ->
    gen_server:call(LimiterPid, {prefetch_count, PrefetchCount}).

% Queries the limiter to ask whether the queue can deliver a message
% without breaching a limit
can_send(LimiterPid, QPid) ->
    gen_server:call(LimiterPid, {can_send, QPid}).

% Lets the limiter know that a queue has received an ack from a consumer
% and hence can reduce the in-use-by-that queue capcity information
decrement_capacity(LimiterPid, QPid) ->
    gen_server:cast(LimiterPid, {decrement_capacity, QPid}).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([ChPid]) ->
    {ok, #lim{ch_pid = ChPid} }.

% This queuries the limiter to ask if it is possible to send a message without
% breaching a limit for this queue process
handle_call({can_send, QPid}, _From, State) ->
    case limit_reached(State) of
        true  -> {reply, false, State};
        false -> {reply, true, update_in_use_capacity(QPid, State)}
    end;

% When the new limit is larger than the existing limit,
% notify all queues and forget about queues with an in-use
% capcity of zero
handle_call({prefetch_count, PrefetchCount}, _From,
            State = #lim{prefetch_count = CurrentLimit})
            when PrefetchCount > CurrentLimit ->
    % TODO implement this requirement
    {reply, ok, State#lim{prefetch_count = PrefetchCount}};

% Default setter of the prefetch count
handle_call({prefetch_count, PrefetchCount}, _From, State) ->
    {reply, ok, State#lim{prefetch_count = PrefetchCount}}.

% This is an asynchronous ack from a queue that it has received an ack from
% a queue. This allows the limiter to update the the in-use-by-that queue
% capacity infromation.
handle_cast({decrement_capacity, QPid}, State) ->
    NewState = decrement_in_use(QPid, State),
    ShouldNotify = limit_reached(State) and not(limit_reached(NewState)),
    if
        ShouldNotify -> notify_queues(State);
        true         -> ok
    end,
    {noreply, NewState}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    State.

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

% Reduces the in-use-count of the queue by one
decrement_in_use(QPid, State = #lim{in_use = InUse}) ->
    NewInUse = dict:update_counter(QPid, -1, InUse),
    Count = dict:fetch(QPid, NewInUse),
    if
        Count < 1 ->
            State#lim{in_use = dict:erase(QPid, NewInUse)};
        true ->
            State#lim{in_use = NewInUse}
    end.

% Unblocks every queue that this limiter knows about
notify_queues(#lim{ch_pid = ChPid, in_use = InUse}) ->
    dict:map(fun(Q,_) -> rabbit_amqqueue:unblock(Q, ChPid) end, InUse).

% Computes the current aggregrate capacity of all of the in-use queues
current_capacity(#lim{in_use = InUse}) ->
    % TODO It *seems* expensive to compute this on the fly
    dict:fold(fun(_, PerQ, Acc) -> PerQ + Acc end, 0, InUse).

% A prefetch limit of zero means unlimited
limit_reached(#lim{prefetch_count = 0}) ->
    false;

% Works out whether the limit is breached for the current limiter state
limit_reached(State = #lim{prefetch_count = Limit}) ->
    current_capacity(State) == Limit.

% Increments the counter for the in-use-capacity of a particular queue
update_in_use_capacity(QPid, State = #lim{in_use = InUse}) ->
    State#lim{in_use = dict:update_counter(QPid, 1, InUse)}.

