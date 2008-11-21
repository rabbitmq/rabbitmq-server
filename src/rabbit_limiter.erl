%% TODO Decide what to do with the license statement now that Cohesive have
%% bailed.
-module(rabbit_limiter).


% I'm starting out with a gen_server because of the synchronous query
% that the queue process makes
-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([start_link/1]).
-export([set_prefetch_count/2, can_send/2, decrement_capacity/1]).

-record(lim, {prefetch_count = 0,
              ch_pid,
              queues = sets:new(),
              in_use = 0}).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

% Kicks this pig
start_link(ChPid) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [ChPid], []),
    Pid.

set_prefetch_count(LimiterPid, PrefetchCount) ->
    gen_server:cast(LimiterPid, {prefetch_count, PrefetchCount}).

% Queries the limiter to ask whether the queue can deliver a message
% without breaching a limit
can_send(LimiterPid, QPid) ->
    gen_server:call(LimiterPid, {can_send, QPid}).

% Lets the limiter know that a queue has received an ack from a consumer
% and hence can reduce the in-use-by-that queue capcity information
decrement_capacity(LimiterPid) ->
    gen_server:cast(LimiterPid, decrement_capacity).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([ChPid]) ->
    {ok, #lim{ch_pid = ChPid} }.

% This queuries the limiter to ask if it is possible to send a message without
% breaching a limit for this queue process
handle_call({can_send, QPid}, _From, State = #lim{in_use = InUse,
                                                  queues = Queues}) ->
    NewState = State#lim{queues = sets:add_element(QPid, Queues)},
    case limit_reached(NewState) of
        true  -> {reply, false, NewState};
        false ->
                {reply, true, NewState#lim{in_use = InUse + 1}}
    end.

% When the new limit is larger than the existing limit,
% notify all queues and forget about queues with an in-use
% capcity of zero
handle_cast({prefetch_count, PrefetchCount},
            State = #lim{prefetch_count = CurrentLimit})
            when PrefetchCount > CurrentLimit ->
    notify_queues(State),
    {noreply, State#lim{prefetch_count = PrefetchCount,
                        queues = sets:new(),
                        in_use = 0}};

% Default setter of the prefetch count
handle_cast({prefetch_count, PrefetchCount}, State) ->
    {noreply, State#lim{prefetch_count = PrefetchCount}};

% This is an asynchronous ack from a queue that it has received an ack from
% a queue. This allows the limiter to update the the in-use-by-that queue
% capacity infromation.
handle_cast(decrement_capacity, State = #lim{in_use = InUse}) ->
    NewState = decrement_in_use(State),
    ShouldNotify = limit_reached(State) and not(limit_reached(NewState)),
    if
        ShouldNotify ->
            notify_queues(State),
            {noreply, State#lim{queues = sets:new(), in_use = InUse - 1}};
        true ->
            {noreply, NewState}
    end.

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
decrement_in_use(State = #lim{in_use = 0}) ->
    State#lim{in_use = 0};

decrement_in_use(State = #lim{in_use = InUse}) ->
    State#lim{in_use = InUse - 1}.

% Unblocks every queue that this limiter knows about
notify_queues(#lim{ch_pid = ChPid, queues = Queues}) ->
    sets:fold(fun(Q,_) -> rabbit_amqqueue:unblock(Q, ChPid) end, [], Queues).

% A prefetch limit of zero means unlimited
limit_reached(#lim{prefetch_count = 0}) ->
    false;

% Works out whether the limit is breached for the current limiter state
limit_reached(#lim{prefetch_count = Limit, in_use = InUse}) ->
    InUse == Limit.

