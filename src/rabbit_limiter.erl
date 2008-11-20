-module(rabbit_limiter).


% I'm starting out with a gen_server because of the synchronous query
% that the queue process makes
-behviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([start_link/1]).
-export([can_send/2, decrement_capacity/2]).

-record(lim, {prefetch_count = 1,
              ch_pid,
              blocked = false,
              in_use = dict:new()}).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

% Kicks this pig
start_link(ChPid) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [ChPid], []),
    Pid.

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
    {CanSend, NewState} = maybe_can_send(QPid, State),
    {reply, CanSend, NewState}.

% This is an asynchronous ack from a queue that it has received an ack from
% a queue. This allows the limiter to update the the in-use-by-that queue
% capacity infromation.
handle_cast({decrement_capacity, QPid}, State) ->
    State1 = decrement_in_use(QPid, State),
    State2 = maybe_notify_queues(State1),
    {noreply, State2}.

% When the prefetch count has not been set,
% e.g. when the channel has not yet been issued a basic.qos
handle_info({prefetch_count, PrefetchCount},
            State = #lim{prefetch_count = 0}) ->
    {noreply, State#lim{prefetch_count = PrefetchCount}};

% When the new limit is larger than the existing limit,
% notify all queues and forget about queues with an in-use
% capcity of zero
handle_info({prefetch_count, PrefetchCount},
            State = #lim{prefetch_count = CurrentLimit})
            when PrefetchCount > CurrentLimit ->
    % TODO implement this requirement
    {noreply, State#lim{prefetch_count = PrefetchCount}};

% Default setter of the prefetch count
handle_info({prefetch_count, PrefetchCount}, State) ->
    {noreply, State#lim{prefetch_count = PrefetchCount}}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    State.

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

% Reduces the in-use-count of the queue by one
decrement_in_use(QPid, State = #lim{in_use = InUse}) ->
    case dict:find(QPid, InUse) of
        {ok, Capacity} ->
            if
                % Is there a lower bound on capacity?
                % i.e. what is the zero mark, how much is unlimited?
                Capacity > 0 ->
                    NewInUse = dict:store(QPid, Capacity - 1, InUse),
                    State#lim{in_use = NewInUse};
                true ->
                    % TODO How should this be handled?
                    rabbit_log:warning(
                        "Ignoring decrement for zero capacity: ~p~n",
                        [QPid]),
                    State
            end;
        error ->
            % TODO How should this case be handled?
            rabbit_log:warning("Ignoring decrement for unknown queue: ~p~n",
                               [QPid]),
            State
    end.

% Works out whether any queues should be notified
% If any notification is required, it propagates a transition
% of the blocked state
maybe_notify_queues(State = #lim{ch_pid = ChPid, in_use = InUse}) ->
    Capacity = current_capacity(State),
    case should_notify(Capacity, State) of
        true  ->
            dict:map(fun(Q,_) -> 
                        rabbit_amqqueue:unblock(Q, ChPid)
                     end, InUse),
            State#lim{blocked = false};
        false ->
            State
    end.

% Computes the current aggregrate capacity of all of the in-use queues
current_capacity(#lim{in_use = InUse}) ->
    % TODO This *seems* expensive to compute this on the fly
    dict:fold(fun(_, PerQ, Acc) -> PerQ + Acc end, 0, InUse).


% This is a very naive way of deciding wether to unblock or not,
% it *might* be better to wait for a time or volume threshold
% instead of broadcasting notifications
should_notify(Capacity, #lim{prefetch_count = Limit, blocked = true})
    when Capacity < Limit ->
        true;

should_notify(_,_) -> false.

maybe_can_send(_, State = #lim{blocked = true}) ->
    {false, State};

maybe_can_send(QPid, State = #lim{prefetch_count = Limit,
                                   in_use = InUse,
                                   blocked = false}) ->
    Capacity = current_capacity(State),
    if
        Capacity < Limit ->
            NewInUse = update_in_use_capacity(QPid, InUse),
            { true, State#lim{in_use = NewInUse} };
        true ->
            { false, State#lim{blocked = true}}
    end.

update_in_use_capacity(QPid, InUse) ->
    case dict:find(QPid, InUse) of
        {ok, Capacity} ->
            dict:store(QPid, Capacity + 1, InUse);
        error ->
            dict:store(QPid, 0, InUse)
    end.

