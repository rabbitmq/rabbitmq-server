-module(rabbit_amqp_sole_conn_local).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    leases = #{} :: #{ {binary(), binary()} => {pid(), reference()} },
    pids   = #{} :: #{ pid() => {binary(), binary()} }
}).

init([]) ->
    {ok, #state{}}.

handle_call({acquire, VHost, ContainerId, NewPid}, _From, #state{leases = Leases0,
                                                                 pids = Pids0} = State) ->
    Key = {VHost, ContainerId},
    case Leases0 of 
        #{Key := _} ->
            {reply, {error, refuse_connection}, State};
        _ ->
            MRef = erlang:monitor(process, NewPid),
            Leases1 = Leases0#{Key => {NewPid, MRef}},
            Pids1 = Pids0#{NewPid => Key},
            {reply, ok, State#state{leases = Leases1, pids = Pids1}}
    end;
handle_call({release, VHost, ContainerId}, _From, State) ->
    Key = {VHost, ContainerId},
    {reply, ok, remove_lease(Key, State)};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) -> 
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, #state{pids = Pids} = State) ->
    case Pids of
        #{Pid := Key} ->
            {noreply, remove_lease(Key, State)};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) -> 
    {noreply, State}.

terminate(_Reason, _State) -> 
    ok.

%% ------------------------------------------------------------------
%% Internal Helpers
%% ------------------------------------------------------------------

remove_lease(Key, #state{leases = Leases0,
                         pids = Pids0} = State) ->
    case Leases0 of
        #{Key := {Pid, _}} ->
            Leases1 = maps:remove(Key, Leases0),
            Pids1 = maps:remove(Pid, Pids0),
            State#state{leases = Leases1, pids = Pids1};
        _ ->
            State
    end.
 
