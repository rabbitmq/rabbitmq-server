%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_db).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0]).

-export([augment_exchanges/2, augment_queues/2, augment_nodes/1,
         get_channels/2, get_connections/1,
         get_all_channels/1, get_all_connections/0,
         get_overview/1, get_overview/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1, format_message_queue/2]).

-import(rabbit_misc, [pget/3, pset/3]).

-record(state, {
          %% "stats" for which no calculations are required
          tables,
          %% database of aggregated samples
          aggregated_stats,
          %% What the previous info item was for any given
          %% {queue/channel/connection}
          old_stats,
          remove_old_samples_timer,
          interval}).
-record(stats, {diffs, base}).

-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
                           channel_queue_exchange_stats]).
-define(TABLES, [queue_stats, connection_stats, channel_stats, consumers,
                 node_stats]).

-define(DELIVER_GET, [deliver, deliver_no_ack, get, get_no_ack]).
-define(FINE_STATS, [publish, publish_in, publish_out,
                     ack, deliver_get, confirm, return_unroutable, redeliver] ++
            ?DELIVER_GET).

-define(OVERVIEW_QUEUE_STATS,
        [messages, messages_ready, messages_unacknowledged]).

%%----------------------------------------------------------------------------

start_link() ->
    %% When failing over it is possible that the mirrored_supervisor
    %% might hear of the death of the old DB, and start a new one,
    %% before the global name server notices. Therefore rather than
    %% telling gen_server:start_link/4 to register it for us, we
    %% invoke global:re_register_name/2 ourselves, and just steal the
    %% name if it existed before. We therefore rely on
    %% mirrored_supervisor to maintain the uniqueness of this process.
    case gen_server2:start_link(?MODULE, [], []) of
        {ok, Pid} -> yes = global:re_register_name(?MODULE, Pid),
                     rabbit:force_event_refresh(),
                     {ok, Pid};
        Else      -> Else
    end.

augment_exchanges(Xs, Mode) -> safe_call({augment_exchanges, Xs, Mode}, Xs).
augment_queues(Qs, Mode)    -> safe_call({augment_queues, Qs, Mode}, Qs).
augment_nodes(Nodes)        -> safe_call({augment_nodes, Nodes}, Nodes).

get_channels(Cs, Mode)      -> safe_call({get_channels, Cs, Mode}, Cs).
get_connections(Cs)         -> safe_call({get_connections, Cs}, Cs).

get_all_channels(Mode)      -> safe_call({get_all_channels, Mode}).
get_all_connections()       -> safe_call(get_all_connections).

get_overview(User)          -> safe_call({get_overview, User}).
get_overview()              -> safe_call({get_overview, all}).

safe_call(Term) -> safe_call(Term, []).

safe_call(Term, Item) ->
    try
        gen_server2:call({global, ?MODULE}, Term, infinity)
    catch exit:{noproc, _} -> Item
    end.

%%----------------------------------------------------------------------------
pget(Key, List) -> pget(Key, List, unknown).

%% id_name() and id() are for use when handling events, id_lookup()
%% for when augmenting. The difference is that when handling events a
%% queue name will be a resource, but when augmenting we will be
%% passed a queue proplist that will already have been formatted -
%% i.e. it will have name and vhost keys.
id_name(node_stats)       -> name;
id_name(queue_stats)      -> name;
id_name(exchange_stats)   -> name;
id_name(channel_stats)    -> pid;
id_name(connection_stats) -> pid.

id(Type, List) -> pget(id_name(Type), List).

id_lookup(queue_stats, List) ->
    rabbit_misc:r(pget(vhost, List), queue, pget(name, List));
id_lookup(exchange_stats, List) ->
    rabbit_misc:r(pget(vhost, List), exchange, pget(name, List));
id_lookup(Type, List) ->
    id(Type, List).

lookup_element(Table, Key) -> lookup_element(Table, Key, 2).

lookup_element(Table, Key, Pos) ->
    try ets:lookup_element(Table, Key, Pos)
    catch error:badarg -> []
    end.

result_or_error([]) -> error;
result_or_error(S)  -> S.

append_samples(Stats, TS, Id, Keys, State = #state{old_stats = Table}) ->
    OldStats = lookup_element(Table, Id),
    OldTS = lookup_element(Table, Id, 3),
    [append_sample(Stats, TS, OldStats, OldTS, Id, Key, State) || Key <- Keys],
    ets:insert(Table, {Id, Stats, TS}).

append_sample(Stats, TS, OldStats, OldTS, Id, Key, State) ->
    case {pget(Key, Stats), pget(Key, OldStats)} of
        {unknown, _}   -> unknown;
        {New, unknown} -> apportion_first_sample(New, TS, Id, Key, State);
        {New, Old}     -> apportion_sample(New, TS, Old, OldTS, Id, Key, State)
    end.

-define(SAMPLE_COMBINE_WINDOW, 5000).

apportion_first_sample(New, NewTS, Id, Key, State) ->
    NewMS = rabbit_mgmt_format:timestamp_ms(NewTS),
    NewMSCeil = apportion_ceiling(NewMS),
    record_sample(Id, {Key, New, NewMSCeil, State}, State).

apportion_sample(New, NewTS, Old, OldTS, Id, Key, State) ->
    OldMS = rabbit_mgmt_format:timestamp_ms(OldTS),
    NewMS = rabbit_mgmt_format:timestamp_ms(NewTS),
    OldMSCeil = apportion_ceiling(OldMS),
    NewMSCeil = apportion_ceiling(NewMS),
    Count = New - Old,
    R = fun(Total, ThisFloat, Ceil) ->
                This = round(ThisFloat),
                record_sample(Id, {Key, This, Ceil, State}, State),
                Total - This
        end,
    case (NewMSCeil - OldMSCeil) / ?SAMPLE_COMBINE_WINDOW of
        0.0 ->
            record_sample(Id, {Key, Count, NewMSCeil, State}, State);
        _ ->
            %% We need a fractional apportionment for the window
            %% before OldMSCeil, then apportionments for all the
            %% full windows in the middle (of which there may be 0),
            %% then a fractional apportionment for the window
            %% before NewMSCeil.
            Rate = Count / (NewMS - OldMS),
            Count1 = R(Count, Rate * (OldMSCeil - OldMS), OldMSCeil),
            Middle = lists:seq(OldMSCeil + ?SAMPLE_COMBINE_WINDOW,
                               NewMSCeil - ?SAMPLE_COMBINE_WINDOW,
                               ?SAMPLE_COMBINE_WINDOW),
            CountFinal =
                lists:foldl(fun(I, CountN) ->
                                    R(CountN, Rate * ?SAMPLE_COMBINE_WINDOW, I)
                            end, Count1, Middle),
            R(CountFinal, CountFinal, NewMSCeil)
    end.

apportion_ceiling(TS) ->
    Round = (TS div ?SAMPLE_COMBINE_WINDOW) * ?SAMPLE_COMBINE_WINDOW,
    case TS - Round > 0 of
        true  -> Round + ?SAMPLE_COMBINE_WINDOW;
        false -> Round
    end.

record_sample({coarse, Id}, Args, State) ->
    record_sample0(Id, Args),
    record_sample0({vhost_stats, vhost(Id, State)}, Args);

%% Deliveries / acks (Q -> Ch)
record_sample({fine, {Ch, Q = #resource{kind = queue}}}, Args, _State) ->
    record_sample0({channel_queue_stats, {Ch, Q}},  Args),
    record_sample0({channel_stats,       Ch},       Args),
    record_sample0({queue_stats,         Q},        Args),
    record_sample0({vhost_stats,         vhost(Q)}, Args);

%% Publishes / confirms (Ch -> X)
record_sample({fine, {Ch, X = #resource{kind = exchange}}}, Args, _State) ->
    record_sample0({channel_exchange_stats, {Ch, X}},  Args),
    record_sample0({channel_stats,          Ch},       Args),
    record_sampleX(publish_in,              X,         Args),
    record_sample0({vhost_stats,            vhost(X)}, Args);

%% Publishes / confirms (Ch -> X -> Q)
record_sample({fine, {_Ch,
                      Q = #resource{kind = queue},
                      X = #resource{kind = exchange}}}, Args, _State) ->
    %% TODO This one logically feels like it should be here. It would
    %% correspond to "publishing channel message rates to queue" -
    %% which would be nice to handle - except we don't. And just
    %% uncommenting this means it gets merged in with "consuming
    %% channel delivery from queue" - which is not very helpful.
    %% record_sample0({channel_queue_stats, {Ch, Q}}, Args),
    record_sample0({queue_exchange_stats,   {Q,  X}},  Args),
    record_sample0({queue_stats,            Q},        Args),
    record_sampleX(publish_out,             X,         Args).

vhost(#resource{virtual_host = VHost}) -> VHost.

vhost({queue_stats, #resource{virtual_host = VHost}}, _State) ->
    VHost;
vhost({TName, Pid}, #state{tables = Tables}) ->
    Table = orddict:fetch(TName, Tables),
    pget(vhost, lookup_element(Table, {Pid, create})).

%% exchanges have two sets of "publish" stats, so rearrange things a touch
record_sampleX(RenamePublishTo, X, {publish, Diff, Ceil, State}) ->
    record_sample0({exchange_stats, X}, {RenamePublishTo, Diff, Ceil, State}).

record_sample0(Id0, {Key, Diff, Ceil, #state{aggregated_stats = ETS}}) ->
    Id = {Id0, Key},
    Old = case lookup_element(ETS, Id) of
              [] -> blank_stats();
              E  -> E
          end,
    ets:insert(ETS, {Id, add(Ceil, Diff, Old)}).

add(Ceil, Diff, Stats = #stats{diffs = Diffs}) ->
    Diffs2 = case gb_trees:lookup(Ceil, Diffs) of
                 {value, Total} -> gb_trees:update(Ceil, Diff + Total, Diffs);
                 none           -> gb_trees:insert(Ceil, Diff, Diffs)
             end,
    Stats#stats{diffs = Diffs2}.

%% TODO be less crude
-define(MAX_SAMPLE_AGE, 60000).

remove_old_samples(#state{aggregated_stats = ETS}) ->
    TS = apportion_ceiling(rabbit_mgmt_format:timestamp_ms(erlang:now())),
    remove_old_samples_it(ets:match(ETS, '$1', 1), TS, ETS). %% TODO incr

remove_old_samples_it('$end_of_table', _, _) ->
    ok;
remove_old_samples_it({Matches, Continuation}, TS, ETS) ->
    [remove_old_samples(Key, Stats, TS, ETS) || [{Key, Stats}] <- Matches],
    remove_old_samples_it(ets:match(Continuation), TS, ETS).

remove_old_samples(Key, Stats, TS, ETS) ->
    Cutoff = TS - ?MAX_SAMPLE_AGE,
    case remove_old_samples0(Cutoff, Stats) of
        Stats  -> ok;
        Stats2 -> ets:insert(ETS, {Key, Stats2})
    end.

remove_old_samples0(Cutoff, Stats = #stats{diffs = Diffs, base = Base}) ->
    case gb_trees:is_empty(Diffs) of
        true  -> Stats;
        false -> {Small, Val} = gb_trees:smallest(Diffs),
                 case Small < Cutoff of
                     true  -> Diffs1 = gb_trees:delete(Small, Diffs),
                              remove_old_samples0(
                                Cutoff, Stats#stats{diffs = Diffs1,
                                                    base  = Base + Val});
                     false -> Stats
                 end
    end.

overview_sum(Type, VHostStats) ->
    Stats = [pget(Type, VHost, blank_stats()) || VHost <- VHostStats],
    {Type, sum_trees(Stats)}.

blank_stats() -> #stats{diffs = gb_trees:empty(), base = 0}.
is_blank_stats(S) -> S =:= blank_stats().

sum_trees([]) -> blank_stats();

sum_trees([Stats | StatsN]) ->
    lists:foldl(
      fun (#stats{diffs = D1, base = B1}, #stats{diffs = D2, base = B2}) ->
              #stats{diffs = add_trees(D1, gb_trees:iterator(D2)),
                     base  = B1 + B2}
      end,
      Stats, StatsN).

add_trees(Tree, It) ->
    case gb_trees:next(It) of
        none        -> Tree;
        {K, V, It2} -> add_trees(
                         case gb_trees:lookup(K, Tree) of
                             {value, V2} -> gb_trees:update(K, V + V2, Tree);
                             none        -> gb_trees:insert(K, V, Tree)
                         end, It2)
    end.

%%----------------------------------------------------------------------------

init([]) ->
    %% When Rabbit is overloaded, it's usually especially important
    %% that the management plugin work.
    process_flag(priority, high),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    rabbit_log:info("Statistics database started.~n"),
    Table = fun () -> ets:new(rabbit_mgmt_db, [ordered_set]) end,
    Tables = orddict:from_list([{Key, Table()} || Key <- ?TABLES]),
    {ok, set_remove_timer(#state{interval         = Interval,
                                 tables           = Tables,
                                 old_stats        = Table(),
                                 aggregated_stats = Table()}), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({augment_exchanges, Xs, basic}, _From, State) ->
    reply(list_exchange_stats(Xs, State), State);

handle_call({augment_exchanges, Xs, full}, _From, State) ->
    reply(detail_exchange_stats(Xs, State), State);

handle_call({augment_queues, Qs, basic}, _From, State) ->
    reply(list_queue_stats(Qs, State), State);

handle_call({augment_queues, Qs, full}, _From, State) ->
    reply(detail_queue_stats(Qs, State), State);

handle_call({augment_nodes, Nodes}, _From, State) ->
    {reply, node_stats(Nodes, State), State};

handle_call({get_channels, Names, Mode}, _From,
            State = #state{tables = Tables}) ->
    Chans = created_event(Names, channel_stats, Tables),
    Result = case Mode of
                 basic -> list_channel_stats(Chans, State);
                 full  -> detail_channel_stats(Chans, State)
             end,
    reply(lists:map(fun result_or_error/1, Result), State);

handle_call({get_connections, Names}, _From,
            State = #state{tables = Tables}) ->
    Conns = created_event(Names, connection_stats, Tables),
    Result = connection_stats(Conns, State),
    reply(lists:map(fun result_or_error/1, Result), State);

handle_call({get_all_channels, Mode}, _From, State = #state{tables = Tables}) ->
    Chans = created_events(channel_stats, Tables),
    Result = case Mode of
                 basic -> list_channel_stats(Chans, State);
                 full  -> detail_channel_stats(Chans, State)
             end,
    reply(Result, State);

handle_call(get_all_connections, _From, State = #state{tables = Tables}) ->
    Conns = created_events(connection_stats, Tables),
    reply(connection_stats(Conns, State), State);

handle_call({get_overview, User}, _From, State = #state{tables = Tables}) ->
    VHosts = case User of
                 all -> rabbit_vhost:list();
                 _   -> rabbit_mgmt_util:list_visible_vhosts(User)
             end,
    %% TODO: there's no reason we can't do an overview of send_oct and
    %% recv_oct now!
    VStats = [read_simple_stats(vhost_stats, VHost, State) ||
                 VHost <- rabbit_vhost:list()],
    MessageStats = [overview_sum(Type, VStats) || Type <- ?FINE_STATS],
    QueueStats = [overview_sum(Type, VStats) || Type <- ?OVERVIEW_QUEUE_STATS],
    F = case User of
            all -> fun (L) -> length(L) end;
            _   -> fun (L) -> length(rabbit_mgmt_util:filter_user(L, User)) end
        end,
    %% Filtering out the user's consumers would be rather expensive so let's
    %% just not show it
    Consumers = case User of
                    all -> [{consumers,
                             ets:info(orddict:fetch(consumers, Tables), size)}];
                    _   -> []
                end,
    ObjectTotals = Consumers ++
        [{queues,      length([Q || V <- VHosts,
                                    Q <- rabbit_amqqueue:list(V)])},
         {exchanges,   length([X || V <- VHosts,
                                    X <- rabbit_exchange:list(V)])},
         {connections, F(created_events(connection_stats, Tables))},
         {channels,    F(created_events(channel_stats, Tables))}],
    reply([{message_stats, calculate_rates(MessageStats, State)},
           {queue_totals,  calculate_rates(QueueStats, State)},
           {object_totals, ObjectTotals}], State);

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

handle_cast({event, Event}, State) ->
    handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info(remove_old_samples, State) ->
    remove_old_samples(State),
    noreply(set_remove_timer(State));

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

set_remove_timer(State = #state{interval = Interval}) ->
    TRef = erlang:send_after(Interval, self(), remove_old_samples),
    State#state{remove_old_samples_timer = TRef}.

handle_pre_hibernate(State) ->
    %% rabbit_event can end up holding on to some memory after a busy
    %% workout, but it's not a gen_server so we can't make it
    %% hibernate. The best we can do is forcibly GC it here (if
    %% rabbit_mgmt_db is hibernating the odds are rabbit_event is
    %% quiescing in some way too).
    rpc:multicall(
      rabbit_mnesia:cluster_nodes(running), rabbit_mgmt_db_handler, gc, []),
    {hibernate, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%----------------------------------------------------------------------------

handle_event(#event{type = queue_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(queue_stats, Stats, Timestamp,
                 [{fun rabbit_mgmt_format:properties/1,[backing_queue_status]},
                  {fun rabbit_mgmt_format:timestamp/1, [idle_since]}],
                 [messages, messages_ready, messages_unacknowledged], State);

handle_event(Event = #event{type = queue_deleted,
                            props = [{name, Name}]}, State) ->
    delete_samples(channel_queue_stats,  {'_', Name}, State),
    delete_samples(queue_exchange_stats, {Name, '_'}, State),
    delete_samples(queue_stats,          Name,        State),
    handle_deleted(queue_stats, Event, State);

handle_event(Event = #event{type = exchange_deleted,
                            props = [{name, Name}]}, State) ->
    delete_samples(channel_exchange_stats,  {'_', Name}, State),
    delete_samples(queue_exchange_stats,    {'_', Name}, State),
    delete_samples(exchange_stats,          Name,        State),
    handle_deleted(exchange_stats, Event, State);

handle_event(#event{type = vhost_deleted,
                    props = [{name, Name}]}, State) ->
    delete_samples(vhost_stats, Name, State),
    {ok, State};

handle_event(#event{type = connection_created, props = Stats}, State) ->
    handle_created(
      connection_stats, Stats,
      [{fun rabbit_mgmt_format:addr/1,         [host, peer_host]},
       {fun rabbit_mgmt_format:port/1,         [port, peer_port]},
       {fun rabbit_mgmt_format:protocol/1,     [protocol]},
       {fun rabbit_mgmt_format:amqp_table/1,   [client_properties]}], State);

handle_event(#event{type = connection_stats, props = Stats,
                    timestamp = Timestamp},
             State) ->
    handle_stats(connection_stats, Stats, Timestamp, [], [recv_oct, send_oct],
                 State);

handle_event(Event = #event{type  = connection_closed,
                            props = [{pid, Pid}]}, State) ->
    delete_samples(connection_stats, Pid, State),
    handle_deleted(connection_stats, Event, State);

handle_event(#event{type = channel_created, props = Stats}, State) ->
    handle_created(channel_stats, Stats, [], State);

handle_event(#event{type = channel_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(channel_stats, Stats, Timestamp,
                 [{fun rabbit_mgmt_format:timestamp/1, [idle_since]}],
                 [], State),
    [handle_fine_stats(Type, Stats, Timestamp, State) ||
        Type <- ?FINE_STATS_TYPES],
    {ok, State};

handle_event(Event = #event{type = channel_closed,
                            props = [{pid, Pid}]}, State) ->
    delete_samples(channel_queue_stats,    {Pid, '_'}, State),
    delete_samples(channel_exchange_stats, {Pid, '_'}, State),
    delete_samples(channel_stats,          Pid,        State),
    handle_deleted(channel_stats, Event, State);

handle_event(#event{type = consumer_created, props = Props}, State) ->
    handle_consumer(fun(Table, Id, P) -> ets:insert(Table, {Id, P}) end,
                    Props, State);

handle_event(#event{type = consumer_deleted, props = Props}, State) ->
    handle_consumer(fun(Table, Id, _P) -> ets:delete(Table, Id) end,
                    Props, State);

handle_event(#event{type = queue_mirror_deaths, props = Props},
             #state{tables = Tables}) ->
    Dead = pget(pids, Props),
    Table = orddict:fetch(queue_stats, Tables),
    %% Only the master can be in the DB, but it's easier just to
    %% delete all of them
    [ets:delete(Table, {Pid, stats}) || Pid <- Dead];

%% TODO: we don't clear up after dead nodes here - this is a very tiny
%% leak every time a node is permanently removed from the cluster. Do
%% we care?
handle_event(#event{type = node_stats, props = Stats, timestamp = Timestamp},
             State = #state{tables = Tables}) ->
    Table = orddict:fetch(node_stats, Tables),
    ets:insert(Table, {{pget(name, Stats), stats},
                       proplists:delete(name, Stats), Timestamp}),
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

%%----------------------------------------------------------------------------

handle_created(TName, Stats, Funs, State = #state{tables = Tables}) ->
    Formatted = rabbit_mgmt_format:format(Stats, Funs),
    ets:insert(orddict:fetch(TName, Tables), {{id(TName, Stats), create},
                                              Formatted,
                                              pget(name, Stats)}),
    {ok, State}.

handle_stats(TName, Stats, Timestamp, Funs, RatesKeys,
             State = #state{tables = Tables}) ->
    Id = id(TName, Stats),
    append_samples(Stats, Timestamp, {coarse, {TName, Id}}, RatesKeys, State),
    Stats1 = lists:foldl(
               fun (K, StatsAcc) -> proplists:delete(K, StatsAcc) end,
               Stats, [id_name(TName)] ++ RatesKeys ++ ?FINE_STATS_TYPES),
    Stats2 = rabbit_mgmt_format:format(Stats1, Funs),
    ets:insert(orddict:fetch(TName, Tables), {{Id, stats}, Stats2, Timestamp}),
    {ok, State}.

handle_deleted(TName, #event{props = Props}, State = #state{tables    = Tables,
                                                            old_stats = Old}) ->
    Id = id(TName, Props),
    case orddict:find(TName, Tables) of
        {ok, Table} -> ets:delete(Table, {Id, create}),
                       ets:delete(Table, {Id, stats});
        error       -> ok
    end,
    ets:delete(Old, {coarse, {TName, Id}}),
    ets:match_delete(Old, {{fine, {Id, '_'}},      '_', '_'}),
    ets:match_delete(Old, {{fine, {Id, '_', '_'}}, '_', '_'}),
    {ok, State}.

handle_consumer(Fun, Props,
                State = #state{tables = Tables}) ->
    P = rabbit_mgmt_format:format(Props, []),
    Table = orddict:fetch(consumers, Tables),
    Fun(Table, {pget(queue, P), pget(channel, P)}, P),
    {ok, State}.

handle_fine_stats(Type, Props, Timestamp, State) ->
    case pget(Type, Props) of
        unknown ->
            ok;
        AllFineStats ->
            ChPid = id(channel_stats, Props),
            [handle_fine_stat(
               fine_stats_id(ChPid, Ids), Stats, Timestamp, State) ||
                {Ids, Stats} <- AllFineStats]
    end.

handle_fine_stat(Id, Stats, Timestamp, State) ->
    Total = lists:sum([V || {K, V} <- Stats, lists:member(K, ?DELIVER_GET)]),
    Stats1 = case Total of
                 0 -> Stats;
                 _ -> [{deliver_get, Total}|Stats]
             end,
    append_samples(Stats1, Timestamp, {fine, Id}, ?FINE_STATS, State).

delete_samples(Type, Id, #state{aggregated_stats = ETS}) ->
    ets:match_delete(ETS, {{{Type, Id}, '_'}, '_'}).

fine_stats_id(ChPid, {Q, X}) -> {ChPid, Q, X};
fine_stats_id(ChPid, QorX)   -> {ChPid, QorX}.

created_event(Names, Type, Tables) ->
    Table = orddict:fetch(Type, Tables),
    [lookup_element(
       Table, {case ets:match(Table, {{'$1', create}, '_', Name}) of
                   []    -> none;
                   [[I]] -> I
               end, create}) || Name <- Names].

created_events(Type, Tables) ->
    [Facts || {{_, create}, Facts, _Name}
                  <- ets:tab2list(orddict:fetch(Type, Tables))].

%%----------------------------------------------------------------------------

merge_stats(Objs, Funs) ->
    [lists:foldl(fun (Fun, Props) -> Fun(Props) ++ Props end, Obj, Funs)
     || Obj <- Objs].

%% i.e. the non-calculated stats
basic_stats_fun(Type, #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    fun (Props) ->
            Id = id_lookup(Type, Props),
            lookup_element(Table, {Id, stats})
    end.

%% i.e. coarse stats, and fine stats aggregated up to a single number per thing
simple_stats_fun(Type, State) ->
    fun (Props) ->
            Id = id_lookup(Type, Props),
            extract_msg_stats(
              calculate_rates(
                read_simple_stats(Type, Id, State), State))
    end.

%% i.e. fine stats that are broken out per sub-thing
detail_stats_fun({IdType, FineSpecs}, State) ->
    fun (Props) ->
            Id = id_lookup(IdType, Props),
            [detail_stats(Name, AggregatedStatsType, IdFun(Id), State)
             || {Name, AggregatedStatsType, IdFun} <- FineSpecs]
    end.

read_simple_stats(Type, Id, #state{aggregated_stats = ETS}) ->
    FromETS = ets:match(ETS, {{{Type, Id}, '$1'}, '$2'}),
    [{K, V} || [K, V] <- FromETS].

read_detail_stats(Type, Id, #state{aggregated_stats = ETS}) ->
    %% Id must contain '$1'
    FromETS = ets:match(ETS, {{{Type, Id}, '$2'}, '$3'}),
    %% [[G, K, V]] -> [{G, [{K, V}]}] where G is Q/X/Ch, K is from
    %% ?FINE_STATS and V is a stats tree
    %% TODO does this need to be optimised?
    lists:foldl(
      fun ([G, K, V], L) ->
              case lists:keyfind(G, 1, L) of
                  false    -> [{G, [{K, V}]} | L];
                  {G, KVs} -> lists:keyreplace(G, 1, L, {G, [{K, V} | KVs]})
              end
      end, [], FromETS).

extract_msg_stats(Stats) ->
    FineStats = lists:append([[K, details_key(K)] || K <- ?FINE_STATS]),
    {MsgStats, Other} =
        lists:partition(fun({K, _}) -> lists:member(K, FineStats) end, Stats),
    [{message_stats, MsgStats} | Other].

detail_stats(Name, AggregatedStatsType, Id, State) ->
    {Name, [[{stats, calculate_rates(KVs, State)} | format_detail_id(G, State)]
            || {G, KVs} <- read_detail_stats(AggregatedStatsType, Id, State)]}.

format_detail_id(ChPid, State) when is_pid(ChPid) ->
    augment_msg_stats([{channel, ChPid}], State);
format_detail_id(#resource{name = Name, virtual_host = Vhost, kind = Kind},
                 _State) ->
    [{Kind, [{name, Name}, {vhost, Vhost}]}].

consumer_details_fun(PatternFun, State = #state{tables = Tables}) ->
    Table = orddict:fetch(consumers, Tables),
    fun ([])    -> [];
        (Props) -> Pattern = PatternFun(Props),
                   [{consumer_details,
                     [augment_msg_stats(augment_consumer(Obj), State)
                      || Obj <- lists:append(
                                  ets:match(Table, {Pattern, '$1'}))]}]
    end.

augment_consumer(Obj) ->
    [{queue, rabbit_mgmt_format:resource(pget(queue, Obj))} |
     proplists:delete(queue, Obj)].

calculate_rates(ManyStats, State) ->
    lists:append(
      [case is_blank_stats(Stats) of
           true  -> [];
           false -> {Details, Counter} = calculate_details(Stats, State),
                    [{K,              Counter},
                     {details_key(K), Details}]
       end || {K, Stats} <- ManyStats]).

calculate_details(#stats{diffs = Diffs, base = Base},
                  #state{interval = Interval}) ->
    {Samples0, Counter} =
        lists:foldl(fun({T, S}, {Samples, CountN}) ->
                            S2 = S + CountN,
                            {[[{sample, S2}, {timestamp, T}] | Samples], S2}
                    end,
                    {[], Base}, gb_trees:to_list(Diffs)),
    %% The last sample will be dubious since the events for its
    %% timeslice won't necessarily have finished arriving. Don't return it.
    Samples = case Samples0 of
                  []     -> [];
                  [_S|Ss] -> Ss
              end,
    case length(Samples) > 1 of
        true ->
            [[{sample, S3}, {timestamp, T3}],
             [{sample, S2}, {timestamp, T2}] | _] = Samples,
            {Inst, Avg} =
                case rabbit_misc:now_ms() - T3 > Interval * 1.5 of
                    true  -> {0, 0};
                    false -> [{sample,    S1},
                              {timestamp, T1}] = lists:last(Samples),
                             {(S3 - S2) * 1000 / (T3 - T2),
                              (S3 - S1) * 1000 / (T3 - T1)}
                end,
            {[{rate,     Inst},
              {interval, T3 - T2},
              {avg_rate, Avg},
              {samples,  Samples}], Counter};
        false ->
            {[{samples, Samples}], Counter}
    end.

details_key(Key) -> list_to_atom(atom_to_list(Key) ++ "_details").

%%----------------------------------------------------------------------------

augment_msg_stats(Props, State) ->
    rabbit_mgmt_format:strip_pids(
      (augment_msg_stats_fun(State))(Props) ++ Props).

augment_msg_stats_fun(State) ->
    Funs = [{connection, fun augment_connection_pid/2},
            {channel,    fun augment_channel_pid/2},
            {owner_pid,  fun augment_connection_pid/2}],
    fun (Props) -> augment(Props, Funs, State) end.

augment(Items, Funs, State) ->
    Augmented = [augment(K, Items, Fun, State) || {K, Fun} <- Funs],
    [{K, V} || {K, V} <- Augmented, V =/= unknown].

augment(K, Items, Fun, State) ->
    Key = details_key(K),
    case pget(K, Items) of
        none    -> {Key, unknown};
        unknown -> {Key, unknown};
        Id      -> {Key, Fun(Id, State)}
    end.

augment_channel_pid(Pid, #state{tables = Tables}) ->
    Ch = lookup_element(orddict:fetch(channel_stats, Tables),
                        {Pid, create}),
    Conn = lookup_element(orddict:fetch(connection_stats, Tables),
                          {pget(connection, Ch), create}),
    [{name,            pget(name,   Ch)},
     {number,          pget(number, Ch)},
     {connection_name, pget(name,         Conn)},
     {peer_port,       pget(peer_port,    Conn)},
     {peer_host,       pget(peer_host,    Conn)}].

augment_connection_pid(Pid, #state{tables = Tables}) ->
    Conn = lookup_element(orddict:fetch(connection_stats, Tables),
                          {Pid, create}),
    [{name,         pget(name,         Conn)},
     {peer_port,    pget(peer_port,    Conn)},
     {peer_host,    pget(peer_host,    Conn)}].

%%----------------------------------------------------------------------------

first(Id)  -> {Id, '$1'}.
second(Id) -> {'$1', Id}.

-define(QUEUE_DETAILS,
        {queue_stats, [{incoming,   queue_exchange_stats, fun first/1},
                       {deliveries, channel_queue_stats,  fun second/1}]}).

-define(EXCHANGE_DETAILS,
        {exchange_stats, [{incoming, channel_exchange_stats, fun second/1},
                          {outgoing, queue_exchange_stats,   fun second/1}]}).

-define(CHANNEL_DETAILS,
        {channel_stats, [{publishes,  channel_exchange_stats, fun first/1},
                         {deliveries, channel_queue_stats,    fun first/1}]}).

list_queue_stats(Objs, State) ->
    adjust_hibernated_memory_use(
      merge_stats(Objs, queue_funs(State))).

detail_queue_stats(Objs, State) ->
    adjust_hibernated_memory_use(
      merge_stats(Objs, [consumer_details_fun(
                           fun (Props) ->
                                   {id_lookup(queue_stats, Props), '_'}
                           end, State), detail_stats_fun(?QUEUE_DETAILS, State)
                         | queue_funs(State)])).

queue_funs(State) ->
    [basic_stats_fun(queue_stats, State), simple_stats_fun(queue_stats, State),
     augment_msg_stats_fun(State)].

list_exchange_stats(Objs, State) ->
    merge_stats(Objs, [simple_stats_fun(exchange_stats, State),
                       augment_msg_stats_fun(State)]).

detail_exchange_stats(Objs, State) ->
    merge_stats(Objs, [simple_stats_fun(exchange_stats, State),
                       detail_stats_fun(?EXCHANGE_DETAILS, State),
                       augment_msg_stats_fun(State)]).

connection_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(connection_stats, State),
                       simple_stats_fun(connection_stats, State),
                       augment_msg_stats_fun(State)]).

list_channel_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(channel_stats, State),
                       simple_stats_fun(channel_stats, State),
                       augment_msg_stats_fun(State)]).

detail_channel_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(channel_stats, State),
                       simple_stats_fun(channel_stats, State),
                       consumer_details_fun(
                         fun (Props) -> {'_', pget(pid, Props)} end, State),
                       detail_stats_fun(?CHANNEL_DETAILS, State),
                       augment_msg_stats_fun(State)]).

node_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(node_stats, State)]).

%%----------------------------------------------------------------------------

%% We do this when retrieving the queue record rather than when
%% storing it since the memory use will drop *after* we find out about
%% hibernation, so to do it when we receive a queue stats event would
%% be fiddly and racy. This should be quite cheap though.
adjust_hibernated_memory_use(Qs) ->
    Pids = [pget(pid, Q) ||
               Q <- Qs, pget(idle_since, Q, not_idle) =/= not_idle],
    {Mem, _BadNodes} = delegate:invoke(
                         Pids, fun (Pid) -> process_info(Pid, memory) end),
    MemDict = dict:from_list([{P, M} || {P, M = {memory, _}} <- Mem]),
    [case dict:find(pget(pid, Q), MemDict) of
         error        -> Q;
         {ok, Memory} -> [Memory|proplists:delete(memory, Q)]
     end || Q <- Qs].
