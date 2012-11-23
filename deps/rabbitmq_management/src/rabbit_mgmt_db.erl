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

-record(state, {tables, interval}).
-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
                           channel_queue_exchange_stats]).
-define(TABLES, [queue_stats, connection_stats, channel_stats,
                 consumers, node_stats] ++
            ?FINE_STATS_TYPES).

-define(DELIVER_GET, [deliver, deliver_no_ack, get, get_no_ack]).
-define(FINE_STATS, [publish, ack, deliver_get, confirm,
                     return_unroutable, redeliver] ++
            ?DELIVER_GET).

-define(
   FINE_STATS_CHANNEL_LIST,
   [{channel_queue_stats,   [channel], message_stats, channel},
    {channel_exchange_stats,[channel], message_stats, channel}]).

-define(
   FINE_STATS_CHANNEL_DETAIL,
   [{channel_queue_stats,    [channel],           message_stats, channel},
    {channel_exchange_stats, [channel],           message_stats, channel},
    {channel_exchange_stats, [channel, exchange], publishes,     channel},
    {channel_queue_stats,    [channel, queue],    deliveries,    channel}]).

-define(
   FINE_STATS_QUEUE_LIST,
   [{channel_queue_stats,          [queue], message_stats, queue},
    {channel_queue_exchange_stats, [queue], message_stats, queue}]).

-define(
   FINE_STATS_QUEUE_DETAIL,
   [{channel_queue_stats,          [queue],           message_stats, queue},
    {channel_queue_exchange_stats, [queue],           message_stats, queue},
    {channel_queue_stats,          [queue, channel],  deliveries, queue},
    {channel_queue_exchange_stats, [queue, exchange], incoming, queue}]).

-define(
   FINE_STATS_EXCHANGE_LIST,
   [{channel_exchange_stats,       [exchange], message_stats_in,  exchange},
    {channel_queue_exchange_stats, [exchange], message_stats_out, exchange}]).

-define(
   FINE_STATS_EXCHANGE_DETAIL,
   [{channel_exchange_stats,       [exchange], message_stats_in,   exchange},
    {channel_queue_exchange_stats, [exchange], message_stats_out,  exchange},
    {channel_exchange_stats,       [exchange, channel],  incoming, exchange},
    {channel_queue_exchange_stats, [exchange, queue],    outgoing, exchange}]).

-define(FINE_STATS_NONE, []).

-define(OVERVIEW_QUEUE_STATS,
        [messages, messages_ready, messages_unacknowledged, messages_details,
         messages_ready_details, messages_unacknowledged_details]).

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
id_name(channel_stats)    -> pid;
id_name(connection_stats) -> pid.

id(Type, List) -> pget(id_name(Type), List).

id_lookup(queue_stats, List) -> rabbit_misc:r(
                                  pget(vhost, List), queue, pget(name, List));
id_lookup(Type,        List) -> id(Type, List).

lookup_element(Table, Key) -> lookup_element(Table, Key, 2).

lookup_element(Table, Key, Pos) ->
    try ets:lookup_element(Table, Key, Pos)
    catch error:badarg -> []
    end.

result_or_error([]) -> error;
result_or_error(S)  -> S.

append_samples(Stats, Timestamp, Id, Table, Keys) ->
    OldStats = lookup_element(Table, Id),
    OldTimestamp = lookup_element(Table, Id, 3),
    Stats ++ [R || Key <- Keys,
                   R   <- [append_sample(Stats, Timestamp,
                                         OldStats, OldTimestamp, Key)],
                   R =/= unknown].

append_sample(Stats, TS, OldStats, OldTS, Key) ->
    KeyDetails = details_key(Key),
    case {pget(Key, Stats), pget(Key, OldStats), pget(KeyDetails, OldStats)} of
        {unknown, _, _}     -> unknown;
        {_, unknown, _}     -> unknown;
        {New, Old, unknown} -> OldMS = rabbit_mgmt_format:timestamp_ms(OldTS),
                               MS = rabbit_mgmt_format:timestamp_ms(TS),
                               {KeyDetails,
                                [[{sample, New}, {timestamp, MS}],
                                 [{sample, Old}, {timestamp, OldMS}]]};
        {New, _, Samples}   -> MS = rabbit_mgmt_format:timestamp_ms(TS),
                               {KeyDetails,
                                [[{sample, New}, {timestamp, MS}] |
                                 remove_old_samples(MS, Samples)]}
    end.

%% TODO be less crude
-define(MAX_SAMPLE_AGE, 60000).
remove_old_samples(Now, Samples) ->
    Cutoff = Now - ?MAX_SAMPLE_AGE,
    [Sample || Sample <- Samples,
               pget(timestamp, Sample) > Cutoff].

sum(Keys, List) ->
    [{Key, sum2(Key, List)} || Key <- Keys].

sum2(Key, List) ->
    case is_details(Key) of
        true ->
            %% TODO this is a bit crude
            [{rate, lists:foldl(fun (Stats, T) ->
                                        T + pget(rate, pget(Key, Stats, []), 0)
                                end, 0, List)}];
        false ->
            lists:foldl(fun (Stats, T) ->
                                T + pget(Key, Stats, 0)
                        end, 0, List)
    end.

%% List = [{ [{channel, Pid}, ...], [{deliver, 123}, ...] } ...]
group_sum([], List) ->
    lists:foldl(fun ({_, Item1}, Item0) ->
                        gs_update(Item0, Item1)
                end, [], List);

group_sum([Group | Groups], List) ->
    D = lists:foldl(
          fun (Next = {Ids, _}, Dict) ->
                  Id = {Group, pget(Group, Ids)},
                  dict:update(Id, fun(Cur) -> [Next | Cur] end, [Next], Dict)
          end, dict:new(), List),
    dict:map(fun(_, SubList) ->
                     group_sum(Groups, SubList)
             end, D).

gs_update(Item0, Item1) ->
    Keys = lists:usort([K || {K, _} <- Item0 ++ Item1]),
    gs_update(Item0, Item1, Keys).

gs_update(Item0, Item1, Keys) ->
    [{Key, gs_update_add(Key, pget(Key, Item0), pget(Key, Item1))} ||
        Key <- Keys].

-define(SAMPLE_COMBINE_WINDOW, 5000).

gs_update_add(Key, Item0, Item1) ->
    case is_details(Key) of
        true  ->
            I0 = if_unknown(Item0, []),
            I1 = if_unknown(Item1, []),
            %% TODO this could *ahem* probably be more efficient
            %% TODO apportion across samples
            %% TODO if I0 and I1 are from different channels then
            %% should we do this at all?
            Tree = lists:foldl(
                     fun ([{sample,    S},
                           {timestamp, T}], D) ->
                             K = (T div ?SAMPLE_COMBINE_WINDOW) *
                                 ?SAMPLE_COMBINE_WINDOW,
                             case gb_trees:lookup(K, D) of
                                 {value, S1} -> gb_trees:update(K, S + S1, D);
                                 none        -> gb_trees:insert(K, S, D)
                             end
                     end, gb_trees:empty(), I0 ++ I1),
            [[{sample,    S},
              {timestamp, T}] || {T, S} <- lists:reverse(
                                             gb_trees:to_list(Tree))];
        false ->
            I0 = if_unknown(Item0, 0),
            I1 = if_unknown(Item1, 0),
            I0 + I1
    end.

if_unknown(unknown, Def) -> Def;
if_unknown(Val,    _Def) -> Val.

%%----------------------------------------------------------------------------

init([]) ->
    %% When Rabbit is overloaded, it's usually especially important
    %% that the management plugin work.
    process_flag(priority, high),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    rabbit_log:info("Statistics database started.~n"),
    {ok, #state{interval = Interval,
                tables = orddict:from_list(
                           [{Key, ets:new(rabbit_mgmt_db,
                                          [private, ordered_set])} ||
                               Key <- ?TABLES])}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({augment_exchanges, Xs, basic}, _From, State) ->
    reply(exchange_stats(Xs, ?FINE_STATS_EXCHANGE_LIST, State), State);

handle_call({augment_exchanges, Xs, full}, _From, State) ->
    reply(exchange_stats(Xs, ?FINE_STATS_EXCHANGE_DETAIL, State), State);

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
    Qs0 = [rabbit_mgmt_format:queue(Q) || V <- VHosts,
                                          Q <- rabbit_amqqueue:list(V)],
    Qs1 = basic_queue_stats(Qs0, State),
    QueueTotals = sum(?OVERVIEW_QUEUE_STATS, Qs1),

    Filter = fun(Id, Name) ->
                     lists:member(pget(vhost, pget(Name, Id)), VHosts)
             end,
    F = fun(Type, Name) ->
                get_fine_stats_from_list(
                  [], [R || R = {Id, _, _}
                                <- ets:tab2list(orddict:fetch(Type, Tables)),
                            Filter(format_id(Id), Name)])
        end,
    Publish = F(channel_exchange_stats, exchange),
    Consume = F(channel_queue_stats, queue),

    F2 = case User of
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
        [{queues,      length(Qs0)},
         {exchanges,   length([X || V <- VHosts,
                                    X <- rabbit_exchange:list(V)])},
         {connections, F2(created_events(connection_stats, Tables))},
         {channels,    F2(created_events(channel_stats, Tables))}],
    reply([{message_stats, calculate_rates(Publish ++ Consume, State)},
           {queue_totals,  QueueTotals},
           {object_totals, ObjectTotals}], State);

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

handle_cast({event, Event}, State) ->
    handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

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

handle_event(Event = #event{type = queue_deleted}, State) ->
    handle_deleted(queue_stats, Event, State);

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

handle_event(Event = #event{type = connection_closed}, State) ->
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
    handle_deleted(channel_stats, Event, State),
    [delete_fine_stats(Type, Pid, State) ||
        Type <- ?FINE_STATS_TYPES],
    {ok, State};

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

handle_stats(TName, Stats0, Timestamp, Funs, RatesKeys,
             State = #state{tables = Tables}) ->
    Stats = lists:foldl(
              fun (K, StatsAcc) -> proplists:delete(K, StatsAcc) end,
              Stats0, ?FINE_STATS_TYPES),
    Table = orddict:fetch(TName, Tables),
    Id = {id(TName, Stats), stats},
    Stats1 = append_samples(Stats, Timestamp, Id, Table, RatesKeys),
    Stats2 = proplists:delete(
               id_name(TName), rabbit_mgmt_format:format(Stats1, Funs)),
    ets:insert(Table, {Id, Stats2, Timestamp}),
    {ok, State}.

handle_deleted(TName, #event{props = Props}, State = #state{tables = Tables}) ->
    Table = orddict:fetch(TName, Tables),
    ets:delete(Table, {id(TName, Props), create}),
    ets:delete(Table, {id(TName, Props), stats}),
    {ok, State}.

handle_consumer(Fun, Props,
                State = #state{tables = Tables}) ->
    P = rabbit_mgmt_format:format(Props, []),
    Table = orddict:fetch(consumers, Tables),
    Fun(Table, {pget(queue, P), pget(channel, P)}, P),
    {ok, State}.

handle_fine_stats(Type, Props, Timestamp, #state{tables = Tables}) ->
    case pget(Type, Props) of
        unknown ->
            ok;
        AllFineStats ->
            ChPid = id(channel_stats, Props),
            Table = orddict:fetch(Type, Tables),
            [handle_fine_stat(
               fine_stats_id(ChPid, Ids), Stats, Timestamp, Table) ||
                {Ids, Stats} <- AllFineStats]
    end.


handle_fine_stat(Id, Stats, Timestamp, Table) ->
    Total = lists:sum([V || {K, V} <- Stats, lists:member(K, ?DELIVER_GET)]),
    Stats1 = case Total of
                 0 -> Stats;
                 _ -> [{deliver_get, Total}|Stats]
             end,
    Res = append_samples(Stats1, Timestamp, Id, Table, ?FINE_STATS),
    ets:insert(Table, {Id, Res, Timestamp}).

delete_fine_stats(Type, ChPid, #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    ets:match_delete(Table, {{ChPid, '_'}, '_', '_'}),
    ets:match_delete(Table, {{ChPid, '_', '_'}, '_', '_'}).

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

get_fine_stats(Type, GroupBy, #state{tables = Tables}) ->
    get_fine_stats_from_list(
      GroupBy, ets:tab2list(orddict:fetch(Type, Tables))).

get_fine_stats_from_list(GroupBy, List) ->
    All = [{format_id(Id), Stats} ||
              {Id, Stats, _Timestamp} <- List],
    group_sum(GroupBy, All).

format_id({ChPid, #resource{name = Name, virtual_host = Vhost, kind = Kind}}) ->
    [{channel, ChPid}, {Kind, [{name, Name}, {vhost, Vhost}]}];
format_id({ChPid,
           #resource{name = QName, virtual_host = QVhost, kind = queue},
           #resource{name = XName, virtual_host = XVhost, kind = exchange}}) ->
    [{channel,  ChPid},
     {queue,    [{name, QName}, {vhost, QVhost}]},
     {exchange, [{name, XName}, {vhost, XVhost}]}].

%%----------------------------------------------------------------------------

merge_stats(Objs, Funs) ->
    [lists:foldl(fun (Fun, Props) -> Fun(Props) ++ Props end, Obj, Funs)
     || Obj <- Objs].

basic_stats_fun(Type, State = #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    fun (Props) ->
            Id = id_lookup(Type, Props),
            calculate_rates(lookup_element(Table, {Id, stats}), State)
    end.

fine_stats_fun(FineSpecs, State) ->
    FineStats = [{AttachName, AttachBy,
                  get_fine_stats(FineStatsType, GroupBy, State)}
                 || {FineStatsType, GroupBy, AttachName, AttachBy}
                        <- FineSpecs],
    fun (Props) ->
            lists:foldl(fun (FineStat, StatProps) ->
                                fine_stat(Props, StatProps, FineStat, State)
                        end, [], FineStats)
    end.

fine_stat(Props, StatProps, {AttachName, AttachBy, Dict}, State) ->
    Id = case AttachBy of
             channel ->
                 pget(pid, Props);
             _ ->
                 [{name, pget(name, Props)}, {vhost, pget(vhost, Props)}]
         end,
    case dict:find({AttachBy, Id}, Dict) of
        {ok, Stats} -> [{AttachName, pget(AttachName, StatProps, []) ++
                             augment_fine_stats(Stats, State)} |
                        proplists:delete(AttachName, StatProps)];
        error       -> StatProps
    end.

augment_fine_stats(Dict, State) when element(1, Dict) == dict ->
    [[{stats, augment_fine_stats(Stats, State)} |
      augment_msg_stats([IdTuple], State)]
     || {IdTuple, Stats} <- dict:to_list(Dict)];
augment_fine_stats(Stats, State) ->
    calculate_rates(Stats, State).

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

calculate_rates(Stats, State) -> [calculate_rate(S, State) || S <- Stats].

calculate_rate({Key, Val}, #state{interval = Interval}) ->
    case is_details(Key) of
        true  ->
            case length(Val) > 1 of
                true ->
                    [[{sample, S3}, {timestamp, T3}],
                     [{sample, S2}, {timestamp, T2}] | _] = Val,
                    {Inst, Avg} =
                        case rabbit_misc:now_ms() - T3 > Interval * 1.5 of
                            true  -> {0, 0};
                            false -> [{sample,    S1},
                                      {timestamp, T1}] = lists:last(Val),
                                     {(S3 - S2) * 1000 / (T3 - T2),
                                      (S3 - S1) * 1000 / (T3 - T1)}
                        end,
                    {Key, [{rate,     Inst},
                           {interval, T3 - T2},
                           {avg_rate, Avg},
                           {samples,  Val}]};
                false ->
                    {Key, [{samples, Val}]}
            end;
        false ->
            {Key, Val}
    end.

is_details(Key) -> lists:suffix("_details", atom_to_list(Key)).

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

basic_queue_stats(Objs, State) ->
    merge_stats(Objs, queue_funs(State)).

list_queue_stats(Objs, State) ->
    adjust_hibernated_memory_use(
      merge_stats(Objs, [fine_stats_fun(?FINE_STATS_QUEUE_LIST, State)] ++
                      queue_funs(State))).

detail_queue_stats(Objs, State) ->
    adjust_hibernated_memory_use(
      merge_stats(Objs, [consumer_details_fun(
                           fun (Props) ->
                                   {id_lookup(queue_stats, Props), '_'}
                           end, State),
                         fine_stats_fun(?FINE_STATS_QUEUE_DETAIL, State)] ++
                      queue_funs(State))).

queue_funs(State) ->
    [basic_stats_fun(queue_stats, State), augment_msg_stats_fun(State)].

exchange_stats(Objs, FineSpecs, State) ->
    merge_stats(Objs, [fine_stats_fun(FineSpecs, State),
                       augment_msg_stats_fun(State)]).

connection_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(connection_stats, State),
                       augment_msg_stats_fun(State)]).

list_channel_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(channel_stats, State),
                       fine_stats_fun(?FINE_STATS_CHANNEL_LIST, State),
                       augment_msg_stats_fun(State)]).

detail_channel_stats(Objs, State) ->
    merge_stats(Objs, [basic_stats_fun(channel_stats, State),
                       consumer_details_fun(
                         fun (Props) -> {'_', pget(pid, Props)} end, State),
                       fine_stats_fun(?FINE_STATS_CHANNEL_DETAIL, State),
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
