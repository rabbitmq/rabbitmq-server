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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_event_collector).

-include("rabbit_mgmt.hrl").
-include("rabbit_mgmt_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0, get_last_queue_length/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1,
         prioritise_cast/3]).

%% For testing
-export([override_lookups/1, reset_lookups/0]).

-import(rabbit_misc, [pget/3]).
-import(rabbit_mgmt_db, [pget/2, id_name/1, id/2, lookup_element/2]).

%% See the comment on rabbit_mgmt_db for the explanation of
%% events and stats.

-record(state, {
          lookups,
          interval,
          event_refresh_ref,
          rates_mode}).

-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
                           channel_queue_exchange_stats]).
-define(TABLES, [queue_stats, connection_stats, channel_stats,
                 consumers_by_queue, consumers_by_channel,
                 node_stats, node_node_stats,
                 %% database of aggregated samples
                 aggregated_stats,
                 %% What the previous info item was for any given
                 %% {queue/channel/connection}
                 old_stats]).

-define(DROP_LENGTH, 1000).

prioritise_cast({event, #event{type  = Type,
                               props = Props}}, Len, _State)
  when (Type =:= channel_stats orelse
        Type =:= queue_stats) andalso Len > ?DROP_LENGTH ->
    put(last_queue_length, Len),
    case pget(idle_since, Props) of
        unknown -> drop;
        _       -> 0
    end;
prioritise_cast(_Msg, Len, _State) ->
    put(last_queue_length, Len),
    0.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() ->
    Ref = make_ref(),
    case gen_server2:start_link({global, ?MODULE}, ?MODULE, [Ref], []) of
        {ok, Pid} -> register(?MODULE, Pid), %% [1]
                     rabbit:force_event_refresh(Ref),
                     {ok, Pid};
        Else      -> Else
    end.
%% [1] For debugging it's helpful to locally register the name too
%% since that shows up in places global names don't.

override_lookups(Lookups) ->
    gen_server2:call({global, ?MODULE}, {override_lookups, Lookups}, infinity).
reset_lookups() ->
    gen_server2:call({global, ?MODULE}, reset_lookups, infinity).
get_last_queue_length() ->
    gen_server2:call({global, ?MODULE}, get_last_queue_length, infinity).

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

init([Ref]) ->
    %% When Rabbit is overloaded, it's usually especially important
    %% that the management plugin work.
    process_flag(priority, high),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    {ok, RatesMode} = application:get_env(rabbitmq_management, rates_mode),
    rabbit_node_monitor:subscribe(self()),
    rabbit_log:info("Statistics event collector started.~n"),
    ?TABLES = [ets:new(Key, [ordered_set, named_table]) || Key <- ?TABLES],
    {ok, reset_lookups(
           #state{interval               = Interval,
                  event_refresh_ref      = Ref,
                  rates_mode             = RatesMode}), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

%% Used in rabbit_mgmt_test_db where we need guarantees events have
%% been handled before querying
handle_call({event, Event = #event{reference = none}}, _From, State) ->
    handle_event(Event, State),
    reply(ok, State);

handle_call({override_lookups, Lookups}, _From, State) ->
    reply(ok, State#state{lookups = Lookups});

handle_call(reset_lookups, _From, State) ->
    reply(ok, reset_lookups(State));

handle_call(get_last_queue_length, _From, State) ->
    reply(get(last_queue_length), State);

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

%% Only handle events that are real, or pertain to a force-refresh
%% that we instigated.
handle_cast({event, Event = #event{reference = none}}, State) ->
    handle_event(Event, State),
    noreply(State);

handle_cast({event, Event = #event{reference = Ref}},
            State = #state{event_refresh_ref = Ref}) ->
    handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info({node_down, Node}, State) ->
    Conns = created_events(connection_stats),
    Chs = created_events(channel_stats),
    delete_all_from_node(connection_closed, Node, Conns, State),
    delete_all_from_node(channel_closed, Node, Chs, State),
    noreply(State);

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

reset_lookups(State) ->
    State#state{lookups = [{exchange, fun rabbit_exchange:lookup/1},
                           {queue,    fun rabbit_amqqueue:lookup/1}]}.

handle_pre_hibernate(State) ->
    %% rabbit_event can end up holding on to some memory after a busy
    %% workout, but it's not a gen_server so we can't make it
    %% hibernate. The best we can do is forcibly GC it here (if
    %% rabbit_mgmt_db is hibernating the odds are rabbit_event is
    %% quiescing in some way too).
    rpc:multicall(
      rabbit_mnesia:cluster_nodes(running), rabbit_mgmt_db_handler, gc, []),
    {hibernate, State}.

delete_all_from_node(Type, Node, [Item | Items], State) ->
    Pid = pget(pid, Item),
    case node(Pid) of
        Node -> handle_event(#event{type = Type, props = [{pid, Pid}]}, State);
        _    -> ok
    end,
    delete_all_from_node(Type, Node, Items, State).

%%----------------------------------------------------------------------------
%% Internal, utilities
%%----------------------------------------------------------------------------

fine_stats_id(ChPid, {Q, X}) -> {ChPid, Q, X};
fine_stats_id(ChPid, QorX)   -> {ChPid, QorX}.

ceil(TS, #state{interval = Interval}) ->
    rabbit_mgmt_util:ceil(TS, Interval).

%%----------------------------------------------------------------------------
%% Internal, event-receiving side
%%----------------------------------------------------------------------------

handle_event(#event{type = queue_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(queue_stats, Stats, Timestamp,
                 {fun rabbit_mgmt_format:format_queue_stats/1, false},
                 ?QUEUE_MSG_COUNTS, ?QUEUE_MSG_RATES, State);

handle_event(Event = #event{type = queue_deleted,
                            props = [{name, Name}],
                            timestamp = Timestamp},
             State) ->
    delete_consumers(Name, consumers_by_queue, consumers_by_channel),
    %% This is fiddly. Unlike for connections and channels, we need to
    %% decrease any amalgamated coarse stats for [messages,
    %% messages_ready, messages_unacknowledged] for this queue - since
    %% the queue's deletion means we have really got rid of messages!
    Id = {coarse, {queue_stats, Name}},
    %% This ceil must correspond to the ceil in append_samples/5
    TS = ceil(Timestamp, State),
    OldStats = lookup_element(old_stats, Id),
    record_sample_list(Id, OldStats, TS, State, ?QUEUE_MSG_COUNTS),
    delete_samples(channel_queue_stats,  {'_', Name}),
    delete_samples(queue_exchange_stats, {Name, '_'}),
    delete_samples(queue_stats,          Name),
    handle_deleted(queue_stats, Event);

handle_event(Event = #event{type = exchange_deleted,
                            props = [{name, Name}]}, _State) ->
    delete_samples(channel_exchange_stats,  {'_', Name}),
    delete_samples(queue_exchange_stats,    {'_', Name}),
    delete_samples(exchange_stats,          Name),
    handle_deleted(exchange_stats, Event);

handle_event(#event{type = vhost_deleted,
                    props = [{name, Name}]}, _State) ->
    delete_samples(vhost_stats, Name);

handle_event(#event{type = connection_created, props = Stats}, _State) ->
    handle_created(
      connection_stats, Stats,
      {fun rabbit_mgmt_format:format_connection_created/1, true});

handle_event(#event{type = connection_stats, props = Stats,
                    timestamp = Timestamp},
             State) ->
    handle_stats(connection_stats, Stats, Timestamp, {[], false}, ?COARSE_CONN_STATS,
                 State);

handle_event(Event = #event{type  = connection_closed,
                            props = [{pid, Pid}]}, _State) ->
    delete_samples(connection_stats, Pid),
    handle_deleted(connection_stats, Event);

handle_event(#event{type = channel_created, props = Stats}, _State) ->
    handle_created(channel_stats, Stats, {[], false});

handle_event(#event{type = channel_stats, props = Stats, timestamp = Timestamp},
             State) ->
    handle_stats(channel_stats, Stats, Timestamp,
                 {fun rabbit_mgmt_format:format_channel_stats/1, true},
                 [], State),
    ChPid = id(channel_stats, Stats),
    AllStats = [old_fine_stats(Type, Stats, State)
                || Type <- ?FINE_STATS_TYPES],
    ets:match_delete(old_stats, {{fine, {ChPid, '_'}},      '_'}),
    ets:match_delete(old_stats, {{fine, {ChPid, '_', '_'}}, '_'}),
    handle_fine_stats_list(Timestamp, State, AllStats);

handle_event(Event = #event{type = channel_closed,
                            props = [{pid, Pid}]},
             _State) ->
    delete_consumers(Pid, consumers_by_channel, consumers_by_queue),
    delete_samples(channel_queue_stats,    {Pid, '_'}),
    delete_samples(channel_exchange_stats, {Pid, '_'}),
    delete_samples(channel_stats,          Pid),
    handle_deleted(channel_stats, Event),
    ets:match_delete(old_stats, {{fine, {Pid, '_'}},      '_'}),
    ets:match_delete(old_stats, {{fine, {Pid, '_', '_'}}, '_'});

handle_event(#event{type = consumer_created, props = Props}, _State) ->
    Fmt = {fun rabbit_mgmt_format:format_arguments/1, true},
    handle_consumer(fun(Table, Id, P0) ->
                            P = rabbit_mgmt_format:format(P0, Fmt),
                            ets:insert(Table, {Id, P})
                    end,
                    Props);

handle_event(#event{type = consumer_deleted, props = Props}, _State) ->
    handle_consumer(fun(Table, Id, _P) -> ets:delete(Table, Id) end,
                    Props);

%% TODO: we don't clear up after dead nodes here - this is a very tiny
%% leak every time a node is permanently removed from the cluster. Do
%% we care?
handle_event(#event{type = node_stats, props = Stats0, timestamp = Timestamp},
             State) ->
    Stats = proplists:delete(persister_stats, Stats0) ++
        pget(persister_stats, Stats0),
    handle_stats(node_stats, Stats, Timestamp, {[], false}, ?COARSE_NODE_STATS, State);

handle_event(#event{type = node_node_stats, props = Stats,
                    timestamp = Timestamp}, State) ->
    handle_stats(node_node_stats, Stats, Timestamp, {[], false}, ?COARSE_NODE_NODE_STATS,
                 State);

handle_event(Event = #event{type  = node_node_deleted,
                            props = [{route, Route}]}, _State) ->
    delete_samples(node_node_stats, Route),
    handle_deleted(node_node_stats, Event);

handle_event(_Event, _State) ->
    ok.

handle_created(TName, Stats, Funs) ->
    Formatted = rabbit_mgmt_format:format(Stats, Funs),
    ets:insert(TName, {{id(TName, Stats), create},
                                              Formatted,
                                              pget(name, Stats)}).

handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, State) ->
    handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, [], State).

handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, NoAggRatesKeys,
             State) ->
    Id = id(TName, Stats),
    IdSamples = {coarse, {TName, Id}},
    OldStats = lookup_element(old_stats, IdSamples),
    append_samples(
      Stats, Timestamp, OldStats, IdSamples, RatesKeys, true, State),
    append_samples(
      Stats, Timestamp, OldStats, IdSamples, NoAggRatesKeys, false, State),
    StripKeys = [id_name(TName)] ++ RatesKeys ++ ?FINE_STATS_TYPES,
    Stats1 = [{K, V} || {K, V} <- Stats, not lists:member(K, StripKeys)],
    Stats2 = rabbit_mgmt_format:format(Stats1, Funs),
    ets:insert(TName, {{Id, stats}, Stats2, Timestamp}).

handle_deleted(TName, #event{props = Props}) ->
    Id = id(TName, Props),
    case lists:member(TName, ?TABLES) of
        true  -> ets:delete(TName, {Id, create}),
                 ets:delete(TName, {Id, stats});
        false -> ok
    end,
    ets:delete(old_stats, {coarse, {TName, Id}}).

handle_consumer(Fun, Props) ->
    P = rabbit_mgmt_format:format(Props, {[], false}),
    CTag = pget(consumer_tag, P),
    Q    = pget(queue,        P),
    Ch   = pget(channel,      P),
    Fun(consumers_by_queue,  {Q, Ch, CTag}, P),
    Fun(consumers_by_channel, {Ch, Q, CTag}, P).

%% The consumer_deleted event is emitted by queues themselves -
%% therefore in the event that a queue dies suddenly we may not get
%% it. The best way to handle this is to make sure we also clean up
%% consumers when we hear about any queue going down.
delete_consumers(PrimId, PrimTableName, SecTableName) ->
    SecIdCTags = ets:match(PrimTableName, {{PrimId, '$1', '$2'}, '_'}),
    ets:match_delete(PrimTableName, {{PrimId, '_', '_'}, '_'}),
    delete_consumers_entry(PrimId, SecTableName, SecIdCTags).

delete_consumers_entry(PrimId, SecTableName, [[SecId, CTag] | SecIdTags]) ->
    ets:delete(SecTableName, {SecId, PrimId, CTag}),
    delete_consumers_entry(PrimId, SecTableName, SecIdTags);
delete_consumers_entry(_PrimId, _SecTableName, []) ->
    ok.

old_fine_stats(Type, Props, #state{}) ->
    case pget(Type, Props) of
        unknown       -> ignore;
        AllFineStats0 -> ChPid = id(channel_stats, Props),
                         [begin
                              Id = fine_stats_id(ChPid, Ids),
                              {Id, Stats, lookup_element(old_stats, {fine, Id})}
                          end || {Ids, Stats} <- AllFineStats0]
    end.

handle_fine_stats_list(Timestamp, State, [AllStatsElem | AllStats]) ->
    handle_fine_stats(Timestamp, AllStatsElem, State),
    handle_fine_stats_list(Timestamp, State, AllStats);
handle_fine_stats_list(_Timestamp, _State, []) ->
    ok.

handle_fine_stats(_Timestamp, ignore, _State) ->
    ok;

handle_fine_stats(Timestamp, [{Id, Stats, OldStats} | AllStats], State) ->
    handle_fine_stat(Id, Stats, Timestamp, OldStats, State),
    handle_fine_stats(Timestamp, AllStats, State);
handle_fine_stats(_Timestamp, [], _State) ->
    ok.

handle_fine_stat(Id, Stats, Timestamp, OldStats, State) ->
    Total = lists:sum([V || {K, V} <- Stats, lists:member(K, ?DELIVER_GET)]),
    Stats1 = case Total of
                 0 -> Stats;
                 _ -> [{deliver_get, Total}|Stats]
             end,
    append_samples(Stats1, Timestamp, OldStats, {fine, Id}, all, true, State).

delete_samples(Type, Id) ->
    Samples = ets:match(aggregated_stats, {{{Type, Id}, '_'}, '$2'}),
    case Samples of
        [] ->
            ok;
        _ ->
            free_stats(Samples),
            ets:match_delete(aggregated_stats, delete_match(Type, Id))
    end.

free_stats([[Stat] | Stats]) ->
    rabbit_mgmt_stats:free(Stat),
    free_stats(Stats);
free_stats([]) ->
    ok.

delete_match(Type, Id) -> {{{Type, Id}, '_'}, '_'}.

append_samples(Stats, TS, OldStats, Id, Keys, Agg, State) ->
    case ignore_coarse_sample(Id, State) of
        false ->
            %% This ceil must correspond to the ceil in handle_event
            %% queue_deleted
            NewMS = ceil(TS, State),
            case Keys of
                all ->
                    append_all_samples(NewMS, OldStats, Id, Agg, State, Stats);
                _   ->
                    append_some_samples(NewMS, OldStats, Id, Agg, State, Stats, Keys)
            end,
            ets:insert(old_stats, {Id, Stats});
        true ->
            ok
    end.

append_some_samples(NewMS, OldStats, Id, Agg, State, Stats, [K | Keys]) ->
    V = pget(K, Stats),
    case V =/= 0 orelse lists:member(K, ?ALWAYS_REPORT_STATS) of
        true ->
            append_sample(K, V, NewMS, OldStats, Id, Agg, State);
        false ->
            ok
    end,
    append_some_samples(NewMS, OldStats, Id, Agg, State, Stats, Keys);
append_some_samples(_NewMS, _OldStats, _Id, _Agg, _State, _Stats, []) ->
    ok.

append_all_samples(NewMS, OldStats, Id, Agg, State, [{K, V} | Stats]) ->
    append_sample(K, V, NewMS, OldStats, Id, Agg, State),
    append_all_samples(NewMS, OldStats, Id, Agg, State, Stats);
append_all_samples(_NewMS, _OldStats, _Id, _Agg, _State, []) ->
    ok.

append_sample(Key, Val, NewMS, OldStats, Id, Agg, State) when is_number(Val) ->
    OldVal = case pget(Key, OldStats, 0) of
        N when is_number(N) -> N;
        _                   -> 0
    end,
    record_sample(Id, {Key, Val - OldVal, NewMS, State}, Agg, State),
    ok;
append_sample(_Key, _Value, _NewMS, _OldStats, _Id, _Agg, _State) ->
    ok.

ignore_coarse_sample({coarse, {queue_stats, Q}}, State) ->
    not object_exists(Q, State);
ignore_coarse_sample(_, _) ->
    false.

record_sample_list(Id, OldStats, TS, State, [Key | Keys]) ->
    record_sample(Id, {Key, -pget(Key, OldStats, 0), TS, State}, true, State),
    record_sample_list(Id, OldStats, TS, State, Keys);
record_sample_list(_Id, _OldStats, _TS, _State, []) ->
    ok.

%% Node stats do not have a vhost of course
record_sample({coarse, {node_stats, _Node} = Id}, Args, true, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, {node_node_stats, _Names} = Id}, Args, true, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, Id}, Args, false, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, Id}, Args, true, _State) ->
    record_sample0(Id, Args),
    record_sample0({vhost_stats, vhost(Id)}, Args);

%% Deliveries / acks (Q -> Ch)
record_sample({fine, {Ch, Q = #resource{kind = queue}}}, Args, true, State) ->
    case object_exists(Q, State) of
        true  -> record_sample0({channel_queue_stats, {Ch, Q}}, Args),
                 record_sample0({queue_stats,         Q},       Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(Q)}, Args);

%% Publishes / confirms (Ch -> X)
record_sample({fine, {Ch, X = #resource{kind = exchange}}}, Args, true,State) ->
    case object_exists(X, State) of
        true  -> record_sample0({channel_exchange_stats, {Ch, X}}, Args),
                 record_sampleX(publish_in,              X,        Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(X)}, Args);

%% Publishes (but not confirms) (Ch -> X -> Q)
record_sample({fine, {_Ch,
                      Q = #resource{kind = queue},
                      X = #resource{kind = exchange}}}, Args, true, State) ->
    %% TODO This one logically feels like it should be here. It would
    %% correspond to "publishing channel message rates to queue" -
    %% which would be nice to handle - except we don't. And just
    %% uncommenting this means it gets merged in with "consuming
    %% channel delivery from queue" - which is not very helpful.
    %% record_sample0({channel_queue_stats, {Ch, Q}}, Args),
    QExists = object_exists(Q, State),
    XExists = object_exists(X, State),
    case QExists of
        true  -> record_sample0({queue_stats,          Q},       Args);
        false -> ok
    end,
    case QExists andalso XExists of
        true  -> record_sample0({queue_exchange_stats, {Q,  X}}, Args);
        false -> ok
    end,
    case XExists of
        true  -> record_sampleX(publish_out,           X,        Args);
        false -> ok
    end.

%% We have to check the queue and exchange objects still exist since
%% their deleted event could be overtaken by a channel stats event
%% which contains fine stats referencing them. That's also why we
%% don't need to check the channels exist - their deleted event can't
%% be overtaken by their own last stats event.
%%
%% Also, sometimes the queue_deleted event is not emitted by the queue
%% (in the nodedown case) - so it can overtake the final queue_stats
%% event (which is not *guaranteed* to be lost). So we make a similar
%% check for coarse queue stats.
%%
%% We can be sure that mnesia will be up to date by the time we receive
%% the event (even though we dirty read) since the deletions are
%% synchronous and we do not emit the deleted event until after the
%% deletion has occurred.
object_exists(Name = #resource{kind = Kind}, #state{lookups = Lookups}) ->
    case (pget(Kind, Lookups))(Name) of
        {ok, _} -> true;
        _       -> false
    end.

vhost(#resource{virtual_host = VHost}) ->
    VHost;
vhost({queue_stats, #resource{virtual_host = VHost}}) ->
    VHost;
vhost({TName, Pid}) ->
    pget(vhost, lookup_element(TName, {Pid, create})).

%% exchanges have two sets of "publish" stats, so rearrange things a touch
record_sampleX(RenamePublishTo, X, {publish, Diff, TS, State}) ->
    record_sample0({exchange_stats, X}, {RenamePublishTo, Diff, TS, State});
record_sampleX(_RenamePublishTo, X, {Type, Diff, TS, State}) ->
    record_sample0({exchange_stats, X}, {Type, Diff, TS, State}).

%% Ignore case where ID1 and ID2 are in a tuple, i.e. detailed stats,
%% when in basic mode
record_sample0({Type, {_ID1, _ID2}}, {_, _, _, #state{rates_mode = basic}})
  when Type =/= node_node_stats ->
    ok;
record_sample0(Id0, {Key, Diff, TS, #state{}}) ->
    Id = {Id0, Key},
    Old = case lookup_element(aggregated_stats, Id) of
              [] -> rabbit_mgmt_stats:blank();
              E  -> E
          end,
    ets:insert(aggregated_stats, {Id, rabbit_mgmt_stats:record(TS, Diff, Old)}).

created_events(Table) ->
    ets:select(Table, [{{{'_', '$1'}, '$2', '_'}, [{'==', 'create', '$1'}],
                        ['$2']}]).
