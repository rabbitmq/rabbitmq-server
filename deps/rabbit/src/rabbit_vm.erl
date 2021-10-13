%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_vm).

-export([memory/0, binary/0, ets_tables_memory/1]).

-define(MAGIC_PLUGINS, ["cowboy", "ranch", "sockjs"]).

%%----------------------------------------------------------------------------

-spec memory() -> rabbit_types:infos().

memory() ->
    All = interesting_sups(),
    {Sums, _Other} = sum_processes(
                       lists:append(All), distinguishers(), [memory]),

    [Qs, QsSlave, Qqs, ConnsReader, ConnsWriter, ConnsChannel, ConnsOther,
     MsgIndexProc, MgmtDbProc, Plugins] =
        [aggregate(Names, Sums, memory, fun (X) -> X end)
         || Names <- distinguished_interesting_sups()],

    MnesiaETS           = mnesia_memory(),
    MsgIndexETS         = ets_memory(msg_stores()),
    MetricsETS          = ets_memory([rabbit_metrics]),
    QuorumETS           = ets_memory([ra_log_ets]),
    MetricsProc  = try
                       [{_, M}] = process_info(whereis(rabbit_metrics), [memory]),
                       M
                   catch
                       error:badarg ->
                           0
                   end,
    MgmtDbETS           = ets_memory([rabbit_mgmt_storage]),
    [{total,     ErlangTotal},
     {processes, Processes},
     {ets,       ETS},
     {atom,      Atom},
     {binary,    Bin},
     {code,      Code},
     {system,    System}] =
        erlang:memory([total, processes, ets, atom, binary, code, system]),

    Strategy = vm_memory_monitor:get_memory_calculation_strategy(),
    Allocated = recon_alloc:memory(allocated),
    Rss = vm_memory_monitor:get_rss_memory(),

    AllocatedUnused = max(Allocated - ErlangTotal, 0),
    OSReserved = max(Rss - Allocated, 0),

    OtherProc = Processes
        - ConnsReader - ConnsWriter - ConnsChannel - ConnsOther
        - Qs - QsSlave - Qqs - MsgIndexProc - Plugins - MgmtDbProc - MetricsProc,

    [
     %% Connections
     {connection_readers,   ConnsReader},
     {connection_writers,   ConnsWriter},
     {connection_channels,  ConnsChannel},
     {connection_other,     ConnsOther},

     %% Queues
     {queue_procs,          Qs},
     {queue_slave_procs,    QsSlave},
     {quorum_queue_procs,   Qqs},

     %% Processes
     {plugins,              Plugins},
     {other_proc,           lists:max([0, OtherProc])}, %% [1]

     %% Metrics
     {metrics,              MetricsETS + MetricsProc},
     {mgmt_db,              MgmtDbETS + MgmtDbProc},

     %% ETS
     {mnesia,               MnesiaETS},
     {quorum_ets,           QuorumETS},
     {other_ets,            ETS - MnesiaETS - MetricsETS - MgmtDbETS - MsgIndexETS - QuorumETS},

     %% Messages (mostly, some binaries are not messages)
     {binary,               Bin},
     {msg_index,            MsgIndexETS + MsgIndexProc},

     %% System
     {code,                 Code},
     {atom,                 Atom},
     {other_system,         System - ETS - Bin - Code - Atom},
     {allocated_unused,     AllocatedUnused},
     {reserved_unallocated, OSReserved},
     {strategy,             Strategy},
     {total,                [{erlang, ErlangTotal},
                             {rss, Rss},
                             {allocated, Allocated}]}
    ].
%% [1] - erlang:memory(processes) can be less than the sum of its
%% parts. Rather than display something nonsensical, just silence any
%% claims about negative memory. See
%% http://erlang.org/pipermail/erlang-questions/2012-September/069320.html

-spec binary() -> rabbit_types:infos().

binary() ->
    All = interesting_sups(),
    {Sums, Rest} =
        sum_processes(
          lists:append(All),
          fun (binary, Info, Acc) ->
                  lists:foldl(fun ({Ptr, Sz, _RefCnt}, Acc0) ->
                                      sets:add_element({Ptr, Sz}, Acc0)
                              end, Acc, Info)
          end, distinguishers(), [{binary, sets:new()}]),
    [Other, Qs, QsSlave, Qqs, ConnsReader, ConnsWriter, ConnsChannel, ConnsOther,
     MsgIndexProc, MgmtDbProc, Plugins] =
        [aggregate(Names, [{other, Rest} | Sums], binary, fun sum_binary/1)
         || Names <- [[other] | distinguished_interesting_sups()]],
    [{connection_readers,  ConnsReader},
     {connection_writers,  ConnsWriter},
     {connection_channels, ConnsChannel},
     {connection_other,    ConnsOther},
     {queue_procs,         Qs},
     {queue_slave_procs,   QsSlave},
     {quorum_queue_procs,  Qqs},
     {plugins,             Plugins},
     {mgmt_db,             MgmtDbProc},
     {msg_index,           MsgIndexProc},
     {other,               Other}].

%%----------------------------------------------------------------------------

mnesia_memory() ->
    case mnesia:system_info(is_running) of
        yes -> lists:sum([bytes(mnesia:table_info(Tab, memory)) ||
                             Tab <- mnesia:system_info(tables)]);
        _   -> 0
    end.

ets_memory(Owners) ->
    lists:sum([V || {_K, V} <- ets_tables_memory(Owners)]).

-spec ets_tables_memory(Owners) -> rabbit_types:infos()
     when Owners :: all | OwnerProcessName | [OwnerProcessName],
          OwnerProcessName :: atom().

ets_tables_memory(all) ->
    [{ets:info(T, name), bytes(ets:info(T, memory))}
     || T <- ets:all(),
        is_atom(T)];
ets_tables_memory(OwnerName) when is_atom(OwnerName) ->
    ets_tables_memory([OwnerName]);
ets_tables_memory(Owners) when is_list(Owners) ->
    OwnerPids = lists:map(fun(O) when is_pid(O) -> O;
                             (O) when is_atom(O) -> whereis(O)
                          end,
                          Owners),
    [{ets:info(T, name), bytes(ets:info(T, memory))}
     || T <- ets:all(),
        lists:member(ets:info(T, owner), OwnerPids)].

bytes(Words) ->  try
                     Words * erlang:system_info(wordsize)
                 catch
                     _:_ -> 0
                 end.

interesting_sups() ->
    [queue_sups(), quorum_sups(), conn_sups() | interesting_sups0()].

queue_sups() ->
    all_vhosts_children(rabbit_amqqueue_sup_sup).

quorum_sups() ->
    %% TODO: in the future not all ra servers may be queues and we needs
    %% some way to filter this
    case whereis(ra_server_sup_sup) of
        undefined ->
            [];
        _ ->
            [Pid || {_, Pid, _, _} <-
                    supervisor:which_children(ra_server_sup_sup)]
    end.

msg_stores() ->
    all_vhosts_children(msg_store_transient)
    ++
    all_vhosts_children(msg_store_persistent).

all_vhosts_children(Name) ->
    case whereis(rabbit_vhost_sup_sup) of
        undefined -> [];
        Pid when is_pid(Pid) ->
            lists:filtermap(
                fun({_, VHostSupWrapper, _, _}) ->
                    case supervisor2:find_child(VHostSupWrapper,
                                                rabbit_vhost_sup) of
                        []         -> false;
                        [VHostSup] ->
                            case supervisor2:find_child(VHostSup, Name) of
                                [QSup] -> {true, QSup};
                                []     -> false
                            end
                    end
                end,
                supervisor:which_children(rabbit_vhost_sup_sup))
    end.

interesting_sups0() ->
    MsgIndexProcs = msg_stores(),
    MgmtDbProcs   = [rabbit_mgmt_sup_sup],
    PluginProcs   = plugin_sups(),
    [MsgIndexProcs, MgmtDbProcs, PluginProcs].

conn_sups()     ->
    Ranches = lists:flatten(ranch_server_sups()),
    [amqp_sup|Ranches].

ranch_server_sups() ->
    try
        [Pid || {_, _, Pid} <- ranch_server:get_connections_sups()]
    catch
        %% Ranch ETS table doesn't exist yet
        error:badarg  -> []
    end.

with(Sups, With) -> [{Sup, With} || Sup <- Sups].

distinguishers() -> with(queue_sups(), fun queue_type/1) ++
                    with(conn_sups(), fun conn_type/1).

distinguished_interesting_sups() ->
    [
     with(queue_sups(), master),
     with(queue_sups(), slave),
     quorum_sups(),
     with(conn_sups(), reader),
     with(conn_sups(), writer),
     with(conn_sups(), channel),
     with(conn_sups(), other)]
        ++ interesting_sups0().

plugin_sups() ->
    lists:append([plugin_sup(App) ||
                     {App, _, _} <- rabbit_misc:which_applications(),
                     is_plugin(atom_to_list(App))]).

plugin_sup(App) ->
    case application_controller:get_master(App) of
        undefined -> [];
        Master    -> case application_master:get_child(Master) of
                         {Pid, _} when is_pid(Pid) -> [process_name(Pid)];
                         Pid      when is_pid(Pid) -> [process_name(Pid)];
                         _                         -> []
                     end
    end.

process_name(Pid) ->
    case process_info(Pid, registered_name) of
        {registered_name, Name} -> Name;
        _                       -> Pid
    end.

is_plugin("rabbitmq_" ++ _) -> true;
is_plugin(App)              -> lists:member(App, ?MAGIC_PLUGINS).

aggregate(Names, Sums, Key, Fun) ->
    lists:sum([extract(Name, Sums, Key, Fun) || Name <- Names]).

extract(Name, Sums, Key, Fun) ->
    case keyfind(Name, Sums) of
        {value, Accs} -> Fun(keyfetch(Key, Accs));
        false         -> 0
    end.

sum_binary(Set) ->
    sets:fold(fun({_Pt, Sz}, Acc) -> Acc + Sz end, 0, Set).

queue_type(PDict) ->
    case keyfind(process_name, PDict) of
        {value, {rabbit_mirror_queue_slave, _}} -> slave;
        _                                       -> master
    end.

conn_type(PDict) ->
    case keyfind(process_name, PDict) of
        {value, {rabbit_reader,  _}} -> reader;
        {value, {rabbit_writer,  _}} -> writer;
        {value, {rabbit_channel, _}} -> channel;
        _                            -> other
    end.

%%----------------------------------------------------------------------------

%% NB: this code is non-rabbit specific.

-type process() :: pid() | atom().
-type info_key() :: atom().
-type info_value() :: any().
-type info_item() :: {info_key(), info_value()}.
-type accumulate() :: fun ((info_key(), info_value(), info_value()) ->
                                  info_value()).
-type distinguisher() :: fun (([{term(), term()}]) -> atom()).
-type distinguishers() :: [{info_key(), distinguisher()}].
-spec sum_processes([process()], distinguishers(), [info_key()]) ->
                              {[{process(), [info_item()]}], [info_item()]}.
-spec sum_processes([process()], accumulate(), distinguishers(),
                          [info_item()]) ->
                              {[{process(), [info_item()]}], [info_item()]}.

sum_processes(Names, Distinguishers, Items) ->
    sum_processes(Names, fun (_, X, Y) -> X + Y end, Distinguishers,
                  [{Item, 0} || Item <- Items]).

%% summarize the process_info of all processes based on their
%% '$ancestor' hierarchy, recorded in their process dictionary.
%%
%% The function takes
%%
%% 1) a list of names/pids of processes that are accumulation points
%%    in the hierarchy.
%%
%% 2) a function that aggregates individual info items -taking the
%%    info item key, value and accumulated value as the input and
%%    producing a new accumulated value.
%%
%% 3) a list of info item key / initial accumulator value pairs.
%%
%% The process_info of a process is accumulated at the nearest of its
%% ancestors that is mentioned in the first argument, or, if no such
%% ancestor exists or the ancestor information is absent, in a special
%% 'other' bucket.
%%
%% The result is a pair consisting of
%%
%% 1) a k/v list, containing for each of the accumulation names/pids a
%%    list of info items, containing the accumulated data, and
%%
%% 2) the 'other' bucket - a list of info items containing the
%%    accumulated data of all processes with no matching ancestors
%%
%% Note that this function operates on names as well as pids, but
%% these must match whatever is contained in the '$ancestor' process
%% dictionary entry. Generally that means for all registered processes
%% the name should be used.
sum_processes(Names, Fun, Distinguishers, Acc0) ->
    Items = [Item || {Item, _Blank0} <- Acc0],
    {NameAccs, OtherAcc} =
        lists:foldl(
          fun (Pid, Acc) ->
                  InfoItems = [registered_name, dictionary | Items],
                  case process_info(Pid, InfoItems) of
                      undefined ->
                          Acc;
                      [{registered_name, RegName}, {dictionary, D} | Vals] ->
                          %% see docs for process_info/2 for the
                          %% special handling of 'registered_name'
                          %% info items
                          Extra = case RegName of
                                      [] -> [];
                                      N  -> [N]
                                  end,
                          Name0 = find_ancestor(Extra, D, Names),
                          Name = case keyfind(Name0, Distinguishers) of
                                     {value, DistFun} -> {Name0, DistFun(D)};
                                     false            -> Name0
                                 end,
                          accumulate(
                            Name, Fun, orddict:from_list(Vals), Acc, Acc0)
                  end
          end, {orddict:new(), Acc0}, processes()),
    %% these conversions aren't strictly necessary; we do them simply
    %% for the sake of encapsulating the representation.
    {[{Name, orddict:to_list(Accs)} ||
         {Name, Accs} <- orddict:to_list(NameAccs)],
     orddict:to_list(OtherAcc)}.

find_ancestor(Extra, D, Names) ->
    Ancestors = case keyfind('$ancestors', D) of
                    {value, Ancs} -> Ancs;
                    false         -> []
                end,
    case lists:splitwith(fun (A) -> not lists:member(A, Names) end,
                         Extra ++ Ancestors) of
        {_,         []} -> undefined;
        {_, [Name | _]} -> Name
    end.

accumulate(undefined, Fun, ValsDict, {NameAccs, OtherAcc}, _Acc0) ->
    {NameAccs, orddict:merge(Fun, ValsDict, OtherAcc)};
accumulate(Name,      Fun, ValsDict, {NameAccs, OtherAcc}, Acc0) ->
    F = fun (NameAcc) -> orddict:merge(Fun, ValsDict, NameAcc) end,
    {case orddict:is_key(Name, NameAccs) of
         true  -> orddict:update(Name, F,       NameAccs);
         false -> orddict:store( Name, F(Acc0), NameAccs)
     end, OtherAcc}.

keyfetch(K, L) -> {value, {_, V}} = lists:keysearch(K, 1, L),
                  V.

keyfind(K, L) -> case lists:keysearch(K, 1, L) of
                     {value, {_, V}} -> {value, V};
                     false           -> false
                 end.
