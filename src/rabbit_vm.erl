%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_vm).

-export([memory/0, binary/0]).

-define(MAGIC_PLUGINS, ["mochiweb", "webmachine", "cowboy", "sockjs",
                        "rfc4627_jsonrpc"]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(memory/0 :: () -> rabbit_types:infos()).
-spec(binary/0 :: () -> rabbit_types:infos()).

-endif.

%%----------------------------------------------------------------------------

%% Like erlang:memory(), but with awareness of rabbit-y things
memory() ->
    All = interesting_sups(),
    {Sums, _Other} = sum_processes(
                       lists:append(All), distinguishers(), [memory]),

    [Qs, QsSlave, ConnsReader, ConnsWriter, ConnsChannel, ConnsOther,
     MsgIndexProc, MgmtDbProc, Plugins] =
        [aggregate(Names, Sums, memory, fun (X) -> X end)
         || Names <- distinguished_interesting_sups()],

    Mnesia       = mnesia_memory(),
    MsgIndexETS  = ets_memory([msg_store_persistent, msg_store_transient]),
    MgmtDbETS    = ets_memory([rabbit_mgmt_db]),

    [{total,     Total},
     {processes, Processes},
     {ets,       ETS},
     {atom,      Atom},
     {binary,    Bin},
     {code,      Code},
     {system,    System}] =
        erlang:memory([total, processes, ets, atom, binary, code, system]),

    OtherProc = Processes
        - ConnsReader - ConnsWriter - ConnsChannel - ConnsOther
        - Qs - QsSlave - MsgIndexProc - Plugins - MgmtDbProc,

    [{total,              Total},
     {connection_readers,  ConnsReader},
     {connection_writers,  ConnsWriter},
     {connection_channels, ConnsChannel},
     {connection_other,    ConnsOther},
     {queue_procs,         Qs},
     {queue_slave_procs,   QsSlave},
     {plugins,             Plugins},
     {other_proc,          lists:max([0, OtherProc])}, %% [1]
     {mnesia,              Mnesia},
     {mgmt_db,             MgmtDbETS + MgmtDbProc},
     {msg_index,           MsgIndexETS + MsgIndexProc},
     {other_ets,           ETS - Mnesia - MsgIndexETS - MgmtDbETS},
     {binary,              Bin},
     {code,                Code},
     {atom,                Atom},
     {other_system,        System - ETS - Atom - Bin - Code}].

%% [1] - erlang:memory(processes) can be less than the sum of its
%% parts. Rather than display something nonsensical, just silence any
%% claims about negative memory. See
%% http://erlang.org/pipermail/erlang-questions/2012-September/069320.html

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
    [Other, Qs, QsSlave, ConnsReader, ConnsWriter, ConnsChannel, ConnsOther,
     MsgIndexProc, MgmtDbProc, Plugins] =
        [aggregate(Names, [{other, Rest} | Sums], binary, fun sum_binary/1)
         || Names <- [[other] | distinguished_interesting_sups()]],
    [{connection_readers,  ConnsReader},
     {connection_writers,  ConnsWriter},
     {connection_channels, ConnsChannel},
     {connection_other,    ConnsOther},
     {queue_procs,         Qs},
     {queue_slave_procs,   QsSlave},
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

ets_memory(OwnerNames) ->
    Owners = [whereis(N) || N <- OwnerNames],
    lists:sum([bytes(ets:info(T, memory)) || T <- ets:all(),
                                             O <- [ets:info(T, owner)],
                                             lists:member(O, Owners)]).

bytes(Words) ->  Words * erlang:system_info(wordsize).

interesting_sups() ->
    [[rabbit_amqqueue_sup_sup], conn_sups() | interesting_sups0()].

interesting_sups0() ->
    MsgIndexProcs = [msg_store_transient, msg_store_persistent],
    MgmtDbProcs   = [rabbit_mgmt_sup_sup],
    PluginProcs   = plugin_sups(),
    [MsgIndexProcs, MgmtDbProcs, PluginProcs].

conn_sups()     -> [rabbit_tcp_client_sup, ssl_connection_sup, amqp_sup].
conn_sups(With) -> [{Sup, With} || Sup <- conn_sups()].

distinguishers() -> [{rabbit_amqqueue_sup_sup, fun queue_type/1} |
                     conn_sups(fun conn_type/1)].

distinguished_interesting_sups() ->
    [[{rabbit_amqqueue_sup_sup, master}],
     [{rabbit_amqqueue_sup_sup, slave}],
     conn_sups(reader),
     conn_sups(writer),
     conn_sups(channel),
     conn_sups(other)]
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

-ifdef(use_specs).
-type(process() :: pid() | atom()).
-type(info_key() :: atom()).
-type(info_value() :: any()).
-type(info_item() :: {info_key(), info_value()}).
-type(accumulate() :: fun ((info_key(), info_value(), info_value()) ->
                                  info_value())).
-type(distinguisher() :: fun (([{term(), term()}]) -> atom())).
-type(distinguishers() :: [{info_key(), distinguisher()}]).
-spec(sum_processes/3 :: ([process()], distinguishers(), [info_key()]) ->
                              {[{process(), [info_item()]}], [info_item()]}).
-spec(sum_processes/4 :: ([process()], accumulate(), distinguishers(),
                          [info_item()]) ->
                              {[{process(), [info_item()]}], [info_item()]}).
-endif.

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
