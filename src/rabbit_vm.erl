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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_vm).

-export([memory/0]).

-define(MAGIC_PLUGINS, ["mochiweb", "webmachine", "cowboy", "sockjs",
                        "rfc4627_jsonrpc"]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(memory/0 :: () -> rabbit_types:infos()).

-endif.

%%----------------------------------------------------------------------------

%% Like erlang:memory(), but with awareness of rabbit-y things
memory() ->
    ConnProcs     = [rabbit_tcp_client_sup, ssl_connection_sup, amqp_sup],
    QProcs        = [rabbit_amqqueue_sup, rabbit_mirror_queue_slave_sup],
    MsgIndexProcs = [msg_store_transient, msg_store_persistent],
    MgmtDbProcs   = [rabbit_mgmt_sup],
    PluginProcs   = plugin_sups(),

    All = [ConnProcs, QProcs, MsgIndexProcs, MgmtDbProcs, PluginProcs],

    {Sums, _Other} = sum_processes(lists:append(All), [memory]),

    [Conns, Qs, MsgIndexProc, MgmtDbProc, AllPlugins] =
        [aggregate_memory(Names, Sums) || Names <- All],

    Mnesia       = mnesia_memory(),
    MsgIndexETS  = ets_memory(rabbit_msg_store_ets_index),
    MgmtDbETS    = ets_memory(rabbit_mgmt_db),
    Plugins      = AllPlugins - MgmtDbProc,

    [{total,     Total},
     {processes, Processes},
     {ets,       ETS},
     {atom,      Atom},
     {binary,    Bin},
     {code,      Code},
     {system,    System}] =
        erlang:memory([total, processes, ets, atom, binary, code, system]),

    OtherProc = Processes - Conns - Qs - MsgIndexProc - AllPlugins,

    [{total,            Total},
     {connection_procs, Conns},
     {queue_procs,      Qs},
     {plugins,          Plugins},
     {other_proc,       lists:max([0, OtherProc])}, %% [1]
     {mnesia,           Mnesia},
     {mgmt_db,          MgmtDbETS + MgmtDbProc},
     {msg_index,        MsgIndexETS + MsgIndexProc},
     {other_ets,        ETS - Mnesia - MsgIndexETS - MgmtDbETS},
     {binary,           Bin},
     {code,             Code},
     {atom,             Atom},
     {other_system,     System - ETS - Atom - Bin - Code}].

%% [1] - erlang:memory(processes) can be less than the sum of its
%% parts. Rather than display something nonsensical, just silence any
%% claims about negative memory. See
%% http://erlang.org/pipermail/erlang-questions/2012-September/069320.html

%%----------------------------------------------------------------------------

mnesia_memory() ->
    case mnesia:system_info(is_running) of
        yes -> lists:sum([bytes(mnesia:table_info(Tab, memory)) ||
                             Tab <- mnesia:system_info(tables)]);
        no  -> 0
    end.

ets_memory(Name) ->
    lists:sum([bytes(ets:info(T, memory)) || T <- ets:all(),
                                             N <- [ets:info(T, name)],
                                             N =:= Name]).

bytes(Words) ->  Words * erlang:system_info(wordsize).

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

aggregate_memory(Names, Sums) ->
    lists:sum([extract_memory(Name, Sums) || Name <- Names]).

extract_memory(Name, Sums) ->
    {value, {_, Accs}} = lists:keysearch(Name, 1, Sums),
    {value, {memory, V}} = lists:keysearch(memory, 1, Accs),
    V.

%%----------------------------------------------------------------------------

%% NB: this code is non-rabbit specific.

-ifdef(use_specs).
-type(process() :: pid() | atom()).
-type(info_key() :: atom()).
-type(info_value() :: any()).
-type(info_item() :: {info_key(), info_value()}).
-type(accumulate() :: fun ((info_key(), info_value(), info_value()) ->
                                  info_value())).
-spec(sum_processes/2 :: ([process()], [info_key()]) ->
                              {[{process(), [info_item()]}], [info_item()]}).
-spec(sum_processes/3 :: ([process()], accumulate(), [info_item()]) ->
                              {[{process(), [info_item()]}], [info_item()]}).
-endif.

sum_processes(Names, Items) ->
    sum_processes(Names, fun (_, X, Y) -> X + Y end,
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
sum_processes(Names, Fun, Acc0) ->
    Items = [Item || {Item, _Val0} <- Acc0],
    Acc0Dict  = orddict:from_list(Acc0),
    NameAccs0 = orddict:from_list([{Name, Acc0Dict} || Name <- Names]),
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
                          accumulate(find_ancestor(Extra, D, Names), Fun,
                                     orddict:from_list(Vals), Acc)
                  end
          end, {NameAccs0, Acc0Dict}, processes()),
    %% these conversions aren't strictly necessary; we do them simply
    %% for the sake of encapsulating the representation.
    {[{Name, orddict:to_list(Accs)} ||
         {Name, Accs} <- orddict:to_list(NameAccs)],
     orddict:to_list(OtherAcc)}.

find_ancestor(Extra, D, Names) ->
    Ancestors = case lists:keysearch('$ancestors', 1, D) of
                    {value, {_, Ancs}} -> Ancs;
                    false              -> []
                end,
    case lists:splitwith(fun (A) -> not lists:member(A, Names) end,
                         Extra ++ Ancestors) of
        {_,         []} -> undefined;
        {_, [Name | _]} -> Name
    end.

accumulate(undefined, Fun, ValsDict, {NameAccs, OtherAcc}) ->
    {NameAccs, orddict:merge(Fun, ValsDict, OtherAcc)};
accumulate(Name,      Fun, ValsDict, {NameAccs, OtherAcc}) ->
    F = fun (NameAcc) -> orddict:merge(Fun, ValsDict, NameAcc) end,
    {orddict:update(Name, F, NameAccs), OtherAcc}.
