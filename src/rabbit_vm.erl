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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
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
    Conns        = (sup_memory(rabbit_tcp_client_sup) +
                        sup_memory(ssl_connection_sup) +
                        sup_memory(amqp_sup)),
    Qs           = (sup_memory(rabbit_amqqueue_sup) +
                        sup_memory(rabbit_mirror_queue_slave_sup)),
    Mnesia       = mnesia_memory(),
    MsgIndexETS  = ets_memory(rabbit_msg_store_ets_index),
    MsgIndexProc = (pid_memory(msg_store_transient) +
                        pid_memory(msg_store_persistent)),
    MgmtDbETS    = ets_memory(rabbit_mgmt_db),
    MgmtDbProc   = sup_memory(rabbit_mgmt_sup),
    Plugins      = plugin_memory() - MgmtDbProc,

    [{total,     Total},
     {processes, Processes},
     {ets,       ETS},
     {atom,      Atom},
     {binary,    Bin},
     {code,      Code},
     {system,    System}] =
        erlang:memory([total, processes, ets, atom, binary, code, system]),

    OtherProc = Processes - Conns - Qs - MsgIndexProc - MgmtDbProc - Plugins,

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

sup_memory(Sup) ->
    lists:sum([child_memory(P, T) || {_, P, T, _} <- sup_children(Sup)]) +
        pid_memory(Sup).

sup_children(Sup) ->
    rabbit_misc:with_exit_handler(
      rabbit_misc:const([]),
      fun () ->
              %% Just in case we end up talking to something that is
              %% not a supervisor by mistake.
              case supervisor:which_children(Sup) of
                  L when is_list(L) -> L;
                  _                 -> []
              end
      end).

pid_memory(Pid)  when is_pid(Pid)   -> case process_info(Pid, memory) of
                                           {memory, M} -> M;
                                           _           -> 0
                                       end;
pid_memory(Name) when is_atom(Name) -> case whereis(Name) of
                                           P when is_pid(P) -> pid_memory(P);
                                           _                -> 0
                                       end.

child_memory(Pid, worker)     when is_pid (Pid) -> pid_memory(Pid);
child_memory(Pid, supervisor) when is_pid (Pid) -> sup_memory(Pid);
child_memory(_, _)                              -> 0.

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

plugin_memory() ->
    lists:sum([plugin_memory(App) ||
                  {App, _, _} <- application:which_applications(),
                  is_plugin(atom_to_list(App))]).

plugin_memory(App) ->
    case application_controller:get_master(App) of
        undefined -> 0;
        Master    -> case application_master:get_child(Master) of
                         {Pid, _} when is_pid(Pid) -> sup_memory(Pid);
                         Pid      when is_pid(Pid) -> sup_memory(Pid);
                         _                         -> 0
                     end
    end.

is_plugin("rabbitmq_" ++ _) -> true;
is_plugin(App)              -> lists:member(App, ?MAGIC_PLUGINS).
