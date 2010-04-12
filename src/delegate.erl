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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(delegate).
-include("delegate.hrl").

-behaviour(gen_server2).

-export([start_link/1, delegate_cast/2, delegate_call/2,
         delegate_gs2_call/3, delegate_gs2_pcall/4,
         delegate_gs2_cast/2, delegate_gs2_pcast/3,
         server/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------


%%----------------------------------------------------------------------------

start_link(Hash) ->
    gen_server2:start_link({local, server(Hash)},
                           ?MODULE, [], []).

delegate_gs2_call(Pid, Msg, Timeout) ->
    {Status, Res} =
        delegate_call(Pid, fun(P) -> gen_server2:call(P, Msg, Timeout) end),
    Res.

delegate_gs2_pcall(Pid, Pri, Msg, Timeout) ->
    {Status, Res} =
        delegate_call(Pid,
                      fun(P) -> gen_server2:pcall(P, Pri, Msg, Timeout) end),
    Res.

delegate_gs2_cast(Pid, Msg) ->
    delegate_cast(Pid, fun(P) -> gen_server2:cast(P, Msg) end).

delegate_gs2_pcast(Pid, Pri, Msg) ->
    delegate_cast(Pid, fun(P) -> gen_server2:pcast(P, Pri, Msg) end).


delegate_call(Node, Thunk) when is_atom(Node) ->
    gen_server2:call({server(), Node}, {thunk, Thunk}, infinity);

delegate_call(Pid, FPid) when is_pid(Pid) ->
    [[{Status, Res, _}]] = delegate_per_node([{node(Pid), [Pid]}],
                                             f_pid_node(fun delegate_call/2, FPid)),
    {Status, Res};

delegate_call(Pids, FPid) when is_list(Pids) ->
    lists:flatten(
        delegate_per_node(split_per_node(Pids),
                          f_pid_node(fun delegate_call/2, FPid))).

delegate_cast(Node, Thunk) when is_atom(Node) ->
    gen_server2:cast({server(), Node}, {thunk, Thunk});

delegate_cast(Pid, FPid) when is_pid(Pid) ->
    delegate_per_node([{node(Pid), [Pid]}],
                      f_pid_node(fun delegate_cast/2, FPid)),
    ok;

delegate_cast(Pids, FPid) when is_list(Pids) ->
    delegate_per_node(split_per_node(Pids),
                      f_pid_node(fun delegate_cast/2, FPid)),
    ok.

%%----------------------------------------------------------------------------

split_per_node(Pids) ->
    dict:to_list(
        lists:foldl(
          fun (Pid, D) ->
                  dict:update(node(Pid),
                              fun (Pids1) -> [Pid | Pids1] end,
                              [Pid], D)
          end,
          dict:new(), Pids)).

f_pid_node(DelegateFun, FPid) ->
    fun(Pid, Node) ->
        DelegateFun(Node, fun() -> FPid(Pid) end)
    end.

% TODO this only gets called when we are ONLY talking to the local node - can
% we improve this?
delegate_per_node([{Node, Pids}], FPidNode) when Node == node() ->
    % optimisation
    [[add_pid(FPidNode(Pid, Node), Pid) || Pid <- Pids]];

delegate_per_node(NodePids, FPidNode) ->
    rabbit_misc:upmap(
          fun ({Node, Pids}) ->
              [add_pid(FPidNode(Pid, Node), Pid) || Pid <- Pids]
          end,
          NodePids).

add_pid({Status, Result}, Pid) -> {Status, Result, Pid};
add_pid(Status, Pid) -> {Status, Pid}.

server() ->
    server(erlang:phash(self(), ?DELEGATE_PROCESSES)).

server(Hash) ->
    list_to_atom(string:concat("delegate_process_", integer_to_list(Hash))).

%%--------------------------------------------------------------------

init([]) ->
    {ok, no_state}.

handle_call({thunk, Thunk}, _From, State) ->
    Res = case catch Thunk() of
        {'EXIT', Reason} ->
            {error, {'EXIT', Reason}};
        Result ->
        {ok, Result}
        end,
    {reply, Res, State}.

handle_cast({thunk, Thunk}, State) ->
    catch Thunk(),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
