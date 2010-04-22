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
-define(DELEGATE_PROCESS_COUNT_MULTIPLIER, 2).

-behaviour(gen_server2).

-export([start_link/1, cast/2, call/2,
         gs2_call/3, gs2_pcall/4,
         gs2_cast/2, gs2_pcast/3,
         server/1, process_count/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------


%%----------------------------------------------------------------------------

start_link(Hash) ->
    gen_server2:start_link({local, server(Hash)},
                           ?MODULE, [], []).

gs2_call(Pid, Msg, Timeout) ->
    {_Status, Res} =
        call(Pid, fun(P) -> gen_server2:call(P, Msg, Timeout) end),
    Res.

gs2_pcall(Pid, Pri, Msg, Timeout) ->
    {_Status, Res} =
        call(Pid, fun(P) -> gen_server2:pcall(P, Pri, Msg, Timeout) end),
    Res.

gs2_cast(Pid, Msg) ->
    cast(Pid, fun(P) -> gen_server2:cast(P, Msg) end).

gs2_pcast(Pid, Pri, Msg) ->
    cast(Pid, fun(P) -> gen_server2:pcast(P, Pri, Msg) end).


call(Pid, FPid) when is_pid(Pid) ->
    [{Status, Res, _}] = call_per_node([{node(Pid), [Pid]}], FPid),
    {Status, Res};

call(Pids, FPid) when is_list(Pids) ->
    call_per_node(split_delegate_per_node(Pids), FPid).

internal_call(Node, Thunk) when is_atom(Node) ->
    gen_server2:call({server(Node), Node}, {thunk, Thunk}, infinity).


cast(Pid, FPid) when is_pid(Pid) ->
    cast_per_node([{node(Pid), [Pid]}], FPid),
    ok;

cast(Pids, FPid) when is_list(Pids) ->
    cast_per_node(split_delegate_per_node(Pids),  FPid),
    ok.

internal_cast(Node, Thunk) when is_atom(Node) ->
    gen_server2:cast({server(Node), Node}, {thunk, Thunk}).

%%----------------------------------------------------------------------------

split_delegate_per_node(Pids) ->
    orddict:to_list(
        lists:foldl(
          fun (Pid, D) ->
                  orddict:update(node(Pid),
                              fun (Pids1) -> [Pid | Pids1] end,
                              [Pid], D)
          end,
          orddict:new(), Pids)).

call_per_node([{Node, Pids}], FPid) when Node == node() ->
    local_delegate(Pids, FPid);
call_per_node(NodePids, FPid) ->
    delegate_per_node(NodePids, FPid, fun internal_call/2).

cast_per_node([{Node, Pids}], FPid) when Node == node() ->
    local_delegate(Pids, FPid);
cast_per_node(NodePids, FPid) ->
    delegate_per_node(NodePids, FPid, fun internal_cast/2).

local_delegate(Pids, FPid) ->
    [safe_invoke(FPid, Pid) || Pid <- Pids].

delegate_per_node(NodePids, FPid, DelegateFun) ->
    lists:flatten(
        [DelegateFun(Node, fun() -> [safe_invoke(FPid, Pid) || Pid <- Pids] end)
        || {Node, Pids} <- NodePids]).

server(Node) when is_atom(Node) ->
    server(erlang:phash2(self(), process_count(Node)));

server(Hash) ->
    list_to_atom("delegate_process_" ++ integer_to_list(Hash)).

safe_invoke(FPid, Pid) ->
    case catch FPid(Pid) of
        {'EXIT', Reason} ->
            {error, {'EXIT', Reason}, Pid};
        Result ->
            {ok, Result, Pid}
    end.

process_count(Node) ->
    case get({process_count, Node}) of
        undefined ->
            case rpc:call(Node, delegate, process_count, []) of
                {badrpc, _} ->
                    1; % Have to return something, if we're just casting then
                       % we don't want to blow up
                Count ->
                    put({process_count, Node}, Count),
                    Count
            end;
        Count -> Count
    end.

process_count() ->
    ?DELEGATE_PROCESS_COUNT_MULTIPLIER * erlang:system_info(schedulers).

%%--------------------------------------------------------------------

init([]) ->
    {ok, no_state}.

handle_call({thunk, Thunk}, _From, State) ->
   {reply, Thunk(), State}.

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
