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

-export([start_link/2, invoke_no_result/2, invoke/2, process_count/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/2 ::
        (atom(), non_neg_integer()) -> {'ok', pid()} | {'error', any()}).
-spec(invoke_no_result/2 ::
        (pid() | [pid()], fun ((pid()) -> any())) -> 'ok').
-spec(invoke/2 :: (pid() | [pid()], fun ((pid()) -> A)) -> A).

-spec(process_count/0 :: () -> non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE,   10000).

%%----------------------------------------------------------------------------

start_link(Prefix, Hash) ->
    gen_server2:start_link({local, server(Prefix, Hash)}, ?MODULE, [], []).

invoke(Pid, Fun) when is_pid(Pid) ->
    [Res] = invoke_per_node(split_delegate_per_node([Pid]), Fun),
    case Res of
        {ok, Result, _} ->
            Result;
        {error, {Class, Reason, StackTrace}, _} ->
            erlang:raise(Class, Reason, StackTrace)
    end;

invoke(Pids, Fun) when is_list(Pids) ->
    lists:foldl(
        fun ({Status, Result, Pid}, {Good, Bad}) ->
            case Status of
                ok    -> {[{Pid, Result}|Good], Bad};
                error -> {Good, [{Pid, Result}|Bad]}
            end
        end,
        {[], []},
        invoke_per_node(split_delegate_per_node(Pids), Fun)).

invoke_no_result(Pid, Fun) when is_pid(Pid) ->
    invoke_no_result_per_node(split_delegate_per_node([Pid]), Fun),
    ok;

invoke_no_result(Pids, Fun) when is_list(Pids) ->
    invoke_no_result_per_node(split_delegate_per_node(Pids),  Fun),
    ok.

%%----------------------------------------------------------------------------

internal_call(Node, Thunk) when is_atom(Node) ->
    gen_server2:call({remote_server(Node), Node}, {thunk, Thunk}, infinity).

internal_cast(Node, Thunk) when is_atom(Node) ->
    gen_server2:cast({remote_server(Node), Node}, {thunk, Thunk}).

split_delegate_per_node(Pids) ->
    LocalNode = node(),
    {Local, Remote} =
        lists:foldl(
          fun (Pid, {L, D}) ->
                  Node = node(Pid),
                  case Node of
                      LocalNode -> {[Pid|L], D};
                      _         -> {L, orddict:append(Node, Pid, D)}
                  end
          end,
          {[], orddict:new()}, Pids),
    {Local, orddict:to_list(Remote)}.

invoke_per_node(NodePids, Fun) ->
    lists:append(delegate_per_node(NodePids, Fun, fun internal_call/2)).

invoke_no_result_per_node(NodePids, Fun) ->
    delegate_per_node(NodePids, Fun, fun internal_cast/2),
    ok.

delegate_per_node({LocalPids, NodePids}, Fun, DelegateFun) ->
    %% In the case where DelegateFun is internal_cast, the safe_invoke
    %% is not actually async! However, in practice Fun will always be
    %% something that does a gen_server:cast or similar, so I don't
    %% think it's a problem unless someone misuses this
    %% function. Making this *actually* async would be painful as we
    %% can't spawn at this point or we break effect ordering.
    [safe_invoke(LocalPids, Fun)|
     delegate_per_remote_node(NodePids, Fun, DelegateFun)].

delegate_per_remote_node(NodePids, Fun, DelegateFun) ->
    Self = self(),
    %% Note that this is unsafe if the Fun requires reentrancy to the
    %% local_server. I.e. if self() == local_server(Node) then we'll
    %% block forever.
    [gen_server2:cast(
       local_server(Node),
       {thunk, fun () ->
                       Self ! {result,
                               DelegateFun(
                                 Node, fun () -> safe_invoke(Pids, Fun) end)}
               end}) || {Node, Pids} <- NodePids],
    [receive {result, Result} -> Result end || _ <- NodePids].

local_server(Node) ->
    case get({delegate_local_server_name, Node}) of
        undefined ->
            Name = server(outgoing,
                          erlang:phash2({self(), Node}, process_count())),
            put({delegate_local_server_name, Node}, Name),
            Name;
        Name -> Name
    end.

remote_server(Node) ->
    case get({delegate_remote_server_name, Node}) of
        undefined ->
            case rpc:call(Node, delegate, process_count, []) of
                {badrpc, _} ->
                    %% Have to return something, if we're just casting
                    %% then we don't want to blow up
                    server(incoming, 1);
                Count ->
                    Name = server(incoming,
                                  erlang:phash2({self(), Node}, Count)),
                    put({delegate_remote_server_name, Node}, Name),
                    Name
            end;
        Name -> Name
    end.

server(Prefix, Hash) ->
    list_to_atom("delegate_" ++
                     atom_to_list(Prefix) ++ "_" ++
                     integer_to_list(Hash)).

safe_invoke(Pids, Fun) when is_list(Pids) ->
    [safe_invoke(Pid, Fun) || Pid <- Pids];
safe_invoke(Pid, Fun) when is_pid(Pid) ->
    try
        {ok, Fun(Pid), Pid}
    catch
        Class:Reason ->
            {error, {Class, Reason, erlang:get_stacktrace()}, Pid}
    end.

process_count() ->
    ?DELEGATE_PROCESS_COUNT_MULTIPLIER * erlang:system_info(schedulers).

%%--------------------------------------------------------------------

init([]) ->
    {ok, no_state, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

%% We don't need a catch here; we always go via safe_invoke. A catch here would
%% be the wrong thing anyway since the Thunk can throw multiple errors.
handle_call({thunk, Thunk}, _From, State) ->
    {reply, Thunk(), State, hibernate}.

handle_cast({thunk, Thunk}, State) ->
    Thunk(),
    {noreply, State, hibernate}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
