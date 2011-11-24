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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_qc).
-ifdef(use_proper_qc).
-include_lib("proper/include/proper.hrl").

-define(MSMOD, rabbit_msg_store).
-define(CONFIRM_COLLECTOR_TIMEOUT, 10000).

-define(PERSISTENT_MSG_STORE, msg_store_persistent).

-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3, confirm/2]).

-record(state, {msgids,      % tree of id => ms5(payload)
                writeids,    % list of {id, writecount}
                removeids,   % list of {id, removecount}
                collect_pid, % pid for handling confirms
                mscstate,    % client state
                ref}).       % client ref

collector(Ids) ->
    receive
        {confirm, Ref, {IdSet, _Written}} ->
            collector([{Ref, gb_sets:to_list(IdSet)} | Ids]);
        {retrieve, Ref, Pid} ->
            Pid ! {Ref, proplists:append_values(Ref, Ids)},
            collector(proplists:delete(Ref, Ids));
        {reset} ->
            collector([]);
        X ->
            throw({unexpected_collector_message, X})
    end.

%% Initialise model

initial_state() ->
    Ref      = rabbit_guid:guid(),
    CollectorPid = spawn(fun () -> collector([]) end),
    MSCState = {call, rabbit_msg_store, client_init,
                    [?PERSISTENT_MSG_STORE,
                     Ref,
                     fun (I, W) ->
                         CollectorPid ! {confirm, Ref, {I, W}},
                         ok
                     end,
                     undefined]},
    #state{msgids      = gb_trees:empty(),
           writeids    = [],
           removeids   = [],
           mscstate    = MSCState,
           collect_pid = CollectorPid,
           ref         = Ref}.

%% Property

restart_msg_store_empty() ->
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
           undefined, {fun (ok) -> finished end, ok}).

prop_msg_store_test() ->
    ?FORALL(Cmds, commands(?MODULE, initial_state()),
        begin
            application:get_env(rabbit, msg_store_file_size_limit),
            application:set_env(rabbit, msg_store_file_size_limit, 512,
                                infinity),
            restart_msg_store_empty(),
            {_H, #state{collect_pid = CP}, Res} = run_commands(?MODULE, Cmds),
            restart_msg_store_empty(),
            CP ! {reset},
            ?WHENFAIL(
                io:format("Result: ~p~n", [Res]),
                aggregate(command_names(Cmds), Res =:= ok))
        end).

%% Commands

command(S) ->
    frequency([{10, qc_write(S)},
               {3,  qc_read(S)},
               {2,  qc_remove(S)},
               {1,  qc_contains(S)},
               {5,  qc_confirm(S)}]).

qc_write(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MSMOD, write, [frequency([{0,  rand_elem(gb_trees:keys(MsgIds))},
                                      {10, qc_msg_id()}]), qc_payload(), MS]}.

qc_read(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MSMOD, read, [frequency([{10, rand_elem(gb_trees:keys(MsgIds))},
                                     {1,  qc_msg_id()}]), MS]}.

qc_remove(#state{mscstate = MS} = S) ->
    {_Count, Ids} = unremoved(S),
    {call, ?MSMOD, remove, [rand_sublist(Ids), MS]}.

qc_contains(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MSMOD, contains, [frequency([{10, rand_elem(gb_trees:keys(MsgIds))},
                                         {1,  qc_msg_id()}]), MS]}.

qc_confirm(#state{collect_pid = Pid, ref = Ref}) ->
    {call, ?MODULE, confirm, [Pid, Ref]}.

%% Preconditions

precondition(#state{}, {call, ?MSMOD, write, [MsgId, _Payload, _MS]}) ->
    MsgId /= none;

precondition(#state{} = S, {call, ?MSMOD, remove, _Arg}) ->
    {Count, _Ids} = unremoved(S),
    Count > 0;

precondition(_S, {call, ?MSMOD, _Fun, _Arg}) ->
    true;

precondition(_S, {call, ?MODULE, confirm, _Arg}) ->
    true.

%% Model updates

next_state(#state{msgids = MsgIds, writeids = WriteIds} = S, _Res,
           {call, ?MSMOD, write, [MsgId, Payload, _MS]}) ->
    WriteIds1 = case lists:keysearch(MsgId, 1, WriteIds) of
                    {value, {MsgId, Count}} -> lists:keyreplace(MsgId, 1, WriteIds, {MsgId, Count + 1});
                    false -> [{MsgId, 1} | WriteIds]
                end,

    S#state{msgids   = gb_trees:enter(MsgId, erlang:md5(Payload), MsgIds),
            writeids = WriteIds1};

next_state(S, Res, {call, ?MSMOD, read, _Args}) ->
    MS1 = {call, erlang, element, [2, Res]},
    S#state{mscstate = MS1};

next_state(#state{removeids = RemoveIds} = S, _Res,
           {call, ?MSMOD, remove, [MsgIdList, _MS]}) ->
    RemoveIds2 =
        lists:foldl(fun (MsgId, RemoveIds1) ->
                           case lists:keysearch(MsgId, 1, RemoveIds1) of
                                {value, {MsgId, Count}} -> lists:keyreplace(MsgId, 1, RemoveIds, {MsgId, Count + 1});
                                false      -> [{MsgId, 1} | RemoveIds1]
                            end
                    end,
                    RemoveIds,
                    MsgIdList),
    S#state{removeids = RemoveIds2};

next_state(S, _Res, {call, ?MSMOD, contains, [_MsgId, _MS]}) ->
    S;

next_state(S, _Res, {call, ?MODULE, confirm, _Args}) ->
    S.

%% Postconditions

postcondition(#state{msgids = MsgIds} = S,
              {call, ?MSMOD, read, [MsgId, _MS]}, Res) ->
    {_Count, Ids} = unremoved(S),
    case Res of
        {{ok, Retrieved}, _MS0} ->
            erlang:md5(Retrieved) == gb_trees:get(MsgId, MsgIds);
        {not_found, _MS0}       ->
            not lists:member(MsgId, Ids)
    end;

postcondition(#state{} = S,
              {call, ?MSMOD, contains, [MsgId, _MS]}, Res) ->
    {_Count, Ids} = unremoved(S),
    Res == lists:member(MsgId, Ids);

postcondition(_S, {call, ?MSMOD, _Fun, _Args}, _Res) ->
    true;

postcondition(#state{msgids = MsgIds}, {call, ?MODULE, confirm, _Args}, Res) ->
    lists:foldl(fun (_I, false) -> false;
                    (I, true)   -> gb_trees:is_defined(I, MsgIds)
                end, true, Res).

%% Helpers

confirm(CollectorPid, Ref) ->
    CollectorPid ! {retrieve, Ref, self()},
    receive
        {Ref, Ids} -> Ids
    after ?CONFIRM_COLLECTOR_TIMEOUT ->
         throw({failed_to_retrieve_ids, Ref})
    end.

unremoved(#state{writeids = WriteIds, removeids = RemoveIds}) ->
    unremoved(WriteIds, RemoveIds, 0, []).

unremoved([], [], Count, Result) ->
    {Count, Result};
unremoved([], Removes, _, _) ->
    throw({more_removes_than_writes, Removes});
unremoved([{WriteId, WriteCount} | WriteRest], Removes, Count, Result) ->
    case lists:keytake(WriteId, 1, Removes) of
         {value, {WriteId, RemoveCount}, Removes1} ->
            case {WriteCount, RemoveCount} of
                {X, Y} when X > Y ->
                    unremoved(WriteRest, Removes1, Count + WriteCount - RemoveCount, [WriteId | Result]);
                {X, X} ->
                    unremoved(WriteRest, Removes1, Count, Result);
                _  ->
                    throw({found_too_many_removes, WriteCount, RemoveCount, WriteId})
            end;
         false ->
             unremoved(WriteRest, Removes, Count + WriteCount, [WriteId | Result])
     end.

qc_payload() ->
    binary().

qc_msg_id() ->
    noshrink(binary(16)).

rand_elem(List) ->
    case List of
        []  -> none;
        _   -> lists:nth(random:uniform(length(List)), List)
    end.

rand_sublist([]) ->
    [];
rand_sublist(List) ->
    rand_sublist(List, [], min(100, random:uniform(length(List)))).

rand_sublist(_List, Result, 0) ->
    Result;
rand_sublist(List, Result, Count) ->
    Picked = rand_elem(List),
    rand_sublist(List -- [Picked], [Picked|Result], Count-1).

-endif.
