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

-define(PERSISTENT_MSG_STORE, msg_store_persistent).

-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-export([sync/2]).

-record(state, {msgids,
                mscstate}).

%% Initialise model

initial_state() ->
    Ref      = {call, rabbit_guid, guid, []},
    MSCState = {call, rabbit_msg_store, client_init,
                    [?PERSISTENT_MSG_STORE, Ref, undefined, undefined]},
    #state{msgids   = gb_trees:empty(),
           mscstate = MSCState}.

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
            {_H, _S, Res} = run_commands(?MODULE, Cmds),
            restart_msg_store_empty(),
            ?WHENFAIL(
                io:format("Result: ~p~n", [Res]),
                aggregate(command_names(Cmds), Res =:= ok))
        end).

%% Commands

command(S) ->
    frequency([{10, qc_write(S)},
               {1,  qc_read(S)},
               {1,  qc_remove(S)},
               {1,  qc_sync(S)},
               {1,  qc_contains(S)}]).

qc_write(#state{mscstate = MS}) ->
    {call, ?MSMOD, write, [qc_msg_id(), qc_payload(), MS]}.

qc_read(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MSMOD, read, [frequency([{10, rand_elem(gb_trees:keys(MsgIds))},
                                     {1,  qc_msg_id()}]), MS]}.

qc_remove(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MSMOD, remove, [rand_sublist(gb_trees:keys(MsgIds)), MS]}.

qc_sync(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MODULE, sync, [rand_sublist(gb_trees:keys(MsgIds)), MS]}.

qc_contains(#state{msgids = MsgIds, mscstate = MS}) ->
    {call, ?MSMOD, contains, [frequency([{10, rand_elem(gb_trees:keys(MsgIds))},
                                         {1,  qc_msg_id()}]), MS]}.

%% Preconditions

precondition(#state{msgids = MsgIds},
             {call, ?MSMOD, write, [MsgId, _Payload, _MS]}) ->
    not gb_trees:is_defined(MsgId, MsgIds);

precondition(#state{msgids = MsgIds}, {call, ?MSMOD, remove, _Arg}) ->
    not gb_trees:is_empty(MsgIds);

precondition(#state{msgids = MsgIds}, {call, ?MODULE, sync, _Arg}) ->
    not gb_trees:is_empty(MsgIds);

precondition(_S, {call, ?MSMOD, _Fun, _Arg}) ->
    true.

%% Model updates

next_state(#state{msgids = MsgIds} = S, _Res,
           {call, ?MSMOD, write, [MsgId, Payload, _MS]}) ->
    S#state{msgids = gb_trees:insert(MsgId, erlang:md5(Payload), MsgIds)};

next_state(S, Res, {call, ?MSMOD, read, _Args}) ->
    MS1 = {call, erlang, element, [2, Res]},
    S#state{mscstate = MS1};

next_state(#state{msgids = MsgIds} = S, _Res,
           {call, ?MSMOD, remove, [MsgIdList, _MS]}) ->
    S#state{msgids = lists:foldl(fun(Elem, Tree) ->
                                     gb_trees:delete(Elem, Tree)
                                 end, MsgIds, MsgIdList)};

next_state(S, _Res, {call, ?MODULE, sync, [_MsgIdList, _MS]}) ->
    S;

next_state(S, _Res, {call, ?MSMOD, contains, [_MsgId, _MS]}) ->
    S.

%% Postconditions

postcondition(#state{msgids = MsgIds},
              {call, ?MSMOD, read, [MsgId, _MS]}, Res) ->
    case Res of
        {{ok, Retrieved}, _MS0} ->
            erlang:md5(Retrieved) == gb_trees:get(MsgId, MsgIds);
        {not_found, _MS0}       ->
            not gb_trees:is_defined(MsgId, MsgIds)
    end;

postcondition(#state{msgids = MsgIds},
              {call, ?MSMOD, contains, [MsgId, _MS]}, Res) ->
    Res == gb_trees:is_defined(MsgId, MsgIds);

postcondition(_S, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

%% Helpers

sync(MsgIds, MS) ->
    Ref = make_ref(),
    Self = self(),
    ok = ?MSMOD:sync(MsgIds,
                     fun () -> Self ! {sync, Ref} end,
                     MS),
    receive
        {sync, Ref} -> ok
    after
        10000 ->
            io:format("Sync from msg_store missing for msg_ids ~p~n", [MsgIds]),
            throw(timeout)
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
