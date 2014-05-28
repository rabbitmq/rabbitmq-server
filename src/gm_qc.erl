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
%% Copyright (c) 2011-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(gm_qc).
-ifdef(use_proper_qc).
%%-include("rabbit.hrl").
-include_lib("proper/include/proper.hrl").

-define(GROUP, test_group).
-define(MAX_SIZE, 5).
-define(MSG_TIMEOUT, 1000000). %% micros

-export([prop_gm_test/0]).

-behaviour(proper_statem).
-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-behaviour(gm).
-export([joined/2, members_changed/3, handle_msg/3, terminate/2]).

%% Helpers
-export([do_join/0, do_leave/1, do_send/2]).

-record(state, {seq, msgs}).

prop_gm_test() ->
    ?FORALL(Cmds, commands(?MODULE, initial_state()),
            gm_test(Cmds)).

gm_test(Cmds) ->
    {_H, State, Res} = run_commands(?MODULE, Cmds),
    cleanup(State),
    ?WHENFAIL(
       io:format("Result: ~p~n", [Res]),
       aggregate(command_names(Cmds), Res =:= ok)).

cleanup(S) ->
    #state{msgs = Msgs} = ensure_outstanding_msgs_received(S),
    All = gms(Msgs),
    [gm:leave(GM) || GM <- All],
    [await_death(GM) || GM <- All],
    ok.

await_death(P) ->
    MRef = erlang:monitor(process, P),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    end.

%% ---------------------------------------------------------------------------
%% proper_statem
%% ---------------------------------------------------------------------------

initial_state() -> #state{seq  = 1,
                          msgs = dict:new()}.

command(S = #state{msgs = Msgs}) ->
    case dict:size(Msgs) of
        0 -> qc_join(S);
        _ -> frequency([{1,  qc_join(S)},
                        {1,  qc_leave(S)},
                        {10, qc_send(S)}])
    end.

qc_join(_S)                   -> {call,?MODULE,do_join, []}.
qc_leave(#state{msgs = Msgs}) -> {call,?MODULE,do_leave,[random(gms(Msgs))]}.
qc_send(#state{seq  = N,
               msgs = Msgs})  -> {call,?MODULE,do_send, [N, random(gms(Msgs))]}.

random([]) -> will_fail_precondition;
random(L)  -> lists:nth(random:uniform(length(L)), L).

precondition(#state{msgs = Msgs}, {call, ?MODULE, do_join, []}) ->
    dict:size(Msgs) < ?MAX_SIZE;

precondition(#state{msgs = Msgs}, {call, ?MODULE, do_leave, [_GM]}) ->
    dict:size(Msgs) > 0;

precondition(#state{msgs = Msgs}, {call, ?MODULE, do_send, [_N, _GM]}) ->
    dict:size(Msgs) > 0.

postcondition(S = #state{msgs = Msgs}, {call, ?MODULE, do_join, []}, GM) ->
    [begin
         gm:broadcast(Existing, heartbeat),
         receive
             {birth, Existing, GM} -> ok
         after 1000 ->
                 exit({birth_timeout, Existing, did_not_announce, GM})
         end
     end || Existing <- gms(Msgs) -- [GM]],
    assert(S);

postcondition(S = #state{msgs = Msgs},
              {call, ?MODULE, do_leave, [Dead]}, _Res) ->
    [await_death(Existing, Dead, 5) || Existing <- gms(Msgs) -- [Dead]],
    assert(S#state{msgs = dict:erase(Dead, Msgs)});

postcondition(S = #state{}, {call, _M, _F, _A}, _Res) ->
    assert(S).

next_state(S = #state{msgs = Msgs}, GM, {call, ?MODULE, do_join, []}) -> 
    S#state{msgs = dict:store(GM, {gb_trees:empty(), gb_sets:empty()}, Msgs)};

next_state(S = #state{msgs = Msgs}, _GM, {call, ?MODULE, do_leave, [GM]}) ->
    true = dict:is_key(GM, Msgs),
    S#state{msgs = dict:erase(GM, Msgs)};

next_state(S = #state{seq  = Seq,
                      msgs = Msgs}, Msg, {call, ?MODULE, do_send, [_, _]}) ->
    TS = timestamp(),
    Msgs1 = dict:map(fun (_GM, {Tree, Set}) ->
                             {gb_trees:insert(Msg, TS, Tree),
                              gb_sets:add_element({TS, Msg}, Set)}
                     end, Msgs),
    drain(S#state{seq  = Seq + 1,
                  msgs = Msgs1}).

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined(Pid, _Members)              -> Pid ! {joined, self()}, ok.
members_changed(Pid, Bs, Ds)       -> [Pid ! {birth, self(), B} || B <- Bs],
                                      [Pid ! {death, self(), D} || D <- Ds],
                                      ok.
handle_msg(_Pid, _From, heartbeat) -> ok;
handle_msg(Pid, _From, Msg)        -> Pid ! {gm, self(), Msg}, ok.
terminate(_Pid, _Reason)           -> ok.

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

do_join() ->
    {ok, GM} = gm:start_link(?GROUP, ?MODULE, self(),
                             fun rabbit_misc:execute_mnesia_transaction/1),
    receive
        {joined, GM} -> ok
    end,
    GM.

do_leave(GM) ->
    gm:leave(GM).

do_send(Seq, GM) ->
    Msg = [{sequence, Seq},
           {first_gm, GM}],
    gm:broadcast(GM, Msg),
    Msg.

await_death(GM, ToDie, 0) ->
    exit({death_msg_timeout, GM, ToDie});
await_death(GM, ToDie, N) ->
    gm:broadcast(GM, heartbeat),
    receive
        {death, GM, ToDie} -> ok
    after 100 ->
            await_death(GM, ToDie, N - 1)
    end.

gms(Msgs) -> dict:fetch_keys(Msgs).

drain(S = #state{msgs = Msgs}) ->
    receive
        {gm, GM, Msg} ->
            case dict:find(GM, Msgs) of
                {ok, {Tree, Set}} ->
                    case gb_trees:lookup(Msg, Tree) of
                        {value, TS} ->
                            Msgs1 = dict:store(
                                      GM, {gb_trees:delete(Msg, Tree),
                                           gb_sets:del_element({TS, Msg}, Set)},
                                      Msgs),
                            drain(S#state{msgs = Msgs1});
                        none ->
                            %% Message from GM that joined after we
                            %% broadcast the message. OK.
                            drain(S)
                    end;
                error ->
                    %% Message from GM that has already died. OK.
                    drain(S)
            end
    after 0 ->
            S
    end.

assert(#state{msgs = Msgs}) ->
    TS = timestamp(),
    dict:fold(fun (GM, {_Tree, Set}, none) ->
                       case gb_sets:size(Set) of
                           0 -> ok;
                           _ -> {TS0, Msg} = gb_sets:smallest(Set),
                                case TS0 + ?MSG_TIMEOUT < TS of
                                    true  -> exit({msg_timeout,
                                                   [{msg, Msg},
                                                    {gm,  GM},
                                                    {all, gms(Msgs)}]});
                                    false -> ok
                                end
                       end,
                      none
              end, none, Msgs),
    true.

ensure_outstanding_msgs_received(S) ->
    case outstanding_msgs(S) of
        false -> S;
        true  -> timer:sleep(100),
                 S2 = drain(S),
                 assert(S2),
                 ensure_outstanding_msgs_received(S2)
    end.

outstanding_msgs(#state{msgs = Msgs}) ->
    dict:fold(fun (_GM, {_Tree, Set},  false) -> not gb_sets:is_empty(Set);
                  (_GM, {_Tree, _Set}, true)  -> true
              end, false, Msgs).

timestamp() -> timer:now_diff(os:timestamp(), {0, 0, 0}).

-else.

-export([prop_disabled/0]).

prop_disabled() ->
    exit({compiled_without_proper,
          "PropEr was not present during compilation of the test module. "
          "Hence all tests are disabled."}).

-endif.
