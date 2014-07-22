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
-export([do_join/0, do_leave/1, do_send/1, do_proceed1/1, do_proceed2/2]).

%% For insertion into gm
-export([call/3, cast/2, monitor/1, demonitor/1, execute_mnesia_transaction/1]).

-record(state, {seq,          %% symbolic and dynamic
                instrumented, %% dynamic only
                outstanding,  %% dynamic only
                monitors,     %% dynamic only
                all_join,     %% for symbolic
                to_join,      %% dynamic only
                to_leave      %% for symbolic
               }).

prop_gm_test() ->
    case ?INSTR_MOD of
        ?MODULE -> ok;
        _       -> exit(compile_with_INSTRUMENT_FOR_QC)
    end,
    process_flag(trap_exit, true),
    erlang:register(?MODULE, self()),
    ?FORALL(Cmds, commands(?MODULE), gm_test(Cmds)).

gm_test(Cmds) ->
    {_H, State, Res} = run_commands(?MODULE, Cmds),
    cleanup(State),
    ?WHENFAIL(
       io:format("Result: ~p~n", [Res]),
       aggregate(command_names(Cmds), Res =:= ok)).

cleanup(S) ->
    S2 = ensure_joiners_joined_and_msgs_received(S),
    All = gms_joined(S2),
    All = gms(S2), %% assertion - none to join
    check_stale_members(All),
    [gm:leave(GM) || GM <- All],
    drain_and_proceed_gms(S2),
    [await_death(GM) || GM <- All],
    gm:forget_group(?GROUP),
    ok.

check_stale_members(All) ->
    GMs = [P || P <- processes(), is_gm_process(?GROUP, P)],
    case GMs -- All of
        []   -> ok;
        Rest -> exit({forgot, Rest})
    end.

is_gm_process(Group, P) ->
    case process_info(P, dictionary) of
        undefined       -> false;
        {dictionary, D} -> {gm, Group} =:= proplists:get_value(process_name, D)
    end.

await_death(P) ->
    MRef = erlang:monitor(process, P),
    await_death(MRef, P).

await_death(MRef, P) ->
    receive
        {'DOWN', MRef, process, P, _} -> ok;
        {'DOWN', _, _, _, _}          -> await_death(MRef, P);
        {'EXIT', _, normal}           -> await_death(MRef, P);
        {'EXIT', _, Reason}           -> exit(Reason);
        {joined, _GM}                 -> await_death(MRef, P);
        {left, _GM}                   -> await_death(MRef, P);
        Anything                      -> exit({stray_msg, Anything})
    end.

%% ---------------------------------------------------------------------------
%% proper_statem
%% ---------------------------------------------------------------------------

initial_state() -> #state{seq           = 1,
                          outstanding   = dict:new(),
                          instrumented  = dict:new(),
                          monitors      = dict:new(),
                          all_join      = sets:new(),
                          to_join       = sets:new(),
                          to_leave      = sets:new()}.

command(S) ->
    case {length(gms_symb_not_left(S)), length(gms_symb(S))} of
        {0, 0} -> qc_join(S);
        {0, _} -> frequency([{1, qc_join(S)},
                             {3, qc_proceed1(S)},
                             {5, qc_proceed2(S)}]);
        _      -> frequency([{1,  qc_join(S)},
                             {1,  qc_leave(S)},
                             {10, qc_send(S)},
                             {5,  qc_proceed1(S)},
                             {15, qc_proceed2(S)}])
    end.

qc_join(_S)    -> {call,?MODULE,do_join, []}.
qc_leave(S)    -> {call,?MODULE,do_leave,[oneof(gms_symb_not_left(S))]}.
qc_send(S)     -> {call,?MODULE,do_send, [oneof(gms_symb_not_left(S))]}.
qc_proceed1(S) -> {call,?MODULE,do_proceed1, [oneof(gms_symb(S))]}.
qc_proceed2(S) -> {call,?MODULE,do_proceed2, [oneof(gms_symb(S)),
                                              oneof(gms_symb(S))]}.

precondition(S, {call, ?MODULE, do_join, []}) ->
    length(gms_symb(S)) < ?MAX_SIZE;

precondition(_S, {call, ?MODULE, do_leave, [_GM]}) ->
    true;

precondition(_S, {call, ?MODULE, do_send, [_GM]}) ->
    true;

precondition(_S, {call, ?MODULE, do_proceed1, [_GM]}) ->
    true;

precondition(_S, {call, ?MODULE, do_proceed2, [GM1, GM2]}) ->
    GM1 =/= GM2.

postcondition(_S, {call, _M, _F, _A}, _Res) ->
    true.

next_state(S = #state{to_join  = ToSet,
                      all_join = AllSet}, GM, {call, ?MODULE, do_join, []}) ->
    S#state{to_join  = sets:add_element(GM, ToSet),
            all_join = sets:add_element(GM, AllSet)};

next_state(S = #state{to_leave = Set}, _Res, {call, ?MODULE, do_leave, [GM]}) ->
    S#state{to_leave = sets:add_element(GM, Set)};

next_state(S = #state{seq         = Seq,
                      outstanding = Outstanding}, _Res,
           {call, ?MODULE, do_send, [GM]}) ->
    case is_pid(GM) andalso lists:member(GM, gms_joined(S)) of
        true ->
            %% Dynamic state, i.e. runtime
            Msg = [{sequence, Seq},
                   {sent_to, GM},
                   {dests,   gms_joined(S)}],
            gm:broadcast(GM, Msg),
            Outstanding1 = dict:map(
                             fun (_GM, Set) ->
                                     gb_sets:add_element(Msg, Set)
                             end, Outstanding),
            drain(S#state{seq         = Seq + 1,
                          outstanding = Outstanding1});
        false ->
            S
    end;

next_state(S, _Res, {call, ?MODULE, do_proceed1, [Pid]}) ->
    proceed(Pid, S);

next_state(S, _Res, {call, ?MODULE, do_proceed2, [From, To]}) ->
    proceed({From, To}, S).

proceed(K, S = #state{instrumented = Msgs}) ->
    case dict:find(K, Msgs) of
        {ok, Q} -> case queue:out(Q) of
                       {{value, Thing}, Q2} ->
                           S2 = proceed(K, Thing, S),
                           S2#state{instrumented = dict:store(K, Q2, Msgs)};
                       {empty, _} ->
                           S
                   end;
        error   -> S
    end.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined(Pid, _Members)           -> Pid ! {joined, self()},
                                   ok.
members_changed(_Pid, _Bs, _Ds) -> ok.
handle_msg(Pid, _From, Msg)     -> Pid ! {gm, self(), Msg}, ok.
terminate(Pid, _Reason)         -> Pid ! {left, self()}.

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

do_join() ->
    {ok, GM} = gm:start_link(?GROUP, ?MODULE, self(),
                             fun execute_mnesia_transaction/1),
    GM.

do_leave(GM) ->
    gm:leave(GM),
    GM.

%% We need to update the state, so do the work in next_state
do_send( _GM)           -> ok.
do_proceed1(_Pid)       -> ok.
do_proceed2(_From, _To) -> ok.

%% All GMs, joined and to join
gms(#state{outstanding = Outstanding,
           to_join     = ToJoin}) ->
    dict:fetch_keys(Outstanding) ++ sets:to_list(ToJoin).

%% All GMs, joined and to join
gms_joined(#state{outstanding = Outstanding}) ->
    dict:fetch_keys(Outstanding).

%% All GMs including those that have left (symbolic)
gms_symb(#state{all_join = AllJoin}) ->
    sets:to_list(AllJoin).

%% All GMs not including those that have left (symbolic)
gms_symb_not_left(#state{all_join = AllJoin,
                         to_leave = ToLeave}) ->
    sets:to_list(sets:subtract(AllJoin, ToLeave)).

drain(S) ->
    receive
        Msg -> drain(handle_msg(Msg, S))
    after 10 -> S
    end.

drain_and_proceed_gms(S0) ->
    S = #state{instrumented = Msgs} = drain(S0),
    case dict:size(Msgs) of
        0 -> S;
        _ -> S1 = dict:fold(
                    fun (Key, Q, Si) ->
                            lists:foldl(
                              fun (Msg, Sij) ->
                                      proceed(Key, Msg, Sij)
                              end, Si, queue:to_list(Q))
                    end, S, Msgs),
             drain_and_proceed_gms(S1#state{instrumented = dict:new()})
    end.

handle_msg({gm, GM, Msg}, S = #state{outstanding = Outstanding}) ->
    case dict:find(GM, Outstanding) of
        {ok, Set} ->
            Set2 = gb_sets:del_element(Msg, Set),
            S#state{outstanding = dict:store(GM, Set2, Outstanding)};
        error ->
            %% Message from GM that has already died. OK.
            S
    end;
handle_msg({instrumented, Key, Thing}, S = #state{instrumented = Msgs}) ->
    Q1 = case dict:find(Key, Msgs) of
             {ok, Q} -> queue:in(Thing, Q);
             error   -> queue:from_list([Thing])
         end,
    S#state{instrumented = dict:store(Key, Q1, Msgs)};
handle_msg({joined, GM}, S = #state{outstanding = Outstanding,
                                    to_join     = ToJoin}) ->
    S#state{outstanding = dict:store(GM, gb_sets:empty(), Outstanding),
            to_join     = sets:del_element(GM, ToJoin)};
handle_msg({left, GM}, S = #state{outstanding = Outstanding,
                                  to_join     = ToJoin}) ->
    true = dict:is_key(GM, Outstanding) orelse sets:is_element(GM, ToJoin),
    S#state{outstanding = dict:erase(GM, Outstanding),
            to_join     = sets:del_element(GM, ToJoin)};
handle_msg({'DOWN', MRef, _, From, _} = Msg, S = #state{monitors = Mons}) ->
    To = dict:fetch(MRef, Mons),
    handle_msg({instrumented, {From, To}, {info, Msg}},
               S#state{monitors = dict:erase(MRef, Mons)});
handle_msg({'EXIT', _From, normal}, S) ->
    S;
handle_msg({'EXIT', _From, Reason}, _S) ->
    %% We just trapped exits to get nicer SASL logging.
    exit(Reason).

proceed({_From, To}, {cast, Msg},   S) -> gen_server2:cast(To, Msg), S;
proceed({_From, To}, {info, Msg},   S) -> To ! Msg,                  S;
proceed({From, _To}, {wait, Ref},   S) -> From ! {proceed, Ref},     S;
proceed({From, To},  {mon, Ref},    S) -> add_monitor(From, To, Ref, S);
proceed(_Pid,        {demon, MRef}, S) -> erlang:demonitor(MRef),    S;
proceed(Pid,         {wait, Ref},   S) -> Pid  ! {proceed, Ref},     S.

%% NB From here is To in handle_msg/DOWN above, since the msg is going
%% the other way
add_monitor(From, To, Ref, S = #state{monitors = Mons}) ->
    MRef = erlang:monitor(process, To),
    From ! {mref, Ref, MRef},
    S#state{monitors = dict:store(MRef, From, Mons)}.

%% ----------------------------------------------------------------------------
%% Assertions
%% ----------------------------------------------------------------------------

ensure_joiners_joined_and_msgs_received(S0) ->
    S = drain_and_proceed_gms(S0),
    case outstanding_joiners(S) of
        true  -> ensure_joiners_joined_and_msgs_received(S);
        false -> case outstanding_msgs(S) of
                     []  -> S;
                     Out -> exit({outstanding_msgs, Out})
                 end
    end.

outstanding_joiners(#state{to_join = ToJoin}) ->
    sets:size(ToJoin) > 0.

outstanding_msgs(#state{outstanding = Outstanding}) ->
    dict:fold(fun (GM, Set, OS) ->
                      case gb_sets:is_empty(Set) of
                          true  -> OS;
                          false -> [{GM, gb_sets:to_list(Set)} | OS]
                      end
              end, [], Outstanding).

%% ---------------------------------------------------------------------------
%% For insertion into GM
%% ---------------------------------------------------------------------------

call(Pid, Msg, infinity) ->
    Ref = make_ref(),
    whereis(?MODULE) ! {instrumented, {self(), Pid}, {wait, Ref}},
    receive
        {proceed, Ref} -> ok
    end,
    gen_server2:call(Pid, Msg, infinity).

cast(Pid, Msg) ->
    whereis(?MODULE) ! {instrumented, {self(), Pid}, {cast, Msg}},
    ok.

monitor(Pid) ->
    Ref = make_ref(),
    whereis(?MODULE) ! {instrumented, {self(), Pid}, {mon, Ref}},
    receive
        {mref, Ref, MRef} -> MRef
    end.

demonitor(MRef) ->
    whereis(?MODULE) ! {instrumented, self(), {demon, MRef}},
    true.

execute_mnesia_transaction(Fun) ->
    Ref = make_ref(),
    whereis(?MODULE) ! {instrumented, self(), {wait, Ref}},
    receive
        {proceed, Ref} -> ok
    end,
    rabbit_misc:execute_mnesia_transaction(Fun).

-else.

-export([prop_disabled/0]).

prop_disabled() ->
    exit({compiled_without_proper,
          "PropEr was not present during compilation of the test module. "
          "Hence all tests are disabled."}).

-endif.
