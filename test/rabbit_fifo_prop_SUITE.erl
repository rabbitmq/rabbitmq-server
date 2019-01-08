-module(rabbit_fifo_prop_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     snapshots,
     scenario1,
     scenario2,
     scenario3,
     scenario4
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

% -type log_op() ::
%     {enqueue, pid(), maybe(msg_seqno()), Msg :: raw_msg()}.

scenario1(_Config) ->
    C1 = {<<>>, c:pid(0,6723,1)},
    C2 = {<<0>>,c:pid(0,6723,1)},
    E = c:pid(0,6720,1),

    Commands = [
                make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E,1,msg1),
                make_enqueue(E,2,msg2),
                make_checkout(C1, cancel), %% both on returns queue
                make_checkout(C2, {auto,1,simple_prefetch}),
                make_return(C2, [0]), %% E1 in returns, E2 with C2
                make_return(C2, [1]), %% E2 in returns E1 with C2
                make_settle(C2, [2]) %% E2 with C2
               ],
    run_snapshot_test(?FUNCTION_NAME, Commands),
    ok.

scenario2(_Config) ->
    C1 = {<<>>, c:pid(0,346,1)},
    C2 = {<<>>,c:pid(0,379,1)},
    E = c:pid(0,327,1),
    Commands = [make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg1),
                make_checkout(C1, cancel),
                make_enqueue(E,2,msg2),
                make_checkout(C2, {auto,1,simple_prefetch}),
                make_settle(C1, [0]),
                make_settle(C2, [0])
               ],
    run_snapshot_test(?FUNCTION_NAME, Commands),
    ok.

scenario3(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E,1,msg1),
                make_return(C1, [0]),
                make_enqueue(E,2,msg2),
                make_enqueue(E,3,msg3),
                make_settle(C1, [1]),
                make_settle(C1, [2])
               ],
    run_snapshot_test(?FUNCTION_NAME, Commands),
    ok.

scenario4(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg),
                make_settle(C1, [0])
               ],
    run_snapshot_test(?FUNCTION_NAME, Commands),
    ok.

snapshots(_Config) ->
    run_proper(
      fun () ->
              ?FORALL(O, ?LET(Ops, log_gen(), expand(Ops)),
                      test1_prop(O))
      end, [], 1000).

test1_prop(Commands) ->
    ct:pal("Commands: ~p~n", [Commands]),
    try run_snapshot_test(?FUNCTION_NAME, Commands) of
        _ -> true
    catch
        Err ->
            ct:pal("Err: ~p~n", [Err]),
            false
    end.

log_gen() ->
    ?LET(EPids, vector(2, pid_gen()),
         ?LET(CPids, vector(2, pid_gen()),
              resize(200,
                     list(
                       frequency(
                         [{20, enqueue_gen(oneof(EPids))},
                          {40, {input_event,
                                frequency([{10, settle},
                                           {2, return},
                                           {1, discard},
                                           {1, requeue}])}},
                          {2, checkout_gen(oneof(CPids))},
                          {1, checkout_cancel_gen(oneof(CPids))},
                          {1, down_gen(oneof(EPids ++ CPids))},
                          {1, purge}
                         ]))))).

pid_gen() ->
    ?LET(_, integer(), spawn(fun () -> ok end)).

down_gen(Pid) ->
    ?LET(E, {down, Pid, oneof([noconnection, noproc])}, E).

enqueue_gen(Pid) ->
    ?LET(E, {enqueue, Pid, frequency([{10, enqueue},
                                      {1, delay}])}, E).

checkout_cancel_gen(Pid) ->
    {checkout, Pid, cancel}.

checkout_gen(Pid) ->
    %% pid, tag, prefetch
    ?LET(C, {checkout, {binary(), Pid}, choose(1, 10)}, C).


-record(t, {state = rabbit_fifo:init(#{name => proper,
                                       queue_resource => blah,
                                       shadow_copy_interval => 1})
                :: rabbit_fifo:state(),
            index = 1 :: non_neg_integer(), %% raft index
            enqueuers = #{} :: #{pid() => term()},
            consumers = #{} :: #{{binary(), pid()} => term()},
            effects = queue:new() :: queue:queue(),
            log = [] :: list(),
            down = #{} :: #{pid() => noproc | noconnection}
           }).

expand(Ops) ->
    %% execute each command against a rabbit_fifo state and capture all releavant
    %% effects
    T = #t{},
    #t{effects = Effs} = T1 = lists:foldl(fun handle_op/2, T, Ops),
    %% process the remaining effects
    #t{log = Log} = lists:foldl(fun do_apply/2,
                                T1#t{effects = queue:new()},
                                queue:to_list(Effs)),

    lists:reverse(Log).


handle_op({enqueue, Pid, When}, #t{enqueuers = Enqs0,
                                   down = Down,
                                   effects = Effs} = T) ->
    case Down of
        #{Pid := noproc} ->
            %% if it's a noproc then it cannot exist - can it?
            %% drop operation
            T;
        _ ->
            Enqs = maps:update_with(Pid, fun (Seq) -> Seq + 1 end, 1, Enqs0),
            MsgSeq = maps:get(Pid, Enqs),
            Cmd = rabbit_fifo:make_enqueue(Pid, MsgSeq, msg),
            case When of
                enqueue ->
                    do_apply(Cmd, T#t{enqueuers = Enqs});
                delay ->
                    %% just put the command on the effects queue
                    ct:pal("delaying ~w", [Cmd]),
                    T#t{effects = queue:in(Cmd, Effs)}
            end
    end;
handle_op({checkout, Pid, cancel}, #t{consumers  = Cons0} = T) ->
    case maps:keys(
           maps:filter(fun ({_, P}, _) when P == Pid -> true;
                           (_, _) -> false
                       end, Cons0)) of
        [CId | _] ->
            Cons = maps:remove(CId, Cons0),
            Cmd = rabbit_fifo:make_checkout(CId, cancel, #{}),
            do_apply(Cmd, T#t{consumers = Cons});
        _ ->
            T
    end;
handle_op({checkout, CId, Prefetch}, #t{consumers  = Cons0} = T) ->
    case Cons0 of
        #{CId := _} ->
            %% ignore if it already exists
            T;
        _ ->
            Cons = maps:put(CId, ok,  Cons0),
            Cmd = rabbit_fifo:make_checkout(CId,
                                            {auto, Prefetch, simple_prefetch},
                                            #{ack => true,
                                              prefetch => Prefetch,
                                              username => <<"user">>,
                                              args => []}),

            do_apply(Cmd, T#t{consumers = Cons})
    end;
handle_op({down, Pid, Reason} = Cmd, #t{down = Down} = T) ->
    case Down of
        #{Pid := noproc} ->
            %% it it permanently down, cannot upgrade
            T;
        _ ->
            %% it is either not down or down with noconnection
            do_apply(Cmd, T#t{down = maps:put(Pid, Reason, Down)})
    end;
handle_op({input_event, requeue}, #t{effects = Effs} = T) ->
    %% this simulates certain settlements arriving out of order
    case queue:out(Effs) of
        {{value, Cmd}, Q} ->
            T#t{effects = queue:in(Cmd, Q)};
        _ ->
            T
    end;
handle_op({input_event, Settlement}, #t{effects = Effs} = T) ->
    case queue:out(Effs) of
        {{value, {settle, MsgIds, CId}}, Q} ->
            Cmd = case Settlement of
                      settle -> rabbit_fifo:make_settle(CId, MsgIds);
                      return -> rabbit_fifo:make_return(CId, MsgIds);
                      discard -> rabbit_fifo:make_discard(CId, MsgIds)
                  end,
            do_apply(Cmd, T#t{effects = Q});
        {{value, Cmd}, Q} when element(1, Cmd) =:= enqueue ->
            do_apply(Cmd, T#t{effects = Q});
        _ ->
            T
    end;
handle_op(purge, T) ->
    do_apply(rabbit_fifo:make_purge(), T).

do_apply(Cmd, #t{effects = Effs, index = Index, state = S0,
                 log = Log} = T) ->
    {St, Effects} = case rabbit_fifo:apply(#{index => Index}, Cmd, S0) of
                       {S, _, E} when is_list(E) ->
                           {S, E};
                       {S, _, E} ->
                           {S, [E]};
                       {S, _} ->
                           {S, []}
                   end,

    T#t{state = St,
        index = Index + 1,
        effects = enq_effs(Effects, Effs),
        log = [Cmd | Log]}.

enq_effs([], Q) -> Q;
enq_effs([{send_msg, P, {delivery, CTag, Msgs}, ra_event} | Rem], Q) ->
    MsgIds = [I || {I, _} <- Msgs],
    %% always make settle commands by default
    %% they can be changed depending on the input event later
    Cmd = rabbit_fifo:make_settle({CTag, P}, MsgIds),
    enq_effs(Rem, queue:in(Cmd, Q));
enq_effs([_ | Rem], Q) ->
    % ct:pal("enq_effs dropping ~w~n", [E]),
    enq_effs(Rem, Q).


%% Utility
run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(
         erlang:apply(Fun, Args),
         [{numtests, NumTests},
          {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).

run_snapshot_test(Name, Commands) ->
    %% create every incremental permuation of the commands lists
    %% and run the snapshot tests against that
    [begin
         % ?debugFmt("~w running command to ~w~n", [?FUNCTION_NAME, lists:last(C)]),
         run_snapshot_test0(Name, C)
     end || C <- prefixes(Commands, 1, [])].

run_snapshot_test0(Name, Commands) ->
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(test_init(Name), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         ?assertEqual(State, S)
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

prefixes(Source, N, Acc) when N > length(Source) ->
    lists:reverse(Acc);
prefixes(Source, N, Acc) ->
    {X, _} = lists:split(N, Source),
    prefixes(Source, N+1, [X | Acc]).

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        case rabbit_fifo:apply(meta(Idx), E, Acc0) of
                            {Acc, _, Efx} when is_list(Efx) ->
                                {Acc, Efx0 ++ Efx};
                            {Acc, _, Efx}  ->
                                {Acc, Efx0 ++ [Efx]};
                            {Acc, _}  ->
                                {Acc, Efx0}
                        end
                end, {InitState, []}, Entries).

test_init(Name) ->
    rabbit_fifo:init(#{name => Name,
                       queue_resource => blah,
                       shadow_copy_interval => 0,
                       metrics_handler => {?MODULE, metrics_handler, []}}).
meta(Idx) ->
    #{index => Idx, term => 1}.

make_checkout(Cid, Spec) ->
    rabbit_fifo:make_checkout(Cid, Spec, #{}).

make_enqueue(Pid, Seq, Msg) ->
    rabbit_fifo:make_enqueue(Pid, Seq, Msg).

make_settle(Cid, MsgIds) ->
    rabbit_fifo:make_settle(Cid, MsgIds).

make_return(Cid, MsgIds) ->
    rabbit_fifo:make_return(Cid, MsgIds).
