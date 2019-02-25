-module(rabbit_fifo_prop_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
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
     scenario4,
     scenario5,
     scenario6,
     scenario7,
     scenario8,
     scenario9,
     scenario10,
     scenario11,
     scenario12,
     scenario13,
     scenario14,
     scenario15,
     scenario16,
     fake_pid
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
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
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
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
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
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario4(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,msg),
                make_settle(C1, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario5(_Config) ->
    C1 = {<<>>, c:pid(0,505,0)},
    E = c:pid(0,465,9),
    Commands = [make_enqueue(E,1,<<0>>),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,2,<<>>),
                make_settle(C1,[0])],
    run_snapshot_test(#{name => ?FUNCTION_NAME}, Commands),
    ok.

scenario6(_Config) ->
    E = c:pid(0,465,9),
    Commands = [make_enqueue(E,1,<<>>), %% 1 msg on queue - snap: prefix 1
                make_enqueue(E,2,<<>>) %% 1. msg on queue - snap: prefix 1
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario7(_Config) ->
    C1 = {<<>>, c:pid(0,208,0)},
    E = c:pid(0,188,0),
    Commands = [
                make_enqueue(E,1,<<>>),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,2,<<>>),
                make_enqueue(E,3,<<>>),
                make_settle(C1,[0])],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario8(_Config) ->
    C1 = {<<>>, c:pid(0,208,0)},
    E = c:pid(0,188,0),
    Commands = [
                make_enqueue(E,1,<<>>),
                make_enqueue(E,2,<<>>),
                make_checkout(C1, {auto,1,simple_prefetch}),
                % make_checkout(C1, cancel),
                {down, E, noconnection},
                make_settle(C1, [0])],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario9(_Config) ->
    E = c:pid(0,188,0),
    Commands = [
                make_enqueue(E,1,<<>>),
                make_enqueue(E,2,<<>>),
                make_enqueue(E,3,<<>>)],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario10(_Config) ->
    C1 = {<<>>, c:pid(0,208,0)},
    E = c:pid(0,188,0),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E,1,<<>>),
                make_settle(C1, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 1}, Commands),
    ok.

scenario11(_Config) ->
    C1 = {<<>>, c:pid(0,215,0)},
    E = c:pid(0,217,0),
    Commands = [
                make_enqueue(E,1,<<>>),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_checkout(C1, cancel),
                make_enqueue(E,2,<<>>),
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_settle(C1, [0]),
                make_checkout(C1, cancel)
                ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 2}, Commands),
    ok.

scenario12(_Config) ->
    E = c:pid(0,217,0),
    Commands = [make_enqueue(E,1,<<0>>),
                make_enqueue(E,2,<<0>>),
                make_enqueue(E,3,<<0>>)],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_bytes => 2}, Commands),
    ok.

scenario13(_Config) ->
    E = c:pid(0,217,0),
    Commands = [make_enqueue(E,1,<<0>>),
                make_enqueue(E,2,<<>>),
                make_enqueue(E,3,<<>>),
                make_enqueue(E,4,<<>>)
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_length => 2}, Commands),
    ok.

scenario14(_Config) ->
    E = c:pid(0,217,0),
    Commands = [make_enqueue(E,1,<<0,0>>)],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        max_bytes => 1}, Commands),
    ok.

scenario15(_Config) ->
    C1 = {<<>>, c:pid(0,179,1)},
    E = c:pid(0,176,1),
    Commands = [make_checkout(C1, {auto,2,simple_prefetch}),
                make_enqueue(E, 1, msg1),
                make_enqueue(E, 2, msg2),
                make_return(C1, [0]),
                make_return(C1, [2]),
                make_settle(C1, [1])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        delivery_limit => 1}, Commands),
    ok.

scenario16(_Config) ->
    C1Pid = c:pid(0,883,1),
    C1 = {<<>>, C1Pid},
    C2 = {<<>>, c:pid(0,882,1)},
    E = c:pid(0,176,1),
    Commands = [
                make_checkout(C1, {auto,1,simple_prefetch}),
                make_enqueue(E, 1, msg1),
                make_checkout(C2, {auto,1,simple_prefetch}),
                {down, C1Pid, noproc}, %% msg1 allocated to C2
                make_return(C2, [0]), %% msg1 returned
                make_enqueue(E, 2, <<>>),
                make_settle(C2, [0])
               ],
    run_snapshot_test(#{name => ?FUNCTION_NAME,
                        delivery_limit => 1}, Commands),
    ok.

fake_pid(_Config) ->
    Pid = fake_external_pid(<<"mynode@banana">>),
    ?assertNotEqual(node(Pid), node()),
    ?assert(is_pid(Pid)),
    ok.

fake_external_pid(Node) when is_binary(Node) ->
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    %% drop the local node data from a local pid
    <<_:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(Node),
    %% replace it with the incoming node binary
    Final = <<131,103, 100, 0, S, Node/binary, LocalPidData/binary>>,
    binary_to_term(Final).

snapshots(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Length, Bytes, SingleActiveConsumer, DeliveryLimit},
                      frequency([{10, {0, 0, false, 0}},
                                 {5, {oneof([range(1, 10), undefined]),
                                      oneof([range(1, 1000), undefined]),
                                      boolean(),
                                      oneof([range(1, 3), undefined])
                                     }}]),
                      ?FORALL(O, ?LET(Ops, log_gen(250), expand(Ops)),
                              collect({log_size, length(O)},
                                      snapshots_prop(
                                        config(?FUNCTION_NAME,
                                               Length,
                                               Bytes,
                                               SingleActiveConsumer,
                                               DeliveryLimit), O))))
      end, [], 2500).

config(Name, Length, Bytes, SingleActive, DeliveryLimit) ->
    #{name => Name,
      max_length => map_max(Length),
      max_bytes => map_max(Bytes),
      single_active_consumer_on => SingleActive,
      delivery_limit => map_max(DeliveryLimit)}.

map_max(0) -> undefined;
map_max(N) -> N.

snapshots_prop(Conf, Commands) ->
    try run_snapshot_test(Conf, Commands) of
        _ -> true
    catch
        Err ->
            ct:pal("Commands: ~p~nConf~p~n", [Commands, Conf]),
            ct:pal("Err: ~p~n", [Err]),
            false
    end.

log_gen(Size) ->
    Nodes = [node(),
             fakenode@fake,
             fakenode@fake2
            ],
    ?LET(EPids, vector(2, pid_gen(Nodes)),
         ?LET(CPids, vector(2, pid_gen(Nodes)),
              resize(Size,
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
                          {1, nodeup_gen(Nodes)},
                          {1, purge}
                         ]))))).

pid_gen(Nodes) ->
    ?LET(Node, oneof(Nodes),
         fake_external_pid(atom_to_binary(Node, utf8))).

down_gen(Pid) ->
    ?LET(E, {down, Pid, oneof([noconnection, noproc])}, E).

nodeup_gen(Nodes) ->
    {nodeup, oneof(Nodes)}.

enqueue_gen(Pid) ->
    ?LET(E, {enqueue, Pid,
             frequency([{10, enqueue},
                        {1, delay}]),
             binary()}, E).

checkout_cancel_gen(Pid) ->
    {checkout, Pid, cancel}.

checkout_gen(Pid) ->
    %% pid, tag, prefetch
    ?LET(C, {checkout, {binary(), Pid}, choose(1, 100)}, C).


-record(t, {state = rabbit_fifo:init(#{name => proper,
                                       queue_resource => blah,
                                       release_cursor_interval => 1})
                :: rabbit_fifo:state(),
            index = 1 :: non_neg_integer(), %% raft index
            enqueuers = #{} :: #{pid() => term()},
            consumers = #{} :: #{{binary(), pid()} => term()},
            effects = queue:new() :: queue:queue(),
            log = [] :: list(),
            down = #{} :: #{pid() => noproc | noconnection}
           }).

expand(Ops) ->
    %% execute each command against a rabbit_fifo state and capture all relevant
    %% effects
    T = #t{},
    #t{effects = Effs} = T1 = lists:foldl(fun handle_op/2, T, Ops),
    %% process the remaining effects
    #t{log = Log} = lists:foldl(fun do_apply/2,
                                T1#t{effects = queue:new()},
                                queue:to_list(Effs)),

    lists:reverse(Log).


handle_op({enqueue, Pid, When, Data},
          #t{enqueuers = Enqs0,
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
            Cmd = rabbit_fifo:make_enqueue(Pid, MsgSeq, Data),
            case When of
                enqueue ->
                    do_apply(Cmd, T#t{enqueuers = Enqs});
                delay ->
                    %% just put the command on the effects queue
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
handle_op({nodeup, _} = Cmd, T) ->
    do_apply(Cmd, T);
handle_op({input_event, requeue}, #t{effects = Effs} = T) ->
    %% this simulates certain settlements arriving out of order
    case queue:out(Effs) of
        {{value, Cmd}, Q} ->
            T#t{effects = queue:in(Cmd, Q)};
        _ ->
            T
    end;
handle_op({input_event, Settlement}, #t{effects = Effs,
                                        down = Down} = T) ->
    case queue:out(Effs) of
        {{value, {settle, MsgIds, CId}}, Q} ->
            Cmd = case Settlement of
                      settle -> rabbit_fifo:make_settle(CId, MsgIds);
                      return -> rabbit_fifo:make_return(CId, MsgIds);
                      discard -> rabbit_fifo:make_discard(CId, MsgIds)
                  end,
            do_apply(Cmd, T#t{effects = Q});
        {{value, Cmd}, Q} when element(1, Cmd) =:= enqueue ->
            case maps:is_key(element(2, Cmd), Down) of
                true ->
                    %% enqueues cannot arrive after down for the same process
                    %% drop message
                    T#t{effects = Q};
                false ->
                    do_apply(Cmd, T#t{effects = Q})
            end;
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

run_snapshot_test(Conf, Commands) ->
    %% create every incremental permutation of the commands lists
    %% and run the snapshot tests against that
    [begin
         % ?debugFmt("~w running command to ~w~n", [?FUNCTION_NAME, lists:last(C)]),
         run_snapshot_test0(Conf, C)
     end || C <- prefixes(Commands, 1, [])].

run_snapshot_test0(Conf, Commands) ->
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State0, Effects} = run_log(test_init(Conf), Entries),
    State = rabbit_fifo:normalize(State0),

    [begin
         % ct:pal("release_cursor: ~b~n", [SnapIdx]),
         %% drop all entries below and including the snapshot
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S0, _} = run_log(SnapState, Filtered),
         S = rabbit_fifo:normalize(S0),
         % assert log can be restored from any release cursor index
         case S of
             State -> ok;
             _ ->
                 ct:pal("Snapshot tests failed run log:~n"
                        "~p~n from ~n~p~n Entries~n~p~n",
                        [Filtered, SnapState, Entries]),
                 ct:pal("Expected~n~p~nGot:~n~p", [State, S]),
                 ?assertEqual(State, S)
         end
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

%% transforms [1,2,3] into [[1,2,3], [1,2], [1]]
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

test_init(Conf) ->
    Default = #{queue_resource => blah,
                release_cursor_interval => 0,
                metrics_handler => {?MODULE, metrics_handler, []}},
    rabbit_fifo:init(maps:merge(Default, Conf)).

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
