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

-module(rabbit_tests).

-compile([export_all]).

-export([all_tests/0, test_parsing/0]).

-compile({parse_transform, cut}).
-compile({parse_transform, do}).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("kernel/include/file.hrl").

-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).
-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

test_content_prop_roundtrip(Datum, Binary) ->
    Types =  [element(1, E) || E <- Datum],
    Values = [element(2, E) || E <- Datum],
    Values = rabbit_binary_parser:parse_properties(Types, Binary), %% assertion
    Binary = rabbit_binary_generator:encode_properties(Types, Values). %% assertion

all_tests() ->
    passed = gm_tests:all_tests(),
    application:set_env(rabbit, file_handles_high_watermark, 10, infinity),
    ok = file_handle_cache:set_limit(10),
    passed = test_file_handle_cache(),
    passed = test_backing_queue(),
    passed = test_priority_queue(),
    passed = test_bpqueue(),
    passed = test_pg_local(),
    passed = test_unfold(),
    passed = test_supervisor_delayed_restart(),
    passed = test_parsing(),
    passed = test_content_framing(),
    passed = test_content_transcoding(),
    passed = test_topic_matching(),
    passed = test_log_management(),
    passed = test_app_management(),
    passed = test_log_management_during_startup(),
    passed = test_statistics(),
    passed = test_option_parser(),
    passed = test_cluster_management(),
    passed = test_user_management(),
    passed = test_server_status(),
    passed = test_confirms(),
    passed = maybe_run_cluster_dependent_tests(),
    passed = test_configurable_server_properties(),
    passed.

maybe_run_cluster_dependent_tests() ->
    SecondaryNode = rabbit_misc:makenode("hare"),

    case net_adm:ping(SecondaryNode) of
        pong -> passed = run_cluster_dependent_tests(SecondaryNode);
        pang -> io:format("Skipping cluster dependent tests with node ~p~n",
                          [SecondaryNode])
    end,
    passed.

run_cluster_dependent_tests(SecondaryNode) ->
    SecondaryNodeS = atom_to_list(SecondaryNode),

    ok = control_action(stop_app, []),
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),
    ok = control_action(start_app, []),

    io:format("Running cluster dependent tests with node ~p~n", [SecondaryNode]),
    passed = test_delegates_async(SecondaryNode),
    passed = test_delegates_sync(SecondaryNode),
    passed = test_queue_cleanup(SecondaryNode),
    passed = test_declare_on_dead_queue(SecondaryNode),

    %% we now run the tests remotely, so that code coverage on the
    %% local node picks up more of the delegate
    Node = node(),
    Self = self(),
    Remote = spawn(SecondaryNode,
                   fun () -> Rs = [ test_delegates_async(Node),
                                    test_delegates_sync(Node),
                                    test_queue_cleanup(Node),
                                    test_declare_on_dead_queue(Node) ],
                             Self ! {self(), Rs}
                   end),
    receive
        {Remote, Result} ->
            Result = lists:duplicate(length(Result), passed)
    after 30000 ->
            throw(timeout)
    end,

    passed.

test_priority_queue() ->

    false = priority_queue:is_queue(not_a_queue),

    %% empty Q
    Q = priority_queue:new(),
    {true, true, 0, [], []} = test_priority_queue(Q),

    %% 1-4 element no-priority Q
    true = lists:all(fun (X) -> X =:= passed end,
                     lists:map(fun test_simple_n_element_queue/1,
                               lists:seq(1, 4))),

    %% 1-element priority Q
    Q1 = priority_queue:in(foo, 1, priority_queue:new()),
    {true, false, 1, [{1, foo}], [foo]} =
        test_priority_queue(Q1),

    %% 2-element same-priority Q
    Q2 = priority_queue:in(bar, 1, Q1),
    {true, false, 2, [{1, foo}, {1, bar}], [foo, bar]} =
        test_priority_queue(Q2),

    %% 2-element different-priority Q
    Q3 = priority_queue:in(bar, 2, Q1),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q3),

    %% 1-element negative priority Q
    Q4 = priority_queue:in(foo, -1, priority_queue:new()),
    {true, false, 1, [{-1, foo}], [foo]} = test_priority_queue(Q4),

    %% merge 2 * 1-element no-priority Qs
    Q5 = priority_queue:join(priority_queue:in(foo, Q),
                             priority_queue:in(bar, Q)),
    {true, false, 2, [{0, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q5),

    %% merge 1-element no-priority Q with 1-element priority Q
    Q6 = priority_queue:join(priority_queue:in(foo, Q),
                             priority_queue:in(bar, 1, Q)),
    {true, false, 2, [{1, bar}, {0, foo}], [bar, foo]} =
        test_priority_queue(Q6),

    %% merge 1-element priority Q with 1-element no-priority Q
    Q7 = priority_queue:join(priority_queue:in(foo, 1, Q),
                             priority_queue:in(bar, Q)),
    {true, false, 2, [{1, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q7),

    %% merge 2 * 1-element same-priority Qs
    Q8 = priority_queue:join(priority_queue:in(foo, 1, Q),
                             priority_queue:in(bar, 1, Q)),
    {true, false, 2, [{1, foo}, {1, bar}], [foo, bar]} =
        test_priority_queue(Q8),

    %% merge 2 * 1-element different-priority Qs
    Q9 = priority_queue:join(priority_queue:in(foo, 1, Q),
                             priority_queue:in(bar, 2, Q)),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q9),

    %% merge 2 * 1-element different-priority Qs (other way around)
    Q10 = priority_queue:join(priority_queue:in(bar, 2, Q),
                              priority_queue:in(foo, 1, Q)),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q10),

    %% merge 2 * 2-element multi-different-priority Qs
    Q11 = priority_queue:join(Q6, Q5),
    {true, false, 4, [{1, bar}, {0, foo}, {0, foo}, {0, bar}],
     [bar, foo, foo, bar]} = test_priority_queue(Q11),

    %% and the other way around
    Q12 = priority_queue:join(Q5, Q6),
    {true, false, 4, [{1, bar}, {0, foo}, {0, bar}, {0, foo}],
     [bar, foo, bar, foo]} = test_priority_queue(Q12),

    %% merge with negative priorities
    Q13 = priority_queue:join(Q4, Q5),
    {true, false, 3, [{0, foo}, {0, bar}, {-1, foo}], [foo, bar, foo]} =
        test_priority_queue(Q13),

    %% and the other way around
    Q14 = priority_queue:join(Q5, Q4),
    {true, false, 3, [{0, foo}, {0, bar}, {-1, foo}], [foo, bar, foo]} =
        test_priority_queue(Q14),

    %% joins with empty queues:
    Q1 = priority_queue:join(Q, Q1),
    Q1 = priority_queue:join(Q1, Q),

    %% insert with priority into non-empty zero-priority queue
    Q15 = priority_queue:in(baz, 1, Q5),
    {true, false, 3, [{1, baz}, {0, foo}, {0, bar}], [baz, foo, bar]} =
        test_priority_queue(Q15),

    passed.

priority_queue_in_all(Q, L) ->
    lists:foldl(fun (X, Acc) -> priority_queue:in(X, Acc) end, Q, L).

priority_queue_out_all(Q) ->
    case priority_queue:out(Q) of
        {empty, _}       -> [];
        {{value, V}, Q1} -> [V | priority_queue_out_all(Q1)]
    end.

test_priority_queue(Q) ->
    {priority_queue:is_queue(Q),
     priority_queue:is_empty(Q),
     priority_queue:len(Q),
     priority_queue:to_list(Q),
     priority_queue_out_all(Q)}.

test_bpqueue() ->
    Q = bpqueue:new(),
    true = bpqueue:is_empty(Q),
    0 = bpqueue:len(Q),
    [] = bpqueue:to_list(Q),

    Q1 = bpqueue_test(fun bpqueue:in/3, fun bpqueue:out/1,
                      fun bpqueue:to_list/1,
                      fun bpqueue:foldl/3, fun bpqueue:map_fold_filter_l/4),
    Q2 = bpqueue_test(fun bpqueue:in_r/3, fun bpqueue:out_r/1,
                      fun (QR) -> lists:reverse(
                                    [{P, lists:reverse(L)} ||
                                        {P, L} <- bpqueue:to_list(QR)])
                      end,
                      fun bpqueue:foldr/3, fun bpqueue:map_fold_filter_r/4),

    [{foo, [1, 2]}, {bar, [3]}] = bpqueue:to_list(bpqueue:join(Q, Q1)),
    [{bar, [3]}, {foo, [2, 1]}] = bpqueue:to_list(bpqueue:join(Q2, Q)),
    [{foo, [1, 2]}, {bar, [3, 3]}, {foo, [2,1]}] =
        bpqueue:to_list(bpqueue:join(Q1, Q2)),

    [{foo, [1, 2]}, {bar, [3]}, {foo, [1, 2]}, {bar, [3]}] =
        bpqueue:to_list(bpqueue:join(Q1, Q1)),

    [{foo, [1, 2]}, {bar, [3]}] =
        bpqueue:to_list(
          bpqueue:from_list(
            [{x, []}, {foo, [1]}, {y, []}, {foo, [2]}, {bar, [3]}, {z, []}])),

    [{undefined, [a]}] = bpqueue:to_list(bpqueue:from_list([{undefined, [a]}])),

    {4, [a,b,c,d]} =
        bpqueue:foldl(
          fun (Prefix, Value, {Prefix, Acc}) ->
                  {Prefix + 1, [Value | Acc]}
          end,
          {0, []}, bpqueue:from_list([{0,[d]}, {1,[c]}, {2,[b]}, {3,[a]}])),

    [{bar,3}, {foo,2}, {foo,1}] =
        bpqueue:foldr(fun (P, V, I) -> [{P,V} | I] end, [], Q2),

    BPQL = [{foo,[1,2,2]}, {bar,[3,4,5]}, {foo,[5,6,7]}],
    BPQ = bpqueue:from_list(BPQL),

    %% no effect
    {BPQL, 0} = bpqueue_mffl([none], {none, []}, BPQ),
    {BPQL, 0} = bpqueue_mffl([foo,bar], {none, [1]}, BPQ),
    {BPQL, 0} = bpqueue_mffl([bar], {none, [3]}, BPQ),
    {BPQL, 0} = bpqueue_mffr([bar], {foo, [5]}, BPQ),

    %% process 1 item
    {[{foo,[-1,2,2]}, {bar,[3,4,5]}, {foo,[5,6,7]}], 1} =
        bpqueue_mffl([foo,bar], {foo, [2]}, BPQ),
    {[{foo,[1,2,2]}, {bar,[-3,4,5]}, {foo,[5,6,7]}], 1} =
        bpqueue_mffl([bar], {bar, [4]}, BPQ),
    {[{foo,[1,2,2]}, {bar,[3,4,5]}, {foo,[5,6,-7]}], 1} =
        bpqueue_mffr([foo,bar], {foo, [6]}, BPQ),
    {[{foo,[1,2,2]}, {bar,[3,4]}, {baz,[-5]}, {foo,[5,6,7]}], 1} =
        bpqueue_mffr([bar], {baz, [4]}, BPQ),

    %% change prefix
    {[{bar,[-1,-2,-2,-3,-4,-5,-5,-6,-7]}], 9} =
        bpqueue_mffl([foo,bar], {bar, []}, BPQ),
    {[{bar,[-1,-2,-2,3,4,5]}, {foo,[5,6,7]}], 3} =
        bpqueue_mffl([foo], {bar, [5]}, BPQ),
    {[{bar,[-1,-2,-2,3,4,5,-5,-6]}, {foo,[7]}], 5} =
        bpqueue_mffl([foo], {bar, [7]}, BPQ),
    {[{foo,[1,2,2,-3,-4]}, {bar,[5]}, {foo,[5,6,7]}], 2} =
        bpqueue_mffl([bar], {foo, [5]}, BPQ),
    {[{bar,[-1,-2,-2,3,4,5,-5,-6,-7]}], 6} =
        bpqueue_mffl([foo], {bar, []}, BPQ),
    {[{foo,[1,2,2,-3,-4,-5,5,6,7]}], 3} =
        bpqueue_mffl([bar], {foo, []}, BPQ),

    %% edge cases
    {[{foo,[-1,-2,-2]}, {bar,[3,4,5]}, {foo,[5,6,7]}], 3} =
        bpqueue_mffl([foo], {foo, [5]}, BPQ),
    {[{foo,[1,2,2]}, {bar,[3,4,5]}, {foo,[-5,-6,-7]}], 3} =
        bpqueue_mffr([foo], {foo, [2]}, BPQ),

    passed.

bpqueue_test(In, Out, List, Fold, MapFoldFilter) ->
    Q = bpqueue:new(),
    {empty, _Q} = Out(Q),

    ok = Fold(fun (Prefix, Value, ok) -> {error, Prefix, Value} end, ok, Q),
    {Q1M, 0} = MapFoldFilter(fun(_P)     -> throw(explosion) end,
                             fun(_V, _N) -> throw(explosion) end, 0, Q),
    [] = bpqueue:to_list(Q1M),

    Q1 = In(bar, 3, In(foo, 2, In(foo, 1, Q))),
    false = bpqueue:is_empty(Q1),
    3 = bpqueue:len(Q1),
    [{foo, [1, 2]}, {bar, [3]}] = List(Q1),

    {{value, foo, 1}, Q3}  = Out(Q1),
    {{value, foo, 2}, Q4}  = Out(Q3),
    {{value, bar, 3}, _Q5} = Out(Q4),

    F = fun (QN) ->
                MapFoldFilter(fun (foo) -> true;
                                  (_)   -> false
                              end,
                              fun (2, _Num) -> stop;
                                  (V, Num)  -> {bar, -V, V - Num} end,
                              0, QN)
        end,
    {Q6, 0} = F(Q),
    [] = bpqueue:to_list(Q6),
    {Q7, 1} = F(Q1),
    [{bar, [-1]}, {foo, [2]}, {bar, [3]}] = List(Q7),

    Q1.

bpqueue_mffl(FF1A, FF2A, BPQ) ->
    bpqueue_mff(fun bpqueue:map_fold_filter_l/4, FF1A, FF2A, BPQ).

bpqueue_mffr(FF1A, FF2A, BPQ) ->
    bpqueue_mff(fun bpqueue:map_fold_filter_r/4, FF1A, FF2A, BPQ).

bpqueue_mff(Fold, FF1A, FF2A, BPQ) ->
    FF1 = fun (Prefixes) ->
                  fun (P) -> lists:member(P, Prefixes) end
          end,
    FF2 = fun ({Prefix, Stoppers}) ->
                  fun (Val, Num) ->
                          case lists:member(Val, Stoppers) of
                              true -> stop;
                              false -> {Prefix, -Val, 1 + Num}
                          end
                  end
          end,
    Queue_to_list = fun ({LHS, RHS}) -> {bpqueue:to_list(LHS), RHS} end,

    Queue_to_list(Fold(FF1(FF1A), FF2(FF2A), 0, BPQ)).

test_simple_n_element_queue(N) ->
    Items = lists:seq(1, N),
    Q = priority_queue_in_all(priority_queue:new(), Items),
    ToListRes = [{0, X} || X <- Items],
    {true, false, N, ToListRes, Items} = test_priority_queue(Q),
    passed.

test_pg_local() ->
    [P, Q] = [spawn(fun () -> receive X -> X end end) || _ <- [x, x]],
    check_pg_local(ok, [], []),
    check_pg_local(pg_local:join(a, P), [P], []),
    check_pg_local(pg_local:join(b, P), [P], [P]),
    check_pg_local(pg_local:join(a, P), [P, P], [P]),
    check_pg_local(pg_local:join(a, Q), [P, P, Q], [P]),
    check_pg_local(pg_local:join(b, Q), [P, P, Q], [P, Q]),
    check_pg_local(pg_local:join(b, Q), [P, P, Q], [P, Q, Q]),
    check_pg_local(pg_local:leave(a, P), [P, Q], [P, Q, Q]),
    check_pg_local(pg_local:leave(b, P), [P, Q], [Q, Q]),
    check_pg_local(pg_local:leave(a, P), [Q], [Q, Q]),
    check_pg_local(pg_local:leave(a, P), [Q], [Q, Q]),
    [begin X ! done,
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [P, Q]],
    check_pg_local(ok, [], []),
    passed.

check_pg_local(ok, APids, BPids) ->
    ok = pg_local:sync(),
    [true, true] = [lists:sort(Pids) == lists:sort(pg_local:get_members(Key)) ||
                       {Key, Pids} <- [{a, APids}, {b, BPids}]].

test_unfold() ->
    {[], test} = rabbit_misc:unfold(fun (_V) -> false end, test),
    List = lists:seq(2,20,2),
    {List, 0} = rabbit_misc:unfold(fun (0) -> false;
                                       (N) -> {true, N*2, N-1}
                                   end, 10),
    passed.

test_parsing() ->
    passed = test_content_properties(),
    passed = test_field_values(),
    passed.

test_content_properties() ->
    test_content_prop_roundtrip([], <<0, 0>>),
    test_content_prop_roundtrip([{bit, true}, {bit, false}, {bit, true}, {bit, false}],
                                <<16#A0, 0>>),
    test_content_prop_roundtrip([{bit, true}, {octet, 123}, {bit, true}, {octet, undefined},
                                 {bit, true}],
                                <<16#E8,0,123>>),
    test_content_prop_roundtrip([{bit, true}, {octet, 123}, {octet, 123}, {bit, true}],
                                <<16#F0,0,123,123>>),
    test_content_prop_roundtrip([{bit, true}, {shortstr, <<"hi">>}, {bit, true},
                                 {shortint, 54321}, {bit, true}],
                                <<16#F8,0,2,"hi",16#D4,16#31>>),
    test_content_prop_roundtrip([{bit, true}, {shortstr, undefined}, {bit, true},
                                 {shortint, 54321}, {bit, true}],
                                <<16#B8,0,16#D4,16#31>>),
    test_content_prop_roundtrip([{table, [{<<"a signedint">>, signedint, 12345678},
                                          {<<"a longstr">>, longstr, <<"yes please">>},
                                          {<<"a decimal">>, decimal, {123, 12345678}},
                                          {<<"a timestamp">>, timestamp, 123456789012345},
                                          {<<"a nested table">>, table,
                                           [{<<"one">>, signedint, 1},
                                            {<<"two">>, signedint, 2}]}]}],
                                <<
                                  %% property-flags
                                  16#8000:16,

                                  %% property-list:

                                  %% table
                                  117:32,                % table length in bytes

                                  11,"a signedint",      % name
                                  "I",12345678:32,       % type and value

                                  9,"a longstr",
                                  "S",10:32,"yes please",

                                  9,"a decimal",
                                  "D",123,12345678:32,

                                  11,"a timestamp",
                                  "T", 123456789012345:64,

                                  14,"a nested table",
                                  "F",
                                  18:32,

                                  3,"one",
                                  "I",1:32,

                                  3,"two",
                                  "I",2:32 >>),
    case catch rabbit_binary_parser:parse_properties([bit, bit, bit, bit], <<16#A0,0,1>>) of
        {'EXIT', content_properties_binary_overflow} -> passed;
        V -> exit({got_success_but_expected_failure, V})
    end.

test_field_values() ->
    %% FIXME this does not test inexact numbers (double and float) yet,
    %% because they won't pass the equality assertions
    test_content_prop_roundtrip(
      [{table, [{<<"longstr">>, longstr, <<"Here is a long string">>},
                {<<"signedint">>, signedint, 12345},
                {<<"decimal">>, decimal, {3, 123456}},
                {<<"timestamp">>, timestamp, 109876543209876},
                {<<"table">>, table, [{<<"one">>, signedint, 54321},
                                      {<<"two">>, longstr, <<"A long string">>}]},
                {<<"byte">>, byte, 255},
                {<<"long">>, long, 1234567890},
                {<<"short">>, short, 655},
                {<<"bool">>, bool, true},
                {<<"binary">>, binary, <<"a binary string">>},
                {<<"void">>, void, undefined},
                {<<"array">>, array, [{signedint, 54321},
                                      {longstr, <<"A long string">>}]}

               ]}],
      <<
        %% property-flags
        16#8000:16,
        %% table length in bytes
        228:32,

        7,"longstr",   "S", 21:32, "Here is a long string",      %      = 34
        9,"signedint", "I", 12345:32/signed,                     % + 15 = 49
        7,"decimal",   "D", 3, 123456:32,                        % + 14 = 63
        9,"timestamp", "T", 109876543209876:64,                  % + 19 = 82
        5,"table",     "F", 31:32, % length of table             % + 11 = 93
        3,"one", "I", 54321:32,                                  % +  9 = 102
        3,"two", "S", 13:32, "A long string",                    % + 22 = 124
        4,"byte",      "b", 255:8,                               % +  7 = 131
        4,"long",      "l", 1234567890:64,                       % + 14 = 145
        5,"short",     "s", 655:16,                              % +  9 = 154
        4,"bool",      "t", 1,                                   % +  7 = 161
        6,"binary",    "x", 15:32, "a binary string",            % + 27 = 188
        4,"void",      "V",                                      % +  6 = 194
        5,"array",     "A", 23:32,                               % + 11 = 205
        "I", 54321:32,                                           % +  5 = 210
        "S", 13:32, "A long string"                              % + 18 = 228
      >>),
    passed.

%% Test that content frames don't exceed frame-max
test_content_framing(FrameMax, BodyBin) ->
    [Header | Frames] =
        rabbit_binary_generator:build_simple_content_frames(
          1,
          rabbit_binary_generator:ensure_content_encoded(
            rabbit_basic:build_content(#'P_basic'{}, BodyBin),
            rabbit_framing_amqp_0_9_1),
          FrameMax,
          rabbit_framing_amqp_0_9_1),
    %% header is formatted correctly and the size is the total of the
    %% fragments
    <<_FrameHeader:7/binary, _ClassAndWeight:4/binary,
      BodySize:64/unsigned, _Rest/binary>> = list_to_binary(Header),
    BodySize = size(BodyBin),
    true = lists:all(
             fun (ContentFrame) ->
                     FrameBinary = list_to_binary(ContentFrame),
                     %% assert
                     <<_TypeAndChannel:3/binary,
                       Size:32/unsigned, _Payload:Size/binary, 16#CE>> =
                         FrameBinary,
                     size(FrameBinary) =< FrameMax
             end, Frames),
    passed.

test_content_framing() ->
    %% no content
    passed = test_content_framing(4096, <<>>),
    %% easily fit in one frame
    passed = test_content_framing(4096, <<"Easy">>),
    %% exactly one frame (empty frame = 8 bytes)
    passed = test_content_framing(11, <<"One">>),
    %% more than one frame
    passed = test_content_framing(11, <<"More than one frame">>),
    passed.

test_content_transcoding() ->
    %% there are no guarantees provided by 'clear' - it's just a hint
    ClearDecoded = fun rabbit_binary_parser:clear_decoded_content/1,
    ClearEncoded = fun rabbit_binary_generator:clear_encoded_content/1,
    EnsureDecoded =
        fun (C0) ->
                C1 = rabbit_binary_parser:ensure_content_decoded(C0),
                true = C1#content.properties =/= none,
                C1
        end,
    EnsureEncoded =
        fun (Protocol) ->
                fun (C0) ->
                        C1 = rabbit_binary_generator:ensure_content_encoded(
                               C0, Protocol),
                        true = C1#content.properties_bin =/= none,
                        C1
                end
        end,
    %% Beyond the assertions in Ensure*, the only testable guarantee
    %% is that the operations should never fail.
    %%
    %% If we were using quickcheck we'd simply stuff all the above
    %% into a generator for sequences of operations. In the absence of
    %% quickcheck we pick particularly interesting sequences that:
    %%
    %% - execute every op twice since they are idempotent
    %% - invoke clear_decoded, clear_encoded, decode and transcode
    %%   with one or both of decoded and encoded content present
    [begin
         sequence_with_content([Op]),
         sequence_with_content([ClearEncoded, Op]),
         sequence_with_content([ClearDecoded, Op])
     end || Op <- [ClearDecoded, ClearEncoded, EnsureDecoded,
                   EnsureEncoded(rabbit_framing_amqp_0_9_1),
                   EnsureEncoded(rabbit_framing_amqp_0_8)]],
    passed.

sequence_with_content(Sequence) ->
    lists:foldl(fun (F, V) -> F(F(V)) end,
                rabbit_binary_generator:ensure_content_encoded(
                  rabbit_basic:build_content(#'P_basic'{}, <<>>),
                  rabbit_framing_amqp_0_9_1),
                Sequence).

test_topic_matching() ->
    XName = #resource{virtual_host = <<"/">>,
                      kind = exchange,
                      name = <<"test_exchange">>},
    X = #exchange{name = XName, type = topic, durable = false,
                  auto_delete = false, arguments = []},
    %% create
    rabbit_exchange_type_topic:validate(X),
    exchange_op_callback(X, create, []),

    %% add some bindings
    Bindings = [#binding{source = XName,
                         key = list_to_binary(Key),
                         destination = #resource{virtual_host = <<"/">>,
                                                 kind = queue,
                                                 name = list_to_binary(Q)}} ||
                   {Key, Q} <- [{"a.b.c",         "t1"},
                                {"a.*.c",         "t2"},
                                {"a.#.b",         "t3"},
                                {"a.b.b.c",       "t4"},
                                {"#",             "t5"},
                                {"#.#",           "t6"},
                                {"#.b",           "t7"},
                                {"*.*",           "t8"},
                                {"a.*",           "t9"},
                                {"*.b.c",         "t10"},
                                {"a.#",           "t11"},
                                {"a.#.#",         "t12"},
                                {"b.b.c",         "t13"},
                                {"a.b.b",         "t14"},
                                {"a.b",           "t15"},
                                {"b.c",           "t16"},
                                {"",              "t17"},
                                {"*.*.*",         "t18"},
                                {"vodka.martini", "t19"},
                                {"a.b.c",         "t20"},
                                {"*.#",           "t21"},
                                {"#.*.#",         "t22"},
                                {"*.#.#",         "t23"},
                                {"#.#.#",         "t24"},
                                {"*",             "t25"},
                                {"#.b.#",         "t26"}]],
    lists:foreach(fun (B) -> exchange_op_callback(X, add_binding, [B]) end,
                  Bindings),

    %% test some matches
    test_topic_expect_match(
      X, [{"a.b.c",               ["t1", "t2", "t5", "t6", "t10", "t11", "t12",
                                   "t18", "t20", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b",                 ["t3", "t5", "t6", "t7", "t8", "t9", "t11",
                                   "t12", "t15", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b.b",               ["t3", "t5", "t6", "t7", "t11", "t12", "t14",
                                   "t18", "t21", "t22", "t23", "t24", "t26"]},
          {"",                    ["t5", "t6", "t17", "t24"]},
          {"b.c.c",               ["t5", "t6", "t18", "t21", "t22", "t23",
                                   "t24", "t26"]},
          {"a.a.a.a.a",           ["t5", "t6", "t11", "t12", "t21", "t22",
                                   "t23", "t24"]},
          {"vodka.gin",           ["t5", "t6", "t8", "t21", "t22", "t23",
                                   "t24"]},
          {"vodka.martini",       ["t5", "t6", "t8", "t19", "t21", "t22", "t23",
                                   "t24"]},
          {"b.b.c",               ["t5", "t6", "t10", "t13", "t18", "t21",
                                   "t22", "t23", "t24", "t26"]},
          {"nothing.here.at.all", ["t5", "t6", "t21", "t22", "t23", "t24"]},
          {"oneword",             ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25"]}]),

    %% remove some bindings
    RemovedBindings = [lists:nth(1, Bindings), lists:nth(5, Bindings),
                       lists:nth(11, Bindings), lists:nth(19, Bindings),
                       lists:nth(21, Bindings)],
    exchange_op_callback(X, remove_bindings, [RemovedBindings]),
    RemainingBindings = ordsets:to_list(
                          ordsets:subtract(ordsets:from_list(Bindings),
                                           ordsets:from_list(RemovedBindings))),

    %% test some matches
    test_topic_expect_match(
      X,
      [{"a.b.c",               ["t2", "t6", "t10", "t12", "t18", "t20", "t22",
                                "t23", "t24", "t26"]},
       {"a.b",                 ["t3", "t6", "t7", "t8", "t9", "t12", "t15",
                                "t22", "t23", "t24", "t26"]},
       {"a.b.b",               ["t3", "t6", "t7", "t12", "t14", "t18", "t22",
                                "t23", "t24", "t26"]},
       {"",                    ["t6", "t17", "t24"]},
       {"b.c.c",               ["t6", "t18", "t22", "t23", "t24", "t26"]},
       {"a.a.a.a.a",           ["t6", "t12", "t22", "t23", "t24"]},
       {"vodka.gin",           ["t6", "t8", "t22", "t23", "t24"]},
       {"vodka.martini",       ["t6", "t8", "t22", "t23", "t24"]},
       {"b.b.c",               ["t6", "t10", "t13", "t18", "t22", "t23",
                                "t24", "t26"]},
       {"nothing.here.at.all", ["t6", "t22", "t23", "t24"]},
       {"oneword",             ["t6", "t22", "t23", "t24", "t25"]}]),

    %% remove the entire exchange
    exchange_op_callback(X, delete, [RemainingBindings]),
    %% none should match now
    test_topic_expect_match(X, [{"a.b.c", []}, {"b.b.c", []}, {"", []}]),
    passed.

exchange_op_callback(X, Fun, Args) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> rabbit_exchange:callback(X, Fun, [transaction, X] ++ Args) end),
    rabbit_exchange:callback(X, Fun, [none, X] ++ Args).

test_topic_expect_match(X, List) ->
    lists:foreach(
      fun ({Key, Expected}) ->
              BinKey = list_to_binary(Key),
              Message = rabbit_basic:message(X#exchange.name, BinKey,
                                             #'P_basic'{}, <<>>),
              Res = rabbit_exchange_type_topic:route(
                      X, #delivery{mandatory = false,
                                   immediate = false,
                                   txn       = none,
                                   sender    = self(),
                                   message   = Message}),
              ExpectedRes = lists:map(
                              fun (Q) -> #resource{virtual_host = <<"/">>,
                                                   kind = queue,
                                                   name = list_to_binary(Q)}
                              end, Expected),
              true = (lists:usort(ExpectedRes) =:= lists:usort(Res))
      end, List).

test_app_management() ->
    %% starting, stopping, status
    ok = control_action(stop_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(status, []),
    ok = control_action(start_app, []),
    ok = control_action(start_app, []),
    ok = control_action(status, []),
    passed.

test_log_management() ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),
    Suffix = ".1",

    %% prepare basic logs
    file:delete([MainLog, Suffix]),
    file:delete([SaslLog, Suffix]),

    %% simple logs reopening
    ok = control_action(rotate_logs, []),
    [true, true] = empty_files([MainLog, SaslLog]),
    ok = test_logs_working(MainLog, SaslLog),

    %% simple log rotation
    ok = control_action(rotate_logs, [Suffix]),
    [true, true] = non_empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),
    [true, true] = empty_files([MainLog, SaslLog]),
    ok = test_logs_working(MainLog, SaslLog),

    %% reopening logs with log rotation performed first
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = file:rename(MainLog, [MainLog, Suffix]),
    ok = file:rename(SaslLog, [SaslLog, Suffix]),
    ok = test_logs_working([MainLog, Suffix], [SaslLog, Suffix]),
    ok = control_action(rotate_logs, []),
    ok = test_logs_working(MainLog, SaslLog),

    %% log rotation on empty file
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = control_action(rotate_logs, [Suffix]),
    [true, true] = empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),

    %% original main log file is not writable
    ok = make_files_non_writable([MainLog]),
    {error, {cannot_rotate_main_logs, _}} = control_action(rotate_logs, []),
    ok = clean_logs([MainLog], Suffix),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog}]),

    %% original sasl log file is not writable
    ok = make_files_non_writable([SaslLog]),
    {error, {cannot_rotate_sasl_logs, _}} = control_action(rotate_logs, []),
    ok = clean_logs([SaslLog], Suffix),
    ok = add_log_handlers([{rabbit_sasl_report_file_h, SaslLog}]),

    %% logs with suffix are not writable
    ok = control_action(rotate_logs, [Suffix]),
    ok = make_files_non_writable([[MainLog, Suffix], [SaslLog, Suffix]]),
    ok = control_action(rotate_logs, [Suffix]),
    ok = test_logs_working(MainLog, SaslLog),

    %% original log files are not writable
    ok = make_files_non_writable([MainLog, SaslLog]),
    {error, {{cannot_rotate_main_logs, _},
             {cannot_rotate_sasl_logs, _}}} = control_action(rotate_logs, []),

    %% logging directed to tty (handlers were removed in last test)
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = application:set_env(sasl, sasl_error_logger, tty),
    ok = application:set_env(kernel, error_logger, tty),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% rotate logs when logging is turned off
    ok = application:set_env(sasl, sasl_error_logger, false),
    ok = application:set_env(kernel, error_logger, silent),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% cleanup
    ok = application:set_env(sasl, sasl_error_logger, {file, SaslLog}),
    ok = application:set_env(kernel, error_logger, {file, MainLog}),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog},
                           {rabbit_sasl_report_file_h, SaslLog}]),
    passed.

test_log_management_during_startup() ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),

    %% start application with simple tty logging
    ok = control_action(stop_app, []),
    ok = application:set_env(kernel, error_logger, tty),
    ok = application:set_env(sasl, sasl_error_logger, tty),
    ok = add_log_handlers([{error_logger_tty_h, []},
                           {sasl_report_tty_h, []}]),
    ok = control_action(start_app, []),

    %% start application with tty logging and
    %% proper handlers not installed
    ok = control_action(stop_app, []),
    ok = error_logger:tty(false),
    ok = delete_log_handlers([sasl_report_tty_h]),
    ok = case catch control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_tty_no_handlers_test});
             {error, {cannot_log_to_tty, _, _}} -> ok
         end,

    %% fix sasl logging
    ok = application:set_env(sasl, sasl_error_logger,
                             {file, SaslLog}),

    %% start application with logging to non-existing directory
    TmpLog = "/tmp/rabbit-tests/test.log",
    delete_file(TmpLog),
    ok = application:set_env(kernel, error_logger, {file, TmpLog}),

    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = control_action(start_app, []),

    %% start application with logging to directory with no
    %% write permissions
    TmpDir = "/tmp/rabbit-tests",
    ok = set_permissions(TmpDir, 8#00400),
    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_no_write_permission_dir_test});
             {error, {cannot_log_to_file, _, _}} -> ok
         end,

    %% start application with logging to a subdirectory which
    %% parent directory has no write permissions
    TmpTestDir = "/tmp/rabbit-tests/no-permission/test/log",
    ok = application:set_env(kernel, error_logger, {file, TmpTestDir}),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotatation_parent_dirs_test});
             {error, {cannot_log_to_file, _,
                      {error, {cannot_create_parent_dirs, _, eacces}}}} -> ok
         end,
    ok = set_permissions(TmpDir, 8#00700),
    ok = set_permissions(TmpLog, 8#00600),
    ok = delete_file(TmpLog),
    ok = file:del_dir(TmpDir),

    %% start application with standard error_logger_file_h
    %% handler not installed
    ok = application:set_env(kernel, error_logger, {file, MainLog}),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% start application with standard sasl handler not installed
    %% and rabbit main log handler installed correctly
    ok = delete_log_handlers([rabbit_sasl_report_file_h]),
    ok = control_action(start_app, []),
    passed.

test_option_parser() ->
    %% command and arguments should just pass through
    ok = check_get_options({["mock_command", "arg1", "arg2"], []},
                           [], ["mock_command", "arg1", "arg2"]),

    %% get flags
    ok = check_get_options(
           {["mock_command", "arg1"], [{"-f", true}, {"-f2", false}]},
           [{flag, "-f"}, {flag, "-f2"}], ["mock_command", "arg1", "-f"]),

    %% get options
    ok = check_get_options(
           {["mock_command"], [{"-foo", "bar"}, {"-baz", "notbaz"}]},
           [{option, "-foo", "notfoo"}, {option, "-baz", "notbaz"}],
           ["mock_command", "-foo", "bar"]),

    %% shuffled and interleaved arguments and options
    ok = check_get_options(
           {["a1", "a2", "a3"], [{"-o1", "hello"}, {"-o2", "noto2"}, {"-f", true}]},
           [{option, "-o1", "noto1"}, {flag, "-f"}, {option, "-o2", "noto2"}],
           ["-f", "a1", "-o1", "hello", "a2", "a3"]),

    passed.

test_cluster_management() ->

    %% 'cluster' and 'reset' should only work if the app is stopped
    {error, _} = control_action(cluster, []),
    {error, _} = control_action(reset, []),
    {error, _} = control_action(force_reset, []),

    ok = control_action(stop_app, []),

    %% various ways of creating a standalone node
    NodeS = atom_to_list(node()),
    ClusteringSequence = [[],
                          [NodeS],
                          ["invalid@invalid", NodeS],
                          [NodeS, "invalid@invalid"]],

    ok = control_action(reset, []),
    lists:foreach(fun (Arg) ->
                          ok = control_action(force_cluster, Arg),
                          ok
                  end,
                  ClusteringSequence),
    lists:foreach(fun (Arg) ->
                          ok = control_action(reset, []),
                          ok = control_action(force_cluster, Arg),
                          ok
                  end,
                  ClusteringSequence),
    ok = control_action(reset, []),
    lists:foreach(fun (Arg) ->
                          ok = control_action(force_cluster, Arg),
                          ok = control_action(start_app, []),
                          ok = control_action(stop_app, []),
                          ok
                  end,
                  ClusteringSequence),
    lists:foreach(fun (Arg) ->
                          ok = control_action(reset, []),
                          ok = control_action(force_cluster, Arg),
                          ok = control_action(start_app, []),
                          ok = control_action(stop_app, []),
                          ok
                  end,
                  ClusteringSequence),

    %% convert a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),

    %% join a non-existing cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),

    SecondaryNode = rabbit_misc:makenode("hare"),
    case net_adm:ping(SecondaryNode) of
        pong -> passed = test_cluster_management2(SecondaryNode);
        pang -> io:format("Skipping clustering tests with node ~p~n",
                          [SecondaryNode])
    end,

    ok = control_action(start_app, []),
    passed.

test_cluster_management2(SecondaryNode) ->
    NodeS = atom_to_list(node()),
    SecondaryNodeS = atom_to_list(SecondaryNode),

    %% make a disk node
    ok = control_action(reset, []),
    ok = control_action(cluster, [NodeS]),
    %% make a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),

    %% join cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(force_cluster, [SecondaryNodeS, "invalid1@invalid"]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% change cluster config while remaining in same cluster
    ok = control_action(force_cluster, ["invalid2@invalid", SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% join non-existing cluster as a ram node
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% join empty cluster as a ram node
    ok = control_action(cluster, []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% turn ram node into disk node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% convert a disk node into a ram node
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),

    %% turn a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% NB: this will log an inconsistent_database error, which is harmless
    %% Turning cover on / off is OK even if we're not in general using cover,
    %% it just turns the engine on / off, doesn't actually log anything.
    cover:stop([SecondaryNode]),
    true = disconnect_node(SecondaryNode),
    pong = net_adm:ping(SecondaryNode),
    cover:start([SecondaryNode]),

    %% leaving a cluster as a ram node
    ok = control_action(reset, []),
    %% ...and as a disk node
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(reset, []),

    %% attempt to leave cluster when no other node is alive
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, SecondaryNode, [], []),
    ok = control_action(stop_app, []),
    {error, {no_running_cluster_nodes, _, _}} =
        control_action(reset, []),

    %% leave system clustered, with the secondary node as a ram node
    ok = control_action(force_reset, []),
    ok = control_action(start_app, []),
    ok = control_action(force_reset, SecondaryNode, [], []),
    ok = control_action(cluster, SecondaryNode, [NodeS], []),
    ok = control_action(start_app, SecondaryNode, [], []),

    passed.

test_user_management() ->

    %% lots if stuff that should fail
    {error, {no_such_user, _}} =
        control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(change_password, ["foo", "baz"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_user, _}} =
        control_action(set_permissions, ["foo", ".*", ".*", ".*"]),
    {error, {no_such_user, _}} =
        control_action(clear_permissions, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(list_user_permissions, ["foo"]),
    {error, {no_such_vhost, _}} =
        control_action(list_permissions, [], [{"-p", "/testhost"}]),
    {error, {invalid_regexp, _, _}} =
        control_action(set_permissions, ["guest", "+foo", ".*", ".*"]),

    %% user creation
    ok = control_action(add_user, ["foo", "bar"]),
    {error, {user_already_exists, _}} =
        control_action(add_user, ["foo", "bar"]),
    ok = control_action(change_password, ["foo", "baz"]),
    ok = control_action(set_admin, ["foo"]),
    ok = control_action(clear_admin, ["foo"]),
    ok = control_action(list_users, []),

    %% vhost creation
    ok = control_action(add_vhost, ["/testhost"]),
    {error, {vhost_already_exists, _}} =
        control_action(add_vhost, ["/testhost"]),
    ok = control_action(list_vhosts, []),

    %% user/vhost mapping
    ok = control_action(set_permissions, ["foo", ".*", ".*", ".*"],
                        [{"-p", "/testhost"}]),
    ok = control_action(set_permissions, ["foo", ".*", ".*", ".*"],
                        [{"-p", "/testhost"}]),
    ok = control_action(set_permissions, ["foo", ".*", ".*", ".*"],
                        [{"-p", "/testhost"}]),
    ok = control_action(list_permissions, [], [{"-p", "/testhost"}]),
    ok = control_action(list_permissions, [], [{"-p", "/testhost"}]),
    ok = control_action(list_user_permissions, ["foo"]),

    %% user/vhost unmapping
    ok = control_action(clear_permissions, ["foo"], [{"-p", "/testhost"}]),
    ok = control_action(clear_permissions, ["foo"], [{"-p", "/testhost"}]),

    %% vhost deletion
    ok = control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/testhost"]),

    %% deleting a populated vhost
    ok = control_action(add_vhost, ["/testhost"]),
    ok = control_action(set_permissions, ["foo", ".*", ".*", ".*"],
                        [{"-p", "/testhost"}]),
    ok = control_action(delete_vhost, ["/testhost"]),

    %% user deletion
    ok = control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(delete_user, ["foo"]),

    passed.

test_server_status() ->
    %% create a few things so there is some useful information to list
    Writer = spawn(fun () -> receive shutdown -> ok end end),
    {ok, Ch} = rabbit_channel:start_link(
                 1, self(), Writer, self(), rabbit_framing_amqp_0_9_1,
                 user(<<"user">>), <<"/">>, [], self(),
                 fun (_) -> {ok, self()} end),
    [Q, Q2] = [Queue || Name <- [<<"foo">>, <<"bar">>],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], none)]],

    ok = rabbit_amqqueue:basic_consume(Q, true, Ch, undefined,
                                       <<"ctag">>, true, undefined),

    %% list queues
    ok = info_action(list_queues, rabbit_amqqueue:info_keys(), true),

    %% list exchanges
    ok = info_action(list_exchanges, rabbit_exchange:info_keys(), true),

    %% list bindings
    ok = info_action(list_bindings, rabbit_binding:info_keys(), true),
    %% misc binding listing APIs
    [_|_] = rabbit_binding:list_for_source(
              rabbit_misc:r(<<"/">>, exchange, <<"">>)),
    [_] = rabbit_binding:list_for_destination(
            rabbit_misc:r(<<"/">>, queue, <<"foo">>)),
    [_] = rabbit_binding:list_for_source_and_destination(
            rabbit_misc:r(<<"/">>, exchange, <<"">>),
            rabbit_misc:r(<<"/">>, queue, <<"foo">>)),

    %% list connections
    [#listener{host = H, port = P} | _] =
        [L || L = #listener{node = N} <- rabbit_networking:active_listeners(),
              N =:= node()],

    {ok, _C} = gen_tcp:connect(H, P, []),
    timer:sleep(100),
    ok = info_action(list_connections,
                     rabbit_networking:connection_info_keys(), false),
    %% close_connection
    [ConnPid] = rabbit_networking:connections(),
    ok = control_action(close_connection, [rabbit_misc:pid_to_string(ConnPid),
                                           "go away"]),

    %% list channels
    ok = info_action(list_channels, rabbit_channel:info_keys(), false),

    %% list consumers
    ok = control_action(list_consumers, []),

    %% cleanup
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),

    passed.

test_writer(Pid) ->
    receive
        shutdown               -> ok;
        {send_command, Method} -> Pid ! Method, test_writer(Pid)
    end.

test_spawn() ->
    Me = self(),
    Writer = spawn(fun () -> test_writer(Me) end),
    {ok, Ch} = rabbit_channel:start_link(
                 1, Me, Writer, Me, rabbit_framing_amqp_0_9_1,
                 user(<<"guest">>), <<"/">>, [], self(),
                 fun (_) -> {ok, self()} end),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after 1000 -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.

user(Username) ->
    #user{username     = Username,
          is_admin     = true,
          auth_backend = rabbit_auth_backend_internal,
          impl         = #internal_user{username = Username,
                                        is_admin = true}}.

test_statistics_event_receiver(Pid) ->
    receive
        Foo -> Pid ! Foo, test_statistics_event_receiver(Pid)
    end.

test_statistics_receive_event(Ch, Matcher) ->
    rabbit_channel:flush(Ch),
    rabbit_channel:emit_stats(Ch),
    test_statistics_receive_event1(Ch, Matcher).

test_statistics_receive_event1(Ch, Matcher) ->
    receive #event{type = channel_stats, props = Props} ->
            case Matcher(Props) of
                true -> Props;
                _    -> test_statistics_receive_event1(Ch, Matcher)
            end
    after 1000 -> throw(failed_to_receive_event)
    end.

test_confirms() ->
    {_Writer, Ch} = test_spawn(),
    DeclareBindDurableQueue =
        fun() ->
                rabbit_channel:do(Ch, #'queue.declare'{durable = true}),
                receive #'queue.declare_ok'{queue = Q0} ->
                        rabbit_channel:do(Ch, #'queue.bind'{
                                            queue = Q0,
                                            exchange = <<"amq.direct">>,
                                            routing_key = "magic" }),
                        receive #'queue.bind_ok'{} ->
                                Q0
                        after 1000 ->
                                throw(failed_to_bind_queue)
                        end
                after 1000 ->
                        throw(failed_to_declare_queue)
                end
        end,
    %% Declare and bind two queues
    QName1 = DeclareBindDurableQueue(),
    QName2 = DeclareBindDurableQueue(),
    %% Get the first one's pid (we'll crash it later)
    {ok, Q1} = rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>, queue, QName1)),
    QPid1 = Q1#amqqueue.pid,
    %% Enable confirms
    rabbit_channel:do(Ch, #'confirm.select'{}),
    receive
        #'confirm.select_ok'{} -> ok
    after 1000 -> throw(failed_to_enable_confirms)
    end,
    %% Publish a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"amq.direct">>,
                                           routing_key = "magic"
                                          },
                      rabbit_basic:build_content(
                        #'P_basic'{delivery_mode = 2}, <<"">>)),
    %% Crash the queue
    QPid1 ! boom,
    %% Wait for a nack
    receive
        #'basic.nack'{} -> ok;
        #'basic.ack'{}  -> throw(received_ack_instead_of_nack)
    after 2000 -> throw(did_not_receive_nack)
    end,
    receive
        #'basic.ack'{} -> throw(received_ack_when_none_expected)
    after 1000 -> ok
    end,
    %% Cleanup
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName2}),
    receive
        #'queue.delete_ok'{} -> ok
    after 1000 -> throw(failed_to_cleanup_queue)
    end,
    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),

    passed.

test_statistics() ->
    application:set_env(rabbit, collect_statistics, fine),

    %% ATM this just tests the queue / exchange stats in channels. That's
    %% by far the most complex code though.

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} ->
                    Q0
            after 1000 -> throw(failed_to_receive_queue_declare_ok)
            end,
    {ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>, queue, QName)),
    QPid = Q#amqqueue.pid,
    X = rabbit_misc:r(<<"/">>, exchange, <<"">>),

    rabbit_tests_event_receiver:start(self()),

    %% Check stats empty
    Event = test_statistics_receive_event(Ch, fun (_) -> true end),
    [] = proplists:get_value(channel_queue_stats, Event),
    [] = proplists:get_value(channel_exchange_stats, Event),
    [] = proplists:get_value(channel_queue_exchange_stats, Event),

    %% Publish and get a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.get'{queue = QName}),

    %% Check the stats reflect that
    Event2 = test_statistics_receive_event(
               Ch,
               fun (E) ->
                       length(proplists:get_value(
                                channel_queue_exchange_stats, E)) > 0
               end),
    [{QPid,[{get,1}]}] = proplists:get_value(channel_queue_stats, Event2),
    [{X,[{publish,1}]}] = proplists:get_value(channel_exchange_stats, Event2),
    [{{QPid,X},[{publish,1}]}] =
        proplists:get_value(channel_queue_exchange_stats, Event2),

    %% Check the stats remove stuff on queue deletion
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    Event3 = test_statistics_receive_event(
               Ch,
               fun (E) ->
                       length(proplists:get_value(
                                channel_queue_exchange_stats, E)) == 0
               end),

    [] = proplists:get_value(channel_queue_stats, Event3),
    [{X,[{publish,1}]}] = proplists:get_value(channel_exchange_stats, Event3),
    [] = proplists:get_value(channel_queue_exchange_stats, Event3),

    rabbit_channel:shutdown(Ch),
    rabbit_tests_event_receiver:stop(),
    passed.

test_delegates_async(SecondaryNode) ->
    Self = self(),
    Sender = fun (Pid) -> Pid ! {invoked, Self} end,

    Responder = make_responder(fun ({invoked, Pid}) -> Pid ! response end),

    ok = delegate:invoke_no_result(spawn(Responder), Sender),
    ok = delegate:invoke_no_result(spawn(SecondaryNode, Responder), Sender),
    await_response(2),

    LocalPids = spawn_responders(node(), Responder, 10),
    RemotePids = spawn_responders(SecondaryNode, Responder, 10),
    ok = delegate:invoke_no_result(LocalPids ++ RemotePids, Sender),
    await_response(20),

    passed.

make_responder(FMsg) -> make_responder(FMsg, timeout).
make_responder(FMsg, Throw) ->
    fun () ->
            receive Msg -> FMsg(Msg)
            after 1000 -> throw(Throw)
            end
    end.

spawn_responders(Node, Responder, Count) ->
    [spawn(Node, Responder) || _ <- lists:seq(1, Count)].

await_response(0) ->
    ok;
await_response(Count) ->
    receive
        response -> ok,
                    await_response(Count - 1)
    after 1000 ->
            io:format("Async reply not received~n"),
            throw(timeout)
    end.

must_exit(Fun) ->
    try
        Fun(),
        throw(exit_not_thrown)
    catch
        exit:_ -> ok
    end.

test_delegates_sync(SecondaryNode) ->
    Sender = fun (Pid) -> gen_server:call(Pid, invoked, infinity) end,
    BadSender = fun (_Pid) -> exit(exception) end,

    Responder = make_responder(fun ({'$gen_call', From, invoked}) ->
                                       gen_server:reply(From, response)
                               end),

    BadResponder = make_responder(fun ({'$gen_call', From, invoked}) ->
                                          gen_server:reply(From, response)
                                  end, bad_responder_died),

    response = delegate:invoke(spawn(Responder), Sender),
    response = delegate:invoke(spawn(SecondaryNode, Responder), Sender),

    must_exit(fun () -> delegate:invoke(spawn(BadResponder), BadSender) end),
    must_exit(fun () ->
                      delegate:invoke(spawn(SecondaryNode, BadResponder), BadSender) end),

    LocalGoodPids = spawn_responders(node(), Responder, 2),
    RemoteGoodPids = spawn_responders(SecondaryNode, Responder, 2),
    LocalBadPids = spawn_responders(node(), BadResponder, 2),
    RemoteBadPids = spawn_responders(SecondaryNode, BadResponder, 2),

    {GoodRes, []} = delegate:invoke(LocalGoodPids ++ RemoteGoodPids, Sender),
    true = lists:all(fun ({_, response}) -> true end, GoodRes),
    GoodResPids = [Pid || {Pid, _} <- GoodRes],

    Good = lists:usort(LocalGoodPids ++ RemoteGoodPids),
    Good = lists:usort(GoodResPids),

    {[], BadRes} = delegate:invoke(LocalBadPids ++ RemoteBadPids, BadSender),
    true = lists:all(fun ({_, {exit, exception, _}}) -> true end, BadRes),
    BadResPids = [Pid || {Pid, _} <- BadRes],

    Bad = lists:usort(LocalBadPids ++ RemoteBadPids),
    Bad = lists:usort(BadResPids),

    MagicalPids = [rabbit_misc:string_to_pid(Str) ||
                      Str <- ["<nonode@nohost.0.1.0>", "<nonode@nohost.0.2.0>"]],
    {[], BadNodes} = delegate:invoke(MagicalPids, Sender),
    true = lists:all(
             fun ({_, {exit, {nodedown, nonode@nohost}, _Stack}}) -> true end,
             BadNodes),
    BadNodesPids = [Pid || {Pid, _} <- BadNodes],

    Magical = lists:usort(MagicalPids),
    Magical = lists:usort(BadNodesPids),

    passed.

test_queue_cleanup(_SecondaryNode) ->
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{ queue = ?CLEANUP_QUEUE_NAME }),
    receive #'queue.declare_ok'{queue = ?CLEANUP_QUEUE_NAME} ->
            ok
    after 1000 -> throw(failed_to_receive_queue_declare_ok)
    end,
    rabbit:stop(),
    rabbit:start(),
    rabbit_channel:do(Ch, #'queue.declare'{ passive = true,
                                            queue   = ?CLEANUP_QUEUE_NAME }),
    receive
        #'channel.close'{reply_code = ?NOT_FOUND} ->
            ok
    after 2000 ->
            throw(failed_to_receive_channel_exit)
    end,
    passed.

test_declare_on_dead_queue(SecondaryNode) ->
    QueueName = rabbit_misc:r(<<"/">>, queue, ?CLEANUP_QUEUE_NAME),
    Self = self(),
    Pid = spawn(SecondaryNode,
                fun () ->
                        {new, #amqqueue{name = QueueName, pid = QPid}} =
                            rabbit_amqqueue:declare(QueueName, false, false, [],
                                                    none),
                        exit(QPid, kill),
                        Self ! {self(), killed, QPid}
                end),
    receive
        {Pid, killed, QPid} ->
            {existing, #amqqueue{name = QueueName,
                                 pid = QPid}} =
                rabbit_amqqueue:declare(QueueName, false, false, [], none),
            false = rabbit_misc:is_process_alive(QPid),
            {new, Q} = rabbit_amqqueue:declare(QueueName, false, false, [],
                                               none),
            true = rabbit_misc:is_process_alive(Q#amqqueue.pid),
            {ok, 0} = rabbit_amqqueue:delete(Q, false, false),
            passed
    after 2000 ->
            throw(failed_to_create_and_kill_queue)
    end.

%%---------------------------------------------------------------------

control_action(Command, Args) ->
    control_action(Command, node(), Args, default_options()).

control_action(Command, Args, NewOpts) ->
    control_action(Command, node(), Args,
                   expand_options(default_options(), NewOpts)).

control_action(Command, Node, Args, Opts) ->
    case catch rabbit_control:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
        ok ->
            io:format("done.~n"),
            ok;
        Other ->
            io:format("failed.~n"),
            Other
    end.

info_action(Command, Args, CheckVHost) ->
    ok = control_action(Command, []),
    if CheckVHost -> ok = control_action(Command, []);
       true       -> ok
    end,
    ok = control_action(Command, lists:map(fun atom_to_list/1, Args)),
    {bad_argument, dummy} = control_action(Command, ["dummy"]),
    ok.

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).

check_get_options({ExpArgs, ExpOpts}, Defs, Args) ->
    {ExpArgs, ResOpts} = rabbit_misc:get_options(Defs, Args),
    true = lists:sort(ExpOpts) == lists:sort(ResOpts), % don't care about the order
    ok.

empty_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo} -> FInfo#file_info.size == 0;
         Error       -> Error
     end || File <- Files].

non_empty_files(Files) ->
    [case EmptyFile of
         {error, Reason} -> {error, Reason};
         _               -> not(EmptyFile)
     end || EmptyFile <- empty_files(Files)].

test_logs_working(MainLogFile, SaslLogFile) ->
    ok = rabbit_log:error("foo bar"),
    ok = error_logger:error_report(crash_report, [foo, bar]),
    %% give the error loggers some time to catch up
    timer:sleep(100),
    [true, true] = non_empty_files([MainLogFile, SaslLogFile]),
    ok.

set_permissions(Path, Mode) ->
    case file:read_file_info(Path) of
        {ok, FInfo} -> file:write_file_info(
                         Path,
                         FInfo#file_info{mode=Mode});
        Error       -> Error
    end.

clean_logs(Files, Suffix) ->
    [begin
         ok = delete_file(File),
         ok = delete_file([File, Suffix])
     end || File <- Files],
    ok.

delete_file(File) ->
    case file:delete(File) of
        ok              -> ok;
        {error, enoent} -> ok;
        Error           -> Error
    end.

make_files_non_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=0}) ||
        File <- Files],
    ok.

add_log_handlers(Handlers) ->
    [ok = error_logger:add_report_handler(Handler, Args) ||
        {Handler, Args} <- Handlers],
    ok.

delete_log_handlers(Handlers) ->
    [[] = error_logger:delete_report_handler(Handler) ||
        Handler <- Handlers],
    ok.

test_supervisor_delayed_restart() ->
    test_sup:test_supervisor_delayed_restart().

test_file_handle_cache() ->
    %% test copying when there is just one spare handle
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5), %% 1 or 2 sockets, 2 msg_stores
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    ok = filelib:ensure_dir(filename:join(TmpDir, "nothing")),
    [Src1, Dst1, Src2, Dst2] = Files =
        [filename:join(TmpDir, Str) || Str <- ["file1", "file2", "file3", "file4"]],
    Content = <<"foo">>,
    CopyFun = fun (Src, Dst) ->
                      ok = rabbit_misc:write_file(Src, Content),
                      {ok, SrcHdl} = file_handle_cache:open(Src, [read], []),
                      {ok, DstHdl} = file_handle_cache:open(Dst, [write], []),
                      Size = size(Content),
                      {ok, Size} = file_handle_cache:copy(SrcHdl, DstHdl, Size),
                      ok = file_handle_cache:delete(SrcHdl),
                      ok = file_handle_cache:delete(DstHdl)
              end,
    Pid = spawn(fun () -> {ok, Hdl} = file_handle_cache:open(
                                        filename:join(TmpDir, "file5"),
                                        [write], []),
                          receive {next, Pid1} -> Pid1 ! {next, self()} end,
                          file_handle_cache:delete(Hdl),
                          %% This will block and never return, so we
                          %% exercise the fhc tidying up the pending
                          %% queue on the death of a process.
                          ok = CopyFun(Src1, Dst1)
                end),
    ok = CopyFun(Src1, Dst1),
    ok = file_handle_cache:set_limit(2),
    Pid ! {next, self()},
    receive {next, Pid} -> ok end,
    timer:sleep(100),
    Pid1 = spawn(fun () -> CopyFun(Src2, Dst2) end),
    timer:sleep(100),
    erlang:monitor(process, Pid),
    erlang:monitor(process, Pid1),
    exit(Pid, kill),
    exit(Pid1, kill),
    receive {'DOWN', _MRef, process, Pid, _Reason} -> ok end,
    receive {'DOWN', _MRef1, process, Pid1, _Reason1} -> ok end,
    [file:delete(File) || File <- Files],
    ok = file_handle_cache:set_limit(Limit),
    passed.

test_backing_queue() ->
    case application:get_env(rabbit, backing_queue_module) of
        {ok, rabbit_variable_queue} ->
            {ok, FileSizeLimit} =
                application:get_env(rabbit, msg_store_file_size_limit),
            application:set_env(rabbit, msg_store_file_size_limit, 512,
                                infinity),
            {ok, MaxJournal} =
                application:get_env(rabbit, queue_index_max_journal_entries),
            application:set_env(rabbit, queue_index_max_journal_entries, 128,
                                infinity),
            passed = test_msg_store(),
            application:set_env(rabbit, msg_store_file_size_limit,
                                FileSizeLimit, infinity),
            passed = test_queue_index(),
            passed = test_queue_index_props(),
            passed = test_variable_queue(),
            passed = test_variable_queue_delete_msg_store_files_callback(),
            passed = test_queue_recover(),
            application:set_env(rabbit, queue_index_max_journal_entries,
                                MaxJournal, infinity),
            passed;
        _ ->
            passed
    end.

restart_msg_store_empty() ->
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
           undefined, {fun (ok) -> finished end, ok}).

msg_id_bin(X) ->
    erlang:md5(term_to_binary(X)).

msg_store_client_init(MsgStore, Ref) ->
    rabbit_msg_store:client_init(MsgStore, Ref, undefined, undefined).

msg_store_contains(Atom, MsgIds, MSCState) ->
    Atom = lists:foldl(
             fun (MsgId, Atom1) when Atom1 =:= Atom ->
                     rabbit_msg_store:contains(MsgId, MSCState) end,
             Atom, MsgIds).

msg_store_sync(MsgIds, MSCState) ->
    Ref = make_ref(),
    Self = self(),
    ok = rabbit_msg_store:sync(MsgIds, fun () -> Self ! {sync, Ref} end,
                               MSCState),
    receive
        {sync, Ref} -> ok
    after
        10000 ->
            io:format("Sync from msg_store missing for msg_ids ~p~n", [MsgIds]),
            throw(timeout)
    end.

msg_store_read(MsgIds, MSCState) ->
    lists:foldl(fun (MsgId, MSCStateM) ->
                        {{ok, MsgId}, MSCStateN} = rabbit_msg_store:read(
                                                     MsgId, MSCStateM),
                        MSCStateN
                end, MSCState, MsgIds).

msg_store_write(MsgIds, MSCState) ->
    ok = lists:foldl(fun (MsgId, ok) ->
                             rabbit_msg_store:write(MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_remove(MsgIds, MSCState) ->
    rabbit_msg_store:remove(MsgIds, MSCState).

msg_store_remove(MsgStore, Ref, MsgIds) ->
    with_msg_store_client(MsgStore, Ref,
                          fun (MSCStateM) ->
                                  ok = msg_store_remove(MsgIds, MSCStateM),
                                  MSCStateM
                          end).

with_msg_store_client(MsgStore, Ref, Fun) ->
    rabbit_msg_store:client_terminate(
      Fun(msg_store_client_init(MsgStore, Ref))).

foreach_with_msg_store_client(MsgStore, Ref, Fun, L) ->
    rabbit_msg_store:client_terminate(
      lists:foldl(fun (MsgId, MSCState) -> Fun(MsgId, MSCState) end,
                  msg_store_client_init(MsgStore, Ref), L)).

test_msg_store() ->
    restart_msg_store_empty(),
    Self = self(),
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1,100)],
    {MsgIds1stHalf, MsgIds2ndHalf} = lists:split(50, MsgIds),
    Ref = rabbit_guid:guid(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% publish the first half
    ok = msg_store_write(MsgIds1stHalf, MSCState),
    %% sync on the first half
    ok = msg_store_sync(MsgIds1stHalf, MSCState),
    %% publish the second half
    ok = msg_store_write(MsgIds2ndHalf, MSCState),
    %% sync on the first half again - the msg_store will be dirty, but
    %% we won't need the fsync
    ok = msg_store_sync(MsgIds1stHalf, MSCState),
    %% check they're all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% publish the latter half twice so we hit the caching and ref count code
    ok = msg_store_write(MsgIds2ndHalf, MSCState),
    %% check they're still all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% sync on the 2nd half, but do lots of individual syncs to try
    %% and cause coalescing to happen
    ok = lists:foldl(
           fun (MsgId, ok) -> rabbit_msg_store:sync(
                                [MsgId], fun () -> Self ! {sync, MsgId} end,
                                MSCState)
           end, ok, MsgIds2ndHalf),
    lists:foldl(
      fun(MsgId, ok) ->
              receive
                  {sync, MsgId} -> ok
              after
                  10000 ->
                      io:format("Sync from msg_store missing (msg_id: ~p)~n",
                                [MsgId]),
                      throw(timeout)
              end
      end, ok, MsgIds2ndHalf),
    %% it's very likely we're not dirty here, so the 1st half sync
    %% should hit a different code path
    ok = msg_store_sync(MsgIds1stHalf, MSCState),
    %% read them all
    MSCState1 = msg_store_read(MsgIds, MSCState),
    %% read them all again - this will hit the cache, not disk
    MSCState2 = msg_store_read(MsgIds, MSCState1),
    %% remove them all
    ok = rabbit_msg_store:remove(MsgIds, MSCState2),
    %% check first half doesn't exist
    false = msg_store_contains(false, MsgIds1stHalf, MSCState2),
    %% check second half does exist
    true = msg_store_contains(true, MsgIds2ndHalf, MSCState2),
    %% read the second half again
    MSCState3 = msg_store_read(MsgIds2ndHalf, MSCState2),
    %% read the second half again, just for fun (aka code coverage)
    MSCState4 = msg_store_read(MsgIds2ndHalf, MSCState3),
    ok = rabbit_msg_store:client_terminate(MSCState4),
    %% stop and restart, preserving every other msg in 2nd half
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
           [], {fun ([]) -> finished;
                    ([MsgId|MsgIdsTail])
                      when length(MsgIdsTail) rem 2 == 0 ->
                        {MsgId, 1, MsgIdsTail};
                    ([MsgId|MsgIdsTail]) ->
                        {MsgId, 0, MsgIdsTail}
                end, MsgIds2ndHalf}),
    MSCState5 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we have the right msgs left
    lists:foldl(
      fun (MsgId, Bool) ->
              not(Bool = rabbit_msg_store:contains(MsgId, MSCState5))
      end, false, MsgIds2ndHalf),
    ok = rabbit_msg_store:client_terminate(MSCState5),
    %% restart empty
    restart_msg_store_empty(),
    MSCState6 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we don't contain any of the msgs
    false = msg_store_contains(false, MsgIds, MSCState6),
    %% publish the first half again
    ok = msg_store_write(MsgIds1stHalf, MSCState6),
    %% this should force some sort of sync internally otherwise misread
    ok = rabbit_msg_store:client_terminate(
           msg_store_read(MsgIds1stHalf, MSCState6)),
    MSCState7 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = rabbit_msg_store:remove(MsgIds1stHalf, MSCState7),
    ok = rabbit_msg_store:client_terminate(MSCState7),
    %% restart empty
    restart_msg_store_empty(), %% now safe to reuse msg_ids
    %% push a lot of msgs in... at least 100 files worth
    {ok, FileSize} = application:get_env(rabbit, msg_store_file_size_limit),
    PayloadSizeBits = 65536,
    BigCount = trunc(100 * FileSize / (PayloadSizeBits div 8)),
    MsgIdsBig = [msg_id_bin(X) || X <- lists:seq(1, BigCount)],
    Payload = << 0:PayloadSizeBits >>,
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   [ok = rabbit_msg_store:write(MsgId, Payload, MSCStateM) ||
                       MsgId <- MsgIdsBig],
                   MSCStateM
           end),
    %% now read them to ensure we hit the fast client-side reading
    ok = foreach_with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MsgId, MSCStateM) ->
                   {{ok, Payload}, MSCStateN} = rabbit_msg_store:read(
                                                  MsgId, MSCStateM),
                   MSCStateN
           end, MsgIdsBig),
    %% .., then 3s by 1...
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount, 1, -3)]),
    %% .., then remove 3s by 2, from the young end first. This hits
    %% GC (under 50% good data left, but no empty files. Must GC).
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount-1, 1, -3)]),
    %% .., then remove 3s by 3, from the young end first. This hits
    %% GC...
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount-2, 1, -3)]),
    %% ensure empty
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   false = msg_store_contains(false, MsgIdsBig, MSCStateM),
                   MSCStateM
           end),
    %% restart empty
    restart_msg_store_empty(),
    passed.

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).

test_queue() ->
    queue_name(<<"test">>).

init_test_queue() ->
    TestQueue = test_queue(),
    Terms = rabbit_queue_index:shutdown_terms(TestQueue),
    PRef = proplists:get_value(persistent_ref, Terms, rabbit_guid:guid()),
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef),
    Res = rabbit_queue_index:recover(
            TestQueue, Terms, false,
            fun (MsgId) ->
                    rabbit_msg_store:contains(MsgId, PersistentClient)
            end,
            fun nop/1),
    ok = rabbit_msg_store:client_delete_and_terminate(PersistentClient),
    Res.

restart_test_queue(Qi) ->
    _ = rabbit_queue_index:terminate([], Qi),
    ok = rabbit_variable_queue:stop(),
    ok = rabbit_variable_queue:start([test_queue()]),
    init_test_queue().

empty_test_queue() ->
    ok = rabbit_variable_queue:stop(),
    ok = rabbit_variable_queue:start([]),
    {0, Qi} = init_test_queue(),
    _ = rabbit_queue_index:delete_and_terminate(Qi),
    ok.

with_empty_test_queue(Fun) ->
    ok = empty_test_queue(),
    {0, Qi} = init_test_queue(),
    rabbit_queue_index:delete_and_terminate(Fun(Qi)).

queue_index_publish(SeqIds, Persistent, Qi) ->
    Ref = rabbit_guid:guid(),
    MsgStore = case Persistent of
                   true  -> ?PERSISTENT_MSG_STORE;
                   false -> ?TRANSIENT_MSG_STORE
               end,
    MSCState = msg_store_client_init(MsgStore, Ref),
    {A, B = [{_SeqId, LastMsgIdWritten} | _]} =
        lists:foldl(
          fun (SeqId, {QiN, SeqIdsMsgIdsAcc}) ->
                  MsgId = rabbit_guid:guid(),
                  QiM = rabbit_queue_index:publish(
                          MsgId, SeqId, #message_properties{}, Persistent, QiN),
                  ok = rabbit_msg_store:write(MsgId, MsgId, MSCState),
                  {QiM, [{SeqId, MsgId} | SeqIdsMsgIdsAcc]}
          end, {Qi, []}, SeqIds),
    %% do this just to force all of the publishes through to the msg_store:
    true = rabbit_msg_store:contains(LastMsgIdWritten, MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    {A, B}.

verify_read_with_published(_Delivered, _Persistent, [], _) ->
    ok;
verify_read_with_published(Delivered, Persistent,
                           [{MsgId, SeqId, _Props, Persistent, Delivered}|Read],
                           [{SeqId, MsgId}|Published]) ->
    verify_read_with_published(Delivered, Persistent, Read, Published);
verify_read_with_published(_Delivered, _Persistent, _Read, _Published) ->
    ko.

test_queue_index_props() ->
    with_empty_test_queue(
      fun(Qi0) ->
              MsgId = rabbit_guid:guid(),
              Props = #message_properties{expiry=12345},
              Qi1 = rabbit_queue_index:publish(MsgId, 1, Props, true, Qi0),
              {[{MsgId, 1, Props, _, _}], Qi2} =
                  rabbit_queue_index:read(1, 2, Qi1),
              Qi2
      end),

    ok = rabbit_variable_queue:stop(),
    ok = rabbit_variable_queue:start([]),

    passed.

test_queue_index() ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    TwoSegs = SegmentSize + SegmentSize,
    MostOfASegment = trunc(SegmentSize*0.75),
    SeqIdsA = lists:seq(0, MostOfASegment-1),
    SeqIdsB = lists:seq(MostOfASegment, 2*MostOfASegment),
    SeqIdsC = lists:seq(0, trunc(SegmentSize/2)),
    SeqIdsD = lists:seq(0, SegmentSize*4),

    with_empty_test_queue(
      fun (Qi0) ->
              {0, 0, Qi1} = rabbit_queue_index:bounds(Qi0),
              {Qi2, SeqIdsMsgIdsA} = queue_index_publish(SeqIdsA, false, Qi1),
              {0, SegmentSize, Qi3} = rabbit_queue_index:bounds(Qi2),
              {ReadA, Qi4} = rabbit_queue_index:read(0, SegmentSize, Qi3),
              ok = verify_read_with_published(false, false, ReadA,
                                              lists:reverse(SeqIdsMsgIdsA)),
              %% should get length back as 0, as all the msgs were transient
              {0, Qi6} = restart_test_queue(Qi4),
              {0, 0, Qi7} = rabbit_queue_index:bounds(Qi6),
              {Qi8, SeqIdsMsgIdsB} = queue_index_publish(SeqIdsB, true, Qi7),
              {0, TwoSegs, Qi9} = rabbit_queue_index:bounds(Qi8),
              {ReadB, Qi10} = rabbit_queue_index:read(0, SegmentSize, Qi9),
              ok = verify_read_with_published(false, true, ReadB,
                                              lists:reverse(SeqIdsMsgIdsB)),
              %% should get length back as MostOfASegment
              LenB = length(SeqIdsB),
              {LenB, Qi12} = restart_test_queue(Qi10),
              {0, TwoSegs, Qi13} = rabbit_queue_index:bounds(Qi12),
              Qi14 = rabbit_queue_index:deliver(SeqIdsB, Qi13),
              {ReadC, Qi15} = rabbit_queue_index:read(0, SegmentSize, Qi14),
              ok = verify_read_with_published(true, true, ReadC,
                                              lists:reverse(SeqIdsMsgIdsB)),
              Qi16 = rabbit_queue_index:ack(SeqIdsB, Qi15),
              Qi17 = rabbit_queue_index:flush(Qi16),
              %% Everything will have gone now because #pubs == #acks
              {0, 0, Qi18} = rabbit_queue_index:bounds(Qi17),
              %% should get length back as 0 because all persistent
              %% msgs have been acked
              {0, Qi19} = restart_test_queue(Qi18),
              Qi19
      end),

    %% These next bits are just to hit the auto deletion of segment files.
    %% First, partials:
    %% a) partial pub+del+ack, then move to new segment
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsC} = queue_index_publish(SeqIdsC,
                                                         false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsC, Qi1),
              Qi3 = rabbit_queue_index:ack(SeqIdsC, Qi2),
              Qi4 = rabbit_queue_index:flush(Qi3),
              {Qi5, _SeqIdsMsgIdsC1} = queue_index_publish([SegmentSize],
                                                          false, Qi4),
              Qi5
      end),

    %% b) partial pub+del, then move to new segment, then ack all in old segment
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsC2} = queue_index_publish(SeqIdsC,
                                                          false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsC, Qi1),
              {Qi3, _SeqIdsMsgIdsC3} = queue_index_publish([SegmentSize],
                                                          false, Qi2),
              Qi4 = rabbit_queue_index:ack(SeqIdsC, Qi3),
              rabbit_queue_index:flush(Qi4)
      end),

    %% c) just fill up several segments of all pubs, then +dels, then +acks
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsD} = queue_index_publish(SeqIdsD,
                                                         false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsD, Qi1),
              Qi3 = rabbit_queue_index:ack(SeqIdsD, Qi2),
              rabbit_queue_index:flush(Qi3)
      end),

    %% d) get messages in all states to a segment, then flush, then do
    %% the same again, don't flush and read. This will hit all
    %% possibilities in combining the segment with the journal.
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, [Seven,Five,Four|_]} = queue_index_publish([0,1,2,4,5,7],
                                                               false, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              Qi4 = rabbit_queue_index:flush(Qi3),
              {Qi5, [Eight,Six|_]} = queue_index_publish([3,6,8], false, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {[], Qi8} = rabbit_queue_index:read(0, 4, Qi7),
              {ReadD, Qi9} = rabbit_queue_index:read(4, 7, Qi8),
              ok = verify_read_with_published(true, false, ReadD,
                                              [Four, Five, Six]),
              {ReadE, Qi10} = rabbit_queue_index:read(7, 9, Qi9),
              ok = verify_read_with_published(false, false, ReadE,
                                              [Seven, Eight]),
              Qi10
      end),

    %% e) as for (d), but use terminate instead of read, which will
    %% exercise journal_minus_segment, not segment_plus_journal.
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsE} = queue_index_publish([0,1,2,4,5,7],
                                                         true, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              {5, Qi4} = restart_test_queue(Qi3),
              {Qi5, _SeqIdsMsgIdsF} = queue_index_publish([3,6,8], true, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {5, Qi8} = restart_test_queue(Qi7),
              Qi8
      end),

    ok = rabbit_variable_queue:stop(),
    ok = rabbit_variable_queue:start([]),

    passed.

variable_queue_init(Q, Recover) ->
    rabbit_variable_queue:init(
      Q, Recover, fun nop/2, fun nop/2, fun nop/2, fun nop/1).

variable_queue_publish(IsPersistent, Count, VQ) ->
    lists:foldl(
      fun (_N, VQN) ->
              rabbit_variable_queue:publish(
                rabbit_basic:message(
                  rabbit_misc:r(<<>>, exchange, <<>>),
                  <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                                       true  -> 2;
                                                       false -> 1
                                                   end}, <<>>),
                #message_properties{}, self(), VQN)
      end, VQ, lists:seq(1, Count)).

variable_queue_fetch(Count, IsPersistent, IsDelivered, Len, VQ) ->
    lists:foldl(fun (N, {AckTagsAcc, VQN}) ->
                        Rem = Len - N,
                        {{#basic_message { is_persistent = IsPersistent },
                          IsDelivered, AckTagN, Rem}, VQM} =
                            rabbit_variable_queue:fetch(true, VQN),
                        {[AckTagN | AckTagsAcc], VQM}
                end, {[], VQ}, lists:seq(1, Count)).

assert_prop(List, Prop, Value) ->
    Value = proplists:get_value(Prop, List).

assert_props(List, PropVals) ->
    [assert_prop(List, Prop, Value) || {Prop, Value} <- PropVals].

test_amqqueue(Durable) ->
    (rabbit_amqqueue:pseudo_queue(test_queue(), self()))
        #amqqueue { durable = Durable }.

with_fresh_variable_queue(Fun) ->
    ok = empty_test_queue(),
    VQ = variable_queue_init(test_amqqueue(true), false),
    S0 = rabbit_variable_queue:status(VQ),
    assert_props(S0, [{q1, 0}, {q2, 0},
                      {delta, {delta, undefined, 0, undefined}},
                      {q3, 0}, {q4, 0},
                      {len, 0}]),
    _ = rabbit_variable_queue:delete_and_terminate(shutdown, Fun(VQ)),
    passed.

test_variable_queue() ->
    [passed = with_fresh_variable_queue(F) ||
        F <- [fun test_variable_queue_dynamic_duration_change/1,
              fun test_variable_queue_partial_segments_delta_thing/1,
              fun test_variable_queue_all_the_bits_not_covered_elsewhere1/1,
              fun test_variable_queue_all_the_bits_not_covered_elsewhere2/1,
              fun test_dropwhile/1,
              fun test_variable_queue_ack_limiting/1]],
    passed.

test_variable_queue_ack_limiting(VQ0) ->
    %% start by sending in a bunch of messages
    Len = 1024,
    VQ1 = variable_queue_publish(false, Len, VQ0),

    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    %% update stats for duration
    {_Duration, VQ3} = rabbit_variable_queue:ram_duration(VQ2),

    %% fetch half the messages
    {_AckTags, VQ4} = variable_queue_fetch(Len div 2, false, false, Len, VQ3),

    VQ5 = check_variable_queue_status(VQ4, [{len          , Len div 2},
                                            {ram_ack_count, Len div 2},
                                            {ram_msg_count, Len div 2}]),

    %% ensure all acks go to disk on 0 duration target
    VQ6 = check_variable_queue_status(
            rabbit_variable_queue:set_ram_duration_target(0, VQ5),
            [{len, Len div 2},
             {target_ram_count, 0},
             {ram_msg_count, 0},
             {ram_ack_count, 0}]),

    VQ6.

test_dropwhile(VQ0) ->
    Count = 10,

    %% add messages with sequential expiry
    VQ1 = lists:foldl(
            fun (N, VQN) ->
                    rabbit_variable_queue:publish(
                      rabbit_basic:message(
                        rabbit_misc:r(<<>>, exchange, <<>>),
                        <<>>, #'P_basic'{}, <<>>),
                      #message_properties{expiry = N}, self(), VQN)
            end, VQ0, lists:seq(1, Count)),

    %% drop the first 5 messages
    VQ2 = rabbit_variable_queue:dropwhile(
            fun(#message_properties { expiry = Expiry }) ->
                    Expiry =< 5
            end, VQ1),

    %% fetch five now
    VQ3 = lists:foldl(fun (_N, VQN) ->
                              {{#basic_message{}, _, _, _}, VQM} =
                                  rabbit_variable_queue:fetch(false, VQN),
                              VQM
                      end, VQ2, lists:seq(6, Count)),

    %% should be empty now
    {empty, VQ4} = rabbit_variable_queue:fetch(false, VQ3),

    VQ4.

test_variable_queue_dynamic_duration_change(VQ0) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),

    %% start by sending in a couple of segments worth
    Len = 2*SegmentSize,
    VQ1 = variable_queue_publish(false, Len, VQ0),
    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    {Duration, VQ3} = rabbit_variable_queue:ram_duration(VQ2),
    VQ7 = lists:foldl(
            fun (Duration1, VQ4) ->
                    {_Duration, VQ5} = rabbit_variable_queue:ram_duration(VQ4),
                    io:format("~p:~n~p~n",
                              [Duration1, rabbit_variable_queue:status(VQ5)]),
                    VQ6 = rabbit_variable_queue:set_ram_duration_target(
                            Duration1, VQ5),
                    publish_fetch_and_ack(Churn, Len, VQ6)
            end, VQ3, [Duration / 4, 0, Duration / 4, infinity]),

    %% drain
    {AckTags, VQ8} = variable_queue_fetch(Len, false, false, Len, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags, VQ8),
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),

    VQ10.

publish_fetch_and_ack(0, _Len, VQ0) ->
    VQ0;
publish_fetch_and_ack(N, Len, VQ0) ->
    VQ1 = variable_queue_publish(false, 1, VQ0),
    {{_Msg, false, AckTag, Len}, VQ2} = rabbit_variable_queue:fetch(true, VQ1),
    {_Guids, VQ3} = rabbit_variable_queue:ack([AckTag], VQ2),
    publish_fetch_and_ack(N-1, Len, VQ3).

test_variable_queue_partial_segments_delta_thing(VQ0) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    HalfSegment = SegmentSize div 2,
    OneAndAHalfSegment = SegmentSize + HalfSegment,
    VQ1 = variable_queue_publish(true, OneAndAHalfSegment, VQ0),
    {_Duration, VQ2} = rabbit_variable_queue:ram_duration(VQ1),
    VQ3 = check_variable_queue_status(
            rabbit_variable_queue:set_ram_duration_target(0, VQ2),
            %% one segment in q3 as betas, and half a segment in delta
            [{delta, {delta, SegmentSize, HalfSegment, OneAndAHalfSegment}},
             {q3, SegmentSize},
             {len, SegmentSize + HalfSegment}]),
    VQ4 = rabbit_variable_queue:set_ram_duration_target(infinity, VQ3),
    VQ5 = check_variable_queue_status(
            variable_queue_publish(true, 1, VQ4),
            %% one alpha, but it's in the same segment as the deltas
            [{q1, 1},
             {delta, {delta, SegmentSize, HalfSegment, OneAndAHalfSegment}},
             {q3, SegmentSize},
             {len, SegmentSize + HalfSegment + 1}]),
    {AckTags, VQ6} = variable_queue_fetch(SegmentSize, true, false,
                                          SegmentSize + HalfSegment + 1, VQ5),
    VQ7 = check_variable_queue_status(
            VQ6,
            %% the half segment should now be in q3 as betas
            [{q1, 1},
             {delta, {delta, undefined, 0, undefined}},
             {q3, HalfSegment},
             {len, HalfSegment + 1}]),
    {AckTags1, VQ8} = variable_queue_fetch(HalfSegment + 1, true, false,
                                           HalfSegment + 1, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags ++ AckTags1, VQ8),
    %% should be empty now
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),
    VQ10.

check_variable_queue_status(VQ0, Props) ->
    VQ1 = variable_queue_wait_for_shuffling_end(VQ0),
    S = rabbit_variable_queue:status(VQ1),
    io:format("~p~n", [S]),
    assert_props(S, Props),
    VQ1.

variable_queue_wait_for_shuffling_end(VQ) ->
    case rabbit_variable_queue:needs_timeout(VQ) of
        false -> VQ;
        _     -> variable_queue_wait_for_shuffling_end(
                   rabbit_variable_queue:timeout(VQ))
    end.

test_variable_queue_all_the_bits_not_covered_elsewhere1(VQ0) ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    VQ1 = variable_queue_publish(true, Count, VQ0),
    VQ2 = variable_queue_publish(false, Count, VQ1),
    VQ3 = rabbit_variable_queue:set_ram_duration_target(0, VQ2),
    {_AckTags, VQ4}  = variable_queue_fetch(Count, true, false,
                                            Count + Count, VQ3),
    {_AckTags1, VQ5} = variable_queue_fetch(Count, false, false,
                                            Count, VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
    {{_Msg1, true, _AckTag1, Count1}, VQ8} =
        rabbit_variable_queue:fetch(true, VQ7),
    VQ9 = variable_queue_publish(false, 1, VQ8),
    VQ10 = rabbit_variable_queue:set_ram_duration_target(0, VQ9),
    {_AckTags2, VQ11} = variable_queue_fetch(Count1, true, true, Count, VQ10),
    {_AckTags3, VQ12} = variable_queue_fetch(1, false, false, 1, VQ11),
    VQ12.

test_variable_queue_all_the_bits_not_covered_elsewhere2(VQ) ->
    StateT = state_t:new(identity_m),
    SM = StateT:modify(_),
    SMR = StateT:modify_and_return(_),
    StateT:exec(
      do([StateT ||
             SM(rabbit_variable_queue:set_ram_duration_target(0, _)),
             SM(variable_queue_publish(false, 4, _)),
             AckTags <- SMR(variable_queue_fetch(2, false, false, 4, _)),
             _Guids <- SMR(rabbit_variable_queue:requeue(
                             AckTags, fun (X) -> X end, _)),
             SM(rabbit_variable_queue:timeout(_)),
             SM(rabbit_variable_queue:terminate(shutdown, _)),
             StateT:put(variable_queue_init(test_amqqueue(true), true)),
             empty <- (rabbit_variable_queue:fetch(false, _)),
             return(passed)]), VQ).

test_queue_recover() ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    TxID = rabbit_guid:guid(),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(test_queue(), true, false, [], none),
    [begin
         Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                    <<>>, #'P_basic'{delivery_mode = 2}, <<>>),
         Delivery = #delivery{mandatory = false, immediate = false, txn = TxID,
                              sender = self(), message = Msg},
         true = rabbit_amqqueue:deliver(QPid, Delivery)
     end || _ <- lists:seq(1, Count)],
    rabbit_amqqueue:commit_all([QPid], TxID, self()),
    exit(QPid, kill),
    MRef = erlang:monitor(process, QPid),
    receive {'DOWN', MRef, process, QPid, _Info} -> ok
    after 10000 -> exit(timeout_waiting_for_queue_death)
    end,
    rabbit_amqqueue:stop(),
    rabbit_amqqueue:start(),
    rabbit_amqqueue:with_or_die(
      QName,
      fun (Q1 = #amqqueue { pid = QPid1 }) ->
              CountMinusOne = Count - 1,
              {ok, CountMinusOne, {QName, QPid1, _AckTag, true, _Msg}} =
                  rabbit_amqqueue:basic_get(Q1, self(), false),
              exit(QPid1, shutdown),
              VQ1 = variable_queue_init(Q, true),
              {{_Msg1, true, _AckTag1, CountMinusOne}, VQ2} =
                  rabbit_variable_queue:fetch(true, VQ1),
              _VQ3 = rabbit_variable_queue:delete_and_terminate(shutdown, VQ2),
              rabbit_amqqueue:internal_delete(QName)
      end),
    passed.

test_variable_queue_delete_msg_store_files_callback() ->
    ok = restart_msg_store_empty(),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(test_queue(), true, false, [], none),
    TxID = rabbit_guid:guid(),
    Payload = <<0:8388608>>, %% 1MB
    Count = 30,
    [begin
         Msg = rabbit_basic:message(
                 rabbit_misc:r(<<>>, exchange, <<>>),
                 <<>>, #'P_basic'{delivery_mode = 2}, Payload),
         Delivery = #delivery{mandatory = false, immediate = false, txn = TxID,
                              sender = self(), message = Msg},
         true = rabbit_amqqueue:deliver(QPid, Delivery)
     end || _ <- lists:seq(1, Count)],
    rabbit_amqqueue:commit_all([QPid], TxID, self()),
    rabbit_amqqueue:set_ram_duration_target(QPid, 0),

    CountMinusOne = Count - 1,
    {ok, CountMinusOne, {QName, QPid, _AckTag, false, _Msg}} =
        rabbit_amqqueue:basic_get(Q, self(), true),
    {ok, CountMinusOne} = rabbit_amqqueue:purge(Q),

    %% give the queue a second to receive the close_fds callback msg
    timer:sleep(1000),

    rabbit_amqqueue:delete(Q, false, false),
    passed.

test_configurable_server_properties() ->
    %% List of the names of the built-in properties do we expect to find
    BuiltInPropNames = [<<"product">>, <<"version">>, <<"platform">>,
                        <<"copyright">>, <<"information">>],

    Protocol = rabbit_framing_amqp_0_9_1,

    %% Verify that the built-in properties are initially present
    ActualPropNames = [Key || {Key, longstr, _} <-
                                  rabbit_reader:server_properties(Protocol)],
    true = lists:all(fun (X) -> lists:member(X, ActualPropNames) end,
                     BuiltInPropNames),

    %% Get the initial server properties configured in the environment
    {ok, ServerProperties} = application:get_env(rabbit, server_properties),

    %% Helper functions
    ConsProp = fun (X) -> application:set_env(rabbit,
                                              server_properties,
                                              [X | ServerProperties]) end,
    IsPropPresent =
        fun (X) ->
                lists:member(X, rabbit_reader:server_properties(Protocol))
        end,

    %% Add a wholly new property of the simplified {KeyAtom, StringValue} form
    NewSimplifiedProperty = {NewHareKey, NewHareVal} = {hare, "soup"},
    ConsProp(NewSimplifiedProperty),
    %% Do we find hare soup, appropriately formatted in the generated properties?
    ExpectedHareImage = {list_to_binary(atom_to_list(NewHareKey)),
                         longstr,
                         list_to_binary(NewHareVal)},
    true = IsPropPresent(ExpectedHareImage),

    %% Add a wholly new property of the {BinaryKey, Type, Value} form
    %% and check for it
    NewProperty = {<<"new-bin-key">>, signedint, -1},
    ConsProp(NewProperty),
    %% Do we find the new property?
    true = IsPropPresent(NewProperty),

    %% Add a property that clobbers a built-in, and verify correct clobbering
    {NewVerKey, NewVerVal} = NewVersion = {version, "X.Y.Z."},
    {BinNewVerKey, BinNewVerVal} = {list_to_binary(atom_to_list(NewVerKey)),
                                    list_to_binary(NewVerVal)},
    ConsProp(NewVersion),
    ClobberedServerProps = rabbit_reader:server_properties(Protocol),
    %% Is the clobbering insert present?
    true = IsPropPresent({BinNewVerKey, longstr, BinNewVerVal}),
    %% Is the clobbering insert the only thing with the clobbering key?
    [{BinNewVerKey, longstr, BinNewVerVal}] =
        [E || {K, longstr, _V} = E <- ClobberedServerProps, K =:= BinNewVerKey],

    application:set_env(rabbit, server_properties, ServerProperties),
    passed.

nop(_) -> ok.
nop(_, _) -> ok.
