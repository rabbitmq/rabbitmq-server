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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_tests).

-compile([export_all]).

-export([all_tests/0, test_parsing/0]).

-import(rabbit_misc, [pget/2]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("kernel/include/file.hrl").

-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).
-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

all_tests() ->
    passed = gm_tests:all_tests(),
    passed = mirrored_supervisor_tests:all_tests(),
    application:set_env(rabbit, file_handles_high_watermark, 10, infinity),
    ok = file_handle_cache:set_limit(10),
    passed = test_multi_call(),
    passed = test_file_handle_cache(),
    passed = test_backing_queue(),
    passed = test_priority_queue(),
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
    passed = test_arguments_parser(),
    passed = test_cluster_management(),
    passed = test_user_management(),
    passed = test_runtime_parameters(),
    passed = test_server_status(),
    passed = test_confirms(),
    passed = maybe_run_cluster_dependent_tests(),
    passed = test_configurable_server_properties(),
    passed.

maybe_run_cluster_dependent_tests() ->
    SecondaryNode = rabbit_nodes:make("hare"),

    case net_adm:ping(SecondaryNode) of
        pong -> passed = run_cluster_dependent_tests(SecondaryNode);
        pang -> io:format("Skipping cluster dependent tests with node ~p~n",
                          [SecondaryNode])
    end,
    passed.

run_cluster_dependent_tests(SecondaryNode) ->
    SecondaryNodeS = atom_to_list(SecondaryNode),

    cover:stop(SecondaryNode),
    ok = control_action(stop_app, []),
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),
    ok = control_action(start_app, []),
    cover:start(SecondaryNode),
    ok = control_action(start_app, SecondaryNode, [], []),

    io:format("Running cluster dependent tests with node ~p~n", [SecondaryNode]),
    passed = test_delegates_async(SecondaryNode),
    passed = test_delegates_sync(SecondaryNode),
    passed = test_queue_cleanup(SecondaryNode),
    passed = test_declare_on_dead_queue(SecondaryNode),
    passed = test_refresh_events(SecondaryNode),

    %% we now run the tests remotely, so that code coverage on the
    %% local node picks up more of the delegate
    Node = node(),
    Self = self(),
    Remote = spawn(SecondaryNode,
                   fun () -> Rs = [ test_delegates_async(Node),
                                    test_delegates_sync(Node),
                                    test_queue_cleanup(Node),
                                    test_declare_on_dead_queue(Node),
                                    test_refresh_events(Node) ],
                             Self ! {self(), Rs}
                   end),
    receive
        {Remote, Result} ->
            Result = lists:duplicate(length(Result), passed)
    after 30000 ->
            throw(timeout)
    end,

    passed.

test_multi_call() ->
    Fun = fun() ->
                  receive
                      {'$gen_call', {From, Mref}, request} ->
                          From ! {Mref, response}
                  end,
                  receive
                      never -> ok
                  end
          end,
    Pid1 = spawn(Fun),
    Pid2 = spawn(Fun),
    Pid3 = spawn(Fun),
    exit(Pid2, bang),
    {[{Pid1, response}, {Pid3, response}], [{Pid2, _Fail}]} =
        rabbit_misc:multi_call([Pid1, Pid2, Pid3], request),
    exit(Pid1, bang),
    exit(Pid3, bang),
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

    %% 1-element infinity priority Q
    Q16 = priority_queue:in(foo, infinity, Q),
    {true, false, 1, [{infinity, foo}], [foo]} = test_priority_queue(Q16),

    %% add infinity to 0-priority Q
    Q17 = priority_queue:in(foo, infinity, priority_queue:in(bar, Q)),
    {true, false, 2, [{infinity, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q17),

    %% and the other way around
    Q18 = priority_queue:in(bar, priority_queue:in(foo, infinity, Q)),
    {true, false, 2, [{infinity, foo}, {0, bar}], [foo, bar]} =
        test_priority_queue(Q18),

    %% add infinity to mixed-priority Q
    Q19 = priority_queue:in(qux, infinity, Q3),
    {true, false, 3, [{infinity, qux}, {2, bar}, {1, foo}], [qux, bar, foo]} =
        test_priority_queue(Q19),

    %% merge the above with a negative priority Q
    Q20 = priority_queue:join(Q19, Q4),
    {true, false, 4, [{infinity, qux}, {2, bar}, {1, foo}, {-1, foo}],
     [qux, bar, foo, foo]} = test_priority_queue(Q20),

    %% merge two infinity priority queues
    Q21 = priority_queue:join(priority_queue:in(foo, infinity, Q),
                              priority_queue:in(bar, infinity, Q)),
    {true, false, 2, [{infinity, foo}, {infinity, bar}], [foo, bar]} =
        test_priority_queue(Q21),

    %% merge two mixed priority with infinity queues
    Q22 = priority_queue:join(Q18, Q20),
    {true, false, 6, [{infinity, foo}, {infinity, qux}, {2, bar}, {1, foo},
                      {0, bar}, {-1, foo}], [foo, qux, bar, foo, bar, foo]} =
        test_priority_queue(Q22),

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

test_content_prop_encoding(Datum, Binary) ->
    Types =  [element(1, E) || E <- Datum],
    Values = [element(2, E) || E <- Datum],
    Binary = rabbit_binary_generator:encode_properties(Types, Values). %% assertion

test_content_properties() ->
    test_content_prop_encoding([], <<0, 0>>),
    test_content_prop_encoding([{bit, true}, {bit, false}, {bit, true}, {bit, false}],
                               <<16#A0, 0>>),
    test_content_prop_encoding([{bit, true}, {octet, 123}, {bit, true}, {octet, undefined},
                                {bit, true}],
                               <<16#E8,0,123>>),
    test_content_prop_encoding([{bit, true}, {octet, 123}, {octet, 123}, {bit, true}],
                               <<16#F0,0,123,123>>),
    test_content_prop_encoding([{bit, true}, {shortstr, <<"hi">>}, {bit, true},
                                {shortint, 54321}, {bit, true}],
                               <<16#F8,0,2,"hi",16#D4,16#31>>),
    test_content_prop_encoding([{bit, true}, {shortstr, undefined}, {bit, true},
                                {shortint, 54321}, {bit, true}],
                               <<16#B8,0,16#D4,16#31>>),
    test_content_prop_encoding([{table, [{<<"a signedint">>, signedint, 12345678},
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
    passed.

test_field_values() ->
    %% FIXME this does not test inexact numbers (double and float) yet,
    %% because they won't pass the equality assertions
    test_content_prop_encoding(
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
    control_action(wait, [rabbit_mnesia:dir() ++ ".pid"]),
    %% Starting, stopping and diagnostics.  Note that we don't try
    %% 'report' when the rabbit app is stopped and that we enable
    %% tracing for the duration of this function.
    ok = control_action(trace_on, []),
    ok = control_action(stop_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(status, []),
    ok = control_action(cluster_status, []),
    ok = control_action(environment, []),
    ok = control_action(start_app, []),
    ok = control_action(start_app, []),
    ok = control_action(status, []),
    ok = control_action(report, []),
    ok = control_action(cluster_status, []),
    ok = control_action(environment, []),
    ok = control_action(trace_off, []),
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

    %% log rotation on empty files (the main log will have a ctl action logged)
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = control_action(rotate_logs, [Suffix]),
    [false, true] = empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),

    %% logs with suffix are not writable
    ok = control_action(rotate_logs, [Suffix]),
    ok = make_files_non_writable([[MainLog, Suffix], [SaslLog, Suffix]]),
    ok = control_action(rotate_logs, [Suffix]),
    ok = test_logs_working(MainLog, SaslLog),

    %% rotate when original log files are not writable
    ok = make_files_non_writable([MainLog, SaslLog]),
    ok = control_action(rotate_logs, []),

    %% logging directed to tty (first, remove handlers)
    ok = delete_log_handlers([rabbit_sasl_report_file_h,
                              rabbit_error_logger_file_h]),
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = application:set_env(rabbit, sasl_error_logger, tty),
    ok = application:set_env(rabbit, error_logger, tty),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% rotate logs when logging is turned off
    ok = application:set_env(rabbit, sasl_error_logger, false),
    ok = application:set_env(rabbit, error_logger, silent),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% cleanup
    ok = application:set_env(rabbit, sasl_error_logger, {file, SaslLog}),
    ok = application:set_env(rabbit, error_logger, {file, MainLog}),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog},
                           {rabbit_sasl_report_file_h, SaslLog}]),
    passed.

test_log_management_during_startup() ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),

    %% start application with simple tty logging
    ok = control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, tty),
    ok = application:set_env(rabbit, sasl_error_logger, tty),
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
    ok = application:set_env(rabbit, sasl_error_logger, {file, SaslLog}),

    %% start application with logging to non-existing directory
    TmpLog = "/tmp/rabbit-tests/test.log",
    delete_file(TmpLog),
    ok = application:set_env(rabbit, error_logger, {file, TmpLog}),

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
    ok = application:set_env(rabbit, error_logger, {file, TmpTestDir}),
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
    ok = application:set_env(rabbit, error_logger, {file, MainLog}),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% start application with standard sasl handler not installed
    %% and rabbit main log handler installed correctly
    ok = delete_log_handlers([rabbit_sasl_report_file_h]),
    ok = control_action(start_app, []),
    passed.

test_arguments_parser() ->
    GlobalOpts1 = [{"-f1", flag}, {"-o1", {option, "foo"}}],
    Commands1 = [command1, {command2, [{"-f2", flag}, {"-o2", {option, "bar"}}]}],

    GetOptions =
        fun (Args) ->
                rabbit_misc:parse_arguments(Commands1, GlobalOpts1, Args)
        end,

    check_parse_arguments(no_command, GetOptions, []),
    check_parse_arguments(no_command, GetOptions, ["foo", "bar"]),
    check_parse_arguments(
      {ok, {command1, [{"-f1", false}, {"-o1", "foo"}], []}},
      GetOptions, ["command1"]),
    check_parse_arguments(
      {ok, {command1, [{"-f1", false}, {"-o1", "blah"}], []}},
      GetOptions, ["command1", "-o1", "blah"]),
    check_parse_arguments(
      {ok, {command1, [{"-f1", true}, {"-o1", "foo"}], []}},
      GetOptions, ["command1", "-f1"]),
    check_parse_arguments(
      {ok, {command1, [{"-f1", false}, {"-o1", "blah"}], []}},
      GetOptions, ["-o1", "blah", "command1"]),
    check_parse_arguments(
      {ok, {command1, [{"-f1", false}, {"-o1", "blah"}], ["quux"]}},
      GetOptions, ["-o1", "blah", "command1", "quux"]),
    check_parse_arguments(
      {ok, {command1, [{"-f1", true}, {"-o1", "blah"}], ["quux", "baz"]}},
      GetOptions, ["command1", "quux", "-f1", "-o1", "blah", "baz"]),
    %% For duplicate flags, the last one counts
    check_parse_arguments(
      {ok, {command1, [{"-f1", false}, {"-o1", "second"}], []}},
      GetOptions, ["-o1", "first", "command1", "-o1", "second"]),
    %% If the flag "eats" the command, the command won't be recognised
    check_parse_arguments(no_command, GetOptions,
                      ["-o1", "command1", "quux"]),
    %% If a flag eats another flag, the eaten flag won't be recognised
    check_parse_arguments(
      {ok, {command1, [{"-f1", false}, {"-o1", "-f1"}], []}},
      GetOptions, ["command1", "-o1", "-f1"]),

    %% Now for some command-specific flags...
    check_parse_arguments(
      {ok, {command2, [{"-f1", false}, {"-f2", false},
                       {"-o1", "foo"}, {"-o2", "bar"}], []}},
      GetOptions, ["command2"]),

    check_parse_arguments(
      {ok, {command2, [{"-f1", false}, {"-f2", true},
                       {"-o1", "baz"}, {"-o2", "bar"}], ["quux", "foo"]}},
      GetOptions, ["-f2", "command2", "quux", "-o1", "baz", "foo"]),

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
    ok = assert_disc_node(),
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),
    ok = assert_ram_node(),

    %% join a non-existing cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),
    ok = assert_ram_node(),

    ok = control_action(reset, []),

    SecondaryNode = rabbit_nodes:make("hare"),
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
    ok = control_action(cluster, [NodeS]),
    ok = assert_disc_node(),
    %% make a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),
    ok = assert_ram_node(),

    %% join cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(force_cluster, [SecondaryNodeS, "invalid1@invalid"]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = assert_ram_node(),

    %% ram node will not start by itself
    ok = control_action(stop_app, []),
    ok = control_action(stop_app, SecondaryNode, [], []),
    {error, _} = control_action(start_app, []),
    ok = control_action(start_app, SecondaryNode, [], []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% change cluster config while remaining in same cluster
    ok = control_action(force_cluster, ["invalid2@invalid", SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% join non-existing cluster as a ram node
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),
    {error, _} = control_action(start_app, []),
    ok = assert_ram_node(),

    %% join empty cluster as a ram node (converts to disc)
    ok = control_action(cluster, []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = assert_disc_node(),

    %% make a new ram node
    ok = control_action(reset, []),
    ok = control_action(force_cluster, [SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = assert_ram_node(),

    %% turn ram node into disk node
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = assert_disc_node(),

    %% convert a disk node into a ram node
    ok = assert_disc_node(),
    ok = control_action(force_cluster, ["invalid1@invalid",
                                        "invalid2@invalid"]),
    ok = assert_ram_node(),

    %% make a new disk node
    ok = control_action(force_reset, []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = assert_disc_node(),

    %% turn a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = assert_ram_node(),

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
    cover:stop(SecondaryNode),
    ok = control_action(reset, []),
    cover:start(SecondaryNode),

    %% attempt to leave cluster when no other node is alive
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, SecondaryNode, [], []),
    ok = control_action(stop_app, []),
    {error, {no_running_cluster_nodes, _, _}} =
        control_action(reset, []),

    %% attempt to change type when no other node is alive
    {error, {no_running_cluster_nodes, _, _}} =
        control_action(cluster, [SecondaryNodeS]),

    %% leave system clustered, with the secondary node as a ram node
    ok = control_action(force_reset, []),
    ok = control_action(start_app, []),
    %% Yes, this is rather ugly. But since we're a clustered Mnesia
    %% node and we're telling another clustered node to reset itself,
    %% we will get disconnected half way through causing a
    %% badrpc. This never happens in real life since rabbitmqctl is
    %% not a clustered Mnesia node.
    cover:stop(SecondaryNode),
    {badrpc, nodedown} = control_action(force_reset, SecondaryNode, [], []),
    pong = net_adm:ping(SecondaryNode),
    cover:start(SecondaryNode),
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
    {error, {no_such_user, _}} =
        control_action(set_user_tags, ["foo", "bar"]),

    %% user creation
    ok = control_action(add_user, ["foo", "bar"]),
    {error, {user_already_exists, _}} =
        control_action(add_user, ["foo", "bar"]),
    ok = control_action(clear_password, ["foo"]),
    ok = control_action(change_password, ["foo", "baz"]),

    TestTags = fun (Tags) ->
                       Args = ["foo" | [atom_to_list(T) || T <- Tags]],
                       ok = control_action(set_user_tags, Args),
                       {ok, #internal_user{tags = Tags}} =
                           rabbit_auth_backend_internal:lookup_user(<<"foo">>),
                       ok = control_action(list_users, [])
               end,
    TestTags([foo, bar, baz]),
    TestTags([administrator]),
    TestTags([]),

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

test_runtime_parameters() ->
    rabbit_runtime_parameters_test:register(),
    Good = fun(L) -> ok                = control_action(set_parameter, L) end,
    Bad  = fun(L) -> {error_string, _} = control_action(set_parameter, L) end,

    %% Acceptable for bijection
    Good(["test", "good", "<<\"ignore\">>"]),
    Good(["test", "good", "123"]),
    Good(["test", "good", "true"]),
    Good(["test", "good", "false"]),
    Good(["test", "good", "null"]),
    Good(["test", "good", "[{<<\"key\">>, <<\"value\">>}]"]),

    %% Various forms of fail due to non-bijectability
    Bad(["test", "good", "atom"]),
    Bad(["test", "good", "{tuple, foo}"]),
    Bad(["test", "good", "[{<<\"key\">>, <<\"value\">>, 1}]"]),
    Bad(["test", "good", "[{key, <<\"value\">>}]"]),

    %% Test actual validation hook
    Good(["test", "maybe", "<<\"good\">>"]),
    Bad(["test", "maybe", "<<\"bad\">>"]),

    ok = control_action(list_parameters, []),

    ok = control_action(clear_parameter, ["test", "good"]),
    ok = control_action(clear_parameter, ["test", "maybe"]),
    {error_string, _} =
        control_action(clear_parameter, ["test", "neverexisted"]),
    rabbit_runtime_parameters_test:unregister(),
    passed.

test_server_status() ->
    %% create a few things so there is some useful information to list
    Writer = spawn(fun () -> receive shutdown -> ok end end),
    {ok, Ch} = rabbit_channel:start_link(
                 1, self(), Writer, self(), "", rabbit_framing_amqp_0_9_1,
                 user(<<"user">>), <<"/">>, [], self(),
                 rabbit_limiter:make_token(self())),
    [Q, Q2] = [Queue || Name <- [<<"foo">>, <<"bar">>],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], none)]],

    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch, rabbit_limiter:make_token(),
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

    %% set vm memory high watermark
    ok = control_action(set_vm_memory_high_watermark, ["1.0"]),

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
                 1, Me, Writer, Me, "", rabbit_framing_amqp_0_9_1,
                 user(<<"guest">>), <<"/">>, [], Me,
                  rabbit_limiter:make_token(self())),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after 1000 -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.

test_spawn(Node) ->
    rpc:call(Node, ?MODULE, test_spawn_remote, []).

%% Spawn an arbitrary long lived process, so we don't end up linking
%% the channel to the short-lived process (RPC, here) spun up by the
%% RPC server.
test_spawn_remote() ->
    RPC = self(),
    spawn(fun () ->
                  {Writer, Ch} = test_spawn(),
                  RPC ! {Writer, Ch},
                  link(Ch),
                  receive
                      _ -> ok
                  end
          end),
    receive Res -> Res
    after 1000  -> throw(failed_to_receive_result)
    end.

user(Username) ->
    #user{username     = Username,
          tags         = [administrator],
          auth_backend = rabbit_auth_backend_internal,
          impl         = #internal_user{username = Username,
                                        tags     = [administrator]}}.

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
    %% We must not kill the queue before the channel has processed the
    %% 'publish'.
    ok = rabbit_channel:flush(Ch),
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

test_statistics_event_receiver(Pid) ->
    receive
        Foo -> Pid ! Foo, test_statistics_event_receiver(Pid)
    end.

test_statistics_receive_event(Ch, Matcher) ->
    rabbit_channel:flush(Ch),
    Ch ! emit_stats,
    test_statistics_receive_event1(Ch, Matcher).

test_statistics_receive_event1(Ch, Matcher) ->
    receive #event{type = channel_stats, props = Props} ->
            case Matcher(Props) of
                true -> Props;
                _    -> test_statistics_receive_event1(Ch, Matcher)
            end
    after 1000 -> throw(failed_to_receive_event)
    end.

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

    rabbit_tests_event_receiver:start(self(), [node()], [channel_stats]),

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

test_refresh_events(SecondaryNode) ->
    rabbit_tests_event_receiver:start(self(), [node(), SecondaryNode],
                                      [channel_created, queue_created]),

    {_Writer, Ch} = test_spawn(),
    expect_events(Ch, channel_created),
    rabbit_channel:shutdown(Ch),

    {_Writer2, Ch2} = test_spawn(SecondaryNode),
    expect_events(Ch2, channel_created),
    rabbit_channel:shutdown(Ch2),

    {new, #amqqueue { pid = QPid } = Q} =
        rabbit_amqqueue:declare(test_queue(), false, false, [], none),
    expect_events(QPid, queue_created),
    rabbit_amqqueue:delete(Q, false, false),

    rabbit_tests_event_receiver:stop(),
    passed.

expect_events(Pid, Type) ->
    expect_event(Pid, Type),
    rabbit:force_event_refresh(),
    expect_event(Pid, Type).

expect_event(Pid, Type) ->
    receive #event{type = Type, props = Props} ->
            case pget(pid, Props) of
                Pid -> ok;
                _   -> expect_event(Pid, Type)
            end
    after 1000 -> throw({failed_to_receive_event, Type})
    end.

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
    rabbit_channel:shutdown(Ch),
    rabbit:stop(),
    rabbit:start(),
    {_Writer2, Ch2} = test_spawn(),
    rabbit_channel:do(Ch2, #'queue.declare'{ passive = true,
                                             queue   = ?CLEANUP_QUEUE_NAME }),
    receive
        #'channel.close'{reply_code = ?NOT_FOUND} ->
            ok
    after 2000 ->
            throw(failed_to_receive_channel_exit)
    end,
    rabbit_channel:shutdown(Ch2),
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

check_parse_arguments(ExpRes, Fun, As) ->
    SortRes =
        fun (no_command)          -> no_command;
            ({ok, {C, KVs, As1}}) -> {ok, {C, lists:sort(KVs), As1}}
        end,

    true = SortRes(ExpRes) =:= SortRes(Fun(As)).

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

assert_ram_node() ->
    case rabbit_mnesia:is_disc_node() of
        true  -> exit('not_ram_node');
        false -> ok
    end.

assert_disc_node() ->
    case rabbit_mnesia:is_disc_node() of
        true  -> ok;
        false -> exit('not_disc_node')
    end.

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
                      {ok, Hdl} = prim_file:open(Src, [binary, write]),
                      ok = prim_file:write(Hdl, Content),
                      ok = prim_file:sync(Hdl),
                      prim_file:close(Hdl),

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
            %% We will have restarted the message store, and thus changed
            %% the order of the children of rabbit_sup. This will cause
            %% problems if there are subsequent failures - see bug 24262.
            ok = restart_app(),
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

on_disk_capture() ->
    receive
        {await, MsgIds, Pid} -> on_disk_capture([], MsgIds, Pid);
        stop                 -> done
    end.

on_disk_capture([_|_], _Awaiting, Pid) ->
    Pid ! {self(), surplus};
on_disk_capture(OnDisk, Awaiting, Pid) ->
    receive
        {on_disk, MsgIdsS} ->
            MsgIds = gb_sets:to_list(MsgIdsS),
            on_disk_capture(OnDisk ++ (MsgIds -- Awaiting), Awaiting -- MsgIds,
                            Pid);
        stop ->
            done
    after (case Awaiting of [] -> 200; _ -> 1000 end) ->
            case Awaiting of
                [] -> Pid ! {self(), arrived}, on_disk_capture();
                _  -> Pid ! {self(), timeout}
            end
    end.

on_disk_await(Pid, MsgIds) when is_list(MsgIds) ->
    Pid ! {await, MsgIds, self()},
    receive
        {Pid, arrived} -> ok;
        {Pid, Error}   -> Error
    end.

on_disk_stop(Pid) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! stop,
    receive {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end.

msg_store_client_init_capture(MsgStore, Ref) ->
    Pid = spawn(fun on_disk_capture/0),
    {Pid, rabbit_msg_store:client_init(
            MsgStore, Ref, fun (MsgIds, _ActionTaken) ->
                                   Pid ! {on_disk, MsgIds}
                           end, undefined)}.

msg_store_contains(Atom, MsgIds, MSCState) ->
    Atom = lists:foldl(
             fun (MsgId, Atom1) when Atom1 =:= Atom ->
                     rabbit_msg_store:contains(MsgId, MSCState) end,
             Atom, MsgIds).

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
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1,100)],
    {MsgIds1stHalf, MsgIds2ndHalf} = lists:split(length(MsgIds) div 2, MsgIds),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState} = msg_store_client_init_capture(
                        ?PERSISTENT_MSG_STORE, Ref),
    Ref2 = rabbit_guid:gen(),
    {Cap2, MSC2State} = msg_store_client_init_capture(
                          ?PERSISTENT_MSG_STORE, Ref2),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% test confirm logic
    passed = test_msg_store_confirms([hd(MsgIds)], Cap, MSCState),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% publish the first half
    ok = msg_store_write(MsgIds1stHalf, MSCState),
    %% sync on the first half
    ok = on_disk_await(Cap, MsgIds1stHalf),
    %% publish the second half
    ok = msg_store_write(MsgIds2ndHalf, MSCState),
    %% check they're all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% publish the latter half twice so we hit the caching and ref
    %% count code. We need to do this through a 2nd client since a
    %% single client is not supposed to write the same message more
    %% than once without first removing it.
    ok = msg_store_write(MsgIds2ndHalf, MSC2State),
    %% check they're still all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% sync on the 2nd half
    ok = on_disk_await(Cap2, MsgIds2ndHalf),
    %% cleanup
    ok = on_disk_stop(Cap2),
    ok = rabbit_msg_store:client_delete_and_terminate(MSC2State),
    ok = on_disk_stop(Cap),
    %% read them all
    MSCState1 = msg_store_read(MsgIds, MSCState),
    %% read them all again - this will hit the cache, not disk
    MSCState2 = msg_store_read(MsgIds, MSCState1),
    %% remove them all
    ok = msg_store_remove(MsgIds, MSCState2),
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
    ok = msg_store_remove(MsgIds1stHalf, MSCState7),
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
    %%
    passed = test_msg_store_client_delete_and_terminate(),
    %% restart empty
    restart_msg_store_empty(),
    passed.

test_msg_store_confirms(MsgIds, Cap, MSCState) ->
    %% write -> confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% remove -> _
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, []),
    %% write, remove -> confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% write, remove, write -> confirmed, confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds ++ MsgIds),
    %% remove, write -> confirmed
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% remove, write, remove -> confirmed
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% confirmation on timer-based sync
    passed = test_msg_store_confirm_timer(),
    passed.

test_msg_store_confirm_timer() ->
    Ref = rabbit_guid:gen(),
    MsgId  = msg_id_bin(1),
    Self = self(),
    MSCState = rabbit_msg_store:client_init(
                 ?PERSISTENT_MSG_STORE, Ref,
                 fun (MsgIds, _ActionTaken) ->
                         case gb_sets:is_member(MsgId, MsgIds) of
                             true  -> Self ! on_disk;
                             false -> ok
                         end
                 end, undefined),
    ok = msg_store_write([MsgId], MSCState),
    ok = msg_store_keep_busy_until_confirm([msg_id_bin(2)], MSCState),
    ok = msg_store_remove([MsgId], MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

msg_store_keep_busy_until_confirm(MsgIds, MSCState) ->
    receive
        on_disk -> ok
    after 0 ->
            ok = msg_store_write(MsgIds, MSCState),
            ok = msg_store_remove(MsgIds, MSCState),
            msg_store_keep_busy_until_confirm(MsgIds, MSCState)
    end.

test_msg_store_client_delete_and_terminate() ->
    restart_msg_store_empty(),
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1, 10)],
    Ref = rabbit_guid:gen(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = msg_store_write(MsgIds, MSCState),
    %% test the 'dying client' fast path for writes
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).

test_queue() ->
    queue_name(<<"test">>).

init_test_queue() ->
    TestQueue = test_queue(),
    Terms = rabbit_queue_index:shutdown_terms(TestQueue),
    PRef = proplists:get_value(persistent_ref, Terms, rabbit_guid:gen()),
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

restart_app() ->
    rabbit:stop(),
    rabbit:start().

queue_index_publish(SeqIds, Persistent, Qi) ->
    Ref = rabbit_guid:gen(),
    MsgStore = case Persistent of
                   true  -> ?PERSISTENT_MSG_STORE;
                   false -> ?TRANSIENT_MSG_STORE
               end,
    MSCState = msg_store_client_init(MsgStore, Ref),
    {A, B = [{_SeqId, LastMsgIdWritten} | _]} =
        lists:foldl(
          fun (SeqId, {QiN, SeqIdsMsgIdsAcc}) ->
                  MsgId = rabbit_guid:gen(),
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
              MsgId = rabbit_guid:gen(),
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
      Q, Recover, fun nop/2, fun nop/2, fun nop/1).

variable_queue_publish(IsPersistent, Count, VQ) ->
    variable_queue_publish(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_publish(IsPersistent, Count, PropFun, VQ) ->
    lists:foldl(
      fun (N, VQN) ->
              rabbit_variable_queue:publish(
                rabbit_basic:message(
                  rabbit_misc:r(<<>>, exchange, <<>>),
                  <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                                       true  -> 2;
                                                       false -> 1
                                                   end}, <<>>),
                PropFun(N, #message_properties{}), self(), VQN)
      end, VQ, lists:seq(1, Count)).

variable_queue_fetch(Count, IsPersistent, IsDelivered, Len, VQ) ->
    lists:foldl(fun (N, {VQN, AckTagsAcc}) ->
                        Rem = Len - N,
                        {{#basic_message { is_persistent = IsPersistent },
                          IsDelivered, AckTagN, Rem}, VQM} =
                            rabbit_variable_queue:fetch(true, VQN),
                        {VQM, [AckTagN | AckTagsAcc]}
                end, {VQ, []}, lists:seq(1, Count)).

assert_prop(List, Prop, Value) ->
    Value = proplists:get_value(Prop, List).

assert_props(List, PropVals) ->
    [assert_prop(List, Prop, Value) || {Prop, Value} <- PropVals].

test_amqqueue(Durable) ->
    (rabbit_amqqueue:pseudo_queue(test_queue(), self()))
        #amqqueue { durable = Durable }.

with_fresh_variable_queue(Fun) ->
    Ref = make_ref(),
    Me = self(),
    %% Run in a separate process since rabbit_msg_store will send
    %% bump_credit messages and we want to ignore them
    spawn_link(fun() ->
                       ok = empty_test_queue(),
                       VQ = variable_queue_init(test_amqqueue(true), false),
                       S0 = rabbit_variable_queue:status(VQ),
                       assert_props(S0, [{q1, 0}, {q2, 0},
                                         {delta,
                                          {delta, undefined, 0, undefined}},
                                         {q3, 0}, {q4, 0},
                                         {len, 0}]),
                       _ = rabbit_variable_queue:delete_and_terminate(
                        shutdown, Fun(VQ)),
                       Me ! Ref
               end),
    receive
        Ref -> ok
    end,
    passed.

publish_and_confirm(Q, Payload, Count) ->
    Seqs = lists:seq(1, Count),
    [begin
         Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                    <<>>, #'P_basic'{delivery_mode = 2},
                                    Payload),
         Delivery = #delivery{mandatory = false, immediate = false,
                              sender = self(), message = Msg, msg_seq_no = Seq},
         {routed, _} = rabbit_amqqueue:deliver([Q], Delivery)
     end || Seq <- Seqs],
    wait_for_confirms(gb_sets:from_list(Seqs)).

wait_for_confirms(Unconfirmed) ->
    case gb_sets:is_empty(Unconfirmed) of
        true  -> ok;
        false -> receive {'$gen_cast', {confirm, Confirmed, _}} ->
                         wait_for_confirms(
                           rabbit_misc:gb_sets_difference(
                             Unconfirmed, gb_sets:from_list(Confirmed)))
                 after 5000 -> exit(timeout_waiting_for_confirm)
                 end
    end.

test_variable_queue() ->
    [passed = with_fresh_variable_queue(F) ||
        F <- [fun test_variable_queue_dynamic_duration_change/1,
              fun test_variable_queue_partial_segments_delta_thing/1,
              fun test_variable_queue_all_the_bits_not_covered_elsewhere1/1,
              fun test_variable_queue_all_the_bits_not_covered_elsewhere2/1,
              fun test_dropwhile/1,
              fun test_dropwhile_varying_ram_duration/1,
              fun test_variable_queue_ack_limiting/1,
              fun test_variable_queue_requeue/1]],
    passed.

test_variable_queue_requeue(VQ0) ->
    Interval = 50,
    Count = rabbit_queue_index:next_segment_boundary(0) + 2 * Interval,
    Seq = lists:seq(1, Count),
    VQ1 = rabbit_variable_queue:set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(false, Count, VQ1),
    {VQ3, Acks} = variable_queue_fetch(Count, false, false, Count, VQ2),
    Subset = lists:foldl(fun ({Ack, N}, Acc) when N rem Interval == 0 ->
                                 [Ack | Acc];
                             (_, Acc) ->
                                 Acc
                         end, [], lists:zip(Acks, Seq)),
    {_MsgIds, VQ4} = rabbit_variable_queue:requeue(Acks -- Subset, VQ3),
    VQ5 = lists:foldl(fun (AckTag, VQN) ->
                              {_MsgId, VQM} = rabbit_variable_queue:requeue(
                                                [AckTag], VQN),
                              VQM
                      end, VQ4, Subset),
    VQ6 = lists:foldl(fun (AckTag, VQa) ->
                              {{#basic_message{}, true, AckTag, _}, VQb} =
                                  rabbit_variable_queue:fetch(true, VQa),
                              VQb
                      end, VQ5, lists:reverse(Acks)),
    {empty, VQ7} = rabbit_variable_queue:fetch(true, VQ6),
    VQ7.

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
    {VQ4, _AckTags} = variable_queue_fetch(Len div 2, false, false, Len, VQ3),

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
    VQ1 = variable_queue_publish(
            false, Count,
            fun (N, Props) -> Props#message_properties{expiry = N} end, VQ0),

    %% drop the first 5 messages
    {undefined, VQ2} = rabbit_variable_queue:dropwhile(
                         fun(#message_properties { expiry = Expiry }) ->
                                 Expiry =< 5
                         end, false, VQ1),

    %% fetch five now
    VQ3 = lists:foldl(fun (_N, VQN) ->
                              {{#basic_message{}, _, _, _}, VQM} =
                                  rabbit_variable_queue:fetch(false, VQN),
                              VQM
                      end, VQ2, lists:seq(6, Count)),

    %% should be empty now
    {empty, VQ4} = rabbit_variable_queue:fetch(false, VQ3),

    VQ4.

test_dropwhile_varying_ram_duration(VQ0) ->
    VQ1 = variable_queue_publish(false, 1, VQ0),
    VQ2 = rabbit_variable_queue:set_ram_duration_target(0, VQ1),
    {undefined, VQ3} = rabbit_variable_queue:dropwhile(
                         fun(_) -> false end, false, VQ2),
    VQ4 = rabbit_variable_queue:set_ram_duration_target(infinity, VQ3),
    VQ5 = variable_queue_publish(false, 1, VQ4),
    {undefined, VQ6} =
        rabbit_variable_queue:dropwhile(fun(_) -> false end, false, VQ5),
    VQ6.

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
    {VQ8, AckTags} = variable_queue_fetch(Len, false, false, Len, VQ7),
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
            %% one segment in q3, and half a segment in delta
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
    {VQ6, AckTags} = variable_queue_fetch(SegmentSize, true, false,
                                          SegmentSize + HalfSegment + 1, VQ5),
    VQ7 = check_variable_queue_status(
            VQ6,
            %% the half segment should now be in q3
            [{q1, 1},
             {delta, {delta, undefined, 0, undefined}},
             {q3, HalfSegment},
             {len, HalfSegment + 1}]),
    {VQ8, AckTags1} = variable_queue_fetch(HalfSegment + 1, true, false,
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
    {VQ4, _AckTags}  = variable_queue_fetch(Count, true, false,
                                            Count + Count, VQ3),
    {VQ5, _AckTags1} = variable_queue_fetch(Count, false, false,
                                            Count, VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
    {{_Msg1, true, _AckTag1, Count1}, VQ8} =
        rabbit_variable_queue:fetch(true, VQ7),
    VQ9 = variable_queue_publish(false, 1, VQ8),
    VQ10 = rabbit_variable_queue:set_ram_duration_target(0, VQ9),
    {VQ11, _AckTags2} = variable_queue_fetch(Count1, true, true, Count, VQ10),
    {VQ12, _AckTags3} = variable_queue_fetch(1, false, false, 1, VQ11),
    VQ12.

test_variable_queue_all_the_bits_not_covered_elsewhere2(VQ0) ->
    VQ1 = rabbit_variable_queue:set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(false, 4, VQ1),
    {VQ3, AckTags} = variable_queue_fetch(2, false, false, 4, VQ2),
    {_Guids, VQ4} =
        rabbit_variable_queue:requeue(AckTags, VQ3),
    VQ5 = rabbit_variable_queue:timeout(VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
    {empty, VQ8} = rabbit_variable_queue:fetch(false, VQ7),
    VQ8.

test_queue_recover() ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(test_queue(), true, false, [], none),
    publish_and_confirm(Q, <<>>, Count),

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
              rabbit_amqqueue:internal_delete(QName, QPid1)
      end),
    passed.

test_variable_queue_delete_msg_store_files_callback() ->
    ok = restart_msg_store_empty(),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(test_queue(), true, false, [], none),
    Payload = <<0:8388608>>, %% 1MB
    Count = 30,
    publish_and_confirm(Q, Payload, Count),

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
