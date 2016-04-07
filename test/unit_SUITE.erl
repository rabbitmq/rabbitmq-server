%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          arguments_parser,
          filtering_flags_parsing,
          {basic_header_handling, [parallel], [
              write_table_with_invalid_existing_type,
              invalid_existing_headers,
              disparate_invalid_header_entries_accumulate_separately,
              corrupt_or_invalid_headers_are_overwritten,
              invalid_same_header_entry_accumulation
            ]},
          content_framing,
          content_transcoding,
          pg_local,
          pmerge,
          plmerge,
          priority_queue,
          {resource_monitor, [parallel], [
              parse_information_unit
            ]},
          {supervisor2, [], [
              check_shutdown_stop,
              check_shutdown_ignored
            ]},
          table_codec,
          {truncate, [parallel], [
              short_examples_exactly,
              term_limit,
              large_examples_for_size
            ]},
          unfold,
          version_equivalance,
          {vm_memory_monitor, [parallel], [
              parse_line_linux
            ]}
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

%% -------------------------------------------------------------------
%% Argument parsing.
%% -------------------------------------------------------------------

arguments_parser(_Config) ->
    GlobalOpts1 = [{"-f1", flag}, {"-o1", {option, "foo"}}],
    Commands1 = [command1, {command2, [{"-f2", flag}, {"-o2", {option, "bar"}}]}],

    GetOptions =
        fun (Args) ->
                rabbit_cli:parse_arguments(Commands1, GlobalOpts1, "-n", Args)
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

check_parse_arguments(ExpRes, Fun, As) ->
    SortRes =
        fun (no_command)          -> no_command;
            ({ok, {C, KVs, As1}}) -> {ok, {C, lists:sort(KVs), As1}}
        end,

    true = SortRes(ExpRes) =:= SortRes(Fun(As)).

filtering_flags_parsing(_Config) ->
    Cases = [{[], [], []}
            ,{[{"--online", true}], ["--offline", "--online", "--third-option"], [false, true, false]}
            ,{[{"--online", true}, {"--third-option", true}, {"--offline", true}], ["--offline", "--online", "--third-option"], [true, true, true]}
            ,{[], ["--offline", "--online", "--third-option"], [true, true, true]}
            ],
    lists:foreach(fun({Vals, Opts, Expect}) ->
                          case rabbit_cli:filter_opts(Vals, Opts) of
                              Expect ->
                                  ok;
                              Got ->
                                  exit({no_match, Got, Expect, {args, Vals, Opts}})
                          end
                  end,
                  Cases).

%% -------------------------------------------------------------------
%% basic_header_handling.
%% -------------------------------------------------------------------

-define(XDEATH_TABLE,
        [{<<"reason">>,       longstr,   <<"blah">>},
         {<<"queue">>,        longstr,   <<"foo.bar.baz">>},
         {<<"exchange">>,     longstr,   <<"my-exchange">>},
         {<<"routing-keys">>, array,     []}]).

-define(ROUTE_TABLE, [{<<"redelivered">>, bool, <<"true">>}]).

-define(BAD_HEADER(K), {<<K>>, longstr, <<"bad ", K>>}).
-define(BAD_HEADER2(K, Suf), {<<K>>, longstr, <<"bad ", K, Suf>>}).
-define(FOUND_BAD_HEADER(K), {<<K>>, array, [{longstr, <<"bad ", K>>}]}).

write_table_with_invalid_existing_type(_Config) ->
    prepend_check(<<"header1">>, ?XDEATH_TABLE, [?BAD_HEADER("header1")]).

invalid_existing_headers(_Config) ->
    Headers =
        prepend_check(<<"header2">>, ?ROUTE_TABLE, [?BAD_HEADER("header2")]),
    {array, [{table, ?ROUTE_TABLE}]} =
        rabbit_misc:table_lookup(Headers, <<"header2">>),
    passed.

disparate_invalid_header_entries_accumulate_separately(_Config) ->
    BadHeaders = [?BAD_HEADER("header2")],
    Headers = prepend_check(<<"header2">>, ?ROUTE_TABLE, BadHeaders),
    Headers2 = prepend_check(<<"header1">>, ?XDEATH_TABLE,
                             [?BAD_HEADER("header1") | Headers]),
    {table, [?FOUND_BAD_HEADER("header1"),
             ?FOUND_BAD_HEADER("header2")]} =
        rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    passed.

corrupt_or_invalid_headers_are_overwritten(_Config) ->
    Headers0 = [?BAD_HEADER("header1"),
                ?BAD_HEADER("x-invalid-headers")],
    Headers1 = prepend_check(<<"header1">>, ?XDEATH_TABLE, Headers0),
    {table,[?FOUND_BAD_HEADER("header1"),
            ?FOUND_BAD_HEADER("x-invalid-headers")]} =
        rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY),
    passed.

invalid_same_header_entry_accumulation(_Config) ->
    BadHeader1 = ?BAD_HEADER2("header1", "a"),
    Headers = prepend_check(<<"header1">>, ?ROUTE_TABLE, [BadHeader1]),
    Headers2 = prepend_check(<<"header1">>, ?ROUTE_TABLE,
                             [?BAD_HEADER2("header1", "b") | Headers]),
    {table, InvalidHeaders} =
        rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    {array, [{longstr,<<"bad header1b">>},
             {longstr,<<"bad header1a">>}]} =
        rabbit_misc:table_lookup(InvalidHeaders, <<"header1">>),
    passed.

prepend_check(HeaderKey, HeaderTable, Headers) ->
    Headers1 = rabbit_basic:prepend_table_header(
                 HeaderKey, HeaderTable, Headers),
    {table, Invalid} =
        rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY),
    {Type, Value} = rabbit_misc:table_lookup(Headers, HeaderKey),
    {array, [{Type, Value} | _]} =
        rabbit_misc:table_lookup(Invalid, HeaderKey),
    Headers1.

%% -------------------------------------------------------------------
%% pg_local.
%% -------------------------------------------------------------------

pg_local(_Config) ->
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

%% -------------------------------------------------------------------
%% priority_queue.
%% -------------------------------------------------------------------

priority_queue(_Config) ->

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

%% ---------------------------------------------------------------------------
%% resource_monitor.
%% ---------------------------------------------------------------------------

parse_information_unit(_Config) ->
    lists:foreach(fun ({S, V}) ->
                          V = rabbit_resource_monitor_misc:parse_information_unit(S)
                  end,
                  [
                   {"1000", {ok, 1000}},

                   {"10kB", {ok, 10000}},
                   {"10MB", {ok, 10000000}},
                   {"10GB", {ok, 10000000000}},

                   {"10kiB", {ok, 10240}},
                   {"10MiB", {ok, 10485760}},
                   {"10GiB", {ok, 10737418240}},

                   {"10k", {ok, 10240}},
                   {"10M", {ok, 10485760}},
                   {"10G", {ok, 10737418240}},

                   {"10KB", {ok, 10000}},
                   {"10K",  {ok, 10240}},
                   {"10m",  {ok, 10485760}},
                   {"10Mb", {ok, 10000000}},

                   {"0MB",  {ok, 0}},

                   {"10 k", {error, parse_error}},
                   {"MB", {error, parse_error}},
                   {"", {error, parse_error}},
                   {"0.5GB", {error, parse_error}},
                   {"10TB", {error, parse_error}}
                  ]),
    passed.

%% ---------------------------------------------------------------------------
%% supervisor2.
%% ---------------------------------------------------------------------------

check_shutdown_stop(_Config) ->
    ok = check_shutdown(stop,    200, 200, 2000).

check_shutdown_ignored(_Config) ->
    ok = check_shutdown(ignored,   1,   2, 2000).

check_shutdown(SigStop, Iterations, ChildCount, SupTimeout) ->
    {ok, Sup} = supervisor2:start_link(dummy_supervisor2, [SupTimeout]),
    Res = lists:foldl(
            fun (I, ok) ->
                    TestSupPid = erlang:whereis(dummy_supervisor2),
                    ChildPids =
                        [begin
                             {ok, ChildPid} =
                                 supervisor2:start_child(TestSupPid, []),
                             ChildPid
                         end || _ <- lists:seq(1, ChildCount)],
                    MRef = erlang:monitor(process, TestSupPid),
                    [P ! SigStop || P <- ChildPids],
                    ok = supervisor2:terminate_child(Sup, test_sup),
                    {ok, _} = supervisor2:restart_child(Sup, test_sup),
                    receive
                        {'DOWN', MRef, process, TestSupPid, shutdown} ->
                            ok;
                        {'DOWN', MRef, process, TestSupPid, Reason} ->
                            {error, {I, Reason}}
                    end;
                (_, R) ->
                    R
            end, ok, lists:seq(1, Iterations)),
    unlink(Sup),
    MSupRef = erlang:monitor(process, Sup),
    exit(Sup, shutdown),
    receive
        {'DOWN', MSupRef, process, Sup, _Reason} ->
            ok
    end,
    Res.

%% ---------------------------------------------------------------------------
%% truncate.
%% ---------------------------------------------------------------------------

short_examples_exactly(_Config) ->
    F = fun (Term, Exp) ->
                Exp = truncate:term(Term, {1, {10, 10, 5, 5}}),
                Term = truncate:term(Term, {100000, {10, 10, 5, 5}})
        end,
    FSmall = fun (Term, Exp) ->
                     Exp = truncate:term(Term, {1, {2, 2, 2, 2}}),
                     Term = truncate:term(Term, {100000, {2, 2, 2, 2}})
             end,
    F([], []),
    F("h", "h"),
    F("hello world", "hello w..."),
    F([[h,e,l,l,o,' ',w,o,r,l,d]], [[h,e,l,l,o,'...']]),
    F([a|b], [a|b]),
    F(<<"hello">>, <<"hello">>),
    F([<<"hello world">>], [<<"he...">>]),
    F(<<1:1>>, <<1:1>>),
    F(<<1:81>>, <<0:56, "...">>),
    F({{{{a}}},{b},c,d,e,f,g,h,i,j,k}, {{{'...'}},{b},c,d,e,f,g,h,i,j,'...'}),
    FSmall({a,30,40,40,40,40}, {a,30,'...'}),
    FSmall([a,30,40,40,40,40], [a,30,'...']),
    P = spawn(fun() -> receive die -> ok end end),
    F([0, 0.0, <<1:1>>, F, P], [0, 0.0, <<1:1>>, F, P]),
    P ! die,
    R = make_ref(),
    F([R], [R]),
    ok.

term_limit(_Config) ->
    W = erlang:system_info(wordsize),
    S = <<"abc">>,
    1 = truncate:term_size(S, 4, W),
    limit_exceeded = truncate:term_size(S, 3, W),
    case 100 - truncate:term_size([S, S], 100, W) of
        22 -> ok; %% 32 bit
        38 -> ok  %% 64 bit
    end,
    case 100 - truncate:term_size([S, [S]], 100, W) of
        30 -> ok; %% ditto
        54 -> ok
    end,
    limit_exceeded = truncate:term_size([S, S], 6, W),
    ok.

large_examples_for_size(_Config) ->
    %% Real world values
    Shrink = fun(Term) -> truncate:term(Term, {1, {1000, 100, 50, 5}}) end,
    TestSize = fun(Term) ->
                       true = 5000000 < size(term_to_binary(Term)),
                       true = 500000 > size(term_to_binary(Shrink(Term)))
               end,
    TestSize(lists:seq(1, 5000000)),
    TestSize(recursive_list(1000, 10)),
    TestSize(recursive_list(5000, 20)),
    TestSize(gb_sets:from_list([I || I <- lists:seq(1, 1000000)])),
    TestSize(gb_trees:from_orddict([{I, I} || I <- lists:seq(1, 1000000)])),
    ok.

recursive_list(S, 0) -> lists:seq(1, S);
recursive_list(S, N) -> [recursive_list(S div N, N-1) || _ <- lists:seq(1, S)].

%% ---------------------------------------------------------------------------
%% vm_memory_monitor.
%% ---------------------------------------------------------------------------

parse_line_linux(_Config) ->
    lists:foreach(fun ({S, {K, V}}) ->
                          {K, V} = vm_memory_monitor:parse_line_linux(S)
                  end,
                  [{"MemTotal:      0 kB",        {'MemTotal', 0}},
                   {"MemTotal:      502968 kB  ", {'MemTotal', 515039232}},
                   {"MemFree:         178232 kB", {'MemFree',  182509568}},
                   {"MemTotal:         50296888", {'MemTotal', 50296888}},
                   {"MemTotal         502968 kB", {'MemTotal', 515039232}},
                   {"MemTotal     50296866   ",   {'MemTotal', 50296866}}]),
    ok.

%% ---------------------------------------------------------------------------
%% Unordered tests (originally from rabbit_tests.erl).
%% ---------------------------------------------------------------------------

%% Test that content frames don't exceed frame-max
content_framing(_Config) ->
    %% no content
    passed = test_content_framing(4096, <<>>),
    %% easily fit in one frame
    passed = test_content_framing(4096, <<"Easy">>),
    %% exactly one frame (empty frame = 8 bytes)
    passed = test_content_framing(11, <<"One">>),
    %% more than one frame
    passed = test_content_framing(11, <<"More than one frame">>),
    passed.

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

content_transcoding(_Config) ->
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

pmerge(_Config) ->
    P = [{a, 1}, {b, 2}],
    P = rabbit_misc:pmerge(a, 3, P),
    [{c, 3} | P] = rabbit_misc:pmerge(c, 3, P),
    passed.

plmerge(_Config) ->
    P1 = [{a, 1}, {b, 2}, {c, 3}],
    P2 = [{a, 2}, {d, 4}],
    [{a, 1}, {b, 2}, {c, 3}, {d, 4}] = rabbit_misc:plmerge(P1, P2),
    passed.

table_codec(_Config) ->
    %% FIXME this does not test inexact numbers (double and float) yet,
    %% because they won't pass the equality assertions
    Table = [{<<"longstr">>,   longstr,   <<"Here is a long string">>},
             {<<"signedint">>, signedint, 12345},
             {<<"decimal">>,   decimal,   {3, 123456}},
             {<<"timestamp">>, timestamp, 109876543209876},
             {<<"table">>,     table,     [{<<"one">>, signedint, 54321},
                                           {<<"two">>, longstr,
                                            <<"A long string">>}]},
             {<<"byte">>,      byte,      -128},
             {<<"long">>,      long,      1234567890},
             {<<"short">>,     short,     655},
             {<<"bool">>,      bool,      true},
             {<<"binary">>,    binary,    <<"a binary string">>},
             {<<"unsignedbyte">>, unsignedbyte, 250},
             {<<"unsignedshort">>, unsignedshort, 65530},
             {<<"unsignedint">>, unsignedint, 4294967290},
             {<<"void">>,      void,      undefined},
             {<<"array">>,     array,     [{signedint, 54321},
                                           {longstr, <<"A long string">>}]}
            ],
    Binary = <<
               7,"longstr",   "S", 21:32, "Here is a long string",
               9,"signedint", "I", 12345:32/signed,
               7,"decimal",   "D", 3, 123456:32,
               9,"timestamp", "T", 109876543209876:64,
               5,"table",     "F", 31:32, % length of table
               3,"one",       "I", 54321:32,
               3,"two",       "S", 13:32, "A long string",
               4,"byte",      "b", -128:8/signed,
               4,"long",      "l", 1234567890:64,
               5,"short",     "s", 655:16,
               4,"bool",      "t", 1,
               6,"binary",    "x", 15:32, "a binary string",
               12,"unsignedbyte", "B", 250:8/unsigned,
               13,"unsignedshort", "u", 65530:16/unsigned,
               11,"unsignedint", "i", 4294967290:32/unsigned,
               4,"void",      "V",
               5,"array",     "A", 23:32,
               "I", 54321:32,
               "S", 13:32, "A long string"
             >>,
    Binary = rabbit_binary_generator:generate_table(Table),
    Table  = rabbit_binary_parser:parse_table(Binary),
    passed.

unfold(_Config) ->
    {[], test} = rabbit_misc:unfold(fun (_V) -> false end, test),
    List = lists:seq(2,20,2),
    {List, 0} = rabbit_misc:unfold(fun (0) -> false;
                                       (N) -> {true, N*2, N-1}
                                   end, 10),
    passed.

version_equivalance(_Config) ->
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.0"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.1"),
    true = rabbit_misc:version_minor_equivalent("%%VSN%%", "%%VSN%%"),
    false = rabbit_misc:version_minor_equivalent("3.0.0", "3.1.0"),
    false = rabbit_misc:version_minor_equivalent("3.0.0", "3.0"),
    false = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.0.1"),
    false = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.foo"),
    passed.
