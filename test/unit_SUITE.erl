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

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          {resource_monitor, [parallel], [
              parse_information_unit
            ]},
          {supervisor2, [], [
              check_shutdown_stop,
              check_shutdown_ignored
            ]},
          {truncate, [parallel], [
              short_examples_exactly,
              term_limit,
              large_examples_for_size
            ]},
          {vm_memory_monitor, [parallel], [
              parse_line_linux
            ]}
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

%% ---------------------------------------------------------------------------
%% rabbit_resource_monitor.
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
