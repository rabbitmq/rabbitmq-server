%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(prop_frame_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_stomp_frame.hrl").
-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_proper_helpers, [run_proper/3]).

%% Default max_headers is 100
-define(LIMIT, 100).

all() ->
    [{group, properties}].

groups() ->
    [{properties, [parallel],
      [
       prop_within_limit_succeeds,
       prop_over_limit_fails,
       prop_duplicates_do_not_exhaust_limit,
       prop_negative_content_length_rejected,
       prop_valid_content_length_round_trips,
       prop_non_numeric_content_length_does_not_crash,
       prop_round_trip,
       prop_incremental_parse,
       prop_max_body_length_enforced
      ]}].

%% Any frame with up to LIMIT unique header names parses successfully.
prop_within_limit_succeeds(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(N, range(0, ?LIMIT),
              begin
                  {ok, _, _} = parse(make_frame(unique_headers(N))),
                  true
              end)
      end, [], 1000).

%% Any frame with more than LIMIT unique header names is rejected.
prop_over_limit_fails(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(N, range(?LIMIT + 1, 300),
              begin
                  {error, {max_headers, ?LIMIT}} = parse(make_frame(unique_headers(N))),
                  true
              end)
      end, [], 1000).

%% Duplicate entries for a header name already seen never trigger the limit.
%% Duplicates are discarded at O(1) with zero allocation.
prop_duplicates_do_not_exhaust_limit(_Config) ->
    run_proper(
      fun() ->
          ?FORALL({Unique, Dups}, {range(1, 50), range(0, 500)},
              begin
                  Headers = unique_headers(Unique) ++
                            [{"h1", "dup"} || _ <- lists:seq(1, Dups)],
                  {ok, _, _} = parse(make_frame(Headers)),
                  true
              end)
      end, [], 1000).

%% Any negative content-length is rejected.
prop_negative_content_length_rejected(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(N, range(-10000, -1),
              begin
                  {error, {invalid_content_length, N}} =
                      parse(send_frame("content-length", integer_to_list(N), "x")),
                  true
              end)
      end, [], 1000).

%% A SEND frame with a matching content-length and body always parses.
prop_valid_content_length_round_trips(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(Body, binary(),
              begin
                  Len = integer_to_list(byte_size(Body)),
                  case parse(send_frame("content-length", Len, Body)) of
                      {ok, #stomp_frame{command = 'SEND'}, _} -> true;
                      {more, _} -> true
                  end
              end)
      end, [], 1000).

%% Non-numeric content-length values must never crash the parser.
prop_non_numeric_content_length_does_not_crash(_Config) ->
    run_proper(
      fun() ->
          ?FORALL(Junk, non_numeric_bin(),
              begin
                  case parse(send_frame("content-length", binary_to_list(Junk), "x")) of
                      {ok, _, _} -> true;
                      {more, _}  -> true;
                      {error, _} -> true
                  end
              end)
      end, [], 1000).

%% Serialize then parse preserves command, headers, and body.
%% Headers with escape-triggering characters (colon, backslash, LF, CR)
%% exercise both the fast and slow parser paths.
prop_round_trip(_Config) ->
    run_proper(
      fun() ->
          ?FORALL({RawPairs, Body},
                  {resize(5, list({stomp_hdr_name(), stomp_hdr_value()})),
                   resize(200, binary())},
              begin
                  Hdrs = maps:from_list(
                           [{<<"destination">>, <<"/queue/t">>} |
                            [{K, V} || {K, V} <- RawPairs,
                             K =/= <<"content-length">>,
                             K =/= <<"destination">>]]),
                  Frame = #stomp_frame{command = 'SEND',
                                       headers = Hdrs,
                                       body_iolist_rev = Body},
                  Bin = iolist_to_binary(rabbit_stomp_frame:serialize(Frame)),
                  {ok, Parsed, _} = parse(Bin),
                  Parsed#stomp_frame.command =:= 'SEND' andalso
                  lists:all(
                    fun({K, V}) ->
                        maps:get(K, Parsed#stomp_frame.headers, undefined) =:= V
                    end, maps:to_list(Hdrs)) andalso
                  Body =:= body_to_binary(Parsed)
              end)
      end, [], 500).

%% Splitting a valid frame at any byte boundary and parsing in two
%% chunks must produce the same frame as parsing in one call.
prop_incremental_parse(_Config) ->
    run_proper(
      fun() ->
          ?FORALL({Body, N},
                  {resize(200, binary()), non_neg_integer()},
              begin
                  Bin = iolist_to_binary(
                          rabbit_stomp_frame:serialize(
                            #stomp_frame{command = 'SEND',
                                         headers = #{<<"destination">> => <<"/queue/t">>},
                                         body_iolist_rev = Body})),
                  Pos = N rem (byte_size(Bin) + 1),
                  <<C1:Pos/binary, C2/binary>> = Bin,
                  {ok, Full, _} = parse(Bin),
                  Chunked = case parse(C1) of
                      {ok, F, _} -> F;
                      {more, St} ->
                          {ok, F2, _} = rabbit_stomp_frame:parse(C2, St),
                          F2
                  end,
                  Full#stomp_frame.command =:= Chunked#stomp_frame.command andalso
                  Full#stomp_frame.headers =:= Chunked#stomp_frame.headers andalso
                  body_to_binary(Full) =:= body_to_binary(Chunked)
              end)
      end, [], 1000).

%% A SEND frame exceeding max_body_length is always rejected.
prop_max_body_length_enforced(_Config) ->
    run_proper(
      fun() ->
          ?FORALL({MaxLen, BodySize},
                  {range(1, 500), range(0, 1000)},
              begin
                  Body = binary:copy(<<"x">>, BodySize),
                  Bin = iolist_to_binary(
                          rabbit_stomp_frame:serialize(
                            #stomp_frame{command = 'SEND',
                                         headers = #{<<"destination">> => <<"/queue/t">>},
                                         body_iolist_rev = Body})),
                  Config = #stomp_parser_config{max_body_length = MaxLen},
                  case rabbit_stomp_frame:parse(Bin, rabbit_stomp_frame:initial_state(Config)) of
                      {ok, _, _} -> BodySize =< MaxLen;
                      {error, _} -> BodySize > MaxLen
                  end
              end)
      end, [], 1000).

%%-------------------------------------------------------------------

unique_headers(N) ->
    [{lists:flatten(io_lib:format("h~b", [I])), "v"} || I <- lists:seq(1, N)].

make_frame(Headers) ->
    HdrStr = lists:flatten([K ++ ":" ++ V ++ "\n" || {K, V} <- Headers]),
    iolist_to_binary(["CONNECT\n", HdrStr, "\n\0"]).

send_frame(HdrName, HdrValue, Body) ->
    iolist_to_binary(["SEND\ndestination:/queue/t\n",
                      HdrName, ":", HdrValue, "\n\n",
                      Body, "\0"]).

%% Produces binaries that are not valid integer strings.
non_numeric_bin() ->
    ?SUCHTHAT(Bin,
              ?LET(Chars, list(range(0, 255)),
                   list_to_binary(
                     [C || C <- Chars, C =/= $\n, C =/= $\r, C =/= $:, C =/= $\\, C =/= 0])),
              not is_integer(catch binary_to_integer(string:trim(Bin)))).

%% Produces non-empty, no NUL header names. Biased towards escaped chars.
stomp_hdr_name() ->
    ?SUCHTHAT(Bin,
              ?LET(Chars, resize(15, non_empty(list(stomp_char()))),
                   list_to_binary(Chars)),
              Bin =/= <<"content-length">> andalso
              Bin =/= <<"destination">>).

%% Produces no-NUL header values. Biased towards escaped chars.
stomp_hdr_value() ->
    ?LET(Chars, resize(20, list(stomp_char())),
         list_to_binary(Chars)).

%% Biased towards characters that trigger the escape slow path.
stomp_char() ->
    frequency([{3, $:}, {3, $\\}, {3, $\n}, {3, $\r},
               {88, range(32, 126)}]).

body_to_binary(#stomp_frame{body_iolist_rev = Rev}) when is_list(Rev) ->
    iolist_to_binary(lists:reverse(Rev));
body_to_binary(#stomp_frame{body_iolist_rev = Bin}) when is_binary(Bin) ->
    Bin.

parse(Bin) ->
    rabbit_stomp_frame:parse(Bin, rabbit_stomp_frame:initial_state()).
