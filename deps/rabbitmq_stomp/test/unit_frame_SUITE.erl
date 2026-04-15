%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_frame_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("rabbit_stomp_frame.hrl").
-compile(export_all).

all() ->
    [
     max_headers_accepted,
     exceeds_max_headers_rejected,
     exceeds_max_headers_rejected_chunked,
     duplicate_at_limit_boundary_accepted,
     duplicates_do_not_count,
     first_occurrence_wins,
     unique_and_duplicate_mix
    ].

%% Exactly 100 unique headers must be accepted.
max_headers_accepted(_) ->
    {ok, _, _} = parse(make_frame(unique_headers(100))).

%% 101 unique headers must be rejected.
exceeds_max_headers_rejected(_) ->
    ?assertEqual({error, too_many_headers}, parse(make_frame(unique_headers(101)))).

%% The same rejection must occur when the frame arrives in two TCP chunks,
%% verifying that the seen-names map is preserved across chunk boundaries.
exceeds_max_headers_rejected_chunked(_) ->
    Full = make_frame(unique_headers(101)),
    Mid  = byte_size(Full) div 2,
    <<Chunk1:Mid/binary, Chunk2/binary>> = Full,
    {more, Resume} = parse(Chunk1),
    ?assertEqual({error, too_many_headers}, parse(Chunk2, Resume)).

%% When seen-names is at the limit a duplicate must be discarded, not rejected.
%% The duplicate check must fire before the count check.
duplicate_at_limit_boundary_accepted(_) ->
    Headers = unique_headers(100) ++ [{"h1", "dup"}],
    {ok, Frame, _} = parse(make_frame(Headers)),
    ?assertEqual({ok, "v"}, rabbit_stomp_frame:header(Frame, "h1")).

%% Duplicate header names do not count toward the limit.
%% A frame with 200 repetitions of the same name has only one unique name.
duplicates_do_not_count(_) ->
    Headers = [{"h", integer_to_list(I)} || I <- lists:seq(1, 200)],
    {ok, Frame, _} = parse(make_frame(Headers)),
    ?assertEqual({ok, "1"}, rabbit_stomp_frame:header(Frame, "h")).

%% The first occurrence of a header name is kept; later ones are ignored.
first_occurrence_wins(_) ->
    Headers = [{"destination", "/queue/a"}, {"destination", "/queue/b"}],
    {ok, Frame, _} = parse(make_frame(Headers)),
    ?assertEqual({ok, "/queue/a"}, rabbit_stomp_frame:header(Frame, "destination")).

%% 50 unique headers plus any number of duplicates stays within the limit.
unique_and_duplicate_mix(_) ->
    Unique = unique_headers(50),
    Dups   = [{"h1", "dup"} || _ <- lists:seq(1, 100)],
    {ok, _, _} = parse(make_frame(Unique ++ Dups)).

%%-------------------------------------------------------------------

unique_headers(N) ->
    [{lists:flatten(io_lib:format("h~b", [I])), "v"} || I <- lists:seq(1, N)].

make_frame(Headers) ->
    HdrStr = lists:flatten([K ++ ":" ++ V ++ "\n" || {K, V} <- Headers]),
    iolist_to_binary(["CONNECT\n", HdrStr, "\n\0"]).

parse(Bin) ->
    rabbit_stomp_frame:parse(Bin, rabbit_stomp_frame:initial_state()).

parse(Bin, State) ->
    rabbit_stomp_frame:parse(Bin, State).
