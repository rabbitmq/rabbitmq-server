%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_log_tail).

-export([tail_n_lines/2]).
-export([init_tail_stream/4]).

-define(GUESS_OFFSET, 200).

init_tail_stream(Filename, Pid, Ref, Duration) ->
    RPCProc = self(),
    Reader = spawn(fun() ->
        link(Pid),
        case file:open(Filename, [read, binary]) of
            {ok, File} ->
                TimeLimit = case Duration of
                    infinity -> infinity;
                    _        -> erlang:system_time(second) + Duration
                end,
                {ok, _} = file:position(File, eof),
                RPCProc ! {Ref, opened},
                read_loop(File, Pid, Ref, TimeLimit);
            {error, _} = Err ->
                RPCProc ! {Ref, Err}
        end
    end),
    receive
        {Ref, opened} -> {ok, Ref};
        {Ref, {error, Err}} -> {error, Err}
    after 5000 ->
        exit(Reader, timeout),
        {error, timeout}
    end.

read_loop(File, Pid, Ref, TimeLimit) ->
    case is_integer(TimeLimit) andalso erlang:system_time(second) > TimeLimit of
        true  -> Pid ! {Ref, <<>>, finished};
        false ->
            case file:read(File, ?GUESS_OFFSET) of
                {ok, Data} ->
                    Pid ! {Ref, Data, confinue},
                    read_loop(File, Pid, Ref, TimeLimit);
                eof ->
                    timer:sleep(1000),
                    read_loop(File, Pid, Ref, TimeLimit);
                {error, _} = Err ->
                    Pid ! {Ref, Err, finished}
            end
    end.

tail_n_lines(Filename, N) ->
    case file:open(Filename, [read, binary]) of
        {ok, File} ->
            {ok, Eof} = file:position(File, eof),
            %% Eof may move. Only read up to the current one.
            Result = reverse_read_n_lines(N, N, File, Eof, Eof),
            file:close(File),
            Result;
        {error, _} = Error -> Error
    end.

reverse_read_n_lines(N, OffsetN, File, Position, Eof) ->
    GuessPosition = offset(Position, OffsetN),
    case read_lines_from_position(File, GuessPosition, Eof) of
        {ok, Lines} ->
            NLines = length(Lines),
            case {NLines >= N, GuessPosition == 0} of
                %% Take only N lines if there is more
                {true, _} -> lists:nthtail(NLines - N, Lines);
                %% Safe to assume that NLines is less then N
                {_, true} -> Lines;
                %% Adjust position
                _ ->
                    reverse_read_n_lines(N, N - NLines + 1, File, GuessPosition, Eof)
            end;
        {error, _} = Error -> Error
    end.

read_from_position(File, GuessPosition, Eof) ->
    file:pread(File, GuessPosition, max(0, Eof - GuessPosition)).

read_lines_from_position(File, GuessPosition, Eof) ->
    case read_from_position(File, GuessPosition, Eof) of
        {ok, Data} ->
            Lines = binary:split(Data, <<"\n">>, [global, trim]),
            case {GuessPosition, Lines} of
                %% If position is 0 - there are no partial lines
                {0, _}          -> {ok, Lines};
                %% Remove first line as it can be partial
                {_, [_ | Rest]} -> {ok, Rest};
                {_, []}         -> {ok, []}
            end;
        {error, _} = Error -> Error
    end.

offset(Base, N) ->
    max(0, Base - N * ?GUESS_OFFSET).
