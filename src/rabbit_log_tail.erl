-module(rabbit_log_tail).

-export([tail_n_lines/2]).
-export([init_tail_stream/3]).

-define(GUESS_OFFSET, 200).

init_tail_stream(Filename, Pid, Ref) ->
    RPCProc = self(),
    Reader = spawn(fun() ->
        link(Pid),
        case file:open(Filename, [read, binary]) of
            {ok, File} ->
                {ok, _} = file:position(File, eof),
                RPCProc ! {Ref, opened},
                read_loop(File, Pid, Ref);
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

read_loop(File, Pid, Ref) ->
    case file:read(File, ?GUESS_OFFSET) of
        {ok, Data} ->
            Pid ! {Ref, Data, confinue},
            read_loop(File, Pid, Ref);
        eof ->
            timer:sleep(1000),
            read_loop(File, Pid, Ref);
        {error, _} = Err ->
            Pid ! {Ref, Err, finished}
    end.

tail_n_lines(Filename, N) ->
    case file:open(Filename, [read, binary]) of
        {ok, File} ->
            {ok, Eof} = file:position(File, eof),
            %% Eof may move. Only read up to the current one.
            Result = reverse_read_n_lines(N, N, File, Eof, Eof),
            file:close(File),
            Result;
        Error -> Error
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
        Error -> Error
    end.

read_from_position(File, GuessPosition, Eof) ->
    file:pread(File, GuessPosition, max(0, Eof - GuessPosition)).

read_lines_from_position(File, GuessPosition, Eof) ->
    case read_from_position(File, GuessPosition, Eof) of
        {ok, Data} -> {ok, crop_lines(Data)};
        Error      -> Error
    end.

crop_lines(Data) ->
    %% Drop the first line, because it's most likely partial.
    case binary:split(Data, <<"\n">>, [global, trim]) of
        [_|Rest] -> Rest;
        [] -> []
    end.

offset(Base, N) ->
    max(0, Base - N * ?GUESS_OFFSET).