-module(rabbit_cli_io).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/resource.hrl").

-export([start_link/1,
         stop/1,
         argparse_def/1,
         display_help/1,
         format/3,
         start_record_stream/4,
         push_new_record/3,
         end_record_stream/2,
         send_keyboard_input/3,
         read_file/2]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(?MODULE, {progname,
                  record_streams = #{},
                  kbd_reader = undefined,
                  kbd_subscribers = []}).

start_link(Progname) ->
    gen_server:start_link(rabbit_cli_io, #{progname => Progname}, []).

stop(IO) ->
    MRef = erlang:monitor(process, IO),
    _ = gen_server:call(IO, stop),
    receive
        {'DOWN', MRef, _, _, _Reason} ->
            ok
    end.

argparse_def(record_stream) ->
    #{arguments =>
      [
       #{name => output,
         long => "-output",
         short => $o,
         type => string,
         nargs => 1,
         help => "Write output to file <FILE>"},
       #{name => format,
         long => "-format",
         short => $f,
         type => {atom, [plain, json]},
         default => plain,
         help => "Format output acccording to <FORMAT>"}
      ]
     };
argparse_def(file_input) ->
    #{arguments =>
      [
       #{name => input,
         long => "-input",
         short => $i,
         type => string,
         nargs => 1,
         help => "Read input from file <FILE>"}
      ]
     }.

display_help(#{io := {transport, Transport}} = Context) ->
    Transport ! {io_cast, {?FUNCTION_NAME, Context}};
display_help(#{io := IO} = Context) ->
    gen_server:cast(IO, {?FUNCTION_NAME, Context}).

format({transport, Transport}, Format, Args) ->
    Transport ! {io_cast, {?FUNCTION_NAME, Format, Args}};
format(IO, Format, Args) ->
    gen_server:cast(IO, {?FUNCTION_NAME, Format, Args}).

start_record_stream({transport, Transport}, Name, Fields, ArgMap) ->
    Transport ! {io_call, self(), {?FUNCTION_NAME, Name, Fields, ArgMap}},
    receive Ret -> Ret end;
start_record_stream(IO, Name, Fields, ArgMap)
  when is_pid(IO) andalso
       is_atom(Name) andalso
       is_map(ArgMap) ->
    gen_server:call(IO, {?FUNCTION_NAME, Name, Fields, ArgMap}).

push_new_record({transport, Transport}, #{name := Name}, Record) ->
    Transport ! {io_cast, {?FUNCTION_NAME, Name, Record}};
push_new_record(IO, #{name := Name}, Record) ->
    gen_server:cast(IO, {?FUNCTION_NAME, Name, Record}).

end_record_stream({transport, Transport}, #{name := Name}) ->
    Transport ! {io_cast, {?FUNCTION_NAME, Name}};
end_record_stream(IO, #{name := Name}) ->
    gen_server:cast(IO, {?FUNCTION_NAME, Name}).

send_keyboard_input({transport, Transport}, ArgMap, Subscriber) ->
    Transport ! {io_call, self(), {?FUNCTION_NAME, ArgMap, Subscriber}},
    receive Ret -> Ret end;
send_keyboard_input(IO, ArgMap, Subscriber)
  when is_pid(IO) andalso
       is_map(ArgMap) ->
    gen_server:call(IO, {?FUNCTION_NAME, ArgMap, Subscriber}).

read_file({transport, Transport}, ArgMap) ->
    Transport ! {io_call, self(), {?FUNCTION_NAME, ArgMap}},
    receive Ret -> Ret end;
read_file(IO, ArgMap)
  when is_pid(IO) andalso
       is_map(ArgMap) ->
    gen_server:call(IO, {?FUNCTION_NAME, ArgMap}).

init(#{progname := Progname}) ->
    process_flag(trap_exit, true),
    State = #?MODULE{progname = Progname},
    {ok, State}.

handle_call(
  {start_record_stream, Name, Fields, ArgMap},
  From,
  #?MODULE{record_streams = Streams} = State) ->
    Stream = #{name => Name, fields => Fields, arg_map => ArgMap},
    Streams1 = Streams#{Name => Stream},
    State1 = State#?MODULE{record_streams = Streams1},
    gen_server:reply(From, {ok, Stream}),

    {ok, State2} = format_record_stream_start(Name, State1),

    {noreply, State2, compute_timeout(State2)};
handle_call(
  {send_keyboard_input, _ArgMap, Subscriber},
  _From,
  #?MODULE{kbd_subscribers = Subscribers} = State) ->
    Subscribers1 = [Subscriber | Subscribers],
    State1 = State#?MODULE{kbd_subscribers = Subscribers1},
    {reply, ok, State1, compute_timeout(State1)};
handle_call({read_file, ArgMap}, From, State) ->
    {ok, State1} = do_read_file(ArgMap, From, State),
    {noreply, State1, compute_timeout(State1)};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State, compute_timeout(State)}.

handle_cast(
  {display_help, #{cmd_path := CmdPath, argparse_def := ArgparseDef}},
  #?MODULE{progname = Progname} = State) ->
    Options = #{progname => Progname,
                %% Work around bug in argparse;
                %% See https://github.com/erlang/otp/pull/9160
                command => tl(CmdPath)},
    Help = argparse:help(ArgparseDef, Options),
    io:format("~s~n", [Help]),
    {noreply, State, compute_timeout(State)};
handle_cast({format, Format, Args}, State) ->
    io:format(Format, Args),
    {noreply, State, compute_timeout(State)};
handle_cast({push_new_record, Name, Record}, State) ->
    {ok, State1} = format_record(Name, Record, State),
    {noreply, State1, compute_timeout(State1)};
handle_cast({end_record_stream, Name}, State) ->
    {ok, State1} = format_record_stream_end(Name, State),
    {noreply, State1, compute_timeout(State1)};
handle_cast(_Request, State) ->
    {noreply, State, compute_timeout(State)}.

handle_info(timeout, #?MODULE{kbd_reader = Reader} = State)
  when is_pid(Reader) ->
    {noreply, State};
handle_info(timeout, #?MODULE{kbd_subscribers = []} = State) ->
    {noreply, State};
handle_info(timeout, #?MODULE{kbd_subscribers = Subscribers} = State) ->
    Parent = self(),
    Reader = spawn_link(
               fun() ->
                       Ret = io:read(""),
                       lists:foreach(
                         fun(Sub) ->
                                 Sub ! {keypress, Ret}
                         end, Subscribers),
                       erlang:unlink(Parent)
               end, Subscribers),
    State1 = State#?MODULE{kbd_reader = Reader},
    {noreply, State1, compute_timeout(State1)};
handle_info(_Info, State) ->
    {noreply, State, compute_timeout(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

compute_timeout(#?MODULE{kbd_subscribers = []}) ->
    infinity;
compute_timeout(#?MODULE{kbd_subscribers = _}) ->
    0.

format_record_stream_start(
  Name,
  #?MODULE{record_streams = Streams} = State) ->
    Stream = maps:get(Name, Streams),
    format_record_stream_start1(Stream, State).

format_record_stream_start1(
  #{name := Name, fields := Fields, arg_map := #{format := plain}} = Stream,
  #?MODULE{record_streams = Streams} = State) ->
    FieldNames = [atom_to_list(FieldName) || #{name := FieldName} <- Fields],
    FieldWidths = [case Field of
                       #{type := string, name := FieldName} ->
                           lists:max([length(atom_to_list(FieldName)), 20]);
                       #{name := FieldName} ->
                           length(atom_to_list(FieldName))
                   end || Field <- Fields],
    Format0 = [rabbit_misc:format("~~-~b.. ts", [Width])
               || Width <- FieldWidths],
    Format1 = string:join(Format0, " "),
    case isatty(standard_io) of
        true ->
            io:format("\033[1m" ++ Format1 ++ "\033[0m~n", FieldNames);
        false ->
            io:format(Format1 ++ "~n", FieldNames)
    end,
    Stream1 = Stream#{format => Format1},
    Streams1 = Streams#{Name => Stream1},
    State1 = State#?MODULE{record_streams = Streams1},
    {ok, State1};
format_record_stream_start1(
  #{name := Name, arg_map := #{format := json}} = Stream,
  #?MODULE{record_streams = Streams} = State) ->
    Stream1 = Stream#{emitted_fields => 0},
    Streams1 = Streams#{Name => Stream1},
    State1 = State#?MODULE{record_streams = Streams1},
    {ok, State1}.

format_record(Name, Record, #?MODULE{record_streams = Streams} = State) ->
    Stream = maps:get(Name, Streams),
    format_record1(Stream, Record, State).

format_record1(
  #{fields := Fields, arg_map := #{format := plain},
    format := Format},
  Record,
  State) ->
    Values = format_fields(Fields, Record),
    io:format(Format ++ "~n", Values),
    {ok, State};
format_record1(
  #{fields := Fields, arg_map := #{format := json},
    name := Name, emitted_fields := Emitted} = Stream,
  Record,
  #?MODULE{record_streams = Streams} = State) ->
    Fields1 = [FieldName || #{name := FieldName} <- Fields],
    Struct = lists:zip(Fields1, Record),
    Json = json:encode(
             Struct,
             fun
                 ([{_, _} | _] = Value, Encode) ->
                     json:encode_key_value_list(Value, Encode);
                 (Value, Encode) ->
                     json:encode_value(Value, Encode)
             end),
    case Emitted of
        0 ->
            io:format("[~n ~ts", [Json]);
        _ ->
            io:format(",~n ~ts", [Json])
    end,
    Stream1 = Stream#{emitted_fields => Emitted + 1},
    Streams1 = Streams#{Name => Stream1},
    State1 = State#?MODULE{record_streams = Streams1},
    {ok, State1}.

format_record_stream_end(
  Name,
  #?MODULE{record_streams = Streams} = State) ->
    Stream = maps:get(Name, Streams),
    {ok, State1} = format_record_stream_end1(Stream, State),
    #?MODULE{record_streams = Streams1} = State1,
    Streams2 = maps:remove(Name, Streams1),
    State2 = State1#?MODULE{record_streams = Streams2},
    {ok, State2}.

format_record_stream_end1(#{arg_map := #{format := plain}}, State) ->
    {ok, State};
format_record_stream_end1(#{arg_map := #{format := json}}, State) ->
    io:format("~n]~n", []),
    {ok, State}.

format_fields(Fields, Values) ->
    format_fields(Fields, Values, []).

format_fields([#{type := string} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~ts", [Value]),
    Acc1 = [String | Acc],
    format_fields(Rest1, Rest2, Acc1);
format_fields([#{type := binary} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~-20.. ts", [Value]),
    Acc1 = [String | Acc],
    format_fields(Rest1, Rest2, Acc1);
format_fields([#{type := integer} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~b", [Value]),
    Acc1 = [String | Acc],
    format_fields(Rest1, Rest2, Acc1);
format_fields([#{type := boolean} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~ts", [if Value -> "☑"; true -> "☐" end]),
    Acc1 = [String | Acc],
    format_fields(Rest1, Rest2, Acc1);
format_fields([#{type := term} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~0p", [Value]),
    Acc1 = [String | Acc],
    format_fields(Rest1, Rest2, Acc1);
format_fields([], [], Acc) ->
    lists:reverse(Acc).

isatty(IoDevice) ->
    Opts = io:getopts(IoDevice),
    case proplists:get_value(stdout, Opts) of
        true ->
            true;
        _ ->
            false
    end.

do_read_file(#{input := "-"}, From, State) ->
    Ret = read_stdin(<<>>),
    gen:reply(From, Ret),
    {ok, State};
do_read_file(#{input := Filename}, From, State) ->
    Ret = file:read_file(Filename),
    gen:reply(From, Ret),
    {ok, State}.

read_stdin(Buf) ->
    case file:read(standard_io, 4096) of
        {ok, Data} ->
            Buf1 = [Buf, Data],
            read_stdin(Buf1);
        eof ->
            {ok, Buf};
        {error, _} = Error ->
            Error
    end.
