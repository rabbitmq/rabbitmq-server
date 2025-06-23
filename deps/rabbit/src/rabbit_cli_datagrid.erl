-module(rabbit_cli_datagrid).

-include_lib("kernel/include/logger.hrl").

-include("src/rabbit_cli_backend.hrl").

-export([argparse_def/0,
         process/6]).

-record(?MODULE, {fields_fun,
                  setup_stream_fun,
                  next_record_fun,
                  teardown_stream_fun,

                  fields,
                  priv,
                  context}).

argparse_def() ->
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
         type => {atom, [plain, legacy_plain, json]},
         default => plain,
         help => "Format output acccording to <FORMAT>"},
       #{name => fields,
         type => binary,
         nargs => list,
         required => false,
         help => "Fields to include"}
      ]
     }.

process(
  FieldsFun, SetupStreamFun, NextRecordFun, TeardownStreamFun,
  Priv, Context) ->
    State = #?MODULE{fields_fun = FieldsFun,
                     setup_stream_fun = SetupStreamFun,
                     next_record_fun = NextRecordFun,
                     teardown_stream_fun = TeardownStreamFun,

                     priv = Priv,
                     context = Context},
    process_fields(State).

process_fields(#?MODULE{fields_fun = FieldsFun, priv = Priv} = State) ->
    maybe
        {ok, Fields, Priv1} ?= FieldsFun(Priv),
        State1 = State#?MODULE{fields = Fields,
                               priv = Priv1},

        {ok, State2} ?= format_fields(State1),
        start_stream(State2)
    end.

start_stream(
  #?MODULE{setup_stream_fun = SetupStreamFun, priv = Priv} = State) ->
    maybe
        {ok, Priv1} ?= SetupStreamFun(Priv),
        State1 = State#?MODULE{priv = Priv1},
        process_records(State1)
    end.

process_records(
  #?MODULE{next_record_fun = NextRecordFun, priv = Priv} = State) ->
    case NextRecordFun(Priv) of
        {ok, Record, Priv1} when is_list(Record) ->
            maybe
                State1 = State#?MODULE{priv = Priv1},
                {ok, State2} ?= format_record(Record, State1),
                process_records(State2)
            end;
        {ok, none, Priv1} ->
            State1 = State#?MODULE{priv = Priv1},
            stop_stream(State1)
    end.

stop_stream(#?MODULE{teardown_stream_fun = TeardownStreamFun, priv = Priv}) ->
    TeardownStreamFun(Priv).

format_fields(#?MODULE{fields = Fields} = State) ->
    Fields1 = [rabbit_misc:format("~s", [Name]) || #{name := Name} <- Fields],
    Fields2 = string:join(Fields1, "\t"),
    io:format("~ts~n", [Fields2]),
    {ok, State}.

format_record(Record, #?MODULE{fields = Fields} = State) ->
    Values1 = format_values(Fields, Record),
    Values2 = string:join(Values1, "\t"),
    io:format("~ts~n", [Values2]),
    {ok, State}.

format_values(Fields, Values) ->
    format_values(Fields, Values, []).

format_values([#{type := string} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~ts", [Value]),
    Acc1 = [String | Acc],
    format_values(Rest1, Rest2, Acc1);
format_values([#{type := binary} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~-20.. ts", [Value]),
    Acc1 = [String | Acc],
    format_values(Rest1, Rest2, Acc1);
format_values([#{type := integer} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~b", [Value]),
    Acc1 = [String | Acc],
    format_values(Rest1, Rest2, Acc1);
format_values([#{type := boolean} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~ts", [if Value -> "☑"; true -> "☐" end]),
    Acc1 = [String | Acc],
    format_values(Rest1, Rest2, Acc1);
format_values([#{type := term} | Rest1], [Value | Rest2], Acc) ->
    String = io_lib:format("~0p", [Value]),
    Acc1 = [String | Acc],
    format_values(Rest1, Rest2, Acc1);
format_values([], [], Acc) ->
    lists:reverse(Acc).
