-module('Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([
         usage/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         switches/0,
         aliases/0,
         output/2,
         formatter/0
        ]).


usage() ->
    <<"add_uaa_key <name> [--json=<json_key>] [--pem=<pem_file>]">>.

switches() ->
    [{json, string},
     {pem, string}].

aliases() -> [].

validate([], _Options) -> {validation_failure, not_enough_args};
validate([_,_|_], _Options) -> {validation_failure, too_many_args};
validate([_], Options) ->
    Json = maps:get(json, Options, undefined),
    Pem = maps:get(pem, Options, undefined),
    case {is_binary(Json), is_binary(Pem)} of
        {false, false} ->
            {validation_failure,
             {bad_argument, <<"No key specified">>}};
        {true, true} ->
            {validation_failure,
             {bad_argument, <<"Key type should be either json or pem">>}};
        {true, false} ->
            validat_json(Json);
        {false, true} ->
            validate_pem_file(Pem)
    end.

validat_json(Json) ->
    case rabbit_json:try_decode(Json) of
        {ok, _} -> ok;
        error   -> {validation_failure, {bad_argument, "Invalid JSON"}}
    end.

validate_pem_file(Pem) ->
    {ok, PemBin} = file:read_file(Pem),
    case public_key:pem_decode(PemBin) of
        []  -> {validation_failure, "Unable to read certificate from PEM file"};
        [_] -> ok;
        _   -> {validation_failure, "PEM file should contain a single key"}
    end.


merge_defaults(Args, #{pem := FileName} = Options) ->
    AbsFileName = filename:absname(FileName),
    {Args, Options#{pem := AbsFileName}};
merge_defaults(Args, Options) -> {Args, Options}.

banner([Name], #{json := Json}) ->
    <<"Adding UAA signing key \"",
      Name/binary,
      "\" in JSON format: \"",
      Json/binary, "\"">>;
banner([Name], #{pem := Pem}) ->
    <<"Adding UAA signing key \"",
      Name/binary,
      "\" filename: \"",
      Pem/binary, "\"">>.

run([Name], #{node := Node} = Options) ->
    {Type, Value} = case Options of
        #{json := Json} -> {json, Json};
        #{pem := Pem}   -> {pem, Pem}
    end,
    case rabbit_misc:rpc_call(Node,
                              'Elixir.UaaJWT', add_signing_key,
                              [Name, Type, Value]) of
        {ok, _Keys}  -> ok;
        {error, Err} -> {error, Err}
    end.

output(E, Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(E, Opts, ?MODULE).

formatter() -> 'Elixir.RabbitMQ.CLI.Formatters.Erlang'.







