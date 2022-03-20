%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module('Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref([
    {'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}
]).

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
    <<"add_uaa_key <name> [--json=<json_key>] [--pem=<public_key>] [--pem-file=<pem_file>]">>.

switches() ->
    [{json, string},
     {pem, string},
     {pem_file, string}].

aliases() -> [].

validate([], _Options) -> {validation_failure, not_enough_args};
validate([_,_|_], _Options) -> {validation_failure, too_many_args};
validate([_], Options) ->
    Json = maps:get(json, Options, undefined),
    Pem = maps:get(pem, Options, undefined),
    PemFile = maps:get(pem_file, Options, undefined),
    case {is_binary(Json), is_binary(Pem), is_binary(PemFile)} of
        {false, false, false} ->
            {validation_failure,
             {bad_argument, <<"No key specified">>}};
        {true, false, false} ->
            validate_json(Json);
        {false, true, false} ->
            validate_pem(Pem);
        {false, false, true} ->
            validate_pem_file(PemFile);
        {_, _, _} ->
            {validation_failure,
             {bad_argument, <<"There can be only one key type">>}}
    end.

validate_json(Json) ->
    case rabbit_json:try_decode(Json) of
        {ok, _} ->
            case uaa_jwt:verify_signing_key(json, Json) of
                ok -> ok;
                {error, {fields_missing_for_kty, Kty}} ->
                    {validation_failure,
                     {bad_argument,
                      <<"Key fields are missing fot kty \"", Kty/binary, "\"">>}};
                {error, unknown_kty} ->
                    {validation_failure,
                     {bad_argument, <<"\"kty\" field is invalid">>}};
                {error, no_kty} ->
                    {validation_failure,
                     {bad_argument, <<"Json key should contain \"kty\" field">>}};
                {error, Err} ->
                    {validation_failure, {bad_argument, Err}}
            end;
        {error, _}   ->
            {validation_failure, {bad_argument, <<"Invalid JSON">>}};
        error   ->
            {validation_failure, {bad_argument, <<"Invalid JSON">>}}
    end.

validate_pem(Pem) ->
    case uaa_jwt:verify_signing_key(pem, Pem) of
        ok -> ok;
        {error, invalid_pem_string} ->
            {validation_failure, <<"Unable to read a key from the PEM string">>};
        {error, Err} ->
            {validation_failure, Err}
    end.

validate_pem_file(PemFile) ->
    case uaa_jwt:verify_signing_key(pem_file, PemFile) of
        ok -> ok;
        {error, enoent} ->
            {validation_failure, {bad_argument, <<"PEM file not found">>}};
        {error, invalid_pem_file} ->
            {validation_failure, <<"Unable to read a key from the PEM file">>};
        {error, Err} ->
            {validation_failure, Err}
    end.

merge_defaults(Args, #{pem_file := FileName} = Options) ->
    AbsFileName = filename:absname(FileName),
    {Args, Options#{pem_file := AbsFileName}};
merge_defaults(Args, Options) -> {Args, Options}.

banner([Name], #{json := Json}) ->
    <<"Adding UAA signing key \"",
      Name/binary,
      "\" in JSON format: \"",
      Json/binary, "\"">>;
banner([Name], #{pem := Pem}) ->
    <<"Adding UAA signing key \"",
      Name/binary,
      "\" public key: \"",
      Pem/binary, "\"">>;
banner([Name], #{pem_file := PemFile}) ->
    <<"Adding UAA signing key \"",
      Name/binary,
      "\" filename: \"",
      PemFile/binary, "\"">>.

run([Name], #{node := Node} = Options) ->
    {Type, Value} = case Options of
        #{json := Json}        -> {json, Json};
        #{pem := Pem}          -> {pem, Pem};
        #{pem_file := PemFile} -> {pem_file, PemFile}
    end,
    case rabbit_misc:rpc_call(Node,
                              uaa_jwt, add_signing_key,
                              [Name, Type, Value]) of
        {ok, _Keys}  -> ok;
        {error, Err} -> {error, Err}
    end.

output(E, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(E).

formatter() -> 'Elixir.RabbitMQ.CLI.Formatters.Erlang'.







