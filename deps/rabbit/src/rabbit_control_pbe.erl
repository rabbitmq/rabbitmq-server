%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_control_pbe).

-export([decode/4, encode/4, list_ciphers/0, list_hashes/0]).

% for testing purposes
-export([evaluate_input_as_term/1]).

list_ciphers() ->
    {ok, io_lib:format("~tp", [rabbit_pbe:supported_ciphers()])}.

list_hashes() ->
    {ok, io_lib:format("~tp", [rabbit_pbe:supported_hashes()])}.

validate(_Cipher, _Hash, Iterations, _Args) when Iterations =< 0 ->
    {error, io_lib:format("The requested number of iterations is incorrect", [])};
validate(_Cipher, _Hash, _Iterations, Args) when length(Args) < 2 ->
    {error, io_lib:format("Please provide a value to encode/decode and a passphrase", [])};
validate(_Cipher, _Hash, _Iterations, Args) when length(Args) > 2 ->
    {error, io_lib:format("Too many arguments. Please provide a value to encode/decode and a passphrase", [])};
validate(Cipher, Hash, _Iterations, _Args) ->
    case lists:member(Cipher, rabbit_pbe:supported_ciphers()) of
        false ->
            {error, io_lib:format("The requested cipher is not supported", [])};
        true ->
            case lists:member(Hash, rabbit_pbe:supported_hashes()) of
                false ->
                    {error, io_lib:format("The requested hash is not supported", [])};
                true -> ok
            end
    end.

encode(Cipher, Hash, Iterations, Args) ->
    case validate(Cipher, Hash, Iterations, Args) of
        {error, Err} -> {error, Err};
        ok ->
            [Value, PassPhrase] = Args,
            try begin
                    TermValue = evaluate_input_as_term(Value),
                    Result = {encrypted, _} = rabbit_pbe:encrypt_term(Cipher, Hash, Iterations,
                                                                      list_to_binary(PassPhrase), TermValue),
                    {ok, io_lib:format("~tp", [Result])}
                end
            catch
                _:Msg -> {error, io_lib:format("Error during cipher operation: ~tp", [Msg])}
            end
    end.

decode(Cipher, Hash, Iterations, Args) ->
    case validate(Cipher, Hash, Iterations, Args) of
        {error, Err} -> {error, Err};
        ok ->
            [Value, PassPhrase] = Args,
            try begin
                    TermValue = evaluate_input_as_term(Value),
                    TermToDecrypt = case TermValue of
                        {encrypted, _}=EncryptedTerm ->
                            EncryptedTerm;
                        _ ->
                            {encrypted, TermValue}
                    end,
                    Result = rabbit_pbe:decrypt_term(Cipher, Hash, Iterations,
                                                     list_to_binary(PassPhrase),
                                                     TermToDecrypt),
                    {ok, io_lib:format("~tp", [Result])}
                end
            catch
                _:Msg -> {error, io_lib:format("Error during cipher operation: ~tp", [Msg])}
            end
    end.

evaluate_input_as_term(Input) ->
    {ok,Tokens,_EndLine} = erl_scan:string(Input ++ "."),
    {ok,AbsForm} = erl_parse:parse_exprs(Tokens),
    {value,TermValue,_Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    TermValue.
