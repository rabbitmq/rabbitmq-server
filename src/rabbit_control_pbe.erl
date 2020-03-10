% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_control_pbe).

-export([decode/4, encode/4, list_ciphers/0, list_hashes/0]).

% for testing purposes
-export([evaluate_input_as_term/1]).

list_ciphers() ->
    {ok, io_lib:format("~p", [rabbit_pbe:supported_ciphers()])}.

list_hashes() ->
    {ok, io_lib:format("~p", [rabbit_pbe:supported_hashes()])}.

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
                    Result = rabbit_pbe:encrypt_term(Cipher, Hash, Iterations,
                                                     list_to_binary(PassPhrase),
                                                     TermValue),
                    {ok, io_lib:format("~p", [{encrypted, Result}])}
                end
            catch
                _:Msg -> {error, io_lib:format("Error during cipher operation: ~p", [Msg])}
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
                        {encrypted, EncryptedTerm} ->
                            EncryptedTerm;
                        _ ->
                            TermValue
                    end,
                    Result = rabbit_pbe:decrypt_term(Cipher, Hash, Iterations,
                                                     list_to_binary(PassPhrase),
                                                     TermToDecrypt),
                    {ok, io_lib:format("~p", [Result])}
                end
            catch
                _:Msg -> {error, io_lib:format("Error during cipher operation: ~p", [Msg])}
            end
    end.

evaluate_input_as_term(Input) ->
    {ok,Tokens,_EndLine} = erl_scan:string(Input ++ "."),
    {ok,AbsForm} = erl_parse:parse_exprs(Tokens),
    {value,TermValue,_Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    TermValue.
