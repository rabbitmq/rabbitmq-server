% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_control_pbe).

-export([encode/7]).

% for testing purposes
-export([evaluate_input_as_term/1]).

encode(ListCiphers, _ListHashes, _Decode, _Cipher, _Hash, _Iterations, _Args) when ListCiphers ->
    {ok, io_lib:format("~p", [rabbit_pbe:supported_ciphers()])};

encode(_ListCiphers, ListHashes, _Decode, _Cipher, _Hash, _Iterations, _Args) when ListHashes ->
    {ok, io_lib:format("~p", [rabbit_pbe:supported_hashes()])};

encode(_ListCiphers, _ListHashes, Decode, Cipher, Hash, Iterations, Args) ->
    CipherExists = lists:member(Cipher, rabbit_pbe:supported_ciphers()),
    HashExists = lists:member(Hash, rabbit_pbe:supported_hashes()),
    encode_encrypt_decrypt(CipherExists, HashExists, Decode, Cipher, Hash, Iterations, Args).

encode_encrypt_decrypt(CipherExists, _HashExists, _Decode, _Cipher, _Hash, _Iterations, _Args) when CipherExists =:= false ->
    {error, io_lib:format("The requested cipher is not supported", [])};

encode_encrypt_decrypt(_CipherExists, HashExists, _Decode, _Cipher, _Hash, _Iterations, _Args) when HashExists =:= false ->
    {error, io_lib:format("The requested hash is not supported", [])};

encode_encrypt_decrypt(_CipherExists, _HashExists, _Decode, _Cipher, _Hash, Iterations, _Args) when Iterations =< 0 ->
    {error, io_lib:format("The requested number of iterations is incorrect", [])};

encode_encrypt_decrypt(_CipherExists, _HashExists, Decode, Cipher, Hash, Iterations, Args) when length(Args) == 2, Decode =:= false ->
    [Value, PassPhrase] = Args,
    try begin
            TermValue = evaluate_input_as_term(Value),
            Result = rabbit_pbe:encrypt_term(Cipher, Hash, Iterations, list_to_binary(PassPhrase), TermValue),
            {ok, io_lib:format("~p", [{encrypted, Result}])}
        end
    catch
        _:Msg -> {error, io_lib:format("Error during cipher operation: ~p", [Msg])}
    end;

encode_encrypt_decrypt(_CipherExists, _HashExists, Decode, Cipher, Hash, Iterations, Args) when length(Args) == 2, Decode ->
    [Value, PassPhrase] = Args,
    try begin
            TermValue = evaluate_input_as_term(Value),
            TermToDecrypt = case TermValue of
                {encrypted, EncryptedTerm} ->
                    EncryptedTerm;
                _ ->
                    TermValue
            end,
            Result = rabbit_pbe:decrypt_term(Cipher, Hash, Iterations, list_to_binary(PassPhrase), TermToDecrypt),
            {ok, io_lib:format("~p", [Result])}
        end
    catch
        _:Msg -> {error, io_lib:format("Error during cipher operation: ~p", [Msg])}
    end;

encode_encrypt_decrypt(_CipherExists, _HashExists, _Decode, _Cipher, _Hash, _Iterations, _Args) ->
    {error, io_lib:format("Please provide a value to encode/decode and a passphrase", [])}.

evaluate_input_as_term(Input) ->
    {ok,Tokens,_EndLine} = erl_scan:string(Input ++ "."),
    {ok,AbsForm} = erl_parse:parse_exprs(Tokens),
    {value,TermValue,_Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    TermValue.
