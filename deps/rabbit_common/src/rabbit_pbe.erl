%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_pbe).

-export([supported_ciphers/0, supported_hashes/0, default_cipher/0, default_hash/0, default_iterations/0]).
-export([encrypt_term/5, decrypt_term/5]).
-export([encrypt/5, decrypt/5]).
-export([encrypt/2, decrypt/2]).

-type encryptable_input() :: iodata() | '$pending-secret'.

-export_type([encryptable_input/0, encryption_result/0]).

supported_ciphers() ->
    credentials_obfuscation_pbe:supported_ciphers().

supported_hashes() ->
    credentials_obfuscation_pbe:supported_hashes().

%% Default encryption parameters.
default_cipher() ->
    credentials_obfuscation_pbe:default_cipher().

default_hash() ->
    credentials_obfuscation_pbe:default_hash().

default_iterations() ->
    credentials_obfuscation_pbe:default_iterations().

%% Encryption/decryption of arbitrary Erlang terms.

encrypt_term(Cipher, Hash, Iterations, PassPhrase, Term) ->
    credentials_obfuscation_pbe:encrypt_term(Cipher, Hash, Iterations, PassPhrase, Term).

decrypt_term(_Cipher, _Hash, _Iterations, _PassPhrase, {plaintext, Term}) ->
    Term;
decrypt_term(Cipher, Hash, Iterations, PassPhrase, {encrypted, _Base64Binary}=Encrypted) ->
    credentials_obfuscation_pbe:decrypt_term(Cipher, Hash, Iterations, PassPhrase, Encrypted).

-type encryption_result() :: {'encrypted', binary()} | {'plaintext', binary()}.

%% crypto:cipher() and crypto:hash_algorithm() are not public
-type crypto_cipher() :: atom().
-type crypto_hash_algorithm() :: atom().

-spec encrypt(crypto_cipher(), crypto_hash_algorithm(),
    pos_integer(), encryptable_input(), binary()) -> encryption_result().
encrypt(Cipher, Hash, Iterations, PassPhrase, ClearText) ->
    credentials_obfuscation_pbe:encrypt(Cipher, Hash, Iterations, PassPhrase, ClearText).


-spec decrypt(crypto_cipher(), crypto_hash_algorithm(),
    pos_integer(), iodata(), encryption_result()) -> any().
decrypt(_Cipher, _Hash, _Iterations, _PassPhrase, {plaintext, Term}) ->
    Term;
decrypt(Cipher, Hash, Iterations, PassPhrase, {encrypted, _Base64Binary}=Encrypted) ->
    credentials_obfuscation_pbe:decrypt(Cipher, Hash, Iterations, PassPhrase, Encrypted).


-spec encrypt(encryptable_input(), binary()) -> encryption_result().
encrypt(PassPhrase, ClearText) ->
    credentials_obfuscation_pbe:encrypt(default_cipher(), default_hash(), default_iterations(), PassPhrase, ClearText).


-spec decrypt(iodata(), encryption_result()) -> any().
decrypt(_PassPhrase, {plaintext, Term}) ->
    Term;
decrypt(PassPhrase, {encrypted, _Base64Binary}=Encrypted) ->
    credentials_obfuscation_pbe:decrypt(default_cipher(), default_hash(), default_iterations(), PassPhrase, Encrypted).
