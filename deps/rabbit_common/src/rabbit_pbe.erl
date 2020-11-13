%% The contents of this file are subject to the Mozilla Public License
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
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_pbe).

-export([supported_ciphers/0, supported_hashes/0, default_cipher/0, default_hash/0, default_iterations/0]).
-export([encrypt_term/5, decrypt_term/5]).
-export([encrypt/5, decrypt/5]).

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

decrypt_term(Cipher, Hash, Iterations, PassPhrase, Base64Binary) ->
    credentials_obfuscation_pbe:decrypt_term(Cipher, Hash, Iterations, PassPhrase, Base64Binary).

-spec encrypt(crypto:block_cipher(), crypto:hash_algorithms(),
    pos_integer(), iodata(), binary()) -> binary().
encrypt(Cipher, Hash, Iterations, PassPhrase, ClearText) ->
    credentials_obfuscation_pbe:encrypt(Cipher, Hash, Iterations, PassPhrase, ClearText).

-spec decrypt(crypto:block_cipher(), crypto:hash_algorithms(),
    pos_integer(), iodata(), binary()) -> binary().
decrypt(Cipher, Hash, Iterations, PassPhrase, Base64Binary) ->
    credentials_obfuscation_pbe:decrypt(Cipher, Hash, Iterations, PassPhrase, Base64Binary).
