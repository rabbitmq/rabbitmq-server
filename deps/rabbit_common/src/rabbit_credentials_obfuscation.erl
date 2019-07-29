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
%% Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_credentials_obfuscation).

-behaviour(application).
-export([encrypt/1,decrypt/1]).

encrypt(none) ->
    none;
encrypt(Term) ->
    rabbit_pbe:encrypt(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(), 
        <<"passphrase">>, Term).

decrypt(none) ->
    none;
decrypt(Base64EncryptedBinary) ->
    rabbit_pbe:decrypt(
        rabbit_pbe:default_cipher(), rabbit_pbe:default_hash(), rabbit_pbe:default_iterations(), 
        <<"passphrase">>, Base64EncryptedBinary).