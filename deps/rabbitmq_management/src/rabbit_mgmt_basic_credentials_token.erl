%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Encrypted credentials token for the management UI.
%%
%% When management.credential_encryption_secret is configured, the
%% management UI stores an AES-256-GCM encrypted token (prefixed "rmqe.")
%% instead of a plaintext base64(username:password) credential.
%%
%% Token wire format (after stripping the "rmqe." prefix and base64-decoding):
%%   <<IV:12/bytes, Tag:16/bytes, Ciphertext/binary>>
%% where Ciphertext decrypts to <<"Username:Password">>.

-module(rabbit_mgmt_basic_credentials_token).

-export([derive_key/1, issue/3, verify/2]).

-define(PREFIX, <<"rmqe.">>).
-define(IV_LEN, 12).
-define(TAG_LEN, 16).
%% Fixed AAD binds the token to its purpose.
-define(AAD, <<"rabbit-mgmt-credentials-v1">>).

%% Derives a 32-byte AES-256 key from an arbitrary-length secret using
%% HMAC-SHA256. The result is deterministic, so the same secret always
%% produces the same key — required for multi-node clusters.
-spec derive_key(binary() | string()) -> binary().
derive_key(Secret) when is_list(Secret) ->
    derive_key(list_to_binary(Secret));
derive_key(Secret) when is_binary(Secret) ->
    crypto:mac(hmac, sha256, Secret, ?AAD).

%% Encrypts Username and Password into a "rmqe."-prefixed bearer token.
-spec issue(binary(), binary(), binary()) -> {ok, binary()}.
issue(Key, Username, Password) ->
    IV = crypto:strong_rand_bytes(?IV_LEN),
    PlainText = <<Username/binary, ":", Password/binary>>,
    {CipherText, Tag} = crypto:crypto_one_time_aead(
                            aes_256_gcm, Key, IV, PlainText, ?AAD, ?TAG_LEN, true),
    Payload = base64:encode(<<IV/binary, Tag/binary, CipherText/binary>>),
    {ok, <<?PREFIX/binary, Payload/binary>>}.

%% Decrypts a token produced by issue/3.
%% Returns {ok, Username, Password}, {error, not_an_encrypted_token} when
%% the token does not carry the "rmqe." prefix, or {error, invalid_token}
%% when decryption or authentication fails.
-spec verify(binary(), binary()) ->
    {ok, binary(), binary()} | {error, not_an_encrypted_token | invalid_token}.
verify(<<"rmqe.", Payload/binary>>, Key) ->
    try
        Bin = base64:decode(Payload),
        <<IV:?IV_LEN/bytes, Tag:?TAG_LEN/bytes, CipherText/binary>> = Bin,
        case crypto:crypto_one_time_aead(
                 aes_256_gcm, Key, IV, CipherText, ?AAD, Tag, false) of
            error ->
                {error, invalid_token};
            PlainText ->
                [Username, Password] = binary:split(PlainText, <<":">>),
                {ok, Username, Password}
        end
    catch _:_ ->
        {error, invalid_token}
    end;
verify(_, _) ->
    {error, not_an_encrypted_token}.
