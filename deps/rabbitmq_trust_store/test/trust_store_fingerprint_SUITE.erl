%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(trust_store_fingerprint_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

all() ->
    [
     whitelisted_cert_is_accepted,
     forged_cert_with_same_issuer_id_is_rejected,
     different_cert_is_rejected,
     is_whitelisted_der_matches_stored_cert
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    catch ets:delete(trust_store_whitelist),
    %% keypos 2 = #entry.fingerprint
    ets:new(trust_store_whitelist,
            [protected, named_table, set,
             {keypos, 2},
             {heir, none}]),
    Config.

end_per_testcase(_Testcase, _Config) ->
    catch ets:delete(trust_store_whitelist),
    ok.

%% A whitelisted certificate must be accepted by is_whitelisted/1.
whitelisted_cert_is_accepted(_Config) ->
    {CertDER, _KeyA} = generate_self_signed_cert("CN=TestCA", 1),
    OTPCert = public_key:pkix_decode_cert(CertDER, otp),
    insert_entry(CertDER, OTPCert),
    ?assert(rabbit_trust_store:is_whitelisted(OTPCert)).

%% A certificate with the same Issuer DN and Serial (but different key/content)
%% must NOT be accepted. This is the core of the V-16 forgery fix.
forged_cert_with_same_issuer_id_is_rejected(_Config) ->
    {CertA_DER, _KeyA} = generate_self_signed_cert("CN=TestCA", 1),
    {CertB_DER, _KeyB} = generate_self_signed_cert("CN=TestCA", 1),
    %% Verify that both certs share the same issuer_id
    OTPCertA = public_key:pkix_decode_cert(CertA_DER, otp),
    OTPCertB = public_key:pkix_decode_cert(CertB_DER, otp),
    IssuerIdA = extract_issuer_id(OTPCertA),
    IssuerIdB = extract_issuer_id(OTPCertB),
    ?assertEqual(IssuerIdA, IssuerIdB),
    %% But their DER encodings (and therefore fingerprints) differ
    ?assertNotEqual(CertA_DER, CertB_DER),
    %% Whitelist CertA only
    insert_entry(CertA_DER, OTPCertA),
    ?assert(rabbit_trust_store:is_whitelisted(OTPCertA)),
    ?assertNot(rabbit_trust_store:is_whitelisted(OTPCertB)).

%% A completely different certificate must be rejected.
different_cert_is_rejected(_Config) ->
    {CertA_DER, _KeyA} = generate_self_signed_cert("CN=AlphaCA", 100),
    {CertB_DER, _KeyB} = generate_self_signed_cert("CN=BetaCA", 200),
    OTPCertA = public_key:pkix_decode_cert(CertA_DER, otp),
    OTPCertB = public_key:pkix_decode_cert(CertB_DER, otp),
    insert_entry(CertA_DER, OTPCertA),
    ?assert(rabbit_trust_store:is_whitelisted(OTPCertA)),
    ?assertNot(rabbit_trust_store:is_whitelisted(OTPCertB)).

%% is_whitelisted_der/1 accepts the raw DER of a whitelisted cert.
is_whitelisted_der_matches_stored_cert(_Config) ->
    {CertDER, _Key} = generate_self_signed_cert("CN=DerTest", 42),
    OTPCert = public_key:pkix_decode_cert(CertDER, otp),
    insert_entry(CertDER, OTPCert),
    ?assert(rabbit_trust_store:is_whitelisted_der(CertDER)),
    {OtherDER, _} = generate_self_signed_cert("CN=DerTest", 42),
    ?assertNot(rabbit_trust_store:is_whitelisted_der(OtherDER)).

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

generate_self_signed_cert(SubjectStr, Serial) ->
    Key = public_key:generate_key({namedCurve, secp256r1}),
    Subject = {rdnSequence, [[#'AttributeTypeAndValue'{
                                  type = ?'id-at-commonName',
                                  value = {utf8String, list_to_binary(SubjectStr)}}]]},
    TBS = #'OTPTBSCertificate'{
        version = v3,
        serialNumber = Serial,
        signature = #'SignatureAlgorithm'{
            algorithm = ?'ecdsa-with-SHA256',
            parameters = asn1_NOVALUE},
        issuer = Subject,
        validity = #'Validity'{
            notBefore = {utcTime, "250101000000Z"},
            notAfter  = {utcTime, "350101000000Z"}},
        subject = Subject,
        subjectPublicKeyInfo = #'OTPSubjectPublicKeyInfo'{
            algorithm = #'PublicKeyAlgorithm'{
                algorithm = ?'id-ecPublicKey',
                parameters = {namedCurve, ?secp256r1}},
            subjectPublicKey = #'ECPoint'{point = Key#'ECPrivateKey'.publicKey}},
        extensions = []
    },
    CertDER = public_key:pkix_sign(TBS, Key),
    {CertDER, Key}.

extract_issuer_id(#'OTPCertificate'{} = C) ->
    {Serial, Issuer} = case public_key:pkix_issuer_id(C, other) of
        {error, _} ->
            {ok, Id} = public_key:pkix_issuer_id(C, self),
            Id;
        {ok, Id} ->
            Id
    end,
    {Issuer, Serial}.

insert_entry(CertDER, OTPCert) ->
    Fingerprint = crypto:hash(sha256, CertDER),
    IssuerId = extract_issuer_id(OTPCert),
    ets:insert(trust_store_whitelist,
               {entry, Fingerprint, undefined, <<"test">>,
                ?MODULE, IssuerId, CertDER}).
