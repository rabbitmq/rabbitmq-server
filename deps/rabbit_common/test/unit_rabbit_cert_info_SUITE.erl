%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_cert_info_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
        shortest_serial_number,
        common_serial_number_length,
        max_serial_number_length,
        serial_number_is_formatted_using_uppercase_hex,
        serial_number_matches_decoded_value,
        subject_and_issuer_from_self_signed
     ]}
    ].

shortest_serial_number(_Config) ->
    {DER, _Key} = generate_self_signed_cert("CN=ShortestSerial", 1),
    ?assertEqual("1", rabbit_cert_info:serial_number(DER)).

common_serial_number_length(_Config) ->
    Serial = 16#ABCDEF,
    {DER, _Key} = generate_self_signed_cert("CN=SerialTypical", Serial),
    ?assertEqual("ABCDEF", rabbit_cert_info:serial_number(DER)).

%% RFC 5280 permits serial numbers up to 20 octets (160 bits). Make sure
%% large serials round-trip without truncation.
max_serial_number_length(_Config) ->
    %% 20-octet serial
    Serial = 16#7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
    {DER, _Key} = generate_self_signed_cert("CN=SerialLarge", Serial),
    Got = rabbit_cert_info:serial_number(DER),
    ?assertEqual("7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", Got).

%% The hex output must be uppercase to match common tooling (namey OpenSSL).
serial_number_is_formatted_using_uppercase_hex(_Config) ->
    {DER, _Key} = generate_self_signed_cert("CN=SerialCase", 16#deadbeef),
    Got = rabbit_cert_info:serial_number(DER),
    ?assertEqual(Got, string:uppercase(Got)),
    ?assertEqual("DEADBEEF", Got).

%% Round trips the integer value.
serial_number_matches_decoded_value(_Config) ->
    Serial = 16#0123456789ABCDEF,
    {DER, _Key} = generate_self_signed_cert("CN=SerialRoundtrip", Serial),
    Hex = rabbit_cert_info:serial_number(DER),
    ?assertEqual(Serial, list_to_integer(Hex, 16)).

subject_and_issuer_from_self_signed(_Config) ->
    {DER, _Key} = generate_self_signed_cert("SanityCheck", 7),
    Subject = rabbit_cert_info:subject(DER),
    Issuer  = rabbit_cert_info:issuer(DER),
    ?assertEqual(Subject, Issuer),
    ?assertEqual("CN=SanityCheck", Subject).

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
