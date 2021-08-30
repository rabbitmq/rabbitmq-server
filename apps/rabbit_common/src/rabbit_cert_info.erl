%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_cert_info).

-include_lib("public_key/include/public_key.hrl").

-export([issuer/1,
         subject/1,
         subject_alternative_names/1,
         validity/1,
         subject_items/2,
         extensions/1
]).

-export([sanitize_other_name/1]).

%%--------------------------------------------------------------------------

-export_type([certificate/0]).

-type certificate() :: public_key:der_encoded().

%% x.509 certificate extensions usually look like key/value pairs but can
%% be just about any value
-type certificate_extension_value() :: any().

%%--------------------------------------------------------------------------
%% High-level functions used by reader
%%--------------------------------------------------------------------------

%% Return a string describing the certificate's issuer.
-spec issuer(certificate()) -> string().

issuer(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       issuer = Issuer }}) ->
                      format_rdn_sequence(Issuer)
              end, Cert).

%% Return a string describing the certificate's subject, as per RFC4514.
-spec subject(certificate()) -> string().

subject(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       subject = Subject }}) ->
                      format_rdn_sequence(Subject)
              end, Cert).

%% Return the parts of the certificate's subject.
-spec subject_items
        (certificate(), tuple()) -> [string()] | 'not_found'.

subject_items(Cert, Type) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       subject = Subject }}) ->
                      find_by_type(Type, Subject)
              end, Cert).

-spec extensions(certificate()) -> [#'Extension'{}].
extensions(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       extensions = Extensions }}) ->
                      Extensions
              end, Cert).

-spec subject_alternative_names(certificate()) -> [certificate_extension_value()].
subject_alternative_names(Cert) ->
    Extensions = extensions(Cert),
    try lists:keyfind(?'id-ce-subjectAltName', #'Extension'.extnID, Extensions) of
        false                         -> [];
        #'Extension'{extnValue = Val} -> Val
    catch _:_ -> []
    end.

%% Return a string describing the certificate's validity.
-spec validity(certificate()) -> string().

validity(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       validity = {'Validity', Start, End} }}) ->
                      rabbit_misc:format("~s - ~s", [format_asn1_value(Start),
                                                     format_asn1_value(End)])
              end, Cert).

%%--------------------------------------------------------------------------

cert_info(F, Cert) ->
    F(public_key:pkix_decode_cert(Cert, otp)).

find_by_type(Type, {rdnSequence, RDNs}) ->
    case [V || #'AttributeTypeAndValue'{type = T, value = V}
                   <- lists:flatten(RDNs),
               T == Type] of
        [] -> not_found;
        L  -> [format_asn1_value(V) || V <- L]
    end.

%%--------------------------------------------------------------------------
%% Formatting functions
%%--------------------------------------------------------------------------

sanitize_other_name(Bin) when is_binary(Bin) ->
    %% We make a wild assumption about the types here
    %% but ASN.1 decoding functions in OTP only offer so much and SAN values
    %% are expected to be "string-like" by RabbitMQ
    case 'OTP-PUB-KEY':decode('DirectoryString', Bin) of
        {ok, {_, Val}} -> Val;
        Other          -> Other
    end.

%% Format and rdnSequence as a RFC4514 subject string.
format_rdn_sequence({rdnSequence, Seq}) ->
    string:join(lists:reverse([format_complex_rdn(RDN) || RDN <- Seq]), ",").

%% Format an RDN set.
format_complex_rdn(RDNs) ->
    string:join([format_rdn(RDN) || RDN <- RDNs], "+").

%% Format an RDN.  If the type name is unknown, use the dotted decimal
%% representation.  See RFC4514, section 2.3.
format_rdn(#'AttributeTypeAndValue'{type = T, value = V}) ->
    FV = escape_rdn_value(format_asn1_value(V)),
    Fmts = [{?'id-at-surname'                , "SN"},
            {?'id-at-givenName'              , "GIVENNAME"},
            {?'id-at-initials'               , "INITIALS"},
            {?'id-at-generationQualifier'    , "GENERATIONQUALIFIER"},
            {?'id-at-commonName'             , "CN"},
            {?'id-at-localityName'           , "L"},
            {?'id-at-stateOrProvinceName'    , "ST"},
            {?'id-at-organizationName'       , "O"},
            {?'id-at-organizationalUnitName' , "OU"},
            {?'id-at-title'                  , "TITLE"},
            {?'id-at-countryName'            , "C"},
            {?'id-at-serialNumber'           , "SERIALNUMBER"},
            {?'id-at-pseudonym'              , "PSEUDONYM"},
            {?'id-domainComponent'           , "DC"},
            {?'id-emailAddress'              , "EMAILADDRESS"},
            {?'street-address'               , "STREET"},
            {{0,9,2342,19200300,100,1,1}     , "UID"}], %% Not in public_key.hrl
    case proplists:lookup(T, Fmts) of
        {_, Fmt} ->
            rabbit_misc:format(Fmt ++ "=~s", [FV]);
        none when is_tuple(T) ->
            TypeL = [rabbit_misc:format("~w", [X]) || X <- tuple_to_list(T)],
            rabbit_misc:format("~s=~s", [string:join(TypeL, "."), FV]);
        none ->
            rabbit_misc:format("~p=~s", [T, FV])
    end.

%% Escape a string as per RFC4514.
escape_rdn_value(V) ->
    escape_rdn_value(V, start).

escape_rdn_value([], _) ->
    [];
escape_rdn_value([C | S], start) when C =:= $ ; C =:= $# ->
    [$\\, C | escape_rdn_value(S, middle)];
escape_rdn_value(S, start) ->
    escape_rdn_value(S, middle);
escape_rdn_value([$ ], middle) ->
    [$\\, $ ];
escape_rdn_value([C | S], middle) when C =:= $"; C =:= $+; C =:= $,; C =:= $;;
                                       C =:= $<; C =:= $>; C =:= $\\ ->
    [$\\, C | escape_rdn_value(S, middle)];
escape_rdn_value([C | S], middle) when C < 32 ; C >= 126 ->
    %% Of ASCII characters only U+0000 needs escaping, but for display
    %% purposes it's handy to escape all non-printable chars. All non-ASCII
    %% characters get converted to UTF-8 sequences and then escaped. We've
    %% already got a UTF-8 sequence here, so just escape it.
    rabbit_misc:format("\\~2.16.0B", [C]) ++ escape_rdn_value(S, middle);
escape_rdn_value([C | S], middle) ->
    [C | escape_rdn_value(S, middle)].

%% Get the string representation of an OTPCertificate field.
format_asn1_value({ST, S}) when ST =:= teletexString; ST =:= printableString;
                                ST =:= universalString; ST =:= utf8String;
                                ST =:= bmpString ->
    format_directory_string(ST, S);
format_asn1_value({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2,
                             Min1, Min2, S1, S2, $Z]}) ->
    rabbit_misc:format("20~c~c-~c~c-~c~cT~c~c:~c~c:~c~cZ",
                       [Y1, Y2, M1, M2, D1, D2, H1, H2, Min1, Min2, S1, S2]);
%% We appear to get an untagged value back for an ia5string
%% (e.g. domainComponent).
format_asn1_value(V) when is_list(V) ->
    V;
format_asn1_value(V) when is_binary(V) ->
    %% OTP does not decode some values when combined with an unknown
    %% type. That's probably wrong, so as a last ditch effort let's
    %% try manually decoding. 'DirectoryString' is semi-arbitrary -
    %% but it is the type which covers the various string types we
    %% handle below.
    try
        {ST, S} = public_key:der_decode('DirectoryString', V),
        format_directory_string(ST, S)
    catch _:_ ->
            rabbit_misc:format("~p", [V])
    end;
format_asn1_value(V) ->
    rabbit_misc:format("~p", [V]).

%% DirectoryString { INTEGER : maxSize } ::= CHOICE {
%%     teletexString     TeletexString (SIZE (1..maxSize)),
%%     printableString   PrintableString (SIZE (1..maxSize)),
%%     bmpString         BMPString (SIZE (1..maxSize)),
%%     universalString   UniversalString (SIZE (1..maxSize)),
%%     uTF8String        UTF8String (SIZE (1..maxSize)) }
%%
%% Precise definitions of printable / teletexString are hard to come
%% by. This is what I reconstructed:
%%
%% printableString:
%% "intended to represent the limited character sets available to
%% mainframe input terminals"
%% A-Z a-z 0-9 ' ( ) + , - . / : = ? [space]
%% https://msdn.microsoft.com/en-us/library/bb540814(v=vs.85).aspx
%%
%% teletexString:
%% "a sizable volume of software in the world treats TeletexString
%% (T61String) as a simple 8-bit string with mostly Windows Latin 1
%% (superset of iso-8859-1) encoding"
%% https://www.mail-archive.com/asn1@asn1.org/msg00460.html
%%
%% (However according to that link X.680 actually defines
%% TeletexString in some much more involved and crazy way. I suggest
%% we treat it as ISO-8859-1 since Erlang does not support Windows
%% Latin 1).
%%
%% bmpString:
%% UCS-2 according to RFC 3641. Hence cannot represent Unicode
%% characters above 65535 (outside the "Basic Multilingual Plane").
%%
%% universalString:
%% UCS-4 according to RFC 3641.
%%
%% utf8String:
%% UTF-8 according to RFC 3641.
%%
%% Within Rabbit we assume UTF-8 encoding. Since printableString is a
%% subset of ASCII it is also a subset of UTF-8. The others need
%% converting. Fortunately since the Erlang SSL library does the
%% decoding for us (albeit into a weird format, see below), we just
%% need to handle encoding into UTF-8. Note also that utf8Strings come
%% back as binary.
%%
%% Note for testing: the default Ubuntu configuration for openssl will
%% only create printableString or teletexString types no matter what
%% you do. Edit string_mask in the [req] section of
%% /etc/ssl/openssl.cnf to change this (see comments there). You
%% probably also need to set utf8 = yes to get it to accept UTF-8 on
%% the command line. Also note I could not get openssl to generate a
%% universalString.

format_directory_string(printableString, S) -> S;
format_directory_string(teletexString,   S) -> utf8_list_from(S);
format_directory_string(bmpString,       S) -> utf8_list_from(S);
format_directory_string(universalString, S) -> utf8_list_from(S);
format_directory_string(utf8String,      S) -> binary_to_list(S).

utf8_list_from(S) ->
    binary_to_list(
          unicode:characters_to_binary(flatten_ssl_list(S), utf32, utf8)).

%% The Erlang SSL implementation invents its own representation for
%% non-ascii strings - looking like [97,{0,0,3,187}] (that's LATIN
%% SMALL LETTER A followed by GREEK SMALL LETTER LAMDA). We convert
%% this into a list of unicode characters, which we can tell
%% unicode:characters_to_binary is utf32.

flatten_ssl_list(L) -> [flatten_ssl_list_item(I) || I <- L].

flatten_ssl_list_item({A, B, C, D}) ->
    A * (1 bsl 24) + B * (1 bsl 16) + C * (1 bsl 8) + D;
flatten_ssl_list_item(N) when is_number (N) ->
    N.
