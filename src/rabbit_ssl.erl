%% The contents of this file are subject to the Mozilla Public License
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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_ssl).

-include("rabbit.hrl").

-include_lib("public_key/include/public_key.hrl").

-export([peer_cert_issuer/1, peer_cert_subject/1, peer_cert_validity/1]).
-export([peer_cert_subject_item/2]).

%%--------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([certificate/0]).

-type(certificate() :: binary()).

-spec(peer_cert_issuer/1        :: (certificate()) -> string()).
-spec(peer_cert_subject/1       :: (certificate()) -> string()).
-spec(peer_cert_validity/1      :: (certificate()) -> string()).
-spec(peer_cert_subject_item/2  ::
        (certificate(), tuple()) -> string() | 'not_found').

-endif.

%%--------------------------------------------------------------------------
%% High-level functions used by reader
%%--------------------------------------------------------------------------

%% Return a string describing the certificate's issuer.
peer_cert_issuer(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       issuer = Issuer }}) ->
                      format_rdn_sequence(Issuer)
              end, Cert).

%% Return a string describing the certificate's subject, as per RFC4514.
peer_cert_subject(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       subject = Subject }}) ->
                      format_rdn_sequence(Subject)
              end, Cert).

%% Return a part of the certificate's subject.
peer_cert_subject_item(Cert, Type) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       subject = Subject }}) ->
                      find_by_type(Type, Subject)
              end, Cert).

%% Return a string describing the certificate's validity.
peer_cert_validity(Cert) ->
    cert_info(fun(#'OTPCertificate' {
                     tbsCertificate = #'OTPTBSCertificate' {
                       validity = {'Validity', Start, End} }}) ->
                      lists:flatten(
                        io_lib:format("~s - ~s", [format_asn1_value(Start),
                                                  format_asn1_value(End)]))
              end, Cert).

%%--------------------------------------------------------------------------

cert_info(F, Cert) ->
    F(case public_key:pkix_decode_cert(Cert, otp) of
          {ok, DecCert} -> DecCert; %%pre R14B
          DecCert       -> DecCert  %%R14B onwards
      end).

find_by_type(Type, {rdnSequence, RDNs}) ->
    case [V || #'AttributeTypeAndValue'{type = T, value = V}
                     <- lists:flatten(RDNs),
                 T == Type] of
        [{printableString, S}] -> S;
        []                     -> not_found
    end.

%%--------------------------------------------------------------------------
%% Formatting functions
%%--------------------------------------------------------------------------

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
            {?'street-address'               , "STREET"}],
    case proplists:lookup(T, Fmts) of
        {_, Fmt} ->
            io_lib:format(Fmt ++ "=~s", [FV]);
        none when is_tuple(T) ->
            TypeL = [io_lib:format("~w", [X]) || X <- tuple_to_list(T)],
            io_lib:format("~s:~s", [string:join(TypeL, "."), FV]);
        none ->
            io_lib:format("~p:~s", [T, FV])
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
escape_rdn_value([C | S], middle) when C < 32 ; C =:= 127 ->
    %% only U+0000 needs escaping, but for display purposes it's handy
    %% to escape all non-printable chars
    lists:flatten(io_lib:format("\\~2.16.0B", [C])) ++
        escape_rdn_value(S, middle);
escape_rdn_value([C | S], middle) ->
    [C | escape_rdn_value(S, middle)].

%% Get the string representation of an OTPCertificate field.
format_asn1_value({ST, S}) when ST =:= teletexString; ST =:= printableString;
                                ST =:= universalString; ST =:= utf8String;
                                ST =:= bmpString ->
    if is_binary(S) -> binary_to_list(S);
       true         -> S
    end;
format_asn1_value({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2,
                            Min1, Min2, S1, S2, $Z]}) ->
    io_lib:format("20~c~c-~c~c-~c~cT~c~c:~c~c:~c~cZ",
                  [Y1, Y2, M1, M2, D1, D2, H1, H2, Min1, Min2, S1, S2]);
format_asn1_value(V) ->
    io_lib:format("~p", [V]).
