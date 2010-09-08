%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_ssl).

-include_lib("public_key/include/public_key.hrl").
-include_lib("ssl/src/ssl_int.hrl").

-export([ssl_issuer/1, ssl_subject/1, ssl_validity/1, ssl_info/2]).


%%--------------------------------------------------------------------------

-ifdef(use_specs).

-type(ssl_info_fun() :: fun((#'OTPCertificate'{}) -> string())).

-spec(ssl_info/2 :: (ssl_info_fun(), #'sslsocket'{}) -> any()).

-spec(ssl_issuer/1 :: (#'OTPCertificate'{}) -> string()).
-spec(ssl_subject/1 :: (#'OTPCertificate'{}) -> string()).
-spec(ssl_validity/1 :: (#'OTPCertificate'{}) -> string()).

-endif. %% use_specs


%%--------------------------------------------------------------------------
%% High-level functions used by reader
%%--------------------------------------------------------------------------

%% Wrapper for applying a function to a socket's certificate.
ssl_info(F, Sock) ->
    case rabbit_net:peercert(Sock) of
        nossl               -> nossl;
        no_peer_certificate -> no_peer_certificate;
        {ok, Cert}          ->
            try F(Cert)  %% here be dragons; decompose an undocumented
                         %% structure
            catch
                C:E ->
                    rabbit_log:info("Problems while processing SSL info: ~p:~p~n",
                                    [C, E]),
                    unknown
            end
    end.

%% Return a string describing the certificate's issuer.
ssl_issuer(#'OTPCertificate' {
              tbsCertificate = #'OTPTBSCertificate' {
                issuer = Issuer }}) ->
    format_ssl_subject(extract_ssl_values(Issuer)).

%% Return a string describing the certificate's subject, as per RFC4514.
ssl_subject(#'OTPCertificate' {
               tbsCertificate = #'OTPTBSCertificate' {
                 subject = Subject }}) ->
    format_ssl_subject(extract_ssl_values(Subject)).

%% Return a string describing the certificate's validity.
ssl_validity(#'OTPCertificate' {
                tbsCertificate = #'OTPTBSCertificate' {
                  validity = Validity }}) ->
    case extract_ssl_values(Validity) of
        {'Validity', Start, End} ->
            io_lib:format("~s to ~s", [format_ssl_value(Start),
                                       format_ssl_value(End)]);
        V ->
            io_lib:format("~p", [V])
    end.


%%--------------------------------------------------------------------------
%% Functions for extracting information from OTPCertificates
%%--------------------------------------------------------------------------

%% Convert OTPCertificate fields to something easier to use.
extract_ssl_values({rdnSequence, List}) ->
    extract_ssl_values_list(List);
extract_ssl_values(V) ->
    V.

%% Convert an rdnSequeuence list to a proplist.
extract_ssl_values_list([[#'AttributeTypeAndValue'{type = T, value = V}]
                         | Rest]) ->
    [{T, V} | extract_ssl_values_list(Rest)];
extract_ssl_values_list([V|_]) ->
    throw({unknown_rdnSequence_element, V});
extract_ssl_values_list([]) ->
    [].


%%--------------------------------------------------------------------------
%% Formatting functions
%%--------------------------------------------------------------------------

%% Convert a proplist to a RFC4514 subject string.
format_ssl_subject(RDNs) ->
    lists:flatten(
      rabbit_misc:intersperse(
        ",", lists:reverse(
               [escape_ssl_string(format_ssl_type_and_value(T, V), start)
                || {T, V} <- RDNs]))).

%% Escape a string as per RFC4514.
escape_ssl_string([], _) ->
    [];
escape_ssl_string([$  | S], start) ->
    ["\\ " | escape_ssl_string(S, start)];
escape_ssl_string([$# | S], start) ->
    ["\\#" | escape_ssl_string(S, start)];
escape_ssl_string(S, start) ->
    escape_ssl_string(S, middle);
escape_ssl_string([$  | S], middle) ->
    case lists:filter(fun(C) -> C =/= $  end, S) of
        []    -> escape_ssl_string([$  | S], ending);
        [_|_] -> [" " | escape_ssl_string(S, middle)]
    end;
escape_ssl_string([C | S], middle) ->
    case lists:member(C, ",+\"\\<>;") of
        false -> [C | escape_ssl_string(S, middle)];
        true  -> ["\\", C | escape_ssl_string(S, middle)]
    end;
escape_ssl_string([$  | S], ending) ->
    ["\\ " | escape_ssl_string(S, ending)].

%% Format a type-value pair as an RDN.  If the type name is unknown,
%% use the dotted decimal representation.  See RFC4514, section 2.3.
format_ssl_type_and_value(Type, Value) ->
    FV = format_ssl_value(Value),
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
    case proplists:lookup(Type, Fmts) of
        {_, Fmt} ->
            io_lib:format(Fmt ++ "=~s", [FV]);
        none when is_tuple(Type) ->
            TypeL = [io_lib:format("~w", [X]) || X <- tuple_to_list(Type)],
            io_lib:format("~s:~s", [rabbit_misc:intersperse(".", TypeL), FV]);
        none ->
            io_lib:format("~p:~s", [Type, FV])
    end.

%% Get the string representation of an OTPCertificate field.
format_ssl_value({printableString, S}) ->
    S;
format_ssl_value({utf8String, Bin}) ->
    binary:bin_to_list(Bin);
format_ssl_value({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2,
                            Min1, Min2, S1, S2, $Z]}) ->
    io_lib:format("20~c~c-~c~c-~c~c ~c~c:~c~c:~c~c",
                  [Y1, Y2, M1, M2, D1, D2, H1, H2, Min1, Min2, S1, S2]);
format_ssl_value(V) ->
    io_lib:format("~p", [V]).
