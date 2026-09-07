%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_health_check_certificate_expiration_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-define(MOD, rabbit_mgmt_wm_health_check_certificate_expiration).

all() ->
    [
     {group, unit_tests}
    ].

groups() ->
    [
     {unit_tests, [parallel], [
         rfc5280_utctime,
         format_error_reason,
         expires_on_list_error,
         parse_time_error,
         cert_validity_malformed_entry,
         cert_validity_no_pem_entry
     ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.
init_per_group(_, Config) -> Config.
end_per_group(_, _Config) -> ok.

%%
%% time_str_2_gregorian_sec/1
%%

rfc5280_utctime(_Config) ->
    Sec1950 = ?MOD:time_str_2_gregorian_sec({utcTime, "500101000000Z"}),
    ?assertEqual({{1950, 1, 1}, {0, 0, 0}}, calendar:gregorian_seconds_to_datetime(Sec1950)),
    Sec1970 = ?MOD:time_str_2_gregorian_sec({utcTime, "700101000000Z"}),
    ?assertEqual({{1970, 1, 1}, {0, 0, 0}}, calendar:gregorian_seconds_to_datetime(Sec1970)),
    Sec1970NoSec = ?MOD:time_str_2_gregorian_sec({utcTime, "7001010000Z"}),
    ?assertEqual({{1970, 1, 1}, {0, 0, 0}}, calendar:gregorian_seconds_to_datetime(Sec1970NoSec)),
    Sec2000 = ?MOD:time_str_2_gregorian_sec({utcTime, "000101000000Z"}),
    ?assertEqual({{2000, 1, 1}, {0, 0, 0}}, calendar:gregorian_seconds_to_datetime(Sec2000)),
    Sec2049 = ?MOD:time_str_2_gregorian_sec({utcTime, "491231235959Z"}),
    ?assertEqual({{2049, 12, 31}, {23, 59, 59}}, calendar:gregorian_seconds_to_datetime(Sec2049)).

%%
%% parse_time/1
%%

parse_time_error(_Config) ->
    ?assertEqual({error, "Invalid date format in certificate"},
                 ?MOD:parse_time({utcTime, "bad-date"})).

%%
%% format_error_reason/1
%%

format_error_reason(_Config) ->
    ?assertEqual(<<"enoent">>, ?MOD:format_error_reason(enoent)),
    ?assertEqual(<<"custom error">>, ?MOD:format_error_reason("custom error")),
    ?assertEqual(<<"binary error">>, ?MOD:format_error_reason(<<"binary error">>)).

%%
%% expires_on_list/1
%%

expires_on_list_error(_Config) ->
    ErrorStr = {error, "Certificate is not yet valid"},
    ?assertEqual([#{error => <<"Certificate is not yet valid">>}],
                 ?MOD:expires_on_list([ErrorStr])),
    ErrorAtom = {error, enoent},
    ?assertEqual([#{error => <<"enoent">>}], ?MOD:expires_on_list(ErrorAtom)),
    ?assertEqual([#{error => <<"enoent">>}], ?MOD:expires_on_list([ErrorAtom])).

%%
%% cert_validity/1
%%

cert_validity_malformed_entry(_Config) ->
    Pem = <<"-----BEGIN CERTIFICATE-----\nAAAAAA==\n-----END CERTIFICATE-----\n">>,
    ?assertEqual([{error, "Malformed certificate entry"}], ?MOD:cert_validity(Pem)).

cert_validity_no_pem_entry(_Config) ->
    ?assertEqual({error, "The certificate file provided does not contain any PEM entry."},
                 ?MOD:cert_validity(<<"not a pem file">>)).
