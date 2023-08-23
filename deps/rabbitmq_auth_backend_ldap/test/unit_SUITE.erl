%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

all() ->
    [
     fill,
     ad_fill,
     user_dn_pattern_gh_7161,
     format_different_types_of_ldap_attribute_values
    ].

fill(_Config) ->
    F = fun(Fmt, Args, Res) ->
                ?assertEqual(Res, rabbit_auth_backend_ldap_util:fill(Fmt, Args))
        end,
    F("x${username}x", [{username,  "ab"}],     "xabx"),
    F("x${username}x", [{username,  ab}],       "xabx"),
    F("x${username}x", [{username,  <<"ab">>}], "xabx"),
    F("x${username}x", [{username,  ""}],       "xx"),
    F("x${username}x", [{fusername, "ab"}],     "x${username}x"),
    F("x${usernamex",  [{username,  "ab"}],     "x${usernamex"),
    F("x${username}x", [{username,  "a\\b"}],   "xa\\bx"),
    F("x${username}x", [{username,  "a&b"}],    "xa&bx"),
    ok.

ad_fill(_Config) ->
    F = fun(Fmt, Args, Res) ->
                ?assertEqual(Res, rabbit_auth_backend_ldap_util:fill(Fmt, Args))
        end,

    U0 = <<"ADDomain\\ADUser">>,
    A0 = rabbit_auth_backend_ldap_util:get_active_directory_args(U0),
    F("x-${ad_domain}-x-${ad_user}-x", A0, "x-ADDomain-x-ADUser-x"),

    U1 = <<"ADDomain\\ADUser\\Extra">>,
    A1 = rabbit_auth_backend_ldap_util:get_active_directory_args(U1),
    F("x-${ad_domain}-x-${ad_user}-x", A1, "x-ADDomain-x-ADUser\\Extra-x"),
    ok.

user_dn_pattern_gh_7161(_Config) ->
    ok = application:load(rabbitmq_auth_backend_ldap),
    {ok, UserDnPattern} = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    ?assertEqual("${username}", UserDnPattern).

utf8_list_to_string(StrangeList) ->
  unicode:characters_to_list(list_to_binary(StrangeList)).

heuristic_encoding_bin(Bin) when is_binary(Bin) ->
    case unicode:characters_to_binary(Bin,utf8,utf8) of
	Bin ->
	    utf8;
	_ ->
	    latin1
    end.

format_different_types_of_ldap_attribute_values(_Config) ->
    AsciiOnlyAttr = [50,56,48,48,48,45],
    ?assertEqual("28000-", rabbit_auth_backend_ldap:format_multi_attr("28000-")),
    ?assertEqual("28000-", rabbit_auth_backend_ldap:format_multi_attr(AsciiOnlyAttr)),

    NonAsciiAttr = [50,56,48,48,48,45,195,159],
    ?assertEqual("28000-ß", rabbit_auth_backend_ldap:format_multi_attr(NonAsciiAttr)),

    ?assertEqual("one; 28000-ß; two; ", rabbit_auth_backend_ldap:format_multi_attr(["one", NonAsciiAttr, "two"])),
    ok.
