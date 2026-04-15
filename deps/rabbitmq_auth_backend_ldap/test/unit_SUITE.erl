%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

all() ->
    [
     fill,
     ad_fill,
     rfc4514_escape_value,
     rfc4514_fill_dn,
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

rfc4514_escape_value(_Config) ->
    E = fun(V, Res) ->
                ?assertEqual(Res, rabbit_ldap_rfc4514:escape_value(V))
        end,
    %% No escaping needed
    E("simple", "simple"),
    E("", ""),
    E(<<"binary">>, <<"binary">>),
    E(atom, "atom"),
    %% Comma — the primary injection vector
    E("user,ou=Evil", "user\\,ou=Evil"),
    %% All special characters
    E("a+b", "a\\+b"),
    E("a\"b", "a\\\"b"),
    E("a\\b", "a\\\\b"),
    E("a<b", "a\\<b"),
    E("a>b", "a\\>b"),
    E("a;b", "a\\;b"),
    %% Leading space and hash
    E(" leading", "\\ leading"),
    E("#leading", "\\#leading"),
    %% Trailing space
    E("trailing ", "trailing\\ "),
    %% Leading AND trailing space
    E(" both ", "\\ both\\ "),
    %% Middle space is not escaped
    E("a b", "a b"),
    %% Multiple specials
    E("a,b+c", "a\\,b\\+c"),
    %% Backslash + comma (escape-the-escape attack)
    E("a\\,b", "a\\\\\\,b"),
    %% NUL byte
    E([0], [$\\, 0]),
    %% Single special characters
    E(",", "\\,"),
    E("\\", "\\\\"),
    %% Non-string passthrough
    E(42, 42),
    E({1,2,3}, {1,2,3}),
    ok.

rfc4514_fill_dn(_Config) ->
    F = fun(Fmt, Args, Res) ->
                ?assertEqual(Res, rabbit_ldap_rfc4514:fill_dn(Fmt, Args))
        end,
    %% DN injection prevented
    F("cn=${username},ou=People", [{username, "user,ou=Evil"}],
      "cn=user\\,ou=Evil,ou=People"),
    %% user_dn is NOT escaped (it is already a complete DN)
    F("${user_dn}", [{user_dn, "cn=John,ou=People,dc=example"}],
      "cn=John,ou=People,dc=example"),
    %% Mixed: user_dn passed through, username escaped
    F("${user_dn}", [{user_dn, "cn=a,dc=b"}, {username, "x,y"}],
      "cn=a,dc=b"),
    F("cn=${username},dc=b", [{user_dn, "cn=a,dc=b"}, {username, "x,y"}],
      "cn=x\\,y,dc=b"),
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
