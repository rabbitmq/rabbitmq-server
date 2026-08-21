%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_auth_backend_ldap/include/logging.hrl").

-compile([export_all]).

all() ->
    [
     fill,
     ad_fill,
     rfc4514_escape_value,
     rfc4514_fill_dn,
     dn_lookup_fallback_dn_escaping,
     user_dn_pattern_escaping_rmq_4282,
     user_bind_pattern_escaping_rmq_4282,
     leading_special_char_no_dn_injection_rmq_4282,
     authz_query_dn_pattern_escaping_rmq_4282,
     user_dn_pattern_gh_7161,
     format_different_types_of_ldap_attribute_values,
     ldap_log_domain_routing,
     ldap_log_callsites_carry_domain
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
    %% Comma escaping
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
    %% Backslash followed by comma
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
    %% A comma in the substituted value is escaped
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

dn_lookup_fallback_dn_escaping(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern,
                             "cn=${username},ou=People,dc=example,dc=com"),
    try
        %% No DN-special characters: escaping is a no-op
        ?assertEqual(rabbit_auth_backend_ldap:fill_user_dn_pattern("alice"),
                     rabbit_auth_backend_ldap:escaped_user_dn("alice")),
        ?assertEqual("cn=alice,ou=People,dc=example,dc=com",
                     rabbit_auth_backend_ldap:escaped_user_dn("alice")),
        %% A comma in the substituted value is escaped
        ?assertEqual("cn=evil\\,ou=admins,ou=People,dc=example,dc=com",
                     rabbit_auth_backend_ldap:escaped_user_dn("evil,ou=admins")),
        %% A binary username (the form used at runtime) is handled identically
        ?assertEqual("cn=evil\\,ou=admins,ou=People,dc=example,dc=com",
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"evil,ou=admins">>)),
        %% Bare fill leaves the substituted value unescaped
        ?assertEqual("cn=evil,ou=admins,ou=People,dc=example,dc=com",
                     rabbit_auth_backend_ldap:fill_user_dn_pattern("evil,ou=admins"))
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% RMQ-4282: a user using the default `user_dn_pattern` ("${username}")
%% with AD down-level logon names ("DOMAIN\username") got "Access Refused"
%% after PR #16101 started RFC 4514-escaping DN template substitutions: the
%% backslash separating domain and user is itself RFC 4514-special, so it
%% was escaped to a literal double backslash, which no longer matched the
%% single-backslash down-level logon name AD expects.
%%
%% get_active_directory_args/1 only recognises the "DOMAIN\user" split for
%% binary usernames (a plain list/string always yields no AD args), and a
%% binary is what every real login supplies (see rabbit_auth_mechanism_plain),
%% so these tests use binaries throughout.
user_dn_pattern_escaping_rmq_4282(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern, "${username}"),
    try
        Username = <<"foo\\bar">>,
        %% Bare fill (used for filter values, e.g. dn_lookup) was already
        %% unescaped, and remains so.
        ?assertEqual("foo\\bar",
                     rabbit_auth_backend_ldap:fill_user_dn_pattern(Username)),
        %% Used as a bind DN, the domain-separator backslash is no longer
        %% doubled: the down-level logon name reaches AD unchanged.
        ?assertEqual("foo\\bar",
                     rabbit_auth_backend_ldap:escaped_user_dn(Username)),
        %% A comma inside the user part is still RFC 4514-escaped -- only the
        %% domain-separator backslash is exempted, not DN injection generally.
        ?assertEqual("foo\\evil\\,ou=admins",
                     rabbit_auth_backend_ldap:escaped_user_dn(
                       <<"foo\\evil,ou=admins">>)),
        %% More than one backslash is not a valid down-level logon name --
        %% which one is the separator is ambiguous -- so the whole value
        %% falls back to ordinary (fully escaped) DN-attribute treatment.
        ?assertEqual("A\\\\B\\\\C",
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"A\\B\\C">>)),
        %% A leading or trailing backslash has no domain or user part to
        %% split on (get_active_directory_args/1 yields no AD args for
        %% either), so it also falls back to whole-value escaping.
        ?assertEqual("\\\\user",
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"\\user">>)),
        ?assertEqual("DOMAIN\\\\",
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"DOMAIN\\">>)),
        %% UPN format (user@domain) has no RFC 4514-special characters, so it
        %% passes through unchanged.
        ?assertEqual("foo@example.test",
                     rabbit_auth_backend_ldap:escaped_user_dn(
                       <<"foo@example.test">>)),
        %% A bare username (no backslash) is unaffected.
        ?assertEqual("alice", rabbit_auth_backend_ldap:escaped_user_dn(<<"alice">>))
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% The same fix applies to `user_bind_pattern` (used for simple binds when
%% `dn_lookup_when/0` is not `prebind`), not just `user_dn_pattern`.
user_bind_pattern_escaping_rmq_4282(_Config) ->
    PrevBindPattern = application:get_env(rabbitmq_auth_backend_ldap, user_bind_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_bind_pattern, "${username}"),
    try
        ?assertEqual("foo\\bar",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"foo\\bar">>))
    after
        restore_env(user_bind_pattern, PrevBindPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% `escaped_username_value/2` (re)joins the escaped domain
%% and user components with a literal, unescaped separator backslash.
%%
%% If the user part's first character is itself RFC 4514-special (a comma, a plus,
%% a quote, a semicolon, or an angle bracket), escaping it produces a value that
%% also starts with a backslash. That backslash pairs up with the separator
%% backslash under RFC 4514's own parsing rule (a backslash always starts a
%% two-character escape), leaving the special character right after the
%% pair completely unescaped and letting it act as a fresh RDN separator.
%%
%% In this case we must fall back to
%% whole-value escaping instead, exactly like the "more than one backslash"
%% and "leading/trailing backslash" cases above.
leading_special_char_no_dn_injection_rmq_4282(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern, "${username}"),
    try
        Injections = [<<"DOMAIN\\,ou=Evil,dc=example,dc=com">>,
                      <<"DOMAIN\\+ou=Evil">>,
                      <<"DOMAIN\\\"ou=Evil">>,
                      <<"DOMAIN\\;ou=Evil">>,
                      <<"DOMAIN\\<ou=Evil">>,
                      <<"DOMAIN\\>ou=Evil">>],
        [?assertEqual(rabbit_ldap_rfc4514:escape_value(binary_to_list(Username)),
                       rabbit_auth_backend_ldap:escaped_user_dn(Username))
         || Username <- Injections],
        %% A user part that does NOT start with an RFC 4514 special still
        %% takes the (safe) domain/user split path, unaffected by the fix.
        ?assertEqual("foo\\evil\\,ou=admins",
                     rabbit_auth_backend_ldap:escaped_user_dn(
                       <<"foo\\evil,ou=admins">>))
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% RMQ-4282 follow-up: `check_vhost_access', `check_resource_access',
%% `check_topic_access' and `do_tag_queries' build the same `${username}'
%% substitution for `exists', `in_group', `in_group_nested' and `attribute'
%% DN patterns (evaluate0/4) as the bind-DN path, so an AD down-level logon
%% name must round-trip there too, and the other query variables (e.g.
%% `${vhost}') must still be RFC 4514-escaped as before.
authz_query_dn_pattern_escaping_rmq_4282(_Config) ->
    Args = fun(Username) -> [{username, Username}, {vhost, <<"a,b">>}] end,
    ?assertEqual("cn=foo\\bar,ou=a\\,b",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "cn=${username},ou=${vhost}", Args(<<"foo\\bar">>))),
    ?assertEqual("cn=" ++ rabbit_ldap_rfc4514:escape_value("A\\B\\C") ++ ",ou=a\\,b",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "cn=${username},ou=${vhost}", Args(<<"A\\B\\C">>))),
    ?assertEqual("cn=" ++ rabbit_ldap_rfc4514:escape_value("DOMAIN\\,ou=Finance") ++ ",ou=a\\,b",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "cn=${username},ou=${vhost}", Args(<<"DOMAIN\\,ou=Finance">>))),
    ok.

restore_env(Key, {ok, V}) -> application:set_env(rabbitmq_auth_backend_ldap, Key, V);
restore_env(Key, undefined) -> application:unset_env(rabbitmq_auth_backend_ldap, Key).

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

%% `?RMQLOG_DOMAIN_LDAP` log even routing
ldap_log_domain_routing(_Config) ->
    HandlerId = ldap_log_capture,
    Ref = make_ref(),
    HandlerCfg = #{config => #{pid => self(), ref => Ref},
                   filter_default => stop,
                   filters => [{ldap_domain,
                                {fun logger_filters:domain/2,
                                 {log, sub, ?RMQLOG_DOMAIN_LDAP}}}],
                   level => all},
    ok = logger:add_handler(HandlerId, ?MODULE, HandlerCfg),
    try
        %% `notice` is higher than the default primary logger level used by CT;
        %% thherefore `info` and `debug` messages  would be dropped before reaching any handler
        logger:log(notice, "ldap-domain event ~tp", [Ref],
                   #{domain => ?RMQLOG_DOMAIN_LDAP}),
        logger:log(notice, "other-domain event ~tp", [Ref],
                   #{domain => [rabbitmq, somewhere_else]}),
        logger:log(notice, "no-domain event ~tp", [Ref], #{}),
        receive
            {Ref, Event} ->
                ?assertMatch(#{meta := #{domain := [rabbitmq, ldap]}}, Event)
        after 5000 ->
            ct:fail("LDAP-domain event was not captured by the test handler")
        end,
        receive
            {Ref, Unexpected} -> ct:fail({non_ldap_event_leaked, Unexpected})
        after 200 ->
            ok
        end
    after
        _ = logger:remove_handler(HandlerId)
    end.

%% Verifies that every `?LOG_*` call site in the LDAP plugin sources passes the
%% LDAP domain in its metadata
ldap_log_callsites_carry_domain(_Config) ->
    SrcDir = filename:join(code:lib_dir(rabbitmq_auth_backend_ldap), "src"),
    Files = ["rabbit_auth_backend_ldap.erl",
             "rabbit_auth_backend_ldap_app.erl"],
    [check_log_callsite_invariant(filename:join(SrcDir, F)) || F <- Files],
    ok.

check_log_callsite_invariant(Path) ->
    {ok, Bin} = file:read_file(Path),
    LogCalls = count_substr(<<"?LOG_">>, Bin),
    Domains  = count_substr(<<"RMQLOG_DOMAIN_LDAP">>, Bin),
    ?assertEqual(LogCalls, Domains,
                 lists:flatten(io_lib:format(
                   "~ts: ~b ?LOG_ macro callsites but ~b RMQLOG_DOMAIN_LDAP "
                   "references; every callsite must pass the LDAP domain",
                   [Path, LogCalls, Domains]))).

count_substr(Needle, Haystack) ->
    length(binary:matches(Haystack, Needle)).

%% Used by `ldap_log_domain_routing/1`
log(LogEvent, #{config := #{pid := Pid, ref := Ref}}) ->
    Pid ! {Ref, LogEvent},
    ok.
