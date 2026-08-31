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
     ad_variable_pattern_escaping_rmq_4282,
     bare_username_pattern_gh_17271,
     non_dn_pattern_no_escaping,
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

%% A `user_dn_pattern' with an `attr=value' pair is a DN template: a value
%% substituted into it is escaped whole, backslashes included (see
%% rabbitmq/rabbitmq-server#17271, RMQ-4282).
%%
%% A pattern without `attr=value' is not a DN template, so its substituted
%% values are not escaped; see non_dn_pattern_no_escaping for that case.
%%
%% Real logins supply binary usernames (only binaries are split into AD
%% args), so these tests use binaries throughout.
user_dn_pattern_escaping_rmq_4282(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern,
                             "cn=${username},ou=People,dc=example,dc=com"),
    Suffix = ",ou=People,dc=example,dc=com",
    try
        %% Raw fill (filter values, e.g. dn_lookup) is unescaped.
        ?assertEqual("cn=foo\\bar" ++ Suffix,
                     rabbit_auth_backend_ldap:fill_user_dn_pattern(<<"foo\\bar">>)),
        ?assertEqual("cn=foo\\\\bar" ++ Suffix,
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"foo\\bar">>)),
        ?assertEqual("cn=foo\\\\evil\\,ou=admins" ++ Suffix,
                     rabbit_auth_backend_ldap:escaped_user_dn(
                       <<"foo\\evil,ou=admins">>)),
        [?assertEqual("cn=" ++
                          rabbit_ldap_rfc4514:escape_value(binary_to_list(U)) ++
                          Suffix,
                      rabbit_auth_backend_ldap:escaped_user_dn(U))
         || U <- [<<"A\\B\\C">>, <<"\\user">>, <<"DOMAIN\\">>,
                  <<"foo@example.test">>, <<"alice">>, <<" alice">>,
                  <<"#alice">>, <<"alice ">>, <<>>, <<"жозефина"/utf8>>]]
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% A bind pattern without an `attr=value' pair does not produce a DN, so its
%% substituted values are not escaped.
non_dn_pattern_no_escaping(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevBindPattern = application:get_env(rabbitmq_auth_backend_ldap, user_bind_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    try
        ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern,
                                 "${username}@example.com"),
        ?assertEqual("O'Brien,Sean@example.com",
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"O'Brien,Sean">>)),
        ok = application:set_env(rabbitmq_auth_backend_ldap, user_bind_pattern,
                                 "${ad_user}@${ad_domain}.example"),
        ?assertEqual("a,b@CORP.example",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(<<"CORP\\a,b">>)),
        ok = application:set_env(rabbitmq_auth_backend_ldap, user_bind_pattern,
                                 "EXAMPLE\\${username}"),
        ?assertEqual("EXAMPLE\\a+b",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(<<"a+b">>))
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(user_bind_pattern, PrevBindPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% GH-17271: with the default `${username}' pattern the username is the whole
%% bind name, so escaping it as an attribute value corrupted client-supplied
%% DNs: every RDN separator gained a backslash and the bind was refused.
bare_username_pattern_gh_17271(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevBindPattern = application:get_env(rabbitmq_auth_backend_ldap, user_bind_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern, "${username}"),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_bind_pattern, none),
    %% A DN whose first RDN value carries an escaped comma, as the client sends it.
    DN = "CN=Lim\\, John,OU=PD,OU=Users,DC=example,DC=com",
    try
        ?assertEqual(DN, rabbit_auth_backend_ldap:escaped_user_dn(
                           list_to_binary(DN))),
        %% With no `user_bind_pattern', the simple bind falls back to
        %% `user_dn_pattern'.
        ?assertEqual(DN, rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                           list_to_binary(DN))),
        %% Other whole bind names are passed through as typed.
        [?assertEqual(N, rabbit_auth_backend_ldap:escaped_user_dn(
                           list_to_binary(N)))
         || N <- ["alice", "alice@example.com", "CORP\\alice",
                  "CN=Smith\\+Jones,DC=example", ""]],
        ?assertEqual(binary_to_list(<<"жозефина"/utf8>>),
                     rabbit_auth_backend_ldap:escaped_user_dn(<<"жозефина"/utf8>>))
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(user_bind_pattern, PrevBindPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% The same rules apply to `user_bind_pattern', not just `user_dn_pattern'.
user_bind_pattern_escaping_rmq_4282(_Config) ->
    PrevBindPattern = application:get_env(rabbitmq_auth_backend_ldap, user_bind_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_bind_pattern,
                             "cn=${username},ou=People"),
    try
        ?assertEqual("cn=foo\\\\bar,ou=People",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"foo\\bar">>))
    after
        restore_env(user_bind_pattern, PrevBindPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% An escaped user part starting with a backslash would pair with the
%% separator backslash, un-escaping the special character after it. Such
%% usernames fall back to whole-value escaping.
leading_special_char_no_dn_injection_rmq_4282(_Config) ->
    PrevPattern = application:get_env(rabbitmq_auth_backend_ldap, user_dn_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    ok = application:set_env(rabbitmq_auth_backend_ldap, user_dn_pattern,
                             "cn=${username},ou=People"),
    try
        Injections = [<<"DOMAIN\\,ou=Evil,dc=example,dc=com">>,
                      <<"DOMAIN\\+ou=Evil">>,
                      <<"DOMAIN\\\"ou=Evil">>,
                      <<"DOMAIN\\;ou=Evil">>,
                      <<"DOMAIN\\<ou=Evil">>,
                      <<"DOMAIN\\>ou=Evil">>],
        [?assertEqual("cn=" ++
                          rabbit_ldap_rfc4514:escape_value(binary_to_list(Username)) ++
                          ",ou=People",
                       rabbit_auth_backend_ldap:escaped_user_dn(Username))
         || Username <- Injections],
        ?assertEqual("cn=foo\\\\evil\\,ou=admins,ou=People",
                     rabbit_auth_backend_ldap:escaped_user_dn(
                       <<"foo\\evil,ou=admins">>))
    after
        restore_env(user_dn_pattern, PrevPattern),
        restore_env(log, PrevLog)
    end,
    ok.

%% The `evaluate0/4' DN patterns escape `${username}' like any other value.
authz_query_dn_pattern_escaping_rmq_4282(_Config) ->
    Args = fun(Username) -> [{username, Username}, {vhost, <<"a,b">>}] end,
    ?assertEqual("cn=foo\\\\bar,ou=a\\,b",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "cn=${username},ou=${vhost}", Args(<<"foo\\bar">>))),
    ?assertEqual("cn=" ++ rabbit_ldap_rfc4514:escape_value("A\\B\\C") ++ ",ou=a\\,b",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "cn=${username},ou=${vhost}", Args(<<"A\\B\\C">>))),
    ?assertEqual("cn=" ++ rabbit_ldap_rfc4514:escape_value("DOMAIN\\,ou=Finance") ++ ",ou=a\\,b",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "cn=${username},ou=${vhost}", Args(<<"DOMAIN\\,ou=Finance">>))),
    %% `${user_dn}' holds a complete DN and is not escaped again.
    ?assertEqual("CN=Lim\\, John,DC=example",
                 rabbit_auth_backend_ldap:fill_dn_with_username(
                   "${user_dn}", [{username, <<"Lim, John">>},
                                  {user_dn, "CN=Lim\\, John,DC=example"}])),
    ok.

%% Patterns can use `${ad_domain}' and `${ad_user}' directly. In DN patterns,
%% an unsafe AD split (see `safe_ad_args/1') leaves both variables unfilled.
ad_variable_pattern_escaping_rmq_4282(_Config) ->
    PrevBindPattern = application:get_env(rabbitmq_auth_backend_ldap, user_bind_pattern),
    PrevLog = application:get_env(rabbitmq_auth_backend_ldap, log),
    ok = application:set_env(rabbitmq_auth_backend_ldap, log, false),
    try
        SetPattern = fun(P) ->
            ok = application:set_env(rabbitmq_auth_backend_ldap, user_bind_pattern, P)
        end,
        SetPattern("${ad_user}-${ad_domain}"),
        ?assertEqual("alice-CORP",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"CORP\\alice">>)),
        %% A username without a down-level split leaves the variables unfilled.
        ?assertEqual("${ad_user}-${ad_domain}",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"alice">>)),
        %% Not a DN pattern: special characters in the user part stay as typed.
        ?assertEqual("a,b-CORP",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"CORP\\a,b">>)),
        SetPattern("${ad_domain}\\${ad_user}"),
        ?assertEqual("CORP\\alice",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"CORP\\alice">>)),
        ?assertEqual("CORP\\,ou=Evil",
                     rabbit_auth_backend_ldap:simple_bind_fill_pattern(
                       <<"CORP\\,ou=Evil">>)),
        %% DN patterns use vetted AD args.
        ?assertEqual("cn=alice,ou=CORP",
                     rabbit_auth_backend_ldap:fill_dn_with_username(
                       "cn=${ad_user},ou=${ad_domain}",
                       [{username, <<"CORP\\alice">>},
                        {ad_domain, <<"CORP">>}, {ad_user, <<"alice">>}])),
        ?assertEqual("cn=${ad_user},ou=${ad_domain}",
                     rabbit_auth_backend_ldap:fill_dn_with_username(
                       "cn=${ad_user},ou=${ad_domain}",
                       [{username, <<"alice">>}])),
        ?assertEqual("ou=Groups\\${ad_user}",
                     rabbit_auth_backend_ldap:fill_dn_with_username(
                       "ou=Groups\\${ad_user}",
                       [{username, <<"CORP\\,ou=Evil">>},
                        {ad_domain, <<"CORP">>}, {ad_user, <<",ou=Evil">>}]))
    after
        restore_env(user_bind_pattern, PrevBindPattern),
        restore_env(log, PrevLog)
    end,
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
