%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap).

%% Connect to an LDAP server for authentication and authorisation

-include_lib("eldap/include/eldap.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         state_can_expire/0]).

-export([get_connections/0]).

%% for tests
-export([purge_connections/0]).

-define(L(F, A),  log("LDAP "         ++ F, A)).
-define(L1(F, A), log("    LDAP "     ++ F, A)).
-define(L2(F, A), log("        LDAP " ++ F, A)).
-define(SCRUBBED_CREDENTIAL,  "xxxx").
-define(RESOURCE_ACCESS_QUERY_VARIABLES, [username, user_dn, vhost, resource, name, permission]).

-define(LDAP_OPERATION_RETRIES, 10).

-import(rabbit_misc, [pget/2]).

-record(impl, { user_dn, password }).

%%--------------------------------------------------------------------

get_connections() ->
    worker_pool:submit(ldap_pool, fun() -> get(ldap_conns) end, reuse).

purge_connections() ->
    [ok = worker_pool:submit(ldap_pool,
                             fun() -> purge_conn(Anon, Servers, Opts) end, reuse)
     || {{Anon, Servers, Opts}, _} <- maps:to_list(get_connections())],
    ok.

user_login_authentication(Username, []) ->
    %% Without password, e.g. EXTERNAL
    ?L("CHECK: passwordless login for ~s", [Username]),
    R = with_ldap(creds(none),
                  fun(LDAP) -> do_login(Username, unknown, none, LDAP) end),
    ?L("DECISION: passwordless login for ~s: ~p",
       [Username, log_result(R)]),
    R;

user_login_authentication(Username, AuthProps) when is_list(AuthProps) ->
    case pget(password, AuthProps) of
        undefined -> user_login_authentication(Username, []);
        <<>> ->
            %% Password "" is special in LDAP, see
            %% https://tools.ietf.org/html/rfc4513#section-5.1.2
            ?L("CHECK: unauthenticated login for ~s", [Username]),
            ?L("DECISION: unauthenticated login for ~s: denied", [Username]),
            {refused, "user '~s' - unauthenticated bind not allowed", [Username]};
        PW ->
            ?L("CHECK: login for ~s", [Username]),
            R = case dn_lookup_when() of
                    prebind -> UserDN = username_to_dn_prebind(Username),
                               with_ldap({ok, {UserDN, PW}},
                                         login_fun(Username, UserDN, PW, AuthProps));
                    _       -> with_ldap({ok, {simple_bind_fill_pattern(Username), PW}},
                                         login_fun(Username, unknown, PW, AuthProps))
                end,
            ?L("DECISION: login for ~s: ~p", [Username, log_result(R)]),
            R
    end;

user_login_authentication(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

user_login_authorization(Username, AuthProps) ->
    case user_login_authentication(Username, AuthProps) of
        {ok, #auth_user{impl = Impl, tags = Tags}} -> {ok, Impl, Tags};
        Else                                       -> Else
    end.

check_vhost_access(User = #auth_user{username = Username,
                                     impl     = #impl{user_dn = UserDN}},
                   VHost, AuthzData) ->
    OptionsArgs = context_as_options(AuthzData, undefined),
    ADArgs = rabbit_auth_backend_ldap_util:get_active_directory_args(Username),
    Args = [{username, Username},
            {user_dn,  UserDN},
            {vhost,    VHost}] ++ OptionsArgs ++ ADArgs,
    ?L("CHECK: ~s for ~s", [log_vhost(Args), log_user(User)]),
    R0 = evaluate_ldap(env(vhost_access_query), Args, User),
    R1 = ensure_rabbit_authz_backend_result(R0),
    ?L("DECISION: ~s for ~s: ~p (~p)",
       [log_vhost(Args), log_user(User),
        log_result(R0), log_result(R1)]),
    R1.

check_resource_access(User = #auth_user{username = Username,
                                        impl     = #impl{user_dn = UserDN}},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission,
                      AuthzContext) ->
    OptionsArgs = context_as_options(AuthzContext, undefined),
    ADArgs = rabbit_auth_backend_ldap_util:get_active_directory_args(Username),
    Args = [{username,   Username},
            {user_dn,    UserDN},
            {vhost,      VHost},
            {resource,   Type},
            {name,       Name},
            {permission, Permission}] ++ OptionsArgs ++ ADArgs,
    ?L("CHECK: ~s for ~s", [log_resource(Args), log_user(User)]),
    R0 = evaluate_ldap(env(resource_access_query), Args, User),
    R1 = ensure_rabbit_authz_backend_result(R0),
    ?L("DECISION: ~s for ~s: ~p (~p)",
       [log_resource(Args), log_user(User),
        log_result(R0), log_result(R1)]),
    R1.

check_topic_access(User = #auth_user{username = Username,
                                     impl     = #impl{user_dn = UserDN}},
                   #resource{virtual_host = VHost, kind = topic = Resource, name = Name},
                   Permission,
                   Context) ->
    OptionsArgs = context_as_options(Context, undefined),
    ADArgs = rabbit_auth_backend_ldap_util:get_active_directory_args(Username),
    Args = [{username,   Username},
            {user_dn,    UserDN},
            {vhost,      VHost},
            {resource,   Resource},
            {name,       Name},
            {permission, Permission}] ++ OptionsArgs ++ ADArgs,
    ?L("CHECK: ~s for ~s", [log_resource(Args), log_user(User)]),
    R0 = evaluate_ldap(env(topic_access_query), Args, User),
    R1 = ensure_rabbit_authz_backend_result(R0),
    ?L("DECISION: ~s for ~s: ~p (~p)",
        [log_resource(Args), log_user(User),
         log_result(R0), log_result(R1)]),
    R1.

state_can_expire() -> false.

%%--------------------------------------------------------------------

ensure_rabbit_authz_backend_result(true) ->
    true;
ensure_rabbit_authz_backend_result(false) ->
    false;
ensure_rabbit_authz_backend_result({error, _}=Error) ->
    Error;
% rabbitmq/rabbitmq-auth-backend-ldap#116
ensure_rabbit_authz_backend_result({refused, _, _}) ->
    false;
ensure_rabbit_authz_backend_result({ok, _}) ->
    true;
ensure_rabbit_authz_backend_result({ok, _, _}) ->
    true.

context_as_options(Context, Namespace) when is_map(Context) ->
    % filter keys that would erase fixed variables
    lists:flatten([begin
         Value = maps:get(Key, Context),
         case Value of
             MapOfValues when is_map(MapOfValues) ->
                 context_as_options(MapOfValues, Key);
             SimpleValue ->
                 case Namespace of
                     undefined ->
                         {rabbit_data_coercion:to_atom(Key), SimpleValue};
                     Namespace ->
                         {create_option_name_with_namespace(Namespace, Key), Value}
                 end
         end
     end || Key <- maps:keys(Context), lists:member(
                    rabbit_data_coercion:to_atom(Key),
                    ?RESOURCE_ACCESS_QUERY_VARIABLES) =:= false]);
context_as_options(_, _) ->
    [].

create_option_name_with_namespace(Namespace, Key) ->
    rabbit_data_coercion:to_atom(
        rabbit_data_coercion:to_list(Namespace) ++ "." ++ rabbit_data_coercion:to_list(Key)
    ).

evaluate(Query, Args, User, LDAP) ->
    ?L1("evaluating query: ~p", [Query]),
    evaluate0(Query, Args, User, LDAP).

evaluate0({constant, Bool}, _Args, _User, _LDAP) ->
    ?L1("evaluated constant: ~p", [Bool]),
    Bool;

evaluate0({for, [{Type, Value, SubQuery}|Rest]}, Args, User, LDAP) ->
    case pget(Type, Args) of
        undefined -> {error, {args_do_not_contain, Type, Args}};
        Value     -> ?L1("selecting subquery ~s = ~s", [Type, Value]),
                     evaluate(SubQuery, Args, User, LDAP);
        _         -> evaluate0({for, Rest}, Args, User, LDAP)
    end;

evaluate0({for, []}, _Args, _User, _LDAP) ->
    {error, {for_query_incomplete}};

evaluate0({exists, DNPattern}, Args, _User, LDAP) ->
    %% eldap forces us to have a filter. objectClass should always be there.
    Filter = eldap:present("objectClass"),
    DN = fill(DNPattern, Args),
    R = object_exists(DN, Filter, LDAP),
    ?L1("evaluated exists for \"~s\": ~p", [DN, R]),
    R;

evaluate0({in_group, DNPattern}, Args, User, LDAP) ->
    evaluate({in_group, DNPattern, "member"}, Args, User, LDAP);

evaluate0({in_group, DNPattern, Desc}, Args,
          #auth_user{impl = #impl{user_dn = UserDN}}, LDAP) ->
    Filter = eldap:equalityMatch(Desc, UserDN),
    DN = fill(DNPattern, Args),
    R = object_exists(DN, Filter, LDAP),
    ?L1("evaluated in_group for \"~s\": ~p", [DN, R]),
    R;

evaluate0({in_group_nested, DNPattern}, Args, User, LDAP) ->
	evaluate({in_group_nested, DNPattern, "member", subtree},
             Args, User, LDAP);
evaluate0({in_group_nested, DNPattern, Desc}, Args, User, LDAP) ->
    evaluate({in_group_nested, DNPattern, Desc, subtree},
             Args, User, LDAP);
evaluate0({in_group_nested, DNPattern, Desc, Scope}, Args,
          #auth_user{impl = #impl{user_dn = UserDN}}, LDAP) ->
    GroupsBase = case env(group_lookup_base) of
                     none ->
                         get_expected_env_str(dn_lookup_base, none);
                     B ->
                         B
                 end,
    GroupDN = fill(DNPattern, Args),
    EldapScope =
        case Scope of
            subtree      -> eldap:wholeSubtree();
            singlelevel  -> eldap:singleLevel();
            single_level -> eldap:singleLevel();
            onelevel     -> eldap:singleLevel();
            one_level    -> eldap:singleLevel()
        end,
    search_nested_group(LDAP, Desc, GroupsBase, EldapScope, UserDN, GroupDN, []);

evaluate0({'not', SubQuery}, Args, User, LDAP) ->
    R = evaluate(SubQuery, Args, User, LDAP),
    ?L1("negated result to ~s", [R]),
    not R;

evaluate0({'and', Queries}, Args, User, LDAP) when is_list(Queries) ->
    R = lists:foldl(fun (Q,  true)    -> evaluate(Q, Args, User, LDAP);
                        % Treat any non-true result as false
                        (_Q, _Result) -> false
                    end, true, Queries),
    ?L1("'and' result: ~s", [R]),
    R;

evaluate0({'or', Queries}, Args, User, LDAP) when is_list(Queries) ->
    R = lists:foldl(fun (_Q, true)    -> true;
                        % Treat any non-true result as false
                        (Q,  _Result) -> evaluate(Q, Args, User, LDAP)
                    end, false, Queries),
    ?L1("'or' result: ~s", [R]),
    R;

evaluate0({equals, StringQuery1, StringQuery2}, Args, User, LDAP) ->
    safe_eval(fun (String1, String2) ->
                      R  = if String1 =:= String2 -> true;
                              true -> is_multi_attr_member(String1, String2)
                           end,
                      ?L1("evaluated equals \"~s\", \"~s\": ~s",
                          [format_multi_attr(String1),
                           format_multi_attr(String2), R]),
                      R
              end,
              evaluate(StringQuery1, Args, User, LDAP),
              evaluate(StringQuery2, Args, User, LDAP));

evaluate0({match, {string, _} = StringQuery, {string, _} = REQuery}, Args, User, LDAP) ->
    safe_eval(fun (String1, String2) ->
                      do_match(String1, String2)
              end,
              evaluate(StringQuery, Args, User, LDAP),
              evaluate(REQuery, Args, User, LDAP));

evaluate0({match, StringQuery, {string, _} = REQuery}, Args, User, LDAP) when is_list(StringQuery)->
    safe_eval(fun (String1, String2) ->
                      do_match(String1, String2)
              end,
        evaluate(StringQuery, Args, User, LDAP),
        evaluate(REQuery, Args, User, LDAP));

evaluate0({match, {string, _} = StringQuery, REQuery}, Args, User, LDAP) when is_list(REQuery) ->
    safe_eval(fun (String1, String2) ->
                      do_match(String1, String2)
              end,
        evaluate(StringQuery, Args, User, LDAP),
        evaluate(REQuery, Args, User, LDAP));

evaluate0({match, StringQuery, REQuery}, Args, User, LDAP) when is_list(StringQuery),
                                                                is_list(REQuery)  ->
    safe_eval(fun (String1, String2) ->
                      do_match(String1, String2)
              end,
        evaluate(StringQuery, Args, User, LDAP),
        evaluate(REQuery, Args, User, LDAP));

evaluate0({match, StringQuery, REQuery}, Args, User, LDAP) ->
    safe_eval(fun (String1, String2) ->
                      do_match_multi(String1, String2)
              end,
        evaluate(StringQuery, Args, User, LDAP),
        evaluate(REQuery, Args, User, LDAP));

evaluate0(StringPattern, Args, User, LDAP) when is_list(StringPattern) ->
    evaluate0({string, StringPattern}, Args, User, LDAP);

evaluate0({string, StringPattern}, Args, _User, _LDAP) ->
    R = fill(StringPattern, Args),
    ?L1("evaluated string for \"~s\"", [R]),
    R;

evaluate0({attribute, DNPattern, AttributeName}, Args, _User, LDAP) ->
    DN = fill(DNPattern, Args),
    R = attribute(DN, AttributeName, LDAP),
    ?L1("evaluated attribute \"~s\" for \"~s\": ~p",
        [AttributeName, DN, format_multi_attr(R)]),
    R;

evaluate0(Q, Args, _User, _LDAP) ->
    {error, {unrecognised_query, Q, Args}}.

search_groups(LDAP, Desc, GroupsBase, Scope, DN) ->
    Filter = eldap:equalityMatch(Desc, DN),
    case eldap:search(LDAP,
                      [{base, GroupsBase},
                       {filter, Filter},
                       {attributes, ["dn"]},
                       {scope, Scope}]) of
        {error, _} = E ->
            ?L("error searching for parent groups for \"~s\": ~p", [DN, E]),
            [];
        {ok, {referral, Referrals}} ->
            {error, {referrals_not_supported, Referrals}};
        {ok, #eldap_search_result{entries = []}} ->
            [];
        {ok, #eldap_search_result{entries = Entries}} ->
            [ON || #eldap_entry{object_name = ON} <- Entries]
    end.

search_nested_group(LDAP, Desc, GroupsBase, Scope, CurrentDN, TargetDN, Path) ->
    case lists:member(CurrentDN, Path) of
        true  ->
            ?L("recursive cycle on DN ~s while searching for group ~s",
               [CurrentDN, TargetDN]),
            false;
        false ->
            GroupDNs = search_groups(LDAP, Desc, GroupsBase, Scope, CurrentDN),
            case lists:member(TargetDN, GroupDNs) of
                true  ->
                    true;
                false ->
                    NextPath = [CurrentDN | Path],
                    lists:any(fun(DN) ->
                        search_nested_group(LDAP, Desc, GroupsBase, Scope,
                                            DN, TargetDN, NextPath)
                    end,
                    GroupDNs)
            end
    end.

safe_eval(_F, {error, _}, _)          -> false;
safe_eval(_F, _,          {error, _}) -> false;
safe_eval(F,  V1,         V2)         -> F(V1, V2).

do_match(S1, S2) ->
    case re:run(S1, S2) of
        {match, _} ->
            log_match(S1, S2, R = true),
            R;
        nomatch ->
            log_match(S1, S2, R = false),
            R
    end.

%% In some cases when fetching regular expressions, LDAP evalution()
%% returns a list of strings, so we need to wrap guards around that.
%% If a list of strings is returned, loop and match versus each element.
do_match_multi(S1, []) ->
    log_match(S1, [], R = false),
    R;
do_match_multi(S1 = [H1|_], [H2|Tail]) when is_list(H2) and not is_list(H1) ->
    case re:run(S1, H2) of
        {match, _} ->
            log_match(S1, H2, R = true),
            R;
        _ ->
            log_match(S1,H2, false),
            do_match_multi(S1, Tail)
    end;
do_match_multi([], S2) ->
    log_match([], S2, R = false),
    R;
do_match_multi([H1|Tail], S2 = [H2|_] ) when is_list(H1) and not is_list(H2) ->
    case re:run(H1, S2) of
        {match, _} ->
            log_match(H1, S2, R = true),
            R;
        _ ->
            log_match(H1, S2, false),
            do_match_multi(Tail, S2)
    end;
do_match_multi([H1|_],[H2|_]) when is_list(H1) and is_list(H2) ->
    false; %% Unsupported combination
do_match_multi(S1, S2) ->
    do_match(S1, S2).

log_match(String, RE, Result) ->
    ?L1("evaluated match \"~s\" against RE \"~s\": ~s",
        [format_multi_attr(String),
         format_multi_attr(RE), Result]).

object_exists(DN, Filter, LDAP) ->
    case eldap:search(LDAP,
                      [{base, DN},
                       {filter, Filter},
                       {attributes, ["objectClass"]}, %% Reduce verbiage
                       {scope, eldap:baseObject()}]) of
        {ok, {referral, Referrals}} ->
            {error, {referrals_not_supported, Referrals}};
        {ok, #eldap_search_result{entries = Entries}} ->
            length(Entries) > 0;
        {error, _} = E ->
            E
    end.

attribute(DN, AttributeName, LDAP) ->
    case eldap:search(LDAP,
                      [{base, DN},
                       {filter, eldap:present("objectClass")},
                       {attributes, [AttributeName]}]) of
        {ok, {referral, Referrals}} ->
            {error, {referrals_not_supported, Referrals}};
        {ok, #eldap_search_result{entries = E = [#eldap_entry{}|_]}} ->
            get_attributes(AttributeName, E);
        {ok, #eldap_search_result{entries = _}} ->
            {error, not_found};
        {error, _} = E ->
            E
    end.

evaluate_ldap(Q, Args, User) ->
    with_ldap(creds(User), fun(LDAP) -> evaluate(Q, Args, User, LDAP) end).

%%--------------------------------------------------------------------

with_ldap(Creds, Fun) -> with_ldap(Creds, Fun, env(servers)).

with_ldap(_Creds, _Fun, undefined) ->
    {error, no_ldap_servers_defined};

with_ldap({error, _} = E, _Fun, _State) ->
    E;

%% TODO - while we now pool LDAP connections we don't make any attempt
%% to avoid rebinding if the connection is already bound as the user
%% of interest, so this could still be more efficient.
with_ldap({ok, Creds}, Fun, Servers) ->
    Opts0 = [{port, env(port)},
             {idle_timeout, env(idle_timeout)},
             {anon_auth, env(anon_auth)}],
    Opts1 = case env(log) of
                network ->
                    Pre = "    LDAP network traffic: ",
                    _ = rabbit_log_ldap:info(
                      "    LDAP connecting to servers: ~p~n", [Servers]),
                    [{log, fun(1, S, A) -> _ = rabbit_log_ldap:warning(Pre ++ S, A);
                              (2, S, A) ->
                                   _ = rabbit_log_ldap:info(Pre ++ S, scrub_creds(A, []))
                           end} | Opts0];
                network_unsafe ->
                    Pre = "    LDAP network traffic: ",
                    _ = rabbit_log_ldap:info(
                      "    LDAP connecting to servers: ~p~n", [Servers]),
                    [{log, fun(1, S, A) -> _ = rabbit_log_ldap:warning(Pre ++ S, A);
                              (2, S, A) -> _ = rabbit_log_ldap:info(   Pre ++ S, A)
                           end} | Opts0];
                _ ->
                    Opts0
            end,
    %% eldap defaults to 'infinity' but doesn't allow you to set that. Harrumph.
    Opts = case env(timeout) of
               infinity -> Opts1;
               MS       -> [{timeout, MS} | Opts1]
           end,

    worker_pool:submit(
      ldap_pool,
      fun () ->
              case with_login(Creds, Servers, Opts, Fun) of
                  {error, {gen_tcp_error, _}} ->
                      purge_connection(Creds, Servers, Opts),
                      with_login(Creds, Servers, Opts, Fun);
                  Result -> Result
              end
      end, reuse).

with_login(Creds, Servers, Opts, Fun) ->
    with_login(Creds, Servers, Opts, Fun, ?LDAP_OPERATION_RETRIES).
with_login(_Creds, _Servers, _Opts, _Fun, 0 = _RetriesLeft) ->
    _ = rabbit_log_ldap:warning("LDAP failed to perform an operation. TCP connection to a LDAP server was closed or otherwise defunct. Exhausted all retries."),
    {error, ldap_connect_error};
with_login(Creds, Servers, Opts, Fun, RetriesLeft) ->
    case get_or_create_conn(Creds == anon, Servers, Opts) of
        {ok, {ConnType, LDAP}} ->
            Result = case Creds of
                         anon ->
                             ?L1("anonymous bind", []),
                             case call_ldap_fun(Fun, LDAP) of
                                 {error, ldap_closed} ->
                                     purge_connection(Creds, Servers, Opts),
                                     with_login(Creds, Servers, Opts, Fun, RetriesLeft - 1);
                                 Other -> Other
                             end;
                         {UserDN, Password} ->
                             case eldap:simple_bind(LDAP, UserDN, Password) of
                                 ok ->
                                     ?L1("bind succeeded: ~s",
                                         [scrub_dn(UserDN, env(log))]),
                                     case call_ldap_fun(Fun, LDAP, UserDN) of
                                         {error, ldap_closed} ->
                                             with_login(Creds, Servers, Opts, Fun, RetriesLeft - 1);
                                         {error, {gen_tcp_error, _}} ->
                                             with_login(Creds, Servers, Opts, Fun, RetriesLeft - 1);
                                         Other -> Other
                                     end;
                                 {error, invalidCredentials} ->
                                     ?L1("bind returned \"invalid credentials\": ~s",
                                         [scrub_dn(UserDN, env(log))]),
                                     {refused, UserDN, []};
                                 {error, ldap_closed} ->
                                     purge_connection(Creds, Servers, Opts),
                                     with_login(Creds, Servers, Opts, Fun, RetriesLeft - 1);
                                 {error, {gen_tcp_error, _}} ->
                                     purge_connection(Creds, Servers, Opts),
                                     with_login(Creds, Servers, Opts, Fun, RetriesLeft - 1);
                                 {error, E} ->
                                     ?L1("bind error: ~p ~p",
                                         [scrub_dn(UserDN, env(log)), E]),
                                     %% Do not report internal bind error to a client
                                     {error, ldap_bind_error}
                             end
                     end,
            ok = case ConnType of
                     eldap_transient -> eldap:close(LDAP);
                     _ -> ok
                 end,
            Result;
        Error ->
            ?L1("connect error: ~p", [Error]),
            case Error of
                {error, {gen_tcp_error, _}} -> Error;
                %% Do not report internal connection error to a client
                _Other                      -> {error, ldap_connect_error}
            end
    end.

purge_connection(Creds, Servers, Opts) ->
    %% purge and retry with a new connection
    _ = rabbit_log_ldap:warning("TCP connection to a LDAP server was closed or otherwise defunct."),
    purge_conn(Creds == anon, Servers, Opts),
    _ = rabbit_log_ldap:warning("LDAP will retry with a new connection.").

call_ldap_fun(Fun, LDAP) ->
    call_ldap_fun(Fun, LDAP, "").

call_ldap_fun(Fun, LDAP, UserDN) ->
    case Fun(LDAP) of
        {error, ldap_closed} ->
            %% LDAP connection was close, let with_login/5 retry
            {error, ldap_closed};
        {error, {gen_tcp_error, E}} ->
            %% ditto
            {error, {gen_tcp_error, E}};
        {error, E} ->
            ?L1("evaluate error: ~s ~p", [scrub_dn(UserDN, env(log)), E]),
            {error, ldap_evaluate_error};
        Other -> Other
    end.

%% Gets either the anonymous or bound (authenticated) connection
get_or_create_conn(IsAnon, Servers, Opts) ->
    Conns = case get(ldap_conns) of
                undefined -> #{};
                Dict      -> Dict
            end,
    Key = {IsAnon, Servers, Opts},
    case maps:find(Key, Conns) of
        {ok, Conn} ->
            Timeout = rabbit_misc:pget(idle_timeout, Opts, infinity),
            %% Defer the timeout by re-setting it.
            set_connection_timeout(Key, Timeout),
            {ok, {eldap_pooled, Conn}};
        error      ->
            {Timeout, EldapOpts} = case lists:keytake(idle_timeout, 1, Opts) of
                false                             -> {infinity, Opts};
                {value, {idle_timeout, T}, EOpts} -> {T, EOpts}
            end,
            case {eldap_open(Servers, EldapOpts), Timeout} of
                %% If the timeout was set to 0, treat it as a one-off connection.
                %% See rabbitmq/rabbitmq-auth-backend-ldap#120 for background.
                {{ok, Conn}, 0} ->
                    {ok, {eldap_transient, Conn}};
                %% Non-zero timeout, put it in the pool
                {{ok, Conn}, Timeout} ->
                    put(ldap_conns, maps:put(Key, Conn, Conns)),
                    set_connection_timeout(Key, Timeout),
                    {ok, {eldap_pooled, Conn}};
                {Error, _} ->
                    Error
            end
    end.

set_connection_timeout(_, infinity) ->
    ok;
set_connection_timeout(Key, Timeout) when is_integer(Timeout) ->
    worker_pool_worker:set_timeout(Key, Timeout,
        fun() ->
            Conns = case get(ldap_conns) of
                undefined -> #{};
                Dict      -> Dict
            end,
            case maps:find(Key, Conns) of
                {ok, Conn} ->
                    eldap:close(Conn),
                    put(ldap_conns, maps:remove(Key, Conns));
                _ -> ok
            end
        end).

%% Get attribute(s) from eldap entry
get_attributes(_AttrName, []) -> {error, not_found};
get_attributes(AttrName, [#eldap_entry{attributes = A}|Rem]) ->
    case pget(AttrName, A) of
        [Attr|[]]                    -> Attr;
        Attrs when length(Attrs) > 1 -> Attrs;
        _                            -> get_attributes(AttrName, Rem)
    end;
get_attributes(AttrName, [_|Rem])    -> get_attributes(AttrName, Rem).

%% Format multiple attribute values for logging
format_multi_attr(Attrs) ->
    format_multi_attr(io_lib:printable_list(Attrs), Attrs).

format_multi_attr(true, Attrs)                     -> Attrs;
format_multi_attr(_,    Attrs) when is_list(Attrs) -> string:join(Attrs, "; ");
format_multi_attr(_,    Error)                     -> Error.


%% In case of multiple attributes, check for equality bi-directionally
is_multi_attr_member(Str1, Str2) ->
    lists:member(Str1, Str2) orelse lists:member(Str2, Str1).

purge_conn(IsAnon, Servers, Opts) ->
    Conns = get(ldap_conns),
    Key = {IsAnon, Servers, Opts},
    {ok, Conn} = maps:find(Key, Conns),
    _ = rabbit_log_ldap:warning("LDAP will purge an already closed or defunct LDAP server connection from the pool"),
    % We cannot close the connection with eldap:close/1 because as of OTP-13327
    % eldap will try to do_unbind first and will fail with a `{gen_tcp_error, closed}`.
    % Since we know that the connection is already closed, we just
    % kill its process.
    unlink(Conn),
    exit(Conn, closed),
    put(ldap_conns, maps:remove(Key, Conns)),
    ok.

eldap_open(Servers, Opts) ->
    case eldap:open(Servers, ssl_conf() ++ Opts) of
        {ok, LDAP} ->
            TLS = env(use_starttls),
            case {TLS, at_least("5.10.4")} of %%R16B03
                {false, _}     -> {ok, LDAP};
                {true,  false} -> exit({starttls_requires_min_r16b3});
                {true,  _}     -> TLSOpts = ssl_options(),
                                  ELDAP = eldap, %% Fool xref
                                  case ELDAP:start_tls(LDAP, TLSOpts) of
                                      ok    -> {ok, LDAP};
                                      Error -> Error
                                  end
            end;
        Error ->
            Error
    end.

ssl_conf() ->
    %% We must make sure not to add SSL options unless a) we have at least R16A
    %% b) we have SSL turned on (or it breaks StartTLS...)
    case env(use_ssl, false) of
        false -> [{ssl, false}];
        true  -> %% Only the unfixed version can be []
                 case env(ssl_options) of
                     []        -> [{ssl, true}];
                     undefined -> [{ssl, true}];
                     _         -> [{ssl, true}, {sslopts, ssl_options()}]
                 end
    end.

ssl_options() ->
    Opts0 = rabbit_networking:fix_ssl_options(env(ssl_options)),
    case env(ssl_hostname_verification, undefined) of
        wildcard ->
            _ = rabbit_log_ldap:debug("Enabling wildcard-aware hostname verification for LDAP client connections"),
            %% Needed for non-HTTPS connections that connect to servers that use wildcard certificates.
            %% See https://erlang.org/doc/man/public_key.html#pkix_verify_hostname_match_fun-1.
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | Opts0];
        _ ->
            Opts0
    end.

at_least(Ver) ->
    rabbit_misc:version_compare(erlang:system_info(version), Ver) =/= lt.

% Note:
% Default is configured in the Makefile
get_expected_env_str(Key, Default) ->
    V = case env(Key) of
            Default ->
                _ = rabbit_log_ldap:warning("rabbitmq_auth_backend_ldap configuration key '~p' is set to "
                                        "the default value of '~p', expected to get a non-default value~n",
                                        [Key, Default]),
                Default;
            V0 ->
                V0
        end,
    rabbit_data_coercion:to_list(V).

env(Key) ->
    case application:get_env(rabbitmq_auth_backend_ldap, Key) of
       {ok, V} -> V;
        undefined -> undefined
    end.

env(Key, Default) ->
    application:get_env(rabbitmq_auth_backend_ldap, Key, Default).

login_fun(User, UserDN, Password, AuthProps) ->
    fun(L) -> case pget(vhost, AuthProps) of
                  undefined -> do_login(User, UserDN, Password, L);
                  VHost     -> do_login(User, UserDN, Password, VHost, L)
              end
    end.

do_login(Username, PrebindUserDN, Password, LDAP) ->
    do_login(Username, PrebindUserDN, Password, <<>>, LDAP).

do_login(Username, PrebindUserDN, Password, VHost, LDAP) ->
    UserDN = case PrebindUserDN of
                 unknown -> username_to_dn(Username, LDAP, dn_lookup_when());
                 _       -> PrebindUserDN
             end,
    User = #auth_user{username     = Username,
                      impl         = #impl{user_dn  = UserDN,
                                           password = Password}},
    DTQ = fun (LDAPn) -> do_tag_queries(Username, UserDN, User, VHost, LDAPn) end,
    TagRes = case env(other_bind) of
                 as_user -> DTQ(LDAP);
                 _       -> with_ldap(creds(User), DTQ)
             end,
    case TagRes of
        {ok, L} -> {ok, User#auth_user{tags = [Tag || {Tag, true} <- L]}};
        E       -> E
    end.

do_tag_queries(Username, UserDN, User, VHost, LDAP) ->
    {ok, [begin
              ?L1("CHECK: does ~s have tag ~s?", [Username, Tag]),
              VhostArgs = vhost_if_defined(VHost),
              ADArgs = rabbit_auth_backend_ldap_util:get_active_directory_args(Username),
              EvalArgs = [{username, Username}, {user_dn, UserDN}] ++ VhostArgs ++ ADArgs,
              R = evaluate(Q, EvalArgs, User, LDAP),
              ?L1("DECISION: does ~s have tag ~s? ~p",
                  [Username, Tag, R]),
              {Tag, R}
          end || {Tag, Q} <- env(tag_queries)]}.

vhost_if_defined([])    -> [];
vhost_if_defined(<<>>)  -> [];
vhost_if_defined(VHost) -> [{vhost, VHost}].

dn_lookup_when() ->
    case {env(dn_lookup_attribute), env(dn_lookup_bind)} of
        {none, _} ->
            never;
        {_, as_user} ->
            postbind;
        %% make it more obvious what the invariants are,
        %% see rabbitmq/rabbitmq-auth-backend-ldap#94. MK.
        {_, anon} ->
            prebind;
        {_, _} ->
            prebind
    end.

username_to_dn_prebind(Username) ->
    with_ldap({ok, env(dn_lookup_bind)},
              fun (LDAP) -> dn_lookup(Username, LDAP) end).

username_to_dn(Username, LDAP,  postbind) -> dn_lookup(Username, LDAP);
username_to_dn(Username, _LDAP, _When)    -> fill_user_dn_pattern(Username).

dn_lookup(Username, LDAP) ->
    Filled = fill_user_dn_pattern(Username),
    DnLookupBase = get_expected_env_str(dn_lookup_base, none),
    DnLookupAttribute = get_expected_env_str(dn_lookup_attribute, none),
    case eldap:search(LDAP,
                      [{base, DnLookupBase},
                       {filter, eldap:equalityMatch(DnLookupAttribute, Filled)},
                       {attributes, ["distinguishedName"]}]) of
        {ok, {referral, Referrals}} ->
            {error, {referrals_not_supported, Referrals}};
        {ok, #eldap_search_result{entries = [#eldap_entry{object_name = DN}]}}->
            ?L1("DN lookup: ~s -> ~s", [Username, DN]),
            DN;
        {ok, #eldap_search_result{entries = Entries}} ->
            _ = rabbit_log_ldap:warning("Searching for DN for ~s, got back ~p~n",
                               [Filled, Entries]),
            Filled;
        {error, _} = E ->
            exit(E)
    end.

fill_user_dn_pattern(Username) ->
    ADArgs = rabbit_auth_backend_ldap_util:get_active_directory_args(Username),
    fill(env(user_dn_pattern), [{username, Username}] ++ ADArgs).

simple_bind_fill_pattern(Username) ->
    simple_bind_fill_pattern(env(user_bind_pattern), Username).

simple_bind_fill_pattern(none, Username) ->
    fill_user_dn_pattern(Username);
simple_bind_fill_pattern(Pattern, Username) ->
    ADArgs = rabbit_auth_backend_ldap_util:get_active_directory_args(Username),
    fill(Pattern, [{username, Username}] ++ ADArgs).

creds(User) -> creds(User, env(other_bind)).

creds(none, as_user) ->
    {error, "'other_bind' set to 'as_user' but no password supplied"};
creds(#auth_user{impl = #impl{user_dn = UserDN, password = PW}}, as_user) ->
    {ok, {UserDN, PW}};
creds(_, Creds) ->
    {ok, Creds}.

%% Scrub credentials
scrub_creds([], Acc)      -> lists:reverse(Acc);
scrub_creds([H|Rem], Acc) ->
    scrub_creds(Rem, [scrub_payload_creds(H)|Acc]).

%% Scrub credentials from specific payloads
scrub_payload_creds({'BindRequest', N, DN, {simple, _PWD}}) ->
  {'BindRequest', N, scrub_dn(DN), {simple, ?SCRUBBED_CREDENTIAL}};
scrub_payload_creds(Any) -> Any.

scrub_dn(DN) -> scrub_dn(DN, network).

scrub_dn(DN, network_unsafe) -> DN;
scrub_dn(DN, false)          -> DN;
scrub_dn(DN, _) ->
    case is_dn(DN) of
        true -> scrub_rdn(string:tokens(DN, ","), []);
        _    ->
            %% We aren't fully certain its a DN, & don't know what sensitive
            %% info could be contained, thus just scrub the entire credential
            ?SCRUBBED_CREDENTIAL
    end.

scrub_rdn([], Acc) ->
    string:join(lists:reverse(Acc), ",");
scrub_rdn([DN|Rem], Acc) ->
    DN0 = case catch string:tokens(DN, "=") of
              L = [RDN, _] -> case string:to_lower(RDN) of
                                  "cn"  -> [RDN, ?SCRUBBED_CREDENTIAL];
                                  "dc"  -> [RDN, ?SCRUBBED_CREDENTIAL];
                                  "ou"  -> [RDN, ?SCRUBBED_CREDENTIAL];
                                  "uid" -> [RDN, ?SCRUBBED_CREDENTIAL];
                                  _     -> L
                              end;
              _Any ->
                  %% There's no RDN, log "xxxx=xxxx"
                  [?SCRUBBED_CREDENTIAL, ?SCRUBBED_CREDENTIAL]
          end,
  scrub_rdn(Rem, [string:join(DN0, "=")|Acc]).

is_dn(S) when is_list(S) ->
    case catch string:tokens(rabbit_data_coercion:to_list(S), "=") of
        L when length(L) > 1 -> true;
        _                    -> false
    end;
is_dn(_S) -> false.

log(Fmt,  Args) -> case env(log) of
                       false -> ok;
                       _     -> _ = rabbit_log_ldap:info(Fmt ++ "~n", Args)
                   end.

fill(Fmt, Args) ->
    ?L2("filling template \"~s\" with~n            ~p", [Fmt, Args]),
    R = rabbit_auth_backend_ldap_util:fill(Fmt, Args),
    ?L2("template result: \"~s\"", [R]),
    R.

log_result({ok, #auth_user{}}) -> ok;
log_result(true)               -> ok;
log_result(false)              -> denied;
log_result({refused, _, _})    -> denied;
log_result(E)                  -> E.

log_user(#auth_user{username = U}) -> rabbit_misc:format("\"~s\"", [U]).

log_vhost(Args) ->
    rabbit_misc:format("access to vhost \"~s\"", [pget(vhost, Args)]).

log_resource(Args) ->
    rabbit_misc:format("~s permission for ~s \"~s\" in \"~s\"",
                       [pget(permission, Args), pget(resource, Args),
                        pget(name, Args), pget(vhost, Args)]).
