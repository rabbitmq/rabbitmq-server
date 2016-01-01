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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap).

%% Connect to an LDAP server for authentication and authorisation

-include_lib("eldap/include/eldap.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/1,
         check_vhost_access/3, check_resource_access/3]).

-define(L(F, A),  log("LDAP "         ++ F, A)).
-define(L1(F, A), log("    LDAP "     ++ F, A)).
-define(L2(F, A), log("        LDAP " ++ F, A)).

-import(rabbit_misc, [pget/2]).

-record(impl, { user_dn, password }).

%%--------------------------------------------------------------------

user_login_authentication(Username, []) ->
    %% Without password, e.g. EXTERNAL
    ?L("CHECK: passwordless login for ~s", [Username]),
    R = with_ldap(creds(none),
                  fun(LDAP) -> do_login(Username, unknown, none, LDAP) end),
    ?L("DECISION: passwordless login for ~s: ~p",
       [Username, log_result(R)]),
    R;

user_login_authentication(Username, [{password, <<>>}]) ->
    %% Password "" is special in LDAP, see
    %% https://tools.ietf.org/html/rfc4513#section-5.1.2
    ?L("CHECK: unauthenticated login for ~s", [Username]),
    ?L("DECISION: unauthenticated login for ~s: denied", [Username]),
    {refused, "user '~s' - unauthenticated bind not allowed", [Username]};

user_login_authentication(User, [{password, PW}]) ->
    ?L("CHECK: login for ~s", [User]),
    R = case dn_lookup_when() of
            prebind -> UserDN = username_to_dn_prebind(User),
                       with_ldap({ok, {UserDN, PW}},
                                 fun(L) -> do_login(User, UserDN,  PW, L) end);
            _       -> with_ldap({ok, {fill_user_dn_pattern(User), PW}},
                                 fun(L) -> do_login(User, unknown, PW, L) end)
        end,
    ?L("DECISION: login for ~s: ~p", [User, log_result(R)]),
    R;

user_login_authentication(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

user_login_authorization(Username) ->
    case user_login_authentication(Username, []) of
        {ok, #auth_user{impl = Impl, tags = Tags}} -> {ok, Impl, Tags};
        Else                                       -> Else
    end.

check_vhost_access(User = #auth_user{username = Username,
                                     impl     = #impl{user_dn = UserDN}},
                   VHost, _Sock) ->
    Args = [{username, Username},
            {user_dn,  UserDN},
            {vhost,    VHost}],
    ?L("CHECK: ~s for ~s", [log_vhost(Args), log_user(User)]),
    R = evaluate_ldap(env(vhost_access_query), Args, User),
    ?L("DECISION: ~s for ~s: ~p",
       [log_vhost(Args), log_user(User), log_result(R)]),
    R.

check_resource_access(User = #auth_user{username = Username,
                                        impl     = #impl{user_dn = UserDN}},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission) ->
    Args = [{username,   Username},
            {user_dn,    UserDN},
            {vhost,      VHost},
            {resource,   Type},
            {name,       Name},
            {permission, Permission}],
    ?L("CHECK: ~s for ~s", [log_resource(Args), log_user(User)]),
    R = evaluate_ldap(env(resource_access_query), Args, User),
    ?L("DECISION: ~s for ~s: ~p",
       [log_resource(Args), log_user(User), log_result(R)]),
    R.

%%--------------------------------------------------------------------

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

evaluate0({'not', SubQuery}, Args, User, LDAP) ->
    R = evaluate(SubQuery, Args, User, LDAP),
    ?L1("negated result to ~s", [R]),
    not R;

evaluate0({'and', Queries}, Args, User, LDAP) when is_list(Queries) ->
    R = lists:foldl(fun (Q,  true)  -> evaluate(Q, Args, User, LDAP);
                        (_Q, false) -> false
                    end, true, Queries),
    ?L1("'and' result: ~s", [R]),
    R;

evaluate0({'or', Queries}, Args, User, LDAP) when is_list(Queries) ->
    R = lists:foldl(fun (_Q, true)  -> true;
                        (Q,  false) -> evaluate(Q, Args, User, LDAP)
                    end, false, Queries),
    ?L1("'or' result: ~s", [R]),
    R;

evaluate0({equals, StringQuery1, StringQuery2}, Args, User, LDAP) ->
    safe_eval(fun (String1, String2) ->
                      R = String1 =:= String2,
                      ?L1("evaluated equals \"~s\", \"~s\": ~s",
                          [String1, String2, R]),
                      R
              end,
              evaluate(StringQuery1, Args, User, LDAP),
              evaluate(StringQuery2, Args, User, LDAP));

evaluate0({match, StringQuery, REQuery}, Args, User, LDAP) ->
    safe_eval(fun (String, RE) ->
                      R = case re:run(String, RE) of
                              {match, _} -> true;
                              nomatch    -> false
                          end,
                      ?L1("evaluated match \"~s\" against RE \"~s\": ~s",
                          [String, RE, R]),
                      R
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
        [AttributeName, DN, R]),
    R;

evaluate0(Q, Args, _User, _LDAP) ->
    {error, {unrecognised_query, Q, Args}}.

safe_eval(_F, {error, _}, _)          -> false;
safe_eval(_F, _,          {error, _}) -> false;
safe_eval(F,  V1,         V2)         -> F(V1, V2).

object_exists(DN, Filter, LDAP) ->
    case eldap:search(LDAP,
                      [{base, DN},
                       {filter, Filter},
                       {attributes, ["objectClass"]}, %% Reduce verbiage
                       {scope, eldap:baseObject()}]) of
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
        {ok, #eldap_search_result{entries = [#eldap_entry{attributes = A}]}} ->
            case pget(AttributeName, A) of
                [Attr] -> Attr;
                _      -> {error, not_found}
            end;
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
    Opts0 = [{port, env(port)}],
    Opts1 = case env(log) of
                network ->
                    Pre = "    LDAP network traffic: ",
                    rabbit_log:info(
                      "    LDAP connecting to servers: ~p~n", [Servers]),
                    [{log, fun(1, S, A) -> rabbit_log:warning(Pre ++ S, A);
                              (2, S, A) -> rabbit_log:info   (Pre ++ S, A)
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
                  {error, {gen_tcp_error, closed}} ->
                      %% retry with new connection
                      ?L1("server closed connection", []),
                      purge_conn(Creds == anon, Servers, Opts),
                      with_login(Creds, Servers, Opts, Fun);
                  Result -> Result
              end
      end, reuse).

with_login(Creds, Servers, Opts, Fun) ->
    case get_or_create_conn(Creds == anon, Servers, Opts) of
        {ok, LDAP} ->
            case Creds of
                anon ->
                    ?L1("anonymous bind", []),
                    Fun(LDAP);
                {UserDN, Password} ->
                    case eldap:simple_bind(LDAP, UserDN, Password) of
                        ok ->
                            ?L1("bind succeeded: ~s", [UserDN]),
                            Fun(LDAP);
                        {error, invalidCredentials} ->
                            ?L1("bind returned \"invalid credentials\": ~s",
                                [UserDN]),
                            {refused, UserDN, []};
                        {error, E} ->
                            ?L1("bind error: ~s ~p", [UserDN, E]),
                            {error, E}
                    end
            end;
        Error ->
            ?L1("connect error: ~p", [Error]),
            Error
    end.

%% Gets either the anonymous or bound (authenticated) connection
get_or_create_conn(IsAnon, Servers, Opts) ->
    Conns = case get(ldap_conns) of
                undefined -> dict:new();
                Dict      -> Dict
            end,
    Key = {IsAnon, Servers, Opts},
    case dict:find(Key, Conns) of
        {ok, Conn} -> Conn;
        error      -> 
            case eldap_open(Servers, Opts) of
                {ok, _} = Conn -> put(ldap_conns, dict:store(Key, Conn, Conns)), Conn;
                Error -> Error
            end
    end.

purge_conn(IsAnon, Servers, Opts) ->
    Conns = get(ldap_conns),
    Key = {IsAnon, Servers, Opts},
    {_, {_, Conn}} = dict:find(Key, Conns),
    ?L1("Purging dead server connection", []),
    eldap:close(Conn), %% May already be closed
    put(ldap_conns, dict:erase(Key, Conns)).

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
    case env(use_ssl) of
        false -> [{ssl, false}];
        true  -> %% Only the unfixed version can be []
                 case {env(ssl_options), at_least("5.10")} of %% R16A
                     {_,  true}  -> [{ssl, true}, {sslopts, ssl_options()}];
                     {[], _}     -> [{ssl, true}];
                     {_,  false} -> exit({ssl_options_requires_min_r16a})
                 end
    end.

ssl_options() ->
    rabbit_networking:fix_ssl_options(env(ssl_options)).

at_least(Ver) ->
    rabbit_misc:version_compare(erlang:system_info(version), Ver) =/= lt.

env(F) ->
    {ok, V} = application:get_env(rabbitmq_auth_backend_ldap, F),
    V.

do_login(Username, PrebindUserDN, Password, LDAP) ->
    UserDN = case PrebindUserDN of
                 unknown -> username_to_dn(Username, LDAP, dn_lookup_when());
                 _       -> PrebindUserDN
             end,
    User = #auth_user{username     = Username,
                      impl         = #impl{user_dn  = UserDN,
                                           password = Password}},
    DTQ = fun (LDAPn) -> do_tag_queries(Username, UserDN, User, LDAPn) end,
    TagRes = case env(other_bind) of
                 as_user -> DTQ(LDAP);
                 _       -> with_ldap(creds(User), DTQ)
             end,
    case TagRes of
        {ok, L} -> case [E || {_, E = {error, _}} <- L] of
                       []      -> Tags = [Tag || {Tag, true} <- L],
                                  {ok, User#auth_user{tags = Tags}};
                       [E | _] -> E
                   end;
        E       -> E
    end.

do_tag_queries(Username, UserDN, User, LDAP) ->
    {ok, [begin
              ?L1("CHECK: does ~s have tag ~s?", [Username, Tag]),
              R = evaluate(Q, [{username, Username},
                               {user_dn,  UserDN}], User, LDAP),
              ?L1("DECISION: does ~s have tag ~s? ~p",
                  [Username, Tag, R]),
              {Tag, R}
          end || {Tag, Q} <- env(tag_queries)]}.

dn_lookup_when() -> case {env(dn_lookup_attribute), env(dn_lookup_bind)} of
                        {none, _}       -> never;
                        {_,    as_user} -> postbind;
                        {_,    _}       -> prebind
                    end.

username_to_dn_prebind(Username) ->
    with_ldap({ok, env(dn_lookup_bind)},
              fun (LDAP) -> dn_lookup(Username, LDAP) end).

username_to_dn(Username, LDAP,  postbind) -> dn_lookup(Username, LDAP);
username_to_dn(Username, _LDAP, _When)    -> fill_user_dn_pattern(Username).

dn_lookup(Username, LDAP) ->
    Filled = fill_user_dn_pattern(Username),
    case eldap:search(LDAP,
                      [{base, env(dn_lookup_base)},
                       {filter, eldap:equalityMatch(
                                  env(dn_lookup_attribute), Filled)},
                       {attributes, ["distinguishedName"]}]) of
        {ok, #eldap_search_result{entries = [#eldap_entry{object_name = DN}]}}->
            ?L1("DN lookup: ~s -> ~s", [Username, DN]),
            DN;
        {ok, #eldap_search_result{entries = Entries}} ->
            rabbit_log:warning("Searching for DN for ~s, got back ~p~n",
                               [Filled, Entries]),
            Filled;
        {error, _} = E ->
            exit(E)
    end.

fill_user_dn_pattern(Username) ->
    fill(env(user_dn_pattern), [{username, Username}]).

creds(User) -> creds(User, env(other_bind)).

creds(none, as_user) ->
    {error, "'other_bind' set to 'as_user' but no password supplied"};
creds(#auth_user{impl = #impl{user_dn = UserDN, password = PW}}, as_user) ->
    {ok, {UserDN, PW}};
creds(_, Creds) ->
    {ok, Creds}.

log(Fmt,  Args) -> case env(log) of
                       false -> ok;
                       _     -> rabbit_log:info(Fmt ++ "~n", Args)
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
