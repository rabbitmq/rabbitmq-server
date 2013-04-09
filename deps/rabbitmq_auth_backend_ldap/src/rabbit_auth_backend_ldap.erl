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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap).

%% Connect to an LDAP server for authentication and authorisation

-include_lib("eldap/include/eldap.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_auth_backend).

-export([description/0]).
-export([check_user_login/2, check_vhost_access/2, check_resource_access/3]).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(L(F, A, S),  log("LDAP "         ++ F, A, S)).
-define(L1(F, A, S), log("    LDAP "     ++ F, A, S)).
-define(L2(F, A, S), log("        LDAP " ++ F, A, S)).

-import(rabbit_misc, [pget/2]).

-record(state, { servers,
                 user_dn_pattern,
                 dn_lookup_attribute,
                 dn_lookup_base,
                 other_bind,
                 vhost_access_query,
                 resource_access_query,
                 tag_queries,
                 use_ssl,
                 log,
                 port }).

-record(impl, { user_dn, password }).

%%--------------------------------------------------------------------

description() ->
    [{name, <<"LDAP">>},
     {description, <<"LDAP authentication / authorisation">>}].

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------

check_user_login(Username, []) ->
    gen_server:call(?SERVER, {login, Username}, infinity);

check_user_login(Username, [{password, Password}]) ->
    gen_server:call(?SERVER, {login, Username, Password}, infinity);

check_user_login(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

check_vhost_access(User = #user{username = Username,
                                impl     = #impl{user_dn = UserDN}}, VHost) ->
    gen_server:call(?SERVER, {check_vhost, [{username, Username},
                                            {user_dn,  UserDN},
                                            {vhost,    VHost}], User},
                    infinity).

check_resource_access(User = #user{username = Username,
                                   impl     = #impl{user_dn = UserDN}},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission) ->
    gen_server:call(?SERVER, {check_resource, [{username,   Username},
                                               {user_dn,    UserDN},
                                               {vhost,      VHost},
                                               {resource,   Type},
                                               {name,       Name},
                                               {permission, Permission}], User},
                    infinity).

%%--------------------------------------------------------------------

evaluate(Query, Args, User, LDAP, State) ->
    ?L1("evaluating query: ~p", [Query], State),
    evaluate0(Query, Args, User, LDAP, State).

evaluate0({constant, Bool}, _Args, _User, _LDAP, State) ->
    ?L1("evaluated constant: ~p", [Bool], State),
    Bool;

evaluate0({for, [{Type, Value, SubQuery}|Rest]}, Args, User, LDAP, State) ->
    case pget(Type, Args) of
        undefined -> {error, {args_do_not_contain, Type, Args}};
        Value     -> ?L1("selecting subquery ~s = ~s", [Type, Value], State),
                     evaluate(SubQuery, Args, User, LDAP, State);
        _         -> evaluate0({for, Rest}, Args, User, LDAP, State)
    end;

evaluate0({for, []}, _Args, _User, _LDAP, _State) ->
    {error, {for_query_incomplete}};

evaluate0({exists, DNPattern}, Args, _User, LDAP, State) ->
    %% eldap forces us to have a filter. objectClass should always be there.
    Filter = eldap:present("objectClass"),
    DN = fill(DNPattern, Args, State),
    R = object_exists(DN, Filter, LDAP),
    ?L1("evaluated exists for \"~s\": ~p", [DN, R], State),
    R;

evaluate0({in_group, DNPattern}, Args, User, LDAP, State) ->
    evaluate({in_group, DNPattern, "member"}, Args, User, LDAP, State);

evaluate0({in_group, DNPattern, Desc}, Args,
          #user{impl = #impl{user_dn = UserDN}}, LDAP, State) ->
    Filter = eldap:equalityMatch(Desc, UserDN),
    DN = fill(DNPattern, Args, State),
    R = object_exists(DN, Filter, LDAP),
    ?L1("evaluated in_group for \"~s\": ~p", [DN, R], State),
    R;

evaluate0({match, StringQuery, REQuery}, Args, User, LDAP, State) ->
    String = evaluate(StringQuery, Args, User, LDAP, State),
    RE = evaluate(REQuery, Args, User, LDAP, State),
    R = case re:run(String, RE) of
            {match, _} -> true;
            nomatch    -> false
        end,
    ?L1("evaluated match \"~s\" against RE \"~s\": ~s", [String, RE, R], State),
    R;

evaluate0({string, StringPattern}, Args, _User, _LDAP, State) ->
    R = fill(StringPattern, Args, State),
    ?L1("evaluated string for \"~s\"", [R], State),
    R;

evaluate0({attribute, DNPattern, AttributeName}, Args, _User, LDAP, State) ->
    DN = fill(DNPattern, Args, State),
    R = attribute(DN, AttributeName, LDAP),
    ?L1("evaluated attribute \"~s\" for \"~s\": ~p",
        [AttributeName, DN, R], State),
    R;

evaluate0(Q, Args, _User, _LDAP, _State) ->
    {error, {unrecognised_query, Q, Args}}.

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
            [Attr] = pget(AttributeName, A),
            Attr;
        {ok, #eldap_search_result{entries = _}} ->
            false;
        {error, _} = E ->
            E
    end.

evaluate_ldap(Q, Args, User, State) ->
    with_ldap(creds(User, State),
              fun(LDAP) -> evaluate(Q, Args, User, LDAP, State) end, State).

%%--------------------------------------------------------------------

%% TODO - ATM we create and destroy a new LDAP connection on every
%% call. This could almost certainly be more efficient.
with_ldap(Creds, Fun, State = #state{servers = Servers,
                                     use_ssl = SSL,
                                     log     = Log,
                                     port    = Port}) ->
    Opts0 = [{ssl, SSL}, {port, Port}],
    Opts = case Log of
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
    case eldap:open(Servers, Opts) of
        {ok, LDAP} ->
            try Creds of
                anon ->
                    ?L1("anonymous bind", [], State),
                    Fun(LDAP);
                {UserDN, Password} ->
                    case eldap:simple_bind(LDAP, UserDN, Password) of
                        ok ->
                            ?L1("bind succeeded: ~s", [UserDN], State),
                            Fun(LDAP);
                        {error, invalidCredentials} ->
                            ?L1("bind returned \"invalid credentials\": ~s",
                                [UserDN], State),
                            {refused, UserDN, []};
                        {error, E} ->
                            ?L1("bind error: ~s ~p", [UserDN, E], State),
                            {error, E}
                    end
            after
                eldap:close(LDAP)
            end;
        Error ->
            ?L1("connect error: ~p", [Error], State),
            Error
    end.

get_env(F) ->
    {ok, V} = application:get_env(F),
    V.

do_login(Username, Password, LDAP,
         State = #state{ tag_queries = TagQueries }) ->
    UserDN = username_to_dn(Username, LDAP, State),
    User = #user{username     = Username,
                 auth_backend = ?MODULE,
                 impl         = #impl{user_dn  = UserDN,
                                      password = Password}},

    TagRes = [begin
                  ?L1("CHECK: does ~s have tag ~s?", [Username, Tag], State),
                  R = evaluate(Q, [{username, Username},
                                   {user_dn,  UserDN}], User, LDAP, State),
                  ?L1("DECISION: does ~s have tag ~s? ~p",
                      [Username, Tag, R], State),
                  {Tag, R}
              end || {Tag, Q} <- TagQueries],
    case [E || {_, E = {error, _}} <- TagRes] of
        []      -> {ok, User#user{tags = [Tag || {Tag, true} <- TagRes]}};
        [E | _] -> E
    end.

username_to_dn(Username, _LDAP, State = #state{dn_lookup_attribute = none}) ->
    fill_user_dn_pattern(Username, State);

username_to_dn(Username, LDAP, State = #state{dn_lookup_attribute = Attr,
                                              dn_lookup_base      = Base}) ->
    Filled = fill_user_dn_pattern(Username, State),
    case eldap:search(LDAP,
                      [{base, Base},
                       {filter, eldap:equalityMatch(Attr, Filled)},
                       {attributes, ["distinguishedName"]}]) of
        {ok, #eldap_search_result{entries = [#eldap_entry{attributes = A}]}} ->
            [DN] = pget("distinguishedName", A),
            DN;
        {ok, #eldap_search_result{entries = Entries}} ->
            rabbit_log:warning("Searching for DN for ~s, got back ~p~n",
                               [Filled, Entries]),
            Filled;
        {error, _} = E ->
            exit(E)
    end.

fill_user_dn_pattern(Username,
                     State = #state{user_dn_pattern = UserDNPattern}) ->
    fill(UserDNPattern, [{username, Username}], State).

creds(none, #state{other_bind = as_user}) ->
    exit(as_user_no_password);
creds(#user{impl = #impl{user_dn = UserDN, password = Password}},
      #state{other_bind = as_user}) ->
    {UserDN, Password};
creds(_, #state{other_bind = Creds}) ->
    Creds.

log(_Fmt, _Args, #state{log = false}) -> ok;
log( Fmt,  Args, _State)              -> rabbit_log:info(Fmt ++ "~n", Args).

fill(Fmt, Args, State) ->
    ?L2("filling template \"~s\" with~n            ~p", [Fmt, Args], State),
    R = rabbit_auth_backend_ldap_util:fill(Fmt, Args),
    ?L2("template result: \"~s\"", [R], State),
    R.

log_result({ok, #user{}})   -> ok;
log_result(true)            -> ok;
log_result(false)           -> denied;
log_result({refused, _, _}) -> denied;
log_result(E)               -> E.

log_user(#user{username = U}) -> rabbit_misc:format("\"~s\"", [U]).

log_vhost(Args) ->
    rabbit_misc:format("access to vhost \"~s\"", [pget(vhost, Args)]).

log_resource(Args) ->
    rabbit_misc:format("~s permission for ~s \"~s\" in \"~s\"",
                       [pget(permission, Args), pget(resource, Args),
                        pget(name, Args), pget(vhost, Args)]).

%%--------------------------------------------------------------------

init([]) ->
    {ok, list_to_tuple(
           [state | [get_env(F) || F <- record_info(fields, state)]])}.

handle_call({login, Username}, _From, State) ->
    %% Without password, e.g. EXTERNAL
    ?L("CHECK: passwordless login for ~s", [Username], State),
    R = with_ldap(creds(none, State),
                  fun(LDAP) -> do_login(Username, none, LDAP, State) end,
                  State),
    ?L("DECISION: passwordless login for ~s: ~p",
       [Username, log_result(R)], State),
    {reply, R, State};

handle_call({login, Username, Password}, _From, State) ->
    ?L("CHECK: login for ~s", [Username], State),
    R = with_ldap({fill_user_dn_pattern(Username, State), Password},
                  fun(LDAP) -> do_login(Username, Password, LDAP, State) end,
                  State),
    ?L("DECISION: login for ~s: ~p", [Username, log_result(R)], State),
    {reply, R, State};

handle_call({check_vhost, Args, User},
            _From, State = #state{vhost_access_query = Q}) ->
    ?L("CHECK: ~s for ~s", [log_vhost(Args), log_user(User)], State),
    R = evaluate_ldap(Q, Args, User, State),
    ?L("DECISION: ~s for ~s: ~p",
       [log_vhost(Args), log_user(User), log_result(R)], State),
    {reply, R, State};

handle_call({check_resource, Args, User},
            _From, State = #state{resource_access_query = Q}) ->
    ?L("CHECK: ~s for ~s", [log_resource(Args), log_user(User)], State),
    R = evaluate_ldap(Q, Args, User, State),
    ?L("DECISION: ~s for ~s: ~p",
       [log_resource(Args), log_user(User), log_result(R)], State),
    {reply, R, State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

