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

-module(rabbit_auth_backend_ldap).

%% Connect to an LDAP server for authentication and authorisation

-include_lib("eldap/include/eldap.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_auth_backend).
%%-include("rabbit_auth_backend_spec.hrl").

-export([description/0]).
-export([check_user_login/2, check_vhost_access/3, check_resource_access/3]).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { servers, user_dn_pattern, vhost_access_query,
                 resource_access_query, is_admin_query, ssl, log, port }).

%%--------------------------------------------------------------------

description() ->
    [{name, <<"LDAP">>},
     {description, <<"LDAP authentication / authorisation">>}].

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------

check_user_login(_Username, []) ->
    exit(passwordless_not_supported_yet);

check_user_login(Username, [{password, Password}]) ->
    gen_server:call(?SERVER, {login, Username, Password}, infinity);

check_user_login(Username, AuthProps) ->
    exit({unknown_auth_props, Username, AuthProps}).

check_vhost_access(User = #user{username = Username}, VHost, Permission) ->
    gen_server:call(?SERVER, {check_vhost, User, [{username,   Username},
                                                  {vhost,      VHost},
                                                  {permission, Permission}]},
                    infinity).

check_resource_access(User = #user{username = Username},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission) ->
    gen_server:call(?SERVER, {check_resource, User, [{username,   Username},
                                                     {vhost,      VHost},
                                                     {res_type,   Type},
                                                     {res_name,   Name},
                                                     {permission, Permission}]},
                    infinity).

%%--------------------------------------------------------------------

evaluate({constant, Bool}, _Args, _LDAP) ->
    Bool;

evaluate({exists, DnPattern}, Args, LDAP) ->
    Dn = rabbit_auth_backend_ldap_util:fill(DnPattern, Args),
    Base = {base, Dn},
    Scope = {scope, eldap:baseObject()},
    %% eldap forces us to have a filter. objectClass should always be there.
    Filter = {filter, eldap:present("objectClass")},
    case eldap:search(LDAP, [Base, Filter, Scope]) of
        {ok, #eldap_search_result{entries = Entries}} ->
            length(Entries) > 0;
        {error, _} ->
            false
    end;

evaluate(Q, Args, _LDAP) ->
    {error, {unrecognised_query, Q, Args}}.

evaluate_ldap(#user{username = U, impl = P}, Q, Args, State) ->
    with_ldap(U, P, fun(LDAP) -> evaluate(Q, Args, LDAP) end, State).

%% TODO - ATM we create and destroy a new LDAP connection on every
%% call. This could almost certainly be more efficient.
with_ldap(Username, Password, Fun,
          State = #state{ servers         = Servers,
                          user_dn_pattern = UserDnPattern,
                          ssl             = SSL,
                          log             = Log,
                          port            = Port }) ->
    Opts0 = [{ssl, SSL}, {port, Port}],
    Opts = case Log of
               true ->
                   [{log, fun(1, S, A) -> rabbit_log:warning(S, A);
                             (2, S, A) -> rabbit_log:info   (S, A)
                          end} | Opts0];
               _ ->
                   Opts0
           end,
    %% TODO do this the same as other args!
    Dn = lists:flatten(io_lib:format(UserDnPattern, [Username])),
    case eldap:open(Servers, Opts) of
        {ok, LDAP} ->
            Reply = try
                        case eldap:simple_bind(LDAP, Dn, Password) of
                            ok ->
                                Fun(LDAP);
                            {error, invalidCredentials} ->
                                {refused, Username};
                            {error, _} = E ->
                                E
                        end
                    after
                        eldap:close(LDAP)
                    end,
            {reply, Reply, State};
        Error ->
            {reply, Error, State}
    end.

%%--------------------------------------------------------------------

init([]) ->
    {ok, Servers}        = application:get_env(servers),
    {ok, UserDnPattern}  = application:get_env(user_dn_pattern),
    {ok, VHostAccess}    = application:get_env(vhost_access_query),
    {ok, ResourceAccess} = application:get_env(resource_access_query),
    {ok, IsAdmin}        = application:get_env(is_admin_query),
    {ok, SSL}            = application:get_env(use_ssl),
    {ok, Log}            = application:get_env(log),
    {ok, Port}           = application:get_env(server_port),
    {ok, #state{ servers               = Servers,
                 user_dn_pattern       = UserDnPattern,
                 vhost_access_query    = VHostAccess,
                 resource_access_query = ResourceAccess,
                 is_admin_query        = IsAdmin,
                 ssl                   = SSL,
                 log                   = Log,
                 port                  = Port }}.

handle_call({login, Username, Password}, _From,
            State = #state{ is_admin_query = IsAdminQuery }) ->
    with_ldap(
      Username, Password,
      fun(LDAP) ->
              case evaluate(IsAdminQuery, [{username, Username}], LDAP) of
                  {error, _} = E ->
                      E;
                  IsAdmin ->
                      {ok, #user{username     = Username,
                                 is_admin     = IsAdmin,
                                 auth_backend = ?MODULE,
                                 impl         = Password}}
              end
      end, State);

handle_call({check_vhost, User, Args},
            _From, State = #state{vhost_access_query = Q}) ->
    evaluate_ldap(User, Q, Args, State);

handle_call({check_resource, User, Args},
            _From, State = #state{resource_access_query = Q}) ->
    evaluate_ldap(User, Q, Args, State);

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

