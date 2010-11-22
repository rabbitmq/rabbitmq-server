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

-record(state, { servers, user_dn_pattern, ssl, log, port }).

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

check_vhost_access(#user{username = _Username}, _VHostPath, _Permission) ->
    true.

check_resource_access(#user{username = _Username},
                      #resource{virtual_host = _VHostPath, name = _Name},
                      _Permission) ->
    true.

%%--------------------------------------------------------------------

init([]) ->
    {ok, Servers}       = application:get_env(servers),
    {ok, UserDnPattern} = application:get_env(user_dn_pattern),
    {ok, SSL}           = application:get_env(use_ssl),
    {ok, Log}           = application:get_env(log),
    {ok, Port}          = application:get_env(server_port),
    {ok, #state{ servers         = Servers,
                 user_dn_pattern = UserDnPattern,
                 ssl             = SSL,
                 log             = Log,
                 port            = Port }}.

handle_call({login, Username, Password}, _From,
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
    Dn = lists:flatten(io_lib:format(UserDnPattern, [Username])),
    case eldap:open(Servers, Opts) of
        {ok, LDAP} ->
            Reply = try
                        case eldap:simple_bind(LDAP, Dn, Password) of
                            ok ->
                                {ok, #user{username     = Username,
                                           is_admin     = false,
                                           auth_backend = ?MODULE,
                                           impl         = none}};
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
    end;

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

