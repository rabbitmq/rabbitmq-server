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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    {ok, Backends} = application:get_env(rabbit, auth_backends),
    case lists:member(rabbit_auth_backend_ldap, Backends) of
        true  -> ok;
        false -> rabbit_log:warning(
                   "LDAP plugin loaded, but rabbit_auth_backend_ldap is not "
                   "in the list of auth_backends. LDAP auth will not work.~n")
    end,
    rabbit_auth_backend_ldap_sup:start_link().

stop(_State) ->
    ok.
