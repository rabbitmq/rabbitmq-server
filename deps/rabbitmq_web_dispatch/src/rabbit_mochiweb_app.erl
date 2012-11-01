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
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mochiweb_app).

-behaviour(application).
-export([start/2,stop/1]).

-define(APP, rabbitmq_mochiweb).

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbit_mochiweb.
start(_Type, _StartArgs) ->
    rabbit_mochiweb_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for rabbit_mochiweb.
stop(_State) ->
    ok.
