%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_vhost_sup).

-include("rabbit.hrl").

%% Each vhost gets an instance of this supervisor that supervises
%% message stores and queues (via rabbit_amqqueue_sup_sup).
-behaviour(supervisor2).
-export([init/1]).
-export([start_link/1]).

start_link(VHost) ->
    supervisor2:start_link(?MODULE, [VHost]).

init([_VHost]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
