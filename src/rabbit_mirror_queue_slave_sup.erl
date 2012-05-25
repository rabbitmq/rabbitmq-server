%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_slave_sup).

-behaviour(supervisor2).

-export([start_link/0, start_child/2]).

-export([init/1]).

-include_lib("rabbit.hrl").

-define(SERVER, ?MODULE).

start_link() -> supervisor2:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Node, Args) -> supervisor2:start_child({?SERVER, Node}, Args).

init([]) ->
    {ok, {{simple_one_for_one_terminate, 10, 10},
          [{rabbit_mirror_queue_slave,
            {rabbit_mirror_queue_slave, start_link, []},
            temporary, ?MAX_WAIT, worker, [rabbit_mirror_queue_slave]}]}}.
