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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_global_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0]).

init([]) ->
    DB = {rabbit_mgmt_db,
          {rabbit_mgmt_db, start_link, []},
          permanent, 5000, worker, [rabbit_mgmt_db]},
    {ok, {{one_for_one, 10, 10}, [DB]}}.

start_link() ->
    case supervisor:start_link({global, ?MODULE}, ?MODULE, []) of
        {error, {already_started, _}} -> ignore;
        Else                          -> Else
    end.
