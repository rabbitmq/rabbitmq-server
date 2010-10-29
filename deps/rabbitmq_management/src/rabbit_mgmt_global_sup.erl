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
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_global_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0]).

init([]) ->
    DB = {rabbit_mgmt_db,
          {rabbit_mgmt_db, start_link, []},
          permanent, 5000, worker, [rabbit_mgmt_external_stats]},
    {ok, {{one_for_one, 10, 10}, [DB]}}.

start_link() ->
    Res = case supervisor:start_link({global, ?MODULE}, ?MODULE, []) of
              {error, {already_started, _}} -> ignore;
              Else                          -> Else
          end,
    %% Needs to happen after we know the DB's up but before boot step finished
    rabbit_mgmt_db_handler:add_handler(),
    Res.
