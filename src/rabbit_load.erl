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

-module(rabbit_load).

-export([local_load/0, remote_loads/0, pick/0]).

-define(FUDGE_FACTOR, 0.98).
-define(TIMEOUT, 100).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(erlang_node() :: atom()).
-type(load() :: {{non_neg_integer(), integer() | 'unknown'}, erlang_node()}).
-spec(local_load/0 :: () -> load()).
-spec(remote_loads/0 :: () -> [load()]).
-spec(pick/0 :: () -> erlang_node()).

-endif.

%%----------------------------------------------------------------------------

local_load() ->
    LoadAvg = case whereis(cpu_sup) of
                  undefined -> unknown;
                  _         -> case cpu_sup:avg1() of
                                   L when is_integer(L) -> L;
                                   {error, timeout}     -> unknown
                               end
              end,
    {{statistics(run_queue), LoadAvg}, node()}.

remote_loads() ->
    {ResL, _BadNodes} =
        rpc:multicall(nodes(), ?MODULE, local_load, [], ?TIMEOUT),
    ResL.

pick() ->
    RemoteLoads = remote_loads(),
    {{RunQ, LoadAvg}, Node} = local_load(),
    %% add bias towards current node; we rely on Erlang's term order
    %% of SomeFloat < local_unknown < unknown.
    AdjustedLoadAvg = case LoadAvg of
                          unknown -> local_unknown;
                          _       -> LoadAvg * ?FUDGE_FACTOR
                      end,
    Loads = [{{RunQ, AdjustedLoadAvg}, Node} | RemoteLoads],
    {_, SelectedNode} = lists:min(Loads),
    SelectedNode.
