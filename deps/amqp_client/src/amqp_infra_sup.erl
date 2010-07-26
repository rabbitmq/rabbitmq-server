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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ____________________.

%% @private
-module(amqp_infra_sup).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/1, start_child/2, start_children/2, child/2]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(InitialChildren) ->
    supervisor:start_link(?MODULE, InitialChildren).

start_child(Sup, Child) ->
    supervisor:start_child(Sup, child_spec(Child)).

start_children(Sup, Children) ->
    Res = lists:map(fun(Child) -> start_child(Sup, Child) end, Children),
    try [Pid || {ok, Pid} <- Res] of
        Pids -> {all_ok, Pids}
    catch
        _ -> Res
    end.

child(Sup, ChildName) ->
    Found = [Pid || {ChildName1, Pid, _, _} <- supervisor:which_children(Sup),
             ChildName1 =:= ChildName],
    case Found of []     -> undefined;
                  [Pid]  -> Pid
    end.

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init(InitialChildren) ->
    {ok, {{one_for_all, 0, 1},
          lists:map(fun child_spec/1, InitialChildren)}}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

child_spec({worker, Name, StartFun = {Module, _, _}}) ->
    {Name, StartFun, permanent, ?MAX_WAIT, worker, [Module]};
child_spec({supervisor, Name, StartFun = {Module, _, _}}) ->
    {Name, StartFun, transient, infinity, supervisor, [Module]};
child_spec({infra_sup, Name, StartFun}) ->
    {Name, StartFun, transient, infinity, supervisor, [amqp_infra_sup]}.
