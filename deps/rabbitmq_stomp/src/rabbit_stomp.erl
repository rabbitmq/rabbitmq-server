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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_stomp).

-export([start/0, stop/0, start/2, stop/1]).

start() ->
    start(normal,[]),
    ok.

stop() ->
    ok.

start(normal, []) ->
    {ok, SupPid} = rabbit_stomp_sup:start_link(),
    case application:get_env(listeners) of
         undefined -> throw({error, {stomp_configuration_not_found}});
         {ok, Listeners} -> 
                         io:format("starting ~s (binding to ~p)  ...", 
                                   ["STOMP Adapter", Listeners]),
                         {ok, _} = rabbit_stomp_server:start(Listeners),
                         io:format("done~n")
    end,
    {ok, SupPid}.

stop(_State) ->
    ok.
