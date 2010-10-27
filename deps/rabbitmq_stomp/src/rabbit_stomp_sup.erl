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
-module(rabbit_stomp_sup).
-behaviour(supervisor).

-export([start_link/1, init/1]).

-export([listener_started/2, listener_stopped/2, start_client/1]).

start_link(Listeners) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Listeners]).

init([Listeners]) ->
    io:format("Listeners:~p~n", [Listeners]),
    ChildSpecs = [
                  {rabbit_stomp_client_sup_sup,
                   {tcp_client_sup, start_link,
                    [{local, rabbit_stomp_client_sup_sup},
                     {rabbit_stomp_client_sup, start_link,[]}]},
                   transient,
                   infinity,
                   supervisor,
                   [tcp_client_sup]} | make_listener_specs(Listeners)
                  ],
    %% TODO rework supervisor creation to be more OTP like
    {ok, {{one_for_all, 10, 10}, ChildSpecs}}.

make_listener_specs(Listeners) ->
    lists:foldl(
      fun({Host, Port}, Acc) ->
              {IPAddress, Name} = rabbit_networking:check_tcp_listener_address(
                                    rabbit_stomp_listener_sup,
                                    Host,
                                    Port),
              [{Name,
                {tcp_listener_sup, start_link,
                 [IPAddress, Port,
                  [{packet, raw},
                   {reuseaddr, true}],
                  {?MODULE, listener_started, []},
                  {?MODULE, listener_stopped, []},
                  {?MODULE, start_client, []}, "STOMP Listener"]},
   transient, infinity, supervisor, [tcp_listener_sup]} | Acc]

      end, [], Listeners).

listener_started(_IPAddress, _Port) ->
    ok.

listener_stopped(_IPAddress, _Port) ->
    ok.

start_client(Sock) ->
    {ok, SupPid, ReaderPid} =
        supervisor:start_child(rabbit_stomp_client_sup_sup, [Sock]),
    ok = gen_tcp:controlling_process(Sock, ReaderPid),
    ReaderPid ! {go, Sock},
    SupPid.


