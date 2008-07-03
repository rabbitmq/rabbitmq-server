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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(buffering_proxy).

-export([start_link/2]).

%% internal

-export([mainloop/4, drain/2]).
-export([proxy_loop/3]).

%%----------------------------------------------------------------------------

start_link(M, A) ->
    spawn_link(
      fun () -> process_flag(trap_exit, true),
                ProxyPid = self(),
                Ref = make_ref(),
                Pid = spawn_link(
                        fun () -> mainloop(ProxyPid, Ref, M,
                                           M:init(ProxyPid, A)) end),
                proxy_loop(Ref, Pid, empty)
      end).

%%----------------------------------------------------------------------------

mainloop(ProxyPid, Ref, M, State) ->
    ProxyPid ! Ref,
    NewState =
        receive
            {Ref, Messages} ->
                lists:foldl(fun (Msg, S) -> 
                                    drain(M, M:handle_message(Msg, S))
                            end, State, lists:reverse(Messages));
            Msg -> M:handle_message(Msg, State)
        end,
    ?MODULE:mainloop(ProxyPid, Ref, M, NewState).

drain(M, State) ->
    receive
        Msg -> ?MODULE:drain(M, M:handle_message(Msg, State))
    after 0 ->
            State
    end.

proxy_loop(Ref, Pid, State) ->
    receive
        Ref ->
            ?MODULE:proxy_loop(
               Ref, Pid,
               case State of
                   empty    -> waiting;
                   waiting  -> exit(duplicate_next);
                   Messages -> Pid ! {Ref, Messages}, empty
               end);
        {'EXIT', Pid, Reason} ->
            exit(Reason);
        {'EXIT', _, Reason} ->
            exit(Pid, Reason),
            ?MODULE:proxy_loop(Ref, Pid, State);
        Msg ->
            ?MODULE:proxy_loop(
               Ref, Pid,
               case State of
                   empty    -> [Msg];
                   waiting  -> Pid ! {Ref, [Msg]}, empty;
                   Messages -> [Msg | Messages]
               end)
    end.
