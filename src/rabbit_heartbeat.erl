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

-module(rabbit_heartbeat).

-export([start_heartbeat/2]).

start_heartbeat(_Sock, 0) ->
    none;
start_heartbeat(Sock, TimeoutSec) ->
    Parent = self(),
    %% we check for incoming data every interval, and time out after
    %% two checks with no change. As a result we will time out between
    %% 2 and 3 intervals after the last data has been received.
    spawn_link(fun () -> heartbeater(Sock, TimeoutSec * 1000,
                                     recv_oct, 1,
                                     fun () ->
                                             Parent ! timeout,
                                             stop
                                     end,
                                     erlang:monitor(process, Parent)) end),
    %% the 'div 2' is there so that we don't end up waiting for nearly
    %% 2 * TimeoutSec before sending a heartbeat in the boundary case
    %% where the last message was sent just after a heartbeat.
    spawn_link(fun () -> heartbeater(Sock, TimeoutSec * 1000 div 2,
                                     send_oct, 0,
                                     fun () ->
                                             catch gen_tcp:send(Sock, rabbit_binary_generator:build_heartbeat_frame()),
                                             continue
                                     end,
                                     erlang:monitor(process, Parent)) end),
    ok.

%% Y-combinator, posted by Vladimir Sekissov to the Erlang mailing list
%% http://www.erlang.org/ml-archive/erlang-questions/200301/msg00053.html
y(X) ->
    F = fun (P) -> X(fun (A) -> (P(P))(A) end) end,
    F(F).

heartbeater(Sock, TimeoutMillisec, StatName, Threshold, Handler, MonitorRef) ->
    Heartbeat =
        fun (F) ->
                fun ({StatVal, SameCount}) ->
                        receive
                            {'DOWN', MonitorRef, process, _Object, _Info} -> ok;
                            Other -> exit({unexpected_message, Other})
                        after TimeoutMillisec ->
                                case inet:getstat(Sock, [StatName]) of
                                    {ok, [{StatName, NewStatVal}]} ->
                                        if NewStatVal =/= StatVal ->
                                                F({NewStatVal, 0});
                                           SameCount < Threshold ->
                                                F({NewStatVal, SameCount + 1});
                                           true ->
                                                case Handler() of
                                                    stop     -> ok;
                                                    continue -> F({NewStatVal, 0})
                                                end
                                        end;
                                    {error, einval} ->
                                        %% the socket is dead, most
                                        %% likely because the
                                        %% connection is being shut
                                        %% down -> terminate
                                        ok;
                                    {error, Reason} ->
                                        exit({cannot_get_socket_stats, Reason})
                                end
                        end
                end
        end,
    (y(Heartbeat))({0, 0}).
