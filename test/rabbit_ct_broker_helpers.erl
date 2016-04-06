%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_broker_helpers).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([
    run_on_broker/4,
    find_listener/0,
    test_channel/0
  ]).

run_on_broker(Node, Module, Function, Args) ->
    %% We add some directories to the broker node search path.
    Path1 = filename:dirname(code:which(Module)),
    Path2 = filename:dirname(code:which(?MODULE)),
    Paths = lists:usort([Path1, Path2]),
    ExistingPaths = rpc:call(Node, code, get_path, []),
    lists:foreach(
      fun(P) ->
          case lists:member(P, ExistingPaths) of
              true  -> ok;
              false -> true = rpc:call(Node, code, add_pathz, [P])
          end
      end, Paths),
    %% If there is an exception, rpc:call/4 returns the exception as
    %% a "normal" return value. If there is an exit signal, we raise
    %% it again. In both cases, we have no idea of the module and line
    %% number which triggered the issue.
    case rpc:call(Node, Module, Function, Args) of
        {badrpc, {'EXIT', Reason}} -> exit(Reason);
        {badrpc, Reason}           -> exit(Reason);
        Ret                        -> Ret
    end.

find_listener() ->
    [#listener{host = H, port = P} | _] =
        [L || L = #listener{node = N, protocol = amqp}
                  <- rabbit_networking:active_listeners(),
              N =:= node()],
    {H, P}.

user(Username) ->
    #user{username       = Username,
          tags           = [administrator],
          authz_backends = [{rabbit_auth_backend_internal, none}]}.

test_channel() ->
    Me = self(),
    Writer = spawn(fun () -> test_writer(Me) end),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    {ok, Ch} = rabbit_channel:start_link(
                 1, Me, Writer, Me, "", rabbit_framing_amqp_0_9_1,
                 user(<<"guest">>), <<"/">>, [], Me, Limiter),
    {Writer, Limiter, Ch}.

test_writer(Pid) ->
    receive
        {'$gen_call', From, flush} -> gen_server:reply(From, ok),
                                      test_writer(Pid);
        {send_command, Method}     -> Pid ! Method,
                                      test_writer(Pid);
        shutdown                   -> ok
    end.
