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
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%
-module(inet_tcp_proxy_manager).

%% The TCP proxies need to decide whether to block based on the node
%% they're running on, and the node connecting to them. The trouble
%% is, they don't have an easy way to determine the latter. Therefore
%% when A connects to B we register the source port used by A here, so
%% that B can later look it up and find out who A is without having to
%% sniff the distribution protocol.
%%
%% That does unfortunately mean that we need a central control
%% thing. We assume here it's running on the node called
%% 'standalone_test' since that's where tests are orchestrated from.
%%
%% Yes, this leaks. For its intended lifecycle, that's fine.

-behaviour(gen_server).

-export([start/0, register/5, lookup/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(NODE, ct).

-record(state, {ports, pending}).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

register(_From, _To, _SrcPort, Port, Port) ->
    %% No proxy, don't register
    ok;
register(From, To, SrcPort, _Port, _ProxyPort) ->
    gen_server:call(name(), {register, From, To, SrcPort}, infinity).

lookup(SrcPort) ->
    gen_server:call(name(), {lookup, SrcPort}, infinity).

controller_node() ->
    {ok, ManagerNode} = application:get_env(kernel,
      inet_tcp_proxy_manager_node),
    ManagerNode.

name() ->
    {?MODULE, controller_node()}.

%%----------------------------------------------------------------------------

init([]) ->
    net_kernel:monitor_nodes(true),
    {ok, #state{ports   = dict:new(),
                pending = []}}.

handle_call({register, FromNode, ToNode, SrcPort}, _From,
            State = #state{ports   = Ports,
                           pending = Pending}) ->
    {Notify, Pending2} =
        lists:partition(fun ({P, _}) -> P =:= SrcPort end, Pending),
    [gen_server:reply(From, {ok, FromNode, ToNode}) || {_, From} <- Notify],
    {reply, ok,
     State#state{ports   = dict:store(SrcPort, {FromNode, ToNode}, Ports),
                 pending = Pending2}};

handle_call({lookup, SrcPort}, From,
            State = #state{ports = Ports, pending = Pending}) ->
    case dict:find(SrcPort, Ports) of
        {ok, {FromNode, ToNode}} ->
            {reply, {ok, FromNode, ToNode}, State};
        error ->
            {noreply, State#state{pending = [{SrcPort, From} | Pending]}}
    end;

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({nodedown, Node}, State = #state{ports = Ports}) ->
    Ports1 = dict:filter(
               fun (_, {From, To}) ->
                       Node =/= From andalso Node =/= To
               end, Ports),
    {noreply, State#state{ports = Ports1}};

handle_info(_I, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) -> {ok, State}.
