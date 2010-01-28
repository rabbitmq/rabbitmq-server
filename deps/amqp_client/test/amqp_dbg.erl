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
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.


-module(amqp_dbg).

-include_lib("stdlib/include/ms_transform.hrl").

-export([tracer/0, all/0]).
-export([network_connection_lifecycle/0, direct_connection_lifecycle/0,
         channel_lifecycle/0, methods/0]).


tracer() ->
    {ok, _} = dbg:tracer(),
    {ok, _} = dbg:p(all, c),
    ok.

all() ->
    network_connection_lifecycle(),
    direct_connection_lifecycle(),
    channel_lifecycle(),
    methods().

network_connection_lifecycle() ->
    {ok, _} = dbg:tpl(amqp_main_reader, start, return_ms()),
    tpl_funcs(amqp_network_connection,
              [set_closing_state, all_channels_closed_event, handshake,
               terminate], []).

direct_connection_lifecycle() ->
    tpl_funcs(amqp_direct_connection,
              [set_closing_state, all_channels_closed_event, init, terminate],
              []).

channel_lifecycle() ->
    tpl_funcs_ms(amqp_channel_util,[{open_channel, []},
                                     {start_channel_infrastructure, return_ms()},
                                     {terminate_channel_infrastructure, []}]).

methods() ->
    tpl_list([{amqp_channel_util, do, []},
              {amqp_channel, handle_method, []},
              {amqp_network_connection, handle_method, []},
              {amqp_network_connection, handshake_recv, return_ms()}]).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

tpl_list(ArgsList) ->
    lists:foreach(fun({Module, Func, Ms}) ->
                      {ok, _} = dbg:tpl(Module, Func, Ms)
                  end, ArgsList),
    ok.

tpl_funcs_ms(Module, FuncMsList) ->
    lists:foreach(fun({Func, Ms}) -> {ok, _} = dbg:tpl(Module, Func, Ms) end,
                  FuncMsList),
    ok.

tpl_funcs(Module, FuncList, Ms) ->
    lists:foreach(fun(Func) -> {ok, _} = dbg:tpl(Module, Func, Ms) end,
                  FuncList),
    ok.

return_ms() ->
    dbg:fun2ms(fun(_) -> return_trace() end).
