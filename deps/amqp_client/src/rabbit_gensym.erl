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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_gensym).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([gensym/0, gensym/1]).

-import(lists).
-import(io_lib).
-import(gen_server).

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [prefix(node(), erlang:universaltime())],
                          []).

gensym() ->
    gensym("gensym").

gensym(PrefixStr) ->
    {Prefix, Counter} = gen_server:call(?SERVER, generate),
    Sym = PrefixStr ++ integer_to_list(Counter) ++ Prefix,
    Sym.

%%--------------------------------------------------------------------

init([Prefix]) ->
    {ok, {Prefix, 0}}.

handle_call(generate, _From, {Prefix, Counter}) ->
    {reply, {Prefix, Counter}, {Prefix, Counter+1}};
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

prefix(Node, {{Year,Month,Day}, {H,M,S}}) ->
    lists:flatten(io_lib:format(
                    "_~s_~4..0B~2..0B~2..0B~2..0B~2..0B~2..0B_",
                    [Node,Year,Month,Day,H,M,S])).

