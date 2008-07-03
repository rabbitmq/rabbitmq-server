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

-module(tcp_acceptor).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {callback, sock, ref}).

%%--------------------------------------------------------------------

start_link(Callback, LSock) ->
    gen_server:start_link(?MODULE, {Callback, LSock}, []).

%%--------------------------------------------------------------------

init({Callback, LSock}) ->
    case prim_inet:async_accept(LSock, -1) of
        {ok, Ref} -> {ok, #state{callback=Callback, sock=LSock, ref=Ref}};
        Error -> {stop, {cannot_accept, Error}}
    end.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({inet_async, LSock, Ref, {ok, Sock}},
            State = #state{callback={M,F,A}, sock=LSock, ref=Ref}) ->

    %% patch up the socket so it looks like one we got from
    %% gen_tcp:accept/1 
    {ok, Mod} = inet_db:lookup_socket(LSock),
    inet_db:register_socket(Sock, Mod),

    %% report
    {ok, {Address, Port}} = inet:sockname(LSock),
    {ok, {PeerAddress, PeerPort}} = inet:peername(Sock),
    error_logger:info_msg("accepted TCP connection on ~s:~p from ~s:~p~n",
                          [inet_parse:ntoa(Address), Port,
                           inet_parse:ntoa(PeerAddress), PeerPort]),

    %% handle
    apply(M, F, A ++ [Sock]),

    %% accept more
    case prim_inet:async_accept(LSock, -1) of
        {ok, NRef} -> {noreply, State#state{ref=NRef}};
        Error -> {stop, {cannot_accept, Error}, none}
    end;
handle_info({inet_async, LSock, Ref, {error, closed}},
            State=#state{sock=LSock, ref=Ref}) ->
    %% It would be wrong to attempt to restart the acceptor when we
    %% know this will fail.
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
