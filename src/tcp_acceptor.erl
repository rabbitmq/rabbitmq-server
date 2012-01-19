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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
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
    gen_server:cast(self(), accept),
    {ok, #state{callback=Callback, sock=LSock}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(accept, State) ->
    ok = file_handle_cache:obtain(),
    accept(State);

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({inet_async, LSock, Ref, {ok, Sock}},
            State = #state{callback={M,F,A}, sock=LSock, ref=Ref}) ->

    %% patch up the socket so it looks like one we got from
    %% gen_tcp:accept/1
    {ok, Mod} = inet_db:lookup_socket(LSock),
    inet_db:register_socket(Sock, Mod),

    try
        %% report
        {Address, Port}         = inet_op(fun () -> inet:sockname(LSock) end),
        {PeerAddress, PeerPort} = inet_op(fun () -> inet:peername(Sock) end),
        error_logger:info_msg("accepted TCP connection on ~s:~p from ~s:~p~n",
                              [rabbit_misc:ntoab(Address), Port,
                               rabbit_misc:ntoab(PeerAddress), PeerPort]),
        %% In the event that somebody floods us with connections we can spew
        %% the above message at error_logger faster than it can keep up.
        %% So error_logger's mailbox grows unbounded until we eat all the
        %% memory available and crash. So here's a meaningless synchronous call
        %% to the underlying gen_event mechanism - when it returns the mailbox
        %% is drained.
        gen_event:which_handlers(error_logger),
        %% handle
        file_handle_cache:transfer(apply(M, F, A ++ [Sock])),
        ok = file_handle_cache:obtain()
    catch {inet_error, Reason} ->
            gen_tcp:close(Sock),
            error_logger:error_msg("unable to accept TCP connection: ~p~n",
                                   [Reason])
    end,

    %% accept more
    accept(State);

handle_info({inet_async, LSock, Ref, {error, closed}},
            State=#state{sock=LSock, ref=Ref}) ->
    %% It would be wrong to attempt to restart the acceptor when we
    %% know this will fail.
    {stop, normal, State};

handle_info({inet_async, LSock, Ref, {error, Reason}},
            State=#state{sock=LSock, ref=Ref}) ->
    {Address, Port} = inet_op(fun () -> inet:sockname(LSock) end),
    error_logger:error_msg("failed to accept TCP connection on ~s:~p: ~p~n",
                           [rabbit_misc:ntoab(Address), Port, Reason]),
    accept(State);

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

accept(State = #state{sock=LSock}) ->
    case prim_inet:async_accept(LSock, -1) of
        {ok, Ref} -> {noreply, State#state{ref=Ref}};
        Error     -> {stop, {cannot_accept, Error}, State}
    end.
