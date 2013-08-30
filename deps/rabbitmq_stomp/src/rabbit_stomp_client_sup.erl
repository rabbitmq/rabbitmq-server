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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_stomp_client_sup).
-behaviour(supervisor2).

-define(MAX_WAIT, 16#ffffffff).
-export([start_link/1, init/1]).

start_link(Configuration) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    %% The processor is intrinsic. When it exits, the supervisor goes too.
    {ok, ProcessorPid} =
        supervisor2:start_child(SupPid,
                                {rabbit_stomp_processor,
                                 {rabbit_stomp_processor, start_link,
                                  [Configuration]},
                                 intrinsic, ?MAX_WAIT, worker,
                                 [rabbit_stomp_processor]}),
    %% We want the reader to be transient since when it exits normally
    %% the processor may have some work still to do (and the reader
    %% tells the processor to exit). However, if the reader terminates
    %% abnormally then we want to take everything down.
    {ok, ReaderPid} = supervisor2:start_child(
                        SupPid,
                        {rabbit_stomp_reader,
                         {rabbit_stomp_reader,
                          start_link, [SupPid, ProcessorPid, Configuration]},
                         transient, ?MAX_WAIT, worker,
                         [rabbit_stomp_reader]}),

    {ok, SupPid, ReaderPid}.

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

