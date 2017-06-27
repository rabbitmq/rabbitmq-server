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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_channel_common).

-export([do/2, do/3, do_flow/3, ready_for_close/1]).

do(Pid, Method) ->
    do(Pid, Method, none).

do(Pid, Method, Content) ->
    gen_server2:cast(Pid, {method, Method, Content, noflow}).

do_flow(Pid, Method, Content) ->
    %% Here we are tracking messages sent by the rabbit_reader
    %% process. We are accessing the rabbit_reader process dictionary.
    credit_flow:send(Pid),
    gen_server2:cast(Pid, {method, Method, Content, flow}).

ready_for_close(Pid) ->
    gen_server2:cast(Pid, ready_for_close).
