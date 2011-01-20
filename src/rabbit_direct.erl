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

-module(rabbit_direct).

-export([boot/0, start_channel/5]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(boot/0 :: () -> 'ok').
-spec(start_channel/5 :: (rabbit_channel:channel_number(), pid(),
                          rabbit_types:user(), rabbit_types:vhost(), pid()) ->
                             {'ok', pid()}).

-endif.

%%----------------------------------------------------------------------------

boot() ->
    {ok, _} =
        supervisor2:start_child(
            rabbit_sup,
            {rabbit_direct_client_sup,
             {rabbit_client_sup, start_link,
              [{local, rabbit_direct_client_sup},
               {rabbit_channel_sup, start_link, []}]},
             transient, infinity, supervisor, [rabbit_client_sup]}),
    ok.

start_channel(Number, ClientChannelPid, User, VHost, Collector) ->
    {ok, _, {ChannelPid, _}} =
        supervisor2:start_child(
            rabbit_direct_client_sup,
            [{direct, Number, ClientChannelPid, User, VHost, Collector}]),
    {ok, ChannelPid}.
