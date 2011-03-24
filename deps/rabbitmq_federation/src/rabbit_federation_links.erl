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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_links).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([go_all/0]).
-export([add_binding/3, remove_binding/3, stop/1]).

go_all() ->
    [{ok, _} = rabbit_federation_exchange_sup:go(Pid) ||
     {_, Pid, _, _} <- supervisor:which_children(?SUPERVISOR)].

add_binding(Serial, X, B) ->
    call(X, {enqueue, Serial, {add_binding, B}}).

remove_binding(Serial, X, B) ->
    call(X, {enqueue, Serial, {remove_binding, B}}).

stop(X) ->
    call(X, stop).

%%----------------------------------------------------------------------------

call(#exchange{ name = Downstream }, Msg) ->
    Sup = rabbit_federation_db:sup_for_exchange(Downstream),
    [gen_server2:call(Pid, Msg, infinity) ||
        {_, Pid, _, _} <- supervisor2:which_children(Sup), Pid =/= undefined].
