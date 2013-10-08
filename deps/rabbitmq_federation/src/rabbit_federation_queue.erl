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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_queue).

-rabbit_boot_step({?MODULE,
                   [{description, "federation queue decorator"},
                    {mfa, {rabbit_registry, register,
                           [queue_decorator, <<"federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(rabbit_queue_decorator).

-export([startup/1, shutdown/1, policy_changed/2, active_for/1, notify/3]).
-export([policy_changed_local/2]).

-import(rabbit_misc, [pget/2]).

%%----------------------------------------------------------------------------

startup(Q) ->
    case active_for(Q) of
        true  -> rabbit_federation_queue_link_sup_sup:start_child(Q);
        false -> ok
    end,
    ok.

shutdown(Q = #amqqueue{name = QName}) ->
    case active_for(Q) of
        true  -> rabbit_federation_queue_link_sup_sup:stop_child(Q),
                 rabbit_federation_status:remove_exchange_or_queue(QName);
        false -> ok
    end,
    ok.

policy_changed(Q1 = #amqqueue{name = QName}, Q2) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, #amqqueue{pid = QPid}} ->
            rpc:call(node(QPid), rabbit_federation_queue,
                     policy_changed_local, [Q1, Q2]);
        {error, not_found} ->
            ok
    end.

policy_changed_local(Q1, Q2) ->
    shutdown(Q1),
    startup(Q2).

active_for(Q) -> rabbit_federation_upstream:federate(Q).

%% We need to reconsider whether we need to run or pause every time
%% something significant changes in the queue. In theory we don't need
%% to respond to absolutely every event the queue emits, but in
%% practice we need to respond to most of them and it doesn't really
%% cost much to respond to all of them. So that's why we ignore the
%% Event parameter.
%%
%% For the record, the events, and why we care about them:
%%
%% consumer_blocked   | We may have no more active consumers, and thus need to
%%                    | pause
%%                    |
%% consumer_unblocked | We don't care
%%                    |
%% queue_empty        | The queue has become empty therefore we need to run to
%%                    | get more messages
%%                    |
%% basic_consume      | We don't care
%%                    |
%% basic_cancel       | We may have no more active consumers, and thus need to
%%                    | pause
%%                    |
%% refresh            | We asked for it (we have started a new link after
%%                    | failover and need something to prod us into action
%%                    | (or not)).

notify(#amqqueue{name = QName}, _Event, Props) ->
    case pget(is_empty, Props) andalso
        active_unfederated(pget(max_active_consumer_priority, Props)) of
        true  -> rabbit_federation_queue_link:run(QName);
        false -> rabbit_federation_queue_link:pause(QName)
    end,
    ok.

active_unfederated(empty)         -> false;
active_unfederated(P) when P >= 0 -> true;
active_unfederated(_P)            -> false.
