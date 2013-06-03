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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_queue).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([maybe_start/1, terminate/1, policy_changed/2]).
-export([run/1, stop/1, basic_get/1]).

maybe_start(Q) ->
    case federate(Q) of
        true  -> rabbit_federation_queue_link_sup_sup:start_child(Q);
        false -> ok
    end.

terminate(Q) ->
    %% TODO naming of stop vs terminate not consistent with exchange
    case federate(Q) of
        true  -> rabbit_federation_queue_link_sup_sup:stop_child(Q);
        false -> ok
    end.

%% TODO serious clustering bug here - policy_changed/2 gets invoked on
%% the node against which rabbitmqctl was run. Which may not be the
%% node the master + hence link processes are running on.
policy_changed(Q1, Q2) ->
    terminate(Q1),
    maybe_start(Q2).

run(#amqqueue{name = QName})       -> rabbit_federation_queue_link:run(QName).
stop(#amqqueue{name = QName})      -> rabbit_federation_queue_link:stop(QName).
basic_get(#amqqueue{name = QName}) -> rabbit_federation_queue_link:basic_get(QName).

%% TODO dedup from _exchange (?)
federate(Q) ->
    case rabbit_federation_upstream:set_for(Q) of
        {ok, _}    -> true;
        {error, _} -> false
    end.
