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

-export([start_link/1, run/1, stop/1, basic_get/1]).

%% TODO Is it worth keeping this as a facade?
start_link(Q) -> rabbit_federation_queue_link_sup:start_link(Q).

run(#amqqueue{name = QName})       -> rabbit_federation_queue_link:run(QName).
stop(#amqqueue{name = QName})      -> rabbit_federation_queue_link:stop(QName).
basic_get(#amqqueue{name = QName}) -> rabbit_federation_queue_link:basic_get(QName).
