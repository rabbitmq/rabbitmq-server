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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_exchange_type_direct).
-include("rabbit.hrl").

-behaviour(rabbit_exchange_behaviour).

-export([description/0, publish/2]).
-export([recover/1, init/1, delete/1, add_binding/2, delete_binding/2]).
-include("rabbit_exchange_behaviour_spec.erl").

description() ->
    [{name, <<"direct">>},
     {description, <<"AMQP direct exchange, as per the AMQP specification">>}].

publish(#exchange{name = Name},
        Delivery = #delivery{message = #basic_message{routing_key = RoutingKey}}) ->
    rabbit_router:deliver(rabbit_router:match_routing_key(Name, RoutingKey), Delivery).

recover(_X) -> ok.
init(_X) -> ok.
delete(_X) -> ok.
add_binding(_X, _B) -> ok.
delete_binding(_X, _B) -> ok.
