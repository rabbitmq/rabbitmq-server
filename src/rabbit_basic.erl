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

-module(rabbit_basic).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([publish/1, message/4, delivery/4]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(publish/1 :: (delivery()) ->
             {ok, routing_result(), [pid()]} | not_found()).
-spec(delivery/4 :: (bool(), bool(), maybe(txn()), message()) -> delivery()). 
-spec(message/4 :: (exchange_name(), routing_key(), binary(), binary()) ->
             message()).

-endif.

%%----------------------------------------------------------------------------

publish(Delivery = #delivery{
          message = #basic_message{exchange_name = ExchangeName}}) ->
    case rabbit_exchange:lookup(ExchangeName) of
        {ok, X} ->
            {RoutingRes, DeliveredQPids} = rabbit_exchange:publish(X, Delivery),
            {ok, RoutingRes, DeliveredQPids};
        Other ->
            Other
    end.

delivery(Mandatory, Immediate, Txn, Message) ->
    #delivery{mandatory = Mandatory, immediate = Immediate, txn = Txn,
              sender = self(), message = Message}.

message(ExchangeName, RoutingKeyBin, ContentTypeBin, BodyBin) ->
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    Content = #content{class_id = ClassId,
                       properties = #'P_basic'{content_type = ContentTypeBin},
                       properties_bin = none,
                       payload_fragments_rev = [BodyBin]},
    #basic_message{exchange_name  = ExchangeName,
                   routing_key    = RoutingKeyBin,
                   content        = Content,
                   persistent_key = none}.
