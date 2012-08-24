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
%% Copyright (c) 2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_jms_topic_exchange).

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/1, route/2]).
-export([validate/1, create/2, delete/3, policy_changed/3, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "JMS topic exchange type"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-jms-topic">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

-define(EXCHANGE, <<"x-jms-topic-exchange">>).

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"jms-topic">>},
     {description, <<"JMS topic exchange">>}].

serialise_events(_X) -> false.

route(#exchange{name = XName},
      #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> H
              end,
    Matchers = get_matchers(XName),
    rabbit_router:match_bindings(
        XName, fun (#binding{args = Spec}) ->
                        apply_matchers(Spec, Matchers, Headers)
                    end).

%%----------------------------------------------------------------------------

validate(_X) -> ok.

create(none, _X) -> ok;
create(_Tx, _X) -> ok.

delete(none, #exchange { name = XName }, Bindings) ->
    ok = delete_bindings(XName, Bindings);
delete(_Tx, _X, _Bs) ->
    ok.

policy_changed(none, _X1, _X2) -> ok.

add_binding(none, #exchange { name = XName }, Binding) ->
    ok = compile_and_store_binding(XName, Binding);
add_binding(_Tx, _X, _B) ->
    ok.

remove_bindings(none, #exchange { name = XName }, Bindings) ->
    ok = delete_bindings(XName, Bindings);
remove_bindings(_Tx, _X, _Bs) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------

%%% get all matchers from store
get_matchers(XName) ->
    %%% TODO
    [].

apply_matchers(_Spec, _Matchers, _Headers) ->
    %%% TODO everything matches at the moment (TBD)
    true.

delete_bindings(_XName, _Bindings) ->
    %%% TODO
    ok.

compile_and_store_binding(_XName, _Binding) ->
    %%% TODO
    ok.

inform(ActionName, ExchangeName, Headers) ->
    inform(ActionName, ExchangeName, Headers, <<>>).

inform(ActionName, #resource { name = ExchangeName }, Headers, Payload) ->
    {Chan, Q, _CTag} = get_channel_and_queue(),
    Method = #'basic.publish' { exchange = ?EXCHANGE,
                                routing_key = ExchangeName },
    Props = #'P_basic'{ reply_to = Q,
                        headers = [{<<"action">>, longstr,
                                    list_to_binary(ActionName)} | Headers]},
    Msg = #amqp_msg { props = Props, payload = Payload },
    ok = amqp_channel:call(Chan, Method, Msg).

encode_bindings(Bindings) ->
    [{<<"bindings">>, array, [{table, encode_binding(B)} || B <- Bindings]}].

encode_binding(#binding { destination = #resource { name = QName },
                          key = Key }) ->
    [{<<"queue_name">>, longstr, QName}, {<<"key">>, longstr, Key}].

%%----------------------------------------------------------------------------
