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
%% Copyright (c) 2012, 2013 VMware, Inc.  All rights reserved.
%%

%% JMS on Rabbit Topic Exchange plugin

-module(rabbit_jms_topic_exchange).

-behaviour(rabbit_exchange_type).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_jms_topic_exchange.hrl").

%% Rabbit exchange type functions:
-export([description/0 ,serialise_events/0 ,route/2]).
-export([ validate/1, create/2, delete/3, add_binding/3
        , remove_bindings/3, assert_args_equivalence/2 ]).

%% Initialisation of database function:
-export([setup_db_schema/0]).

%%----------------------------------------------------------------------------

%% Register exchange type
-rabbit_boot_step({ ?MODULE
                  , [ {description,"JMS topic exchange type"}
                    , {mfa, {rabbit_registry, register, [exchange, ?X_TYPE_NAME, ?MODULE]}}
                    , {requires, rabbit_registry}
                    , {enables, kernel_ready} ] }).

%% Initialise database
-rabbit_boot_step({ rabbit_jms_topic_exchange_mnesia
                  , [ {description, "JMS topic exchange: mnesia"}
                    , {mfa, {?MODULE, setup_db_schema, []}}
                    , {requires, database}
                    , {enables, external_infrastructure} ] }).

%%----------------------------------------------------------------------------

% Initialise database table for all exchanges of type <<"x-trial">>
setup_db_schema() ->
  case mnesia:create_table( ?JMS_TOPIC_TABLE
                          , [ {attributes, record_info(fields, jms_topic_xs)}
                            , {record_name, jms_topic_xs}
                            , {type, set} ] )
  of
    {atomic, ok} ->ok;
    {aborted, {already_exists, ?JMS_TOPIC_TABLE}} -> ok
  end.

%%----------------------------------------------------------------------------

% Exchange description
description() -> [ {name, <<"jms-topic">>}
                 , {description, <<"JMS topic exchange">>} ].

% Binding event serialisation
serialise_events() -> false.

% Binding removal
remove_bindings(_Tx, _X, _Bs) -> ok.

% Validate
validate(_X) -> ok.

% Create binding
create(_Tx, _X) -> ok.

% Exchange argument equivalence
assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).

% Route messages
route( #exchange{name = XName}
     , #delivery{message = #basic_message{content = Content}}
     ) ->
  do_extra_function(XName, Content),
  rabbit_router:match_routing_key(XName, ['_']).

%%----------------------------------------------------------------------------
% extra work we can do with this exchange
do_extra_function(XName, Content) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      store_last_msg(XName, Content)
    end
  ).

get_last_msg_from_table(XName) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      case mnesia:read(?JMS_TOPIC_TABLE, XName)
      of
        [] -> [];
        [#jms_topic_xs{key = XName, x_state = LastMessage}] -> LastMessage
      end
    end
  ).

store_last_msg(XName, LastMessage) ->
  mnesia:write( ?JMS_TOPIC_TABLE
              , #jms_topic_xs{key = XName, x_state = LastMessage}
              , write
              ).

% Delete an exchange
delete(_Tx, #exchange{name = XName}, _Bs) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      mnesia:delete(?JMS_TOPIC_TABLE, XName, write)
    end
  ),
  ok.

% Add a new binding
add_binding( _Tx
           , #exchange{name = XName}
           , #binding{destination = QName}
           ) ->
  case rabbit_amqqueue:lookup(QName)
  of
    {error, not_found} ->
      queue_not_found_error(QName);
    {ok, #amqqueue{pid = QPid}} ->
      case get_last_msg_from_table(XName)
      of
        [] -> ok;
        LastMessage -> deliver_message(QPid, XName, LastMessage)
      end
  end,
  ok.

% Deliver a message we saved
deliver_message(QPid, XName, MsgContent) ->
  {Props, Payload} = rabbit_basic:from_content(MsgContent),
  rabbit_amqqueue:deliver
    ( QPid
    , rabbit_basic:delivery
      ( false
      , false
      , rabbit_basic:message(XName, <<"">>, Props, Payload)
      , undefined
      )
    ).

% protocol error
queue_not_found_error(QName) ->
  rabbit_misc:protocol_error( internal_error
                            , "could not find queue '~s'"
                            , [QName] ).

%%----------------------------------------------------------------------------
