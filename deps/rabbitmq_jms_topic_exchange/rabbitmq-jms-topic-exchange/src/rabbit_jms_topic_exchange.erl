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

%% JMS on Rabbit Topic Selector Exchange plugin

-module(rabbit_jms_topic_exchange).

-behaviour(rabbit_exchange_type).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_jms_topic_exchange.hrl").

%% Rabbit exchange type functions:
-export([ description/0
        , serialise_events/0
        , route/2
        , validate/1
        , create/2
        , delete/3
        , add_binding/3
        , remove_bindings/3
        , assert_args_equivalence/2 ]).

%% Initialisation of database function:
-export([setup_db_schema/0]).

%%----------------------------------------------------------------------------

%% Register exchange type
-rabbit_boot_step({ ?MODULE
                  , [ {description, "exchange type JMS topic selector"}
                    , {mfa, {rabbit_registry, register, [exchange, ?X_TYPE_NAME, ?MODULE]}}
                    , {requires, rabbit_registry}
                    , {enables, kernel_ready} ] }).

%% Initialise database
-rabbit_boot_step({ rabbit_jms_topic_exchange_mnesia
                  , [ {description, "database exchange type JMS topic selector"}
                    , {mfa, {?MODULE, setup_db_schema, []}}
                    , {requires, database}
                    , {enables, external_infrastructure} ] }).

%%----------------------------------------------------------------------------

% Initialise database table for all exchanges of type <<"x-jms-topic">>
setup_db_schema() ->
  case mnesia:create_table( ?JMS_TOPIC_TABLE
                          , [ {attributes, record_info(fields, ?JMS_TOPIC_RECORD)}
                            , {record_name, ?JMS_TOPIC_RECORD}
                            , {type, set} ] )
  of
    {atomic, ok} ->ok;
    {aborted, {already_exists, ?JMS_TOPIC_TABLE}} -> ok
  end.

%%----------------------------------------------------------------------------
%% E X P O R T E D   E X C H A N G E   F U N C T I O N S

% Exchange description
description() -> [ {name, <<"jms-topic-selector">>}
                 , {description, <<"JMS topic selector exchange">>} ].

% Binding event serialisation
serialise_events() -> false.

% Route messages
route( #exchange{name = XName}
     , #delivery{message = #basic_message{content = Content}}
     ) ->
  rabbit_router:match_routing_key(XName, ['_']).

% Validate
validate(_X) -> ok.

% Create binding
create(_Tx, _X) -> ok.

% Delete an exchange
delete(_Tx, #exchange{name = XName}, _Bs) ->
  delete_state(XName),
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
      case get_state(XName)
      of
        []    -> save_state(XName, {XName});
        State -> io:format("~n~nState already exists~n~n")
      end
  end,
  ok.

% Binding removal
remove_bindings(_Tx, _X, _Bs) -> ok.

% Exchange argument equivalence
assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------
%% P R I V A T E   W O R K   F U N C T I O N S

% save the exchange state
save_state(XName, State) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      mnesia:write( ?JMS_TOPIC_TABLE
                  , #?JMS_TOPIC_RECORD{key = XName, x_state = State}
                  , write
                  )
    end
  ).

% extract exchange state from db
get_state(XName) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      case mnesia:read(?JMS_TOPIC_TABLE, XName)
      of
        [] -> [];
        [#?JMS_TOPIC_RECORD{key = XName, x_state = State}] -> State
      end
    end
  ).

delete_state(XName) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      mnesia:delete(?JMS_TOPIC_TABLE, XName, write)
    end
  ).

% Deliver a message
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
