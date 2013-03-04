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
        , assert_args_equivalence/2
        , policy_changed/3 ]).

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
                            , {type, set} ]
                          ) of
    {atomic, ok} ->ok;
    {aborted, {already_exists, ?JMS_TOPIC_TABLE}} -> ok
  end.

%%----------------------------------------------------------------------------
%% R E F E R E N C E   T Y P E   I N F O R M A T I O N

%% -type(binding() ::
%%         #binding{source      :: rabbit_exchange:name(),
%%                  destination :: binding_destination(),
%%                  key         :: rabbit_binding:key(),
%%                  args        :: rabbit_framing:amqp_table()}).
%%
%% -type(exchange() ::
%%         #exchange{name        :: rabbit_exchange:name(),
%%                   type        :: rabbit_exchange:type(),
%%                   durable     :: boolean(),
%%                   auto_delete :: boolean(),
%%                   arguments   :: rabbit_framing:amqp_table()}).

%%----------------------------------------------------------------------------
%% E X P O R T E D   E X C H A N G E   B E H A V I O U R

% Exchange description
description() -> [ {name, <<"jms-topic-selector">>}
                 , {description, <<"JMS topic selector exchange">>} ].

% Binding event serialisation
serialise_events() -> false.

% Route messages
route( #exchange{name = XName}
     , #delivery{message = #basic_message{content = Content}}
     ) ->
  Headers = get_headers(Content),
  BindingFuns = get_binding_funs(XName),
  rabbit_router:match_bindings( XName
                              , fun(#binding{key = Key}) ->
                                  binding_fun_match(Key, Headers, BindingFuns)
                                end
                              ).

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
           , #binding{key = BindingKey, destination = QName, args = Args}
           ) ->
  case rabbit_amqqueue:lookup(QName) of
    {error, not_found} ->
      queue_not_found_error(QName);
    {ok, #amqqueue{pid = QPid}} ->
      add_binding_fun(XName, {BindingKey, generate_binding_fun(XName, BindingKey, Args)})
  end,
  ok.

% Binding removal
remove_bindings( _Tx
               , #exchange{name = XName}
               , Bindings
               ) ->
  remove_binding_funs(XName, Bindings),
  ok.

% Exchange argument equivalence
assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).

% Policy change notifications ignored
policy_changed(_Tx, _X1, _X2) -> ok.

%%----------------------------------------------------------------------------
%% P R I V A T E   F U N C T I O N S

% Select binding function from Funs dictionary, apply it to Headers and return result (true|false)
binding_fun_match(Key, Headers, FunsDict) ->
  case orddict:find(Key, FunsDict) of
    {ok, Fun} when is_function(Fun, 1) -> Fun(Headers);
    error                              -> false          % do not match if no function found
  end.

% get Headers from message content
get_headers(Content) ->
  case (Content#content.properties)#'P_basic'.headers of
    undefined -> [];
    H         -> rabbit_misc:sort_field_table(H)
  end.

% generate the function that checks the message against the SQL expression
generate_binding_fun(XName, BindingKey, Args) ->
  CompiledSQLExp = compile_sql(get_sql_from_args(Args)),
  fun(Headers) ->
    sql_match(XName, CompiledSQLExp, Headers)
  end.

get_sql_from_args(Args) ->
  case rabbit_misc:table_lookup(Args, ?RJMS_SELECTOR_ARG) of
    {longstr, BinSQL} -> binary_to_list(BinSQL);
    _                 -> ""  %% not found or wrong type
  end.

% scan and parse SQL expression
compile_sql(SQL) ->
  {ok, Tokens, _} = sjx_scanner:string(SQL),
  {ok, CompSQL} = sjx_parser:parse(Tokens),
  CompSQL.

% Evaluate the SQL parsed tree and check against the Headers
sql_match(XName, SQL, Headers) ->
  case sjx_evaluator:evaluate(SQL, Headers) of
    true -> true;
    false -> false;
    error -> evaluation_error(XName, SQL)
  end.

% get binding funs from state
get_binding_funs(XName) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      #?JMS_TOPIC_RECORD{x_state = BindingFuns} = read_state(XName),
      write_state_fun(XName, put_item(BindingFuns, BindingKeyAndFun))
    end
  ).

% add binding fun to binding fun dictionary
add_binding_fun(XName, BindingKeyAndFun) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      #?JMS_TOPIC_RECORD{x_state = BindingFuns} = read_state_for_update(XName),
      write_state_fun(XName, put_item(BindingFuns, BindingKeyAndFun))
    end
  ).

% remove binding funs from binding fun dictionary
remove_binding_funs(XName, Bindings) ->
  BindingKeys = [ BindingKey || #binding{key = BindingKey} <- Bindings ],
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      #?JMS_TOPIC_RECORD{x_state = BindingFuns} = read_state_for_update(XName),
      write_state_fun(XName, remove_items(BindingFuns, BindingKeys))
    end
  ).

% add an item to the dictionary of binding functions
put_item(Dict, {Key, Item}) -> orddict:store(Key, Item, Dict).

% remove a list of keyed items from the dictionary, by key
remove_items(Dict, []) -> Dict;
remove_items(Dict, [Key | Keys]) -> remove_items(orddict:erase(Key, Dict), Keys).

% delete all the state saved for this exchange
delete_state(XName) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      mnesia:delete(?JMS_TOPIC_TABLE, XName, write)
    end
  ).

% Basic read for update
read_state_for_update(XName) -> read_state(XName, write).

% Basic read
read_state(XName) -> read_state(XName, read).

% Lockable read
read_state(XName, Lock) ->
  case mnesia:read(?JMS_TOPIC_TABLE, XName, Lock) of
    []    -> #?JMS_TOPIC_RECORD{x_name = XName, x_state = orddict:new()};
    [Rec] -> Rec;
    _     -> exchange_state_corrupt_error(XName)
  end.

% Basic write after read for update
write_state_fun(XName, State) ->
  mnesia:write( ?JMS_TOPIC_TABLE
              , #?JMS_TOPIC_RECORD{x_name = XName, x_state = State}
              , write ).

% protocol error
queue_not_found_error(QName) ->
  rabbit_misc:protocol_error( internal_error
                            , "could not find queue '~s'"
                            , [QName] ).
% state error
exchange_state_corrupt_error(XName) ->
  rabbit_misc:protocol_error( internal_error
                            , "exchange named '~s' has incorrect saved state"
                            , [XName] ).
% state error
evaluation_error(XName, SQL) ->
  rabbit_misc:protocol_error( internal_error
                            , "exchange named '~s' could not evaluate SQL(~p)"
                            , [XName, SQL] ).

%%----------------------------------------------------------------------------
