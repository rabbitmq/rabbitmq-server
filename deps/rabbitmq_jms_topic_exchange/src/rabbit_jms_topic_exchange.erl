%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%% -----------------------------------------------------------------------------

%% JMS on Rabbit Selector Exchange plugin

%% -----------------------------------------------------------------------------
-module(rabbit_jms_topic_exchange).

-behaviour(rabbit_exchange_type).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_jms_topic_exchange.hrl").

%% Rabbit exchange type functions:
-export([ description/0
        , serialise_events/0
        , route/3
        , validate/1
        , create/2
        , delete/2
        , validate_binding/2
        , add_binding/3
        , remove_bindings/3
        , assert_args_equivalence/2
        , policy_changed/2 ]).

%% Initialisation of database function:
-export([setup_db_schema/0]).

-export([info/1, info/2]).

%%----------------------------------------------------------------------------

%% Register exchange type
-rabbit_boot_step({ ?MODULE
                  , [ {description, "exchange type JMS topic selector"}
                    , {mfa, {rabbit_registry, register, [exchange, ?X_TYPE_NAME, ?MODULE]}}
                    , {cleanup, {rabbit_registry, unregister, [exchange, ?X_TYPE_NAME]}}
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
    rabbit_db_jms_exchange:setup_schema().

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
%%
%% -type(amqp_field_type() ::
%%       'longstr' | 'signedint' | 'decimal' | 'timestamp' |
%%       'table' | 'byte' | 'double' | 'float' | 'long' |
%%       'short' | 'bool' | 'binary' | 'void' | 'array').

%%----------------------------------------------------------------------------
%% E X P O R T E D   E X C H A N G E   B E H A V I O U R

% Exchange description
description() -> [ {name, <<"jms-selector">>}
                 , {description, <<"JMS selector exchange">>} ].

% Binding event serialisation
serialise_events() -> false.

% Route messages
route(#exchange{name = XName}, Msg, _Opts) ->
    Content = mc:protocol_state(mc:convert(mc_amqpl, Msg)),
    case get_binding_funs_x(XName) of
        not_found ->
            [];
        BindingFuns ->
            match_bindings(XName, Content, BindingFuns)
    end.


% Before exchange declaration
validate(_X) -> ok.

% After exchange declaration and recovery
create(_Tx, #exchange{name = XName}) ->
  add_initial_record(XName).

% Delete an exchange
delete(_Tx, #exchange{name = XName}) ->
    delete_state(XName),
    ok.

% Before add binding
validate_binding(_X, _B) -> ok.

% A new binding has ben added or recovered
add_binding( _Tx
           , #exchange{name = XName}
           , #binding{key = BindingKey, destination = Dest, args = Args}
           ) ->
  Selector = get_string_arg(Args, ?RJMS_COMPILED_SELECTOR_ARG),
  BindGen = generate_binding_fun(Selector),
  case BindGen of
    {ok, BindFun} ->
      add_binding_fun(XName, {{BindingKey, Dest}, BindFun});
    error ->
      parsing_error(XName, Selector, Dest)
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
policy_changed(_X1, _X2) -> ok.

% Stub for type-specific exchange information
info(_X) -> [].
info(_X, _) -> [].


%%----------------------------------------------------------------------------
%% P R I V A T E   F U N C T I O N S

% Get a string argument from the args or arguments parameters
get_string_arg(Args, ArgName) -> get_string_arg(Args, ArgName, error).

get_string_arg(Args, ArgName, Default) ->
  case rabbit_misc:table_lookup(Args, ArgName) of
    {longstr, BinVal} -> binary_to_list(BinVal);
    _ -> Default
  end.

% Match bindings for the current message
match_bindings(XName, MessageContent, BindingFuns) ->
  MessageHeaders = get_headers(MessageContent),
  rabbit_router:match_bindings( XName
                              , fun(#binding{key = Key, destination = Dest}) ->
                                  binding_fun_match({Key, Dest}, MessageHeaders, BindingFuns)
                                end
                              ).

% Select binding function from Funs dictionary, apply it to Headers and return result (true|false)
binding_fun_match(DictKey, Headers, FunsDict) ->
  case dict:find(DictKey, FunsDict) of
    {ok, Fun} when is_function(Fun, 1) -> Fun(Headers);
    error                              -> false          % do not match if no function found
  end.

% get Headers from message content
get_headers(Content) ->
  case (Content#content.properties)#'P_basic'.headers of
    undefined -> [];
    H         -> rabbit_misc:sort_field_table(H)
  end.

% generate the function that checks the message against the selector
generate_binding_fun(ERL) ->
  case decode_term(ERL) of
    {error, _}    -> error;
    {ok, ErlTerm} -> check_fun(ErlTerm)
  end.

% build checking function from compiled expression
check_fun(CompiledExp) ->
  { ok,
    fun(Headers) ->
      selector_match(CompiledExp, Headers)
    end
  }.

% get an erlang term from a string
decode_term(Str) ->
  try
    {ok, Ts, _} = erl_scan:string(Str),
    {ok, Term} = erl_parse:parse_term(Ts),
    {ok, Term}
  catch
    Err -> {error, {invalid_erlang_term, Err}}
  end.

% Evaluate the selector and check against the Headers
selector_match(Selector, Headers) ->
  case sjx_evaluator:evaluate(Selector, Headers) of
    true -> true;
    _    -> false
  end.

% get binding funs from state (using dirty_reads)
get_binding_funs_x(XName) ->
    rabbit_db_jms_exchange:get(XName).

add_initial_record(XName) ->
  write_state_fun(XName, dict:new()).

% add binding fun to binding fun dictionary
add_binding_fun(XName, BindingKeyAndFun) ->
    rabbit_db_jms_exchange:create_or_update(XName, BindingKeyAndFun, fun exchange_state_corrupt_error/1).

% remove binding funs from binding fun dictionary
remove_binding_funs(XName, Bindings) ->
  BindingKeys = [ {BindingKey, DestName} || #binding{key = BindingKey, destination = DestName} <- Bindings ],
    rabbit_db_jms_exchange:delete(XName, BindingKeys, fun exchange_state_corrupt_error/1).

% delete all the state saved for this exchange
delete_state(XName) ->
    rabbit_db_jms_exchange:delete(XName).

% Basic write
write_state_fun(XName, BFuns) ->
    rabbit_db_jms_exchange:insert(XName, BFuns).

%%----------------------------------------------------------------------------
%% E R R O R S

% state error
-spec exchange_state_corrupt_error(#resource{}) -> no_return().
exchange_state_corrupt_error(#resource{name = XName}) ->
  rabbit_misc:protocol_error( internal_error
                            , "exchange named '~ts' has no saved state or incorrect saved state"
                            , [XName] ).

% parsing error
-spec parsing_error(#resource{}, term(), #resource{}) -> no_return().
parsing_error(#resource{name = XName}, S, #resource{name = DestName}) ->
  rabbit_misc:protocol_error( precondition_failed
                            , "cannot parse selector '~tp' binding destination '~ts' to exchange '~ts'"
                            , [S, DestName, XName] ).

%%----------------------------------------------------------------------------
