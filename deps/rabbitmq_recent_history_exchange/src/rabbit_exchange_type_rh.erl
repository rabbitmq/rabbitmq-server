-module(rabbit_exchange_type_rh).
-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_rh_plugin.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, route/2]).
-export([validate/1, create/2, recover/2, delete/3,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

-define(TX, false).
-define(KEEP_NB, 20).

-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").

description() ->
    [{name, <<"recent-history">>},
     {description, <<"List of Last-value caches exchange.">>}].

route(#exchange{name = Name},
      #delivery{message = #basic_message{
                             content = Content
                            }}) ->
    rabbit_misc:execute_mnesia_transaction(
        fun () ->
            InCache = mnesia:read(?RH_TABLE, Name, write),
            Msgs = case InCache of
                [#cached{key=Name, content=Cached}] ->
                    Cached;
                _ ->
                    []
                end,
            mnesia:write(?RH_TABLE,
                      #cached{key = Name, content = [Content|lists:sublist(Msgs, ?KEEP_NB)]},
                      write)
        end),
    rabbit_router:match_routing_key(Name, ['_']).

validate(_X) -> ok.
create(_Tx, _X) -> ok.
recover(_X, _Bs) -> ok.

delete(?TX, #exchange{ name = Name }, _Bs) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
          mnesia:delete(?RH_TABLE, Name, write)
      end),
    ok;
delete(_Tx, _X, _Bs) ->
    ok.

add_binding(?TX, #exchange{ name = XName },
            #binding{ destination = QueueName }) ->
    case rabbit_amqqueue:lookup(QueueName) of
        {error, not_found} ->
            rabbit_misc:protocol_error(
              internal_error,
              "could not find queue '~s'",
              [QueueName]);
        {ok, #amqqueue{ pid = Q }} ->
            Values = case mnesia:dirty_read(?RH_TABLE, XName) of
                [] ->
                    [];
                [#cached{content=Cached}] ->
                    lists:map(
                    fun(Content) ->
                        {Props, Payload} = rabbit_basic:from_content(Content),
                        rabbit_basic:message(XName, <<"">>, Props, Payload)
                    end, Cached)
            end,
            [rabbit_amqqueue:deliver(
               Q, rabbit_basic:delivery(false, false, none, V, undefined)) ||
                V <- lists:reverse(Values)]
    end,
    ok;
add_binding(_Tx, _X, _B) ->
    ok.

remove_bindings(_Tx, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange_type_direct:assert_args_equivalence(X, Args).