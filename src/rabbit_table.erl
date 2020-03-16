%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_table).

-export([create/0, create_local_copy/1, wait_for_replicated/1, wait/1, wait/2,
         force_load/0, is_present/0, is_empty/0, needs_default_data/0,
         check_schema_integrity/1, clear_ram_only_tables/0, retry_timeout/0,
         wait_for_replicated/0]).

%% for testing purposes
-export([definitions/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-type retry() :: boolean().

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

-spec create() -> 'ok'.

create() ->
    lists:foreach(fun ({Tab, TabDef}) ->
                          TabDef1 = proplists:delete(match, TabDef),
                          case mnesia:create_table(Tab, TabDef1) of
                              {atomic, ok} -> ok;
                              {aborted, Reason} ->
                                  throw({error, {table_creation_failed,
                                                 Tab, TabDef1, Reason}})
                          end
                  end, definitions()),
    ensure_secondary_indexes(),
    ok.

%% Sets up secondary indexes in a blank node database.
ensure_secondary_indexes() ->
  ensure_secondary_index(rabbit_queue, vhost),
  ok.

ensure_secondary_index(Table, Field) ->
  case mnesia:add_table_index(Table, Field) of
    {atomic, ok}                          -> ok;
    {aborted, {already_exists, Table, _}} -> ok
  end.

%% The sequence in which we delete the schema and then the other
%% tables is important: if we delete the schema first when moving to
%% RAM mnesia will loudly complain since it doesn't make much sense to
%% do that. But when moving to disc, we need to move the schema first.

-spec create_local_copy('disc' | 'ram') -> 'ok'.

create_local_copy(disc) ->
    create_local_copy(schema, disc_copies),
    create_local_copies(disc);
create_local_copy(ram)  ->
    create_local_copies(ram),
    create_local_copy(schema, ram_copies).

%% This arity only exists for backwards compatibility with certain
%% plugins. See https://github.com/rabbitmq/rabbitmq-clusterer/issues/19.

-spec wait_for_replicated() -> 'ok'.

wait_for_replicated() ->
    wait_for_replicated(false).

-spec wait_for_replicated(retry()) -> 'ok'.

wait_for_replicated(Retry) ->
    wait([Tab || {Tab, TabDef} <- definitions(),
                 not lists:member({local_content, true}, TabDef)], Retry).

-spec wait([atom()]) -> 'ok'.

wait(TableNames) ->
    wait(TableNames, _Retry = false).

wait(TableNames, Retry) ->
    {Timeout, Retries} = retry_timeout(Retry),
    wait(TableNames, Timeout, Retries).

wait(TableNames, Timeout, Retries) ->
    %% We might be in ctl here for offline ops, in which case we can't
    %% get_env() for the rabbit app.
    rabbit_log:info("Waiting for Mnesia tables for ~p ms, ~p retries left~n",
                    [Timeout, Retries - 1]),
    Result = case mnesia:wait_for_tables(TableNames, Timeout) of
                 ok ->
                     ok;
                 {timeout, BadTabs} ->
                     AllNodes = rabbit_mnesia:cluster_nodes(all),
                     {error, {timeout_waiting_for_tables, AllNodes, BadTabs}};
                 {error, Reason} ->
                     AllNodes = rabbit_mnesia:cluster_nodes(all),
                     {error, {failed_waiting_for_tables, AllNodes, Reason}}
             end,
    case {Retries, Result} of
        {_, ok} ->
            rabbit_log:info("Successfully synced tables from a peer"),
            ok;
        {1, {error, _} = Error} ->
            throw(Error);
        {_, {error, Error}} ->
            rabbit_log:warning("Error while waiting for Mnesia tables: ~p~n", [Error]),
            wait(TableNames, Timeout, Retries - 1)
    end.

retry_timeout(_Retry = false) ->
    {retry_timeout(), 1};
retry_timeout(_Retry = true) ->
    Retries = case application:get_env(rabbit, mnesia_table_loading_retry_limit) of
                  {ok, T}   -> T;
                  undefined -> 10
              end,
    {retry_timeout(), Retries}.

-spec retry_timeout() -> non_neg_integer() | infinity.

retry_timeout() ->
    case application:get_env(rabbit, mnesia_table_loading_retry_timeout) of
        {ok, T}   -> T;
        undefined -> 30000
    end.

-spec force_load() -> 'ok'.

force_load() -> [mnesia:force_load_table(T) || T <- names()], ok.

-spec is_present() -> boolean().

is_present() -> names() -- mnesia:system_info(tables) =:= [].

-spec is_empty() -> boolean().

is_empty()           -> is_empty(names()).

-spec needs_default_data() -> boolean().

needs_default_data() -> is_empty([rabbit_user, rabbit_user_permission,
                                  rabbit_vhost]).

is_empty(Names) ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              Names).

-spec check_schema_integrity(retry()) -> rabbit_types:ok_or_error(any()).

check_schema_integrity(Retry) ->
    Tables = mnesia:system_info(tables),
    case check(fun (Tab, TabDef) ->
                       case lists:member(Tab, Tables) of
                           false -> {error, {table_missing, Tab}};
                           true  -> check_attributes(Tab, TabDef)
                       end
               end) of
        ok     -> wait(names(), Retry),
                  check(fun check_content/2);
        Other  -> Other
    end.

-spec clear_ram_only_tables() -> 'ok'.

clear_ram_only_tables() ->
    Node = node(),
    lists:foreach(
      fun (TabName) ->
              case lists:member(Node, mnesia:table_info(TabName, ram_copies)) of
                  true  -> {atomic, ok} = mnesia:clear_table(TabName);
                  false -> ok
              end
      end, names()),
    ok.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

create_local_copies(Type) ->
    lists:foreach(
      fun ({Tab, TabDef}) ->
              HasDiscCopies     = has_copy_type(TabDef, disc_copies),
              HasDiscOnlyCopies = has_copy_type(TabDef, disc_only_copies),
              LocalTab          = proplists:get_bool(local_content, TabDef),
              StorageType =
                  if
                      Type =:= disc orelse LocalTab ->
                          if
                              HasDiscCopies     -> disc_copies;
                              HasDiscOnlyCopies -> disc_only_copies;
                              true              -> ram_copies
                          end;
                      Type =:= ram ->
                          ram_copies
                  end,
              ok = create_local_copy(Tab, StorageType)
      end, definitions(Type)),
    ok.

create_local_copy(Tab, Type) ->
    StorageType = mnesia:table_info(Tab, storage_type),
    {atomic, ok} =
        if
            StorageType == unknown ->
                mnesia:add_table_copy(Tab, node(), Type);
            StorageType /= Type ->
                mnesia:change_table_copy_type(Tab, node(), Type);
            true -> {atomic, ok}
        end,
    ok.

has_copy_type(TabDef, DiscType) ->
    lists:member(node(), proplists:get_value(DiscType, TabDef, [])).

check_attributes(Tab, TabDef) ->
    {_, ExpAttrs} = proplists:lookup(attributes, TabDef),
    case mnesia:table_info(Tab, attributes) of
        ExpAttrs -> ok;
        Attrs    -> {error, {table_attributes_mismatch, Tab, ExpAttrs, Attrs}}
    end.

check_content(Tab, TabDef) ->
    {_, Match} = proplists:lookup(match, TabDef),
    case mnesia:dirty_first(Tab) of
        '$end_of_table' ->
            ok;
        Key ->
            ObjList = mnesia:dirty_read(Tab, Key),
            MatchComp = ets:match_spec_compile([{Match, [], ['$_']}]),
            case ets:match_spec_run(ObjList, MatchComp) of
                ObjList -> ok;
                _       -> {error, {table_content_invalid, Tab, Match, ObjList}}
            end
    end.

check(Fun) ->
    case [Error || {Tab, TabDef} <- definitions(),
                   begin
                       {Ret, Error} = case Fun(Tab, TabDef) of
                           ok         -> {false, none};
                           {error, E} -> {true, E}
                       end,
                       Ret
                   end] of
        []     -> ok;
        Errors -> {error, Errors}
    end.

%%--------------------------------------------------------------------
%% Table definitions
%%--------------------------------------------------------------------

names() -> [Tab || {Tab, _} <- definitions()].

%% The tables aren't supposed to be on disk on a ram node
definitions(disc) ->
    definitions();
definitions(ram) ->
    [{Tab, [{disc_copies, []}, {ram_copies, [node()]} |
            proplists:delete(
              ram_copies, proplists:delete(disc_copies, TabDef))]} ||
        {Tab, TabDef} <- definitions()].

definitions() ->
    [{rabbit_user,
      [{record_name, internal_user},
       {attributes, record_info(fields, internal_user)},
       {disc_copies, [node()]},
       {match, #internal_user{_='_'}}]},
     {rabbit_user_permission,
      [{record_name, user_permission},
       {attributes, record_info(fields, user_permission)},
       {disc_copies, [node()]},
       {match, #user_permission{user_vhost = #user_vhost{_='_'},
                                permission = #permission{_='_'},
                                _='_'}}]},
     {rabbit_topic_permission,
      [{record_name, topic_permission},
       {attributes, record_info(fields, topic_permission)},
       {disc_copies, [node()]},
       {match, #topic_permission{topic_permission_key = #topic_permission_key{_='_'},
                                 permission = #permission{_='_'},
                                 _='_'}}]},
     {rabbit_vhost,
      [
       {record_name, vhost},
       {attributes, vhost:fields()},
       {disc_copies, [node()]},
       {match, vhost:pattern_match_all()}]},
     {rabbit_listener,
      [{record_name, listener},
       {attributes, record_info(fields, listener)},
       {type, bag},
       {match, #listener{_='_'}}]},
     {rabbit_durable_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {disc_copies, [node()]},
       {match, #route{binding = binding_match(), _='_'}}]},
     {rabbit_semi_durable_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {type, ordered_set},
       {match, #route{binding = binding_match(), _='_'}}]},
     {rabbit_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {type, ordered_set},
       {match, #route{binding = binding_match(), _='_'}}]},
     {rabbit_reverse_route,
      [{record_name, reverse_route},
       {attributes, record_info(fields, reverse_route)},
       {type, ordered_set},
       {match, #reverse_route{reverse_binding = reverse_binding_match(),
                              _='_'}}]},
     {rabbit_topic_trie_node,
      [{record_name, topic_trie_node},
       {attributes, record_info(fields, topic_trie_node)},
       {type, ordered_set},
       {match, #topic_trie_node{trie_node = trie_node_match(), _='_'}}]},
     {rabbit_topic_trie_edge,
      [{record_name, topic_trie_edge},
       {attributes, record_info(fields, topic_trie_edge)},
       {type, ordered_set},
       {match, #topic_trie_edge{trie_edge = trie_edge_match(), _='_'}}]},
     {rabbit_topic_trie_binding,
      [{record_name, topic_trie_binding},
       {attributes, record_info(fields, topic_trie_binding)},
       {type, ordered_set},
       {match, #topic_trie_binding{trie_binding = trie_binding_match(),
                                   _='_'}}]},
     {rabbit_durable_exchange,
      [{record_name, exchange},
       {attributes, record_info(fields, exchange)},
       {disc_copies, [node()]},
       {match, #exchange{name = exchange_name_match(), _='_'}}]},
     {rabbit_exchange,
      [{record_name, exchange},
       {attributes, record_info(fields, exchange)},
       {match, #exchange{name = exchange_name_match(), _='_'}}]},
     {rabbit_exchange_serial,
      [{record_name, exchange_serial},
       {attributes, record_info(fields, exchange_serial)},
       {match, #exchange_serial{name = exchange_name_match(), _='_'}}]},
     {rabbit_runtime_parameters,
      [{record_name, runtime_parameters},
       {attributes, record_info(fields, runtime_parameters)},
       {disc_copies, [node()]},
       {match, #runtime_parameters{_='_'}}]},
     {rabbit_durable_queue,
      [{record_name, amqqueue},
       {attributes, amqqueue:fields()},
       {disc_copies, [node()]},
       {match, amqqueue:pattern_match_on_name(queue_name_match())}]},
     {rabbit_queue,
      [{record_name, amqqueue},
       {attributes, amqqueue:fields()},
       {match, amqqueue:pattern_match_on_name(queue_name_match())}]}]
        ++ gm:table_definitions()
        ++ mirrored_supervisor:table_definitions().

binding_match() ->
    #binding{source = exchange_name_match(),
             destination = binding_destination_match(),
             _='_'}.
reverse_binding_match() ->
    #reverse_binding{destination = binding_destination_match(),
                     source = exchange_name_match(),
                     _='_'}.
binding_destination_match() ->
    resource_match('_').
trie_node_match() ->
    #trie_node{exchange_name = exchange_name_match(), _='_'}.
trie_edge_match() ->
    #trie_edge{exchange_name = exchange_name_match(), _='_'}.
trie_binding_match() ->
    #trie_binding{exchange_name = exchange_name_match(), _='_'}.
exchange_name_match() ->
    resource_match(exchange).
queue_name_match() ->
    resource_match(queue).
resource_match(Kind) ->
    #resource{kind = Kind, _='_'}.
