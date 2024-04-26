%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%% -----------------------------------------------------------------------------
-module(rabbit_db_jms_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_jms_topic_exchange.hrl").

-export([
         setup_schema/0,
         create_or_update/3,
         insert/2,
         get/1,
         delete/1,
         delete/3
        ]).

%% -------------------------------------------------------------------
%% setup_schema()
%% -------------------------------------------------------------------

setup_schema() ->
    rabbit_db:run(
      #{mnesia => fun() -> setup_schema_in_mnesia() end
       }).

setup_schema_in_mnesia() ->
    TableName = ?JMS_TOPIC_TABLE,
    rabbit_log:info(
      "Creating table ~ts for JMS topic exchange",
      [TableName]),
    _ = try
            rabbit_table:create(
              TableName,
              [{attributes, record_info(fields, ?JMS_TOPIC_RECORD)},
               {record_name, ?JMS_TOPIC_RECORD},
               {type, set}]),
            %% The JMS topic exchange table must be available on all nodes.
            %% If it existed on only one node, messages could not be published
            %% to JMS topic exchanges and routed to topic subscribers if the node
            %% was unavailable.
            %% The call below makes sure this node has a copy of the table.
            case rabbit_table:ensure_table_copy(TableName, node(), ram_copies) of
                ok ->
                    %% Next, we try to fix other nodes in the cluster if they are
                    %% running a version of RabbitMQ which does not replicate the
                    %% table. All nodes must have a replica for Mnesia operations
                    %% to work properly. Therefore the code below is to make older
                    %% compatible with newer nodes.
                    Replicas = mnesia:table_info(TableName, all_nodes),
                    Members = rabbit_nodes:list_running(),
                    MissingOn = Members -- Replicas,
                    lists:foreach(
                      fun(Node) ->
                              %% Errors from adding a replica on those older nodes
                              %% are ignored however. They should not be fatal. The
                              %% problem will solve by itself once all nodes are
                              %% upgraded.
                              _ = rpc:call(
                                    Node,
                                    rabbit_table, ensure_table_copy,
                                    [TableName, Node, ram_copies])
                      end, MissingOn),
                    ok;
                Error ->
                    Error
            end
        catch throw:Reason  ->
                  rabbit_log:error(
                    "Failed to create JMS topic exchange table: ~tp",
                    [Reason])
        end,
    ok.

%% -------------------------------------------------------------------
%% create_or_update().
%% -------------------------------------------------------------------

create_or_update(XName, BindingKeyAndFun, ErrorFun) ->
    rabbit_db:run(
      #{mnesia =>
            fun() -> create_or_update_in_mnesia(XName, BindingKeyAndFun, ErrorFun) end
       }).

create_or_update_in_mnesia(XName, BindingKeyAndFun, ErrorFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              #?JMS_TOPIC_RECORD{x_selector_funs = BindingFuns} =
                  read_state_in_mnesia(XName, ErrorFun),
              write_state_fun_in_mnesia(XName, put_item(BindingFuns, BindingKeyAndFun))
      end).

%% -------------------------------------------------------------------
%% insert().
%% -------------------------------------------------------------------

insert(XName, BFuns) ->
    rabbit_db:run(
      #{mnesia => fun() -> insert_in_mnesia(XName, BFuns) end
       }).

insert_in_mnesia(XName, BFuns) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              write_state_fun_in_mnesia(XName, BFuns)
      end).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

get(XName) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(XName) end
       }).

get_in_mnesia(XName) ->
    mnesia:async_dirty(
      fun() ->
              case mnesia:read(?JMS_TOPIC_TABLE, XName, read) of
                  [#?JMS_TOPIC_RECORD{x_selector_funs = BindingFuns}] ->
                      BindingFuns;
                  _ ->
                      not_found
              end
      end,
      []
     ).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

delete(XName) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(XName) end
       }).

delete_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> mnesia:delete(?JMS_TOPIC_TABLE, XName, write) end).

delete(XName, BindingKeys, ErrorFun) ->
    rabbit_db:run(
      #{mnesia =>
            fun() -> delete_in_mnesia(XName, BindingKeys, ErrorFun) end
       }).

delete_in_mnesia(XName, BindingKeys, ErrorFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              #?JMS_TOPIC_RECORD{x_selector_funs = BindingFuns} =
                  read_state_in_mnesia(XName, ErrorFun),
              write_state_fun_in_mnesia(XName, remove_items(BindingFuns, BindingKeys))
      end).

read_state_in_mnesia(XName, ErrorFun) ->
    case mnesia:read(?JMS_TOPIC_TABLE, XName, write) of
        [Rec] -> Rec;
        _     -> ErrorFun(XName)
    end.

write_state_fun_in_mnesia(XName, BFuns) ->
    mnesia:write( ?JMS_TOPIC_TABLE
                , #?JMS_TOPIC_RECORD{x_name = XName, x_selector_funs = BFuns}
                , write ).

%% -------------------------------------------------------------------
%% dictionary handling
%% -------------------------------------------------------------------

% add an item to the dictionary of binding functions
put_item(Dict, {Key, Item}) -> dict:store(Key, Item, Dict).

% remove a list of keyed items from the dictionary, by key
remove_items(Dict, []) -> Dict;
remove_items(Dict, [Key | Keys]) -> remove_items(dict:erase(Key, Dict), Keys).
