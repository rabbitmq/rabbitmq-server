%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_rh_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_recent_history.hrl").

-export([
         setup_schema/0,
         get/1,
         insert/3,
         delete/0,
         delete/1
        ]).

%% -------------------------------------------------------------------
%% setup_schema().
%% -------------------------------------------------------------------

setup_schema() ->
    rabbit_db:run(
      #{mnesia => fun() -> setup_schema_in_mnesia() end
       }).

setup_schema_in_mnesia() ->
    _ = mnesia:create_table(?RH_TABLE,
                            [{attributes, record_info(fields, cached)},
                             {record_name, cached},
                             {type, set}]),
    _ = mnesia:add_table_copy(?RH_TABLE, node(), ram_copies),
    rabbit_table:wait([?RH_TABLE]),
    ok.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

get(XName) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(XName) end
       }).

get_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> get_in_mnesia_tx(XName) end).

get_in_mnesia_tx(XName) ->
    case mnesia:read(?RH_TABLE, XName) of
        [] ->
            [];
        [#cached{key = XName, content=Cached}] ->
            Cached
    end.

%% -------------------------------------------------------------------
%% insert().
%% -------------------------------------------------------------------

insert(XName, Message, Length) ->
    rabbit_db:run(
      #{mnesia => fun() -> insert_in_mnesia(XName, Message, Length) end
       }).

insert_in_mnesia(XName, Message, Length) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Cached = get_in_mnesia_tx(XName),
              insert_in_mnesia(XName, Cached, Message, Length)
      end).

insert_in_mnesia(Key, Cached, Message, undefined) ->
    insert0_in_mnesia(Key, Cached, Message, ?KEEP_NB);
insert_in_mnesia(Key, Cached, Message, {_Type, Length}) ->
    insert0_in_mnesia(Key, Cached, Message, Length).

insert0_in_mnesia(Key, Cached, Message, Length) ->
    mnesia:write(?RH_TABLE,
                 #cached{key     = Key,
                         content = [Message|lists:sublist(Cached, Length-1)]},
                 write).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

delete() ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia() end
       }).

delete_in_mnesia() ->
    _ = mnesia:delete_table(?RH_TABLE).

delete(XName) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(XName) end
       }).

delete_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              mnesia:delete(?RH_TABLE, XName, write)
      end).
