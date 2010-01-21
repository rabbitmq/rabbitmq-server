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

-module(rabbit_msg_store_ets_index).
-export([init/1, lookup/2, insert/2, update/2, update_fields/3, delete/2,
         delete_by_file/2, terminate/1]).

-define(MSG_LOC_NAME, rabbit_msg_store_ets_index).

-include("rabbit_msg_store.hrl").

init(_Dir) ->
    ets:new(?MSG_LOC_NAME, [set, public, {keypos, #msg_location.msg_id}]).

lookup(Key, MsgLocations) ->
    case ets:lookup(MsgLocations, Key) of
        []      -> not_found;
        [Entry] -> Entry
    end.

insert(Obj, MsgLocations) ->
    true = ets:insert_new(MsgLocations, Obj),
    ok.

update(Obj, MsgLocations) ->
    true = ets:insert(MsgLocations, Obj),
    ok.

update_fields(Key, Updates, MsgLocations) ->
    true = ets:update_element(MsgLocations, Key, Updates),
    ok.

delete(Key, MsgLocations) ->
    true = ets:delete(MsgLocations, Key),
    ok.

delete_by_file(File, MsgLocations) ->
    MatchHead = #msg_location { file = File, _ = '_' },
    ets:select_delete(MsgLocations, [{MatchHead, [], [true]}]),
    ok.

terminate(MsgLocations) ->
    ets:delete(MsgLocations).
