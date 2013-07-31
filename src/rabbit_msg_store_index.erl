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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_index).

-include("rabbit_msg_store.hrl").

-ifdef(use_specs).

-type(dir() :: any()).
-type(index_state() :: any()).
-type(keyvalue() :: any()).
-type(fieldpos() :: non_neg_integer()).
-type(fieldvalue() :: any()).

-callback new(dir()) -> index_state().
-callback recover(dir()) -> rabbit_types:ok_or_error2(index_state(), any()).
-callback lookup(rabbit_types:msg_id(), index_state()) -> ('not_found' | keyvalue()).
-callback insert(keyvalue(), index_state()) -> 'ok'.
-callback update(keyvalue(), index_state()) -> 'ok'.
-callback update_fields(rabbit_types:msg_id(), ({fieldpos(), fieldvalue()} |
                                                [{fieldpos(), fieldvalue()}]),
                        index_state()) -> 'ok'.
-callback delete(rabbit_types:msg_id(), index_state()) -> 'ok'.
-callback delete_object(keyvalue(), index_state()) -> 'ok'.
-callback delete_by_file(fieldvalue(), index_state()) -> 'ok'.
-callback terminate(index_state()) -> any().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{new,            1},
     {recover,        1},
     {lookup,         2},
     {insert,         2},
     {update,         2},
     {update_fields,  3},
     {delete,         2},
     {delete_by_file, 2},
     {terminate,      1}];
behaviour_info(_Other) ->
    undefined.

-endif.
