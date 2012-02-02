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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_vhost).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-export([add/1, delete/1, exists/1, list/0, with/2]).
-export([info/1, info/2, info_all/0, info_all/1]).

-ifdef(use_specs).

-spec(add/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(delete/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(exists/1 :: (rabbit_types:vhost()) -> boolean()).
-spec(list/0 :: () -> [rabbit_types:vhost()]).
-spec(with/2 :: (rabbit_types:vhost(), rabbit_misc:thunk(A)) -> A).

-spec(info/1 :: (rabbit_types:vhost()) -> rabbit_types:infos()).
-spec(info/2 :: (rabbit_types:vhost(), rabbit_types:info_keys())
                -> rabbit_types:infos()).
-spec(info_all/0 :: () -> [rabbit_types:infos()]).
-spec(info_all/1 :: (rabbit_types:info_keys()) -> [rabbit_types:infos()]).

-endif.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [name, tracing]).

add(VHostPath) ->
    rabbit_log:info("Adding vhost '~s'~n", [VHostPath]),
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_vhost, VHostPath}) of
                      []  -> ok = mnesia:write(rabbit_vhost,
                                               #vhost{virtual_host = VHostPath},
                                               write);
                      [_] -> mnesia:abort({vhost_already_exists, VHostPath})
                  end
          end,
          fun (ok, true) ->
                  ok;
              (ok, false) ->
                  [rabbit_exchange:declare(
                     rabbit_misc:r(VHostPath, exchange, Name),
                     Type, true, false, false, []) ||
                      {Name,Type} <-
                          [{<<"">>,                   direct},
                           {<<"amq.direct">>,         direct},
                           {<<"amq.topic">>,          topic},
                           {<<"amq.match">>,          headers}, %% per 0-9-1 pdf
                           {<<"amq.headers">>,        headers}, %% per 0-9-1 xml
                           {<<"amq.fanout">>,         fanout},
                           {<<"amq.rabbitmq.trace">>, topic}]],
                  ok
          end),
    R.

delete(VHostPath) ->
    %% FIXME: We are forced to delete the queues and exchanges outside
    %% the TX below. Queue deletion involves sending messages to the queue
    %% process, which in turn results in further mnesia actions and
    %% eventually the termination of that process. Exchange deletion causes
    %% notifications which must be sent outside the TX
    rabbit_log:info("Deleting vhost '~s'~n", [VHostPath]),
    [{ok,_} = rabbit_amqqueue:delete(Q, false, false) ||
        Q <- rabbit_amqqueue:list(VHostPath)],
    [ok = rabbit_exchange:delete(Name, false) ||
        #exchange{name = Name} <- rabbit_exchange:list(VHostPath)],
    R = rabbit_misc:execute_mnesia_transaction(
          with(VHostPath, fun () ->
                                  ok = internal_delete(VHostPath)
                          end)),
    R.

internal_delete(VHostPath) ->
    lists:foreach(
      fun (Info) ->
              ok = rabbit_auth_backend_internal:clear_permissions(
                     proplists:get_value(user, Info), VHostPath)
      end,
      rabbit_auth_backend_internal:list_vhost_permissions(VHostPath)),
    ok = mnesia:delete({rabbit_vhost, VHostPath}),
    ok.

exists(VHostPath) ->
    mnesia:dirty_read({rabbit_vhost, VHostPath}) /= [].

list() ->
    mnesia:dirty_all_keys(rabbit_vhost).

with(VHostPath, Thunk) ->
    fun () ->
            case mnesia:read({rabbit_vhost, VHostPath}) of
                [] ->
                    mnesia:abort({no_such_vhost, VHostPath});
                [_V] ->
                    Thunk()
            end
    end.

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> VHost;
i(tracing, VHost) -> rabbit_trace:tracing(VHost);
i(Item, _)        -> throw({bad_argument, Item}).

info(VHost)        -> infos(?INFO_KEYS, VHost).
info(VHost, Items) -> infos(Items, VHost).

info_all()      -> info_all(?INFO_KEYS).
info_all(Items) -> [info(VHost, Items) || VHost <- list()].
