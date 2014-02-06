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

-module(rabbit_vhost).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-export([add/1, delete/1, exists/1, list/0, with/2, assert/1]).
-export([info/1, info/2, info_all/0, info_all/1]).

-ifdef(use_specs).

-spec(add/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(delete/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(exists/1 :: (rabbit_types:vhost()) -> boolean()).
-spec(list/0 :: () -> [rabbit_types:vhost()]).
-spec(with/2 :: (rabbit_types:vhost(), rabbit_misc:thunk(A)) -> A).
-spec(assert/1 :: (rabbit_types:vhost()) -> 'ok').

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
                     Type, true, false, Internal, []) ||
                      {Name, Type, Internal} <-
                          [{<<"">>,                   direct,  false},
                           {<<"amq.direct">>,         direct,  false},
                           {<<"amq.topic">>,          topic,   false},
                           %% per 0-9-1 pdf
                           {<<"amq.match">>,          headers, false},
                           %% per 0-9-1 xml
                           {<<"amq.headers">>,        headers, false},
                           {<<"amq.fanout">>,         fanout,  false},
                           {<<"amq.rabbitmq.trace">>, topic,   true}]],
                  ok
          end),
    rabbit_event:notify(vhost_created, info(VHostPath)),
    R.

delete(VHostPath) ->
    rabbit_log:info("Deleting vhost '~s'~n", [VHostPath]),
    %% We get hold of all the exchanges and queues in a vhost and then
    %% delete the vhost; all in one tx. This reduces the paths by
    %% which new exchanges and queues can be created in that vhost.
    %%
    %% TODO It is still possible for resources to get created in the
    %% deleted vhost via existing connections, which is effectively a
    %% leak and there is no way to subsequently delete those
    %% resources, at least not via AMQP & ctl. To prevent that we
    %% should really first remove the vhost entry, then close all
    %% connections associated with the vhost, and only then list the
    %% resources.
    case rabbit_misc:execute_mnesia_transaction(
           with(VHostPath, fun () ->
                                   Xs = rabbit_exchange:list(VHostPath),
                                   Qs = rabbit_amqqueue:list(VHostPath),
                                   ok = internal_delete(VHostPath),
                                   {ok, Xs, Qs}
                           end)) of
        {ok, Xs, Qs} ->
            %% we need to cope with (by ignoring) concurrent deletion
            %% of exchanges/queues.
            [assert_benign(rabbit_exchange:delete(Name, false)) ||
                #exchange{name = Name} <- Xs],
            %% TODO: for efficiency, perform these deletions in parallel
            [assert_benign(rabbit_amqqueue:with(
                             Name, fun (Q) -> rabbit_amqqueue:delete(
                                                Q, false, false)
                                   end)) ||
                #amqqueue{name = Name} <- Qs],
            ok = rabbit_event:notify(vhost_deleted, [{name, VHostPath}]);
        Other ->
            Other
    end.

assert_benign(ok)                   -> ok;
assert_benign({ok, _})              -> ok;
assert_benign({error, not_found})   -> ok;
assert_benign({error, {absent, Q}}) ->
    %% We have a durable queue on a down node. Removing the mnesia
    %% entries here is safe. If/when the down node restarts, it will
    %% clear out the on-disk storage of the queue.
    case rabbit_amqqueue:internal_delete(Q#amqqueue.name) of
        ok                 -> ok;
        {error, not_found} -> ok
    end.

internal_delete(VHostPath) ->
    ok = rabbit_auth_backend_internal:clear_vhost_permissions(VHostPath),
    ok = rabbit_runtime_parameters:clear_vhost(VHostPath),
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

%% Like with/2 but outside an Mnesia tx
assert(VHostPath) -> case exists(VHostPath) of
                         true  -> ok;
                         false -> throw({error, {no_such_vhost, VHostPath}})
                     end.

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> VHost;
i(tracing, VHost) -> rabbit_trace:enabled(VHost);
i(Item, _)        -> throw({bad_argument, Item}).

info(VHost)        -> infos(?INFO_KEYS, VHost).
info(VHost, Items) -> infos(Items, VHost).

info_all()      -> info_all(?INFO_KEYS).
info_all(Items) -> [info(VHost, Items) || VHost <- list()].
