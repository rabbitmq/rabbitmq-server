%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_queue_link_sup_sup).

-behaviour(mirrored_supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-define(SUPERVISOR, ?MODULE).

%% Supervises the upstream links for all queues (but not exchanges). We need
%% different handling here since queues do not want a mirrored sup.

-export([start_link/0, start_child/1, adjust/1, stop_child/1]).
-export([init/1]).

%%----------------------------------------------------------------------------

start_link() ->
    _ = pg:start_link(),
    %% This scope is used by concurrently starting exchange and queue links,
    %% and other places, so we have to start it very early outside of the supervision tree.
    %% The scope is stopped in stop/1.
    rabbit_federation_pg:start_scope(),
    mirrored_supervisor:start_link({local, ?SUPERVISOR}, ?SUPERVISOR,
                                   fun rabbit_misc:execute_mnesia_transaction/1,
                                   ?MODULE, []).

%% Note that the next supervisor down, rabbit_federation_link_sup, is common
%% between exchanges and queues.
start_child(Q) ->
    case mirrored_supervisor:start_child(
           ?SUPERVISOR,
           {id(Q), {rabbit_federation_link_sup, start_link, [Q]},
            transient, ?SUPERVISOR_WAIT, supervisor,
            [rabbit_federation_link_sup]}) of
        {ok, _Pid}               -> ok;
        {error, {already_started, _Pid}} ->
          QueueName = amqqueue:get_name(Q),
          _ = rabbit_log_federation:warning("Federation link for queue ~p was already started",
                                        [rabbit_misc:rs(QueueName)]),
          ok;
        %% A link returned {stop, gone}, the link_sup shut down, that's OK.
        {error, {shutdown, _}} -> ok
    end.


adjust({clear_upstream, VHost, UpstreamName}) ->
    [rabbit_federation_link_sup:adjust(Pid, Q, {clear_upstream, UpstreamName}) ||
        {Q, Pid, _, _} <- mirrored_supervisor:which_children(?SUPERVISOR),
        ?amqqueue_vhost_equals(Q, VHost)],
    ok;
adjust(Reason) ->
    [rabbit_federation_link_sup:adjust(Pid, Q, Reason) ||
        {Q, Pid, _, _} <- mirrored_supervisor:which_children(?SUPERVISOR)],
    ok.

stop_child(Q) ->
    case mirrored_supervisor:terminate_child(?SUPERVISOR, id(Q)) of
      ok -> ok;
      {error, Err} ->
        QueueName = amqqueue:get_name(Q),
        _ = rabbit_log_federation:warning(
          "Attempt to stop a federation link for queue ~p failed: ~p",
          [rabbit_misc:rs(QueueName), Err]),
        ok
    end,
    _ = mirrored_supervisor:delete_child(?SUPERVISOR, id(Q)).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 1200, 60}, []}}.

%% Clean out all mutable aspects of the queue except policy. We need
%% to keep the entire queue around rather than just take its name
%% since we will want to know its policy to determine how to federate
%% it, and its immutable properties in case we want to redeclare it
%% upstream. We don't just take its name and look it up again since
%% that would introduce race conditions when policies change
%% frequently.  Note that since we take down all the links and start
%% again when policies change, the policy will always be correct, so
%% we don't clear it out here and can trust it.
id(Q) when ?is_amqqueue(Q) ->
    Policy = amqqueue:get_policy(Q),
    Q1 = rabbit_amqqueue:immutable(Q),
    amqqueue:set_policy(Q1, Policy).
