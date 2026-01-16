%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_exchange_federation_app).

-include_lib("rabbitmq_federation_common/include/rabbit_federation.hrl").
-include_lib("rabbitmq_federation_common/include/logging.hrl").
-include("rabbit_exchange_federation.hrl").

-behaviour(application).
-export([start/2, prep_stop/1, stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.org/pipermail/erlang-questions/2010-April/050508.html

%% All of our actual server processes are supervised by
%% rabbit_federation_sup, which is started by a rabbit_boot_step
%% (since it needs to start up before queue / exchange recovery, so it
%% can't be part of our application).
%%
%% However, we still need an application behaviour since we need to
%% know when our application has started since then the Erlang client
%% will have started and we can therefore start our links going. Since
%% the application behaviour needs a tree of processes to supervise,
%% this is it...
-behaviour(supervisor).
-export([init/1]).

-define(DEFAULT_SHUTDOWN_LINK_BATCH_SIZE, 128).
-define(DEFAULT_SHUTDOWN_TIMEOUT, 180000).
-define(DEFAULT_SHUTDOWN_THROTTLE_DELAY, 50).

start(_Type, _StartArgs) ->
    ets:insert(?FEDERATION_ETS,
               {rabbitmq_exchange_federation,
                #{link_module => rabbit_federation_exchange_link_sup_sup}}),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

prep_stop(State) ->
    rabbit_federation_app_state:mark_as_shutting_down(),
    rabbit_federation_exchange_link:disconnect_all(),
    BatchSize = application:get_env(rabbitmq_exchange_federation,
                                    shutdown_link_batch_size,
                                    ?DEFAULT_SHUTDOWN_LINK_BATCH_SIZE),
    Timeout = application:get_env(rabbitmq_exchange_federation,
                                  shutdown_timeout,
                                  ?DEFAULT_SHUTDOWN_TIMEOUT),
    ThrottleDelay = application:get_env(rabbitmq_exchange_federation,
                                        shutdown_throttle_delay,
                                        ?DEFAULT_SHUTDOWN_THROTTLE_DELAY),
    rabbit_federation_pg:terminate_all_local_members(
        ?FEDERATION_PG_SCOPE, Timeout, BatchSize, ThrottleDelay),
    State.

stop(_State) ->
    ok = rabbit_exchange_federation_sup:stop(),
    ets:delete(?FEDERATION_ETS, rabbitmq_exchange_federation),
    rabbit_federation_pg:stop_scope(?FEDERATION_PG_SCOPE),
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_FEDERATION}),
    Flags = #{
        strategy  => one_for_one,
        intensity => 3,
        period    => 10
    },
    {ok, {Flags, []}}.
