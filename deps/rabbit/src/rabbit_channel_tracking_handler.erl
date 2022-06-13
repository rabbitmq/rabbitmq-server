%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_channel_tracking_handler).

%% This module keeps track of channel creation and termination events
%% on its local node. Similar to the rabbit_connection_tracking_handler,
%% the primary goal here is to decouple channel tracking from rabbit_reader
%% and isolate channel tracking to its own process to avoid blocking connection
%% creation events. Additionaly, creation events are also non-blocking in that
%% they spawn a short-live process for updating the tracking tables in realtime.
%%
%% Events from other nodes are ignored.

-behaviour(gen_event).

-export([add_handler/0,
         delete_handler/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "channel tracking event handler"},
                    {mfa,         {?MODULE, add_handler, []}},
                    {cleanup,     {?MODULE, delete_handler, []}},
                    {requires,    [channel_tracking]},
                    {enables,     recovery}]}).

%%
%% API
%%

-spec add_handler() -> ok | {error, term()}.
add_handler() ->
    case rabbit_feature_flags:is_enabled(user_limits) of
        true ->
            %% channel-tracking is only used by per-user max-channel limits
            %% hence the handler is not enabled if that feature is disabled
            case gen_event:add_handler(rabbit_event, ?MODULE, []) of
                ok ->
                    ok;
                {'EXIT', Reason} ->
                    {error, Reason}
            end;
        false ->
            ok
    end.

-spec delete_handler() -> ok.
delete_handler() ->
    case gen_event:delete_handler(rabbit_event, ?MODULE, []) of
        {error, module_not_found} ->
            %% the channel_tracking_handler is not installed
            ok;
        Result ->
            Result
    end.

%%
%% gen_event callbacks
%%

init([]) ->
    {ok, []}.

handle_event(#event{type = channel_created, props = Details}, State) ->
    ok = rabbit_channel_tracking:update_tracked({channel_created, Details}),
    {ok, State};
handle_event(#event{type = channel_closed, props = Details}, State) ->
    ok = rabbit_channel_tracking:update_tracked({channel_closed, Details}),
    {ok, State};
handle_event(#event{type = connection_closed, props = Details}, State) ->
    ok = rabbit_channel_tracking:update_tracked({connection_closed, Details}),
    {ok, State};
handle_event(#event{type = user_deleted, props = Details}, State) ->
    ok = rabbit_channel_tracking:update_tracked({user_deleted, Details}),
    {ok, State};
%% A node had been deleted from the cluster.
handle_event(#event{type = node_deleted, props = Details}, State) ->
    ok = rabbit_channel_tracking:update_tracked({node_deleted, Details}),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
