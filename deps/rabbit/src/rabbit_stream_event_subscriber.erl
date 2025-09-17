%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_event_subscriber).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2]).
-export([register/0, unregister/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step({rabbit_stream_event_subscriber,
                   [{description, "stream event subscriber"},
                    {mfa,         {?MODULE, register, []}},
                    {cleanup,     {?MODULE, unregister, []}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).

register() ->
    gen_event:add_handler(rabbit_alarm, ?MODULE, []),
    gen_event:add_handler(rabbit_event, ?MODULE, []).

unregister() ->
    gen_event:delete_handler(rabbit_alarm, ?MODULE, []),
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

init([]) ->
    {ok, []}.

handle_call( _, State) ->
    {ok, ok, State}.

handle_event({node_up, Node}, State) ->
    rabbit_stream_periodic_membership_reconciliation:on_node_up(Node),
    {ok, State};
handle_event({node_down, Node}, State) ->
    rabbit_stream_periodic_membership_reconciliation:on_node_down(Node),
    {ok, State};
handle_event(#event{type = policy_set}, State) ->
    rabbit_stream_periodic_membership_reconciliation:policy_set(),
    {ok, State};
handle_event(#event{type = operator_policy_set}, State) ->
    rabbit_stream_periodic_membership_reconciliation:policy_set(),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

handle_info(_, State) ->
    {ok, State}.
