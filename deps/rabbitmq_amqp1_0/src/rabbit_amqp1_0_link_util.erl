%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_link_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-export([outcomes/1, ctag_to_handle/1, handle_to_ctag/1, durable/1]).

-define(EXCHANGE_SUB_LIFETIME, "delete-on-close").
-define(DEFAULT_OUTCOME, #'v1_0.released'{}).
-define(SUPPORTED_OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                             ?V_1_0_SYMBOL_REJECTED,
                             ?V_1_0_SYMBOL_RELEASED]).

-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED,
                   ?V_1_0_SYMBOL_MODIFIED]).

outcomes(Source) ->
    {DefaultOutcome, Outcomes} =
        case Source of
            #'v1_0.source' {
                      default_outcome = DO,
                      outcomes = Os
                     } ->
                DO1 = case DO of
                          undefined -> ?DEFAULT_OUTCOME;
                          _         -> DO
                      end,
                Os1 = case Os of
                          undefined    -> ?SUPPORTED_OUTCOMES;
                          {array, symbol, Syms} -> Syms;
                          Bad1         -> rabbit_amqp1_0_util:protocol_error(
                                            ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                                            "Outcomes not supported: ~p",
                                            [Bad1])
                      end,
                {DO1, Os1};
            _ ->
                {?DEFAULT_OUTCOME, ?SUPPORTED_OUTCOMES}
        end,
    case [O || O <- Outcomes, not lists:member(O, ?OUTCOMES)] of
        []  -> {DefaultOutcome, {array, symbol, Outcomes}};
        Bad -> rabbit_amqp1_0_util:protocol_error(
                 ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                 "Outcomes not supported: ~p", [Bad])
    end.

handle_to_ctag({uint, H}) ->
    <<"ctag-", H:32/integer>>.

ctag_to_handle(<<"ctag-", H:32/integer>>) ->
    {uint, H}.

durable(undefined)                                  -> false; %% default: none
durable(?V_1_0_TERMINUS_DURABILITY_NONE)            -> false;
%% This one means "existence of the thing is durable, but unacked msgs
%% aren't". We choose to upgrade that.
durable(?V_1_0_TERMINUS_DURABILITY_CONFIGURATION)   -> true;
durable(?V_1_0_TERMINUS_DURABILITY_UNSETTLED_STATE) -> true.
