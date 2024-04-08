%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp10_util).
-include_lib("amqp10_common/include/amqp10_types.hrl").
-export([link_credit_snd/3]).

%% AMQP 1.0 §2.6.7
-spec link_credit_snd(sequence_no(), uint(), sequence_no()) -> uint().
link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd) ->
    LinkCreditSnd = serial_number:diff(
                      serial_number:add(DeliveryCountRcv, LinkCreditRcv),
                      DeliveryCountSnd),
    %% LinkCreditSnd can be negative when receiver decreases credits
    %% while messages are in flight. Maintain a floor of zero.
    max(0, LinkCreditSnd).
