%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-include_lib("amqp10_common/include/amqp10_types.hrl").

%% see AMQP 1.0 §2.6.7
-type delivery_count() :: sequence_no().
-type credit() :: uint().

-type link_state_properties() :: #{atom() => term()}.

-record(credit_reply, {ctag :: rabbit_types:ctag(),
                       delivery_count :: delivery_count(),
                       credit :: credit(),
                       available :: non_neg_integer(),
                       drain :: boolean(),
                       properties :: link_state_properties()}).
