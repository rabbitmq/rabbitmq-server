%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-record(dlx_consumer,
        {pid :: pid(),
         prefetch :: non_neg_integer(),
         checked_out = #{} :: #{msg_id() => optimised_tuple(rabbit_dead_letter:reason(), msg())},
         next_msg_id = 0 :: msg_id()}).

-record(rabbit_fifo_dlx,
        {consumer :: option(#dlx_consumer{}),
         %% Queue of dead-lettered messages.
         discards = lqueue:new() :: lqueue:lqueue(optimised_tuple(rabbit_dead_letter:reason(), msg())),
         %% Raft indexes of messages in both discards queue and dlx_consumer's checked_out map
         %% so that we get the smallest ra index in O(1).
         ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
         msg_bytes = 0 :: non_neg_integer(),
         msg_bytes_checkout = 0 :: non_neg_integer()}).
