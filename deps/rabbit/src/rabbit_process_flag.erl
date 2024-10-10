%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_process_flag).


-export([adjust_for_message_handling_proc/0
        ]).

%% Adjust process flags for processes that handle RabbitMQ messages.
%% For example any process that uses the `rabbit_queue_type' module
%% may benefit from this tuning.
%% @returns `ok'
-spec adjust_for_message_handling_proc() -> ok.
adjust_for_message_handling_proc() ->
    process_flag(message_queue_data, off_heap),
    case code_version:get_otp_version() of
        OtpMaj when OtpMaj >= 27 ->
            %% 46422 is the default min_bin_vheap_size and for OTP 27 and above
            %% we want to substantially increase it for processes that may buffer
            %% messages. 32x has proven workable in testing whilst not being
            %% ridiculously large
            process_flag(min_bin_vheap_size, 46422 * 32),
            ok;
        _ ->
            ok
    end.
