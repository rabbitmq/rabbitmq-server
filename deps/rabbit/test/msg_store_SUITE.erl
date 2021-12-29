%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(msg_store_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

-define(T(Fun, Args), (catch apply(rabbit, Fun, Args))).

all() ->
    [
      parameter_validation
    ].

parameter_validation(_Config) ->
    %% make sure it works with default values
    ok = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
            [?CREDIT_DISC_BOUND, ?IO_BATCH_SIZE]),

    %% IO_BATCH_SIZE must be greater than CREDIT_DISC_BOUND initial credit
    ok = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
            [{4000, 800}, 5000]),
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{4000, 800}, 1500]),

    %% All values must be integers
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{2000, 500}, "1500"]),
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{"2000", 500}, abc]),
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{2000, "500"}, 2048]),

    %% CREDIT_DISC_BOUND must be a tuple
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [[2000, 500], 1500]),
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [2000, 1500]),

    %% config values can't be smaller than default values
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{1999, 500}, 2048]),
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{2000, 499}, 2048]),
    {error, _} = ?T(validate_msg_store_io_batch_size_and_credit_disc_bound,
                    [{2000, 500}, 2047]).
