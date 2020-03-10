%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
