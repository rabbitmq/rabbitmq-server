%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mnesia_to_khepri_record_converter).

%% Khepri paths don't support tuples or records, so the key part of the
%% #mirrored_sup_childspec{} used by some plugins must be  transformed in a
%% valid Khepri path during the migration from Mnesia to Khepri.
%% `rabbit_db_msup_m2k_converter` iterates over all declared converters, which
%% must implement `rabbit_mnesia_to_khepri_record_converter` behaviour callbacks.
%%
%% This mechanism could be reused by any other rabbit_db_*_m2k_converter

-callback upgrade_record(Table, Record) -> Record when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple().

-callback upgrade_key(Table, Key) -> Key when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any().
