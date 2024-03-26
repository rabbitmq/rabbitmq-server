%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(my_plugin).

-rabbit_feature_flag({plugin_ff, #{desc => "Plugin's feature flag A"}}).

-rabbit_feature_flag({required_plugin_ff, #{desc => "Plugin's feature flag B",
                                            stability => required}}).
