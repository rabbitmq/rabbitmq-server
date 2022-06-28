%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(ffcommand, {
          name :: rabbit_feature_flags:feature_name(),
          props :: rabbit_feature_flags:feature_props_extended(),
          command :: enable | post_enable,
          extra :: #{nodes := [node()]}
         }).
