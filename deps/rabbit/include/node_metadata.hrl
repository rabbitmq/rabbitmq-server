%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2026 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-type node_tag() :: {Key :: binary(), Value :: binary()}.
-type node_metadata_map() :: #{node_tags => [node_tag()]}.

-record(node_metadata, {
          node :: node(),
          metadata = #{} :: node_metadata_map()
         }).
