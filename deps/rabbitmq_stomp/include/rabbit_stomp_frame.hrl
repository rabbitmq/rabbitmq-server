%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-record(stomp_frame, {command, headers, body_iolist_rev}).

-record(stomp_parser_config, {max_header_length = 1024*100,
                              max_headers       = 1000,
                              max_body_length   = 1024*1024*100}).
-define(DEFAULT_STOMP_PARSER_CONFIG, #stomp_parser_config{}).
