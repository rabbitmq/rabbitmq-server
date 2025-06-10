%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-define(AMQP_PROTOCOL_HEADER, <<"AMQP", 0, 1, 0, 0>>).
-define(SASL_PROTOCOL_HEADER, <<"AMQP", 3, 1, 0, 0>>).
-define(FRAME_HEADER_SIZE, 8).

-define(TIMEOUT, 5000).

% -define(debug, true).
-ifdef(debug).
-define(DBG(F, A), error_logger:info_msg(F, A)).
-else.
-define(DBG(F, A), ok).
-endif.

-record(link_ref, {role :: sender | receiver,
                   session :: pid(),
                   %% locally chosen output handle
                   link_handle :: non_neg_integer()}).
