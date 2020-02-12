%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-define(AMQP_PROTOCOL_HEADER, <<"AMQP", 0, 1, 0, 0>>).
-define(SASL_PROTOCOL_HEADER, <<"AMQP", 3, 1, 0, 0>>).
-define(MIN_MAX_FRAME_SIZE, 512).
-define(MAX_MAX_FRAME_SIZE, 1024 * 1024).
-define(FRAME_HEADER_SIZE, 8).


% -define(debug, true).
-ifdef(debug).
-define(DBG(F, A), error_logger:info_msg(F, A)).
-else.
-define(DBG(F, A), ok).
-endif.

-record(link_ref, {role :: sender | receiver, session :: pid(),
                   link_handle :: non_neg_integer()}).
