%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-record(upstream, {uris,
                   exchange_name,
                   prefetch_count,
                   max_hops,
                   reconnect_delay,
                   expires,
                   message_ttl,
                   trust_user_id,
                   ack_mode,
                   ha_policy,
                   name}).

-record(upstream_params, {uri, params, exchange}).

-define(ROUTING_HEADER, <<"x-received-from">>).
-define(BINDING_HEADER, <<"x-bound-from">>).
-define(MAX_HOPS_ARG, <<"x-max-hops">>).
-define(DEFAULT_PREFETCH, 1000).
