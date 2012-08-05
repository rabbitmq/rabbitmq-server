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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-define(CLIENT_ID_MAXLEN, 23).

-record(state, { socket,
                 conn_name,
                 await_recv,
                 credit_flow,
                 conserve,
                 parse_state,
                 clean_sess,
                 client_id,
                 subscriptions,
                 consumer_tags,
                 unacked_pubs,
                 awaiting_ack,
                 awaiting_seqno,
                 will_msg,
                 message_id,
                 exchange,
                 channels,
                 connection,
                 adapter_info }).
