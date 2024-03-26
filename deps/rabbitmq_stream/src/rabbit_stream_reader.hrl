%% The contents of this file are subject to the Mozilla Public License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2024 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-include_lib("rabbit_common/include/rabbit.hrl").

-type stream() :: binary().
-type publisher_id() :: byte().
-type publisher_reference() :: binary().
-type subscription_id() :: byte().
-type internal_id() :: integer().

-record(publisher,
        {publisher_id :: publisher_id(),
         stream :: stream(),
         reference :: undefined | publisher_reference(),
         leader :: pid(),
         %% We do not use atomics here for concurrent access. Instead, we use atomics
         %% to reduce memory copy overhead for record fields that change often.
         message_counters :: atomics:atomics_ref(),
         %% use to distinguish a stale publisher from a live publisher with the same ID
         %% used only for publishers without a reference (dedup off)
         internal_id :: internal_id()}).
-record(consumer_configuration,
        {socket :: rabbit_net:socket(), %% ranch_transport:socket(),
         member_pid :: pid(),
         subscription_id :: subscription_id(),
         stream :: stream(),
         offset :: osiris:offset(),
         counters :: atomics:atomics_ref(),
         properties :: map(),
         active :: boolean()}).
-record(consumer,
        {configuration :: #consumer_configuration{},
         credit :: non_neg_integer(),
         send_limit :: non_neg_integer(),
         log = undefined :: undefined | osiris_log:state(),
         last_listener_offset = undefined :: undefined | osiris:offset()}).
-record(request,
        {start :: integer(),
         content :: term()}).
-record(stream_connection_state,
        {data :: rabbit_stream_core:state(), blocked :: boolean(),
         consumers :: #{subscription_id() => #consumer{}}}).
-record(stream_connection,
        {name :: binary(),
         %% server host
         host,
         %% client host
         peer_host,
         %% server port
         port,
         %% client port
         peer_port,
         auth_mechanism,
         authentication_state :: any(),
         connected_at :: integer(),
         helper_sup :: pid(),
         socket :: rabbit_net:socket(),
         publishers = #{} :: #{publisher_id() => #publisher{}},
         publisher_to_ids = #{} :: #{{stream(), publisher_reference()} => publisher_id()},
         stream_leaders = #{} :: #{stream() => pid()},
         stream_subscriptions = #{} :: #{stream() => [subscription_id()]},
         credits :: atomics:atomics_ref(),
         user :: undefined | #user{},
         virtual_host :: undefined | binary(),
         connection_step ::
             atom(), % tcp_connected, peer_properties_exchanged, authenticating, authenticated, tuning, tuned, opened, failure, closing, closing_done
         frame_max :: integer(),
         heartbeat :: undefined | integer(),
         heartbeater :: any(),
         client_properties = #{} :: #{binary() => binary()},
         monitors = #{} :: #{reference() => {pid(), stream()}},
         stats_timer :: undefined | rabbit_event:state(),
         resource_alarm :: boolean(),
         send_file_oct ::
             atomics:atomics_ref(), % number of bytes sent with send_file (for metrics)
         transport :: tcp | ssl,
         proxy_socket :: undefined | ranch_transport:socket(),
         correlation_id_sequence :: integer(),
         outstanding_requests :: #{integer() => #request{}},
         deliver_version :: rabbit_stream_core:command_version(),
         request_timeout :: pos_integer(),
         outstanding_requests_timer :: undefined | erlang:reference(),
         filtering_supported :: boolean(),
         %% internal sequence used for publishers
         internal_sequence = 0 :: integer(),
         token_expiry_timer = undefined :: undefined | erlang:reference()}).
-record(configuration,
        {initial_credits :: integer(),
         credits_required_for_unblocking :: integer(),
         frame_max :: integer(),
         heartbeat :: integer(),
         connection_negotiation_step_timeout :: integer()}).
