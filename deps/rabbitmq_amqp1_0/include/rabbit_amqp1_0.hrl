
-ifdef(debug).
-define(DEBUG0(F), io:format(F, [])).
-define(DEBUG(F, A), io:format(F, A)).
-else.
-define(DEBUG0(F), ok).
-define(DEBUG(F, A), ok).
-endif.

%% General consts

-define(FRAME_1_0_MIN_SIZE, 4096).

%% Encoding categories

-define(DESCRIBED, 0:8).
-define(DESCRIBED_BIN, <<?DESCRIBED>>).
-define(FIXED_0, 4).
-define(FIXED_1, 5).
-define(FIXED_2, 6).
-define(FIXED_4, 7).
-define(FIXED_8, 8).
-define(FIXED_16, 9).
-define(VAR_1, 10).
-define(VAR_4, 11).
-define(COMPOUND_1, 12).
-define(COMPOUND_4, 13).
-define(ARRAY_1, 14).
-define(ARRAY_4, 15).

-record('v1_0.open',
        {options,
         container_id,
         hostname,
         max_frame_size,
         channel_max,
         heartbeat_interval,
         outgoing_locales,
         incoming_locales,
         offered_capabilities,
         desired_capabilities,
         properties}).

-record('v1_0.begin',
        {options,
         remote_channel,
         offered_capabilities,
         desired_capabilities,
         properties}).

-record('v1_0.attach',
        {options,
         name,
         handle,
         flow_state,
         role,
         local,
         remote,
         durable,
         expiry_policy,
         timeout,
         unsettled,
         transfer_unit,
         max_message_size,
         error_mode,
         properties}).

-record('v1_0.flow',
        {options,
         handle,
         flow_state,
         echo}).

-record('v1_0.transfer',
        {options,
         handle,
         flow_state,
         delivery_tag,
         transfer_id,
         settled,
         state,
         resume,
         more,
         aborted,
         batchable,
         fragments}).

-record('v1_0.disposition',
        {options,
         role,
         batchable,
         extents}).

-record('v1_0.detach',
         {options,
          handle,
          local,
          remote,
          error}).

-record('v1_0.end',
         {options,
          error}).

-record('v1_0.close',
        {options,
         error}).

%% Other types

-record('v1_0.linkage',
        {source,
         target}).

-record('v1_0.flow_state',
        {unsettled_lwm,
         session_credit,
         transfer_count,
         link_credit,
         available,
         drain}).

-record('v1_0.source',
        {address,
         dynamic,
         distribution_mode,
         filter,
         default_outcome,
         outcomes,
         capabilities}).

-record('v1_0.target',
        {address,
         dynamic,
         capabilities}).

-record('v1_0.fragment',
        {first,
         last,
         format_code,
         fragment_offset,
         payload}).

-record('v1_0.header',
        {durable,
         priority,
         transmit_time,
         ttl,
         former_acquirers,
         delivery_failures,
         format_code,
         message_attrs,
         delivery_attrs}).

-record('v1_0.properties',
        {message_id,
         user_id,
         to,
         subject,
         reply_to,
         correlation_id,
         content_length,
         content_type}).

-record('v1_0.footer',
        {message_attrs,
         delivery_attrs}).

-record('v1_0.transfer_state',
        {bytes_transferred,
         outcome,
         txn_id}).

-record('v1_0.accepted',
        {}).

-record('v1_0.rejected',
        {error}).
