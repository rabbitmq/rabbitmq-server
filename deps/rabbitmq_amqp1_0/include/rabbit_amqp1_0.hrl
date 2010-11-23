
%% General consts

-define(FRAME_MIN_SIZE, 4096).

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
        {options = null,
         container_id = null,
         hostname = null,
         max_frame_size = null,
         channel_max = null,
         heartbeat_interval = null,
         outgoing_locales = null,
         incoming_locales = null,
         offered_capabilities = null,
         desired_capabilities = null,
         properties = null}).

-record('v1_0.begin',
        {options = null,
         remote_channel = null,
         offered_capabilities = null,
         desired_capabilities = null,
         properties = null}).

-record('v1_0.attach',
        {options = null,
         name = null,
         handle = null,
         flow_state = null,
         role = null,
         local = null,
         remote = null,
         durable = null,
         expiry_policy = null,
         timeout = null,
         unsettled = null,
         transfer_unit = null,
         max_message_size = null,
         error_mode = null,
         properties = null}).

-record('v1_0.flow',
        {options = null,
         handle = null,
         flow_state = null,
         echo = null}).

-record('v1_0.transfer',
        {options = null,
         handle = null,
         flow_state = null,
         delivery_tag = null,
         transfer_id = null,
         settled = null,
         state = null,
         resume = null,
         more = null,
         aborted = null,
         batchable = null,
         fragments = null}).

-record('v1_0.disposition',
        {options = null,
         role = null,
         batchable = null,
         extents = null}).

-record('v1_0.detach',
         {options = null,
          handle = null,
          local = null,
          remote = null,
          error = null}).

-record('v1_0.end',
         {options = null,
          error = null}).

-record('v1_0.close',
        {options = null,
         error = null}).

%% Other types

-record('v1_0.linkage',
        {source = null,
         target = null}).

-record('v1_0.flow_state',
        {unsettled_lwm = null,
         session_credit = null,
         transfer_count = null,
         link_credit = null,
         available = null,
         drain = null}).
