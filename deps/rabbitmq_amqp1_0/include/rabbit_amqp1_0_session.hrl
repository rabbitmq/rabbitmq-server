%% TODO remove this once we have fully abstracted session bookkeeping
%% into rabbit_amqp1_0_session
-record(session, {channel_num, %% we just use the incoming (AMQP 1.0) channel number
                  next_transfer_number = 0, % next outgoing id
                  max_outgoing_id, % based on the remote incoming window size
                  next_incoming_id, % just to keep a check
                  next_publish_id, %% the 0-9-1-side counter for confirms
                  %% we make incoming and outgoing session buffers the
                  %% same size
                  window_size,
                  ack_counter = 0,
                  incoming_unsettled_map,
                  outgoing_unsettled_map }).

-record(outgoing_transfer, {delivery_tag, expected_outcome}).
