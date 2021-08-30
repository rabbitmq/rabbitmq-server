-include_lib("rabbit/include/rabbit_global_counters.hrl").

-define(TABLE_CONSUMER, rabbit_stream_consumer_created).
-define(TABLE_PUBLISHER, rabbit_stream_publisher_created).

-define(STREAM_DOES_NOT_EXIST, ?NUM_PROTOCOL_COUNTERS + 1).
-define(SUBSCRIPTION_ID_ALREADY_EXISTS, ?NUM_PROTOCOL_COUNTERS + 2).
-define(SUBSCRIPTION_ID_DOES_NOT_EXIST, ?NUM_PROTOCOL_COUNTERS + 3).
-define(STREAM_ALREADY_EXISTS, ?NUM_PROTOCOL_COUNTERS + 4).
-define(STREAM_NOT_AVAILABLE, ?NUM_PROTOCOL_COUNTERS + 5).
-define(SASL_MECHANISM_NOT_SUPPORTED, ?NUM_PROTOCOL_COUNTERS + 6).
-define(AUTHENTICATION_FAILURE, ?NUM_PROTOCOL_COUNTERS + 7).
-define(SASL_ERROR, ?NUM_PROTOCOL_COUNTERS + 8).
-define(SASL_CHALLENGE, ?NUM_PROTOCOL_COUNTERS + 9).
-define(SASL_AUTHENTICATION_FAILURE_LOOPBACK, ?NUM_PROTOCOL_COUNTERS + 10).
-define(VHOST_ACCESS_FAILURE, ?NUM_PROTOCOL_COUNTERS + 11).
-define(UNKNOWN_FRAME, ?NUM_PROTOCOL_COUNTERS + 12).
-define(FRAME_TOO_LARGE, ?NUM_PROTOCOL_COUNTERS + 13).
-define(INTERNAL_ERROR, ?NUM_PROTOCOL_COUNTERS + 14).
-define(ACCESS_REFUSED, ?NUM_PROTOCOL_COUNTERS + 15).
-define(PRECONDITION_FAILED, ?NUM_PROTOCOL_COUNTERS + 16).
-define(PUBLISHER_DOES_NOT_EXIST, ?NUM_PROTOCOL_COUNTERS + 17).

-define(PROTOCOL_COUNTERS,
        [
         {
          stream_error_stream_does_not_exist_total, ?STREAM_DOES_NOT_EXIST, counter,
          "Total number of commands rejected with stream does not exist error"
         },
         {
          stream_error_subscription_id_already_exists_total, ?SUBSCRIPTION_ID_ALREADY_EXISTS, counter,
          "Total number of commands failed with subscription id already exists"
         },
         {
          stream_error_subscription_id_does_not_exist_total, ?SUBSCRIPTION_ID_DOES_NOT_EXIST, counter,
          "Total number of commands failed with subscription id does not exist"
         },
         {
          stream_error_stream_already_exists_total, ?STREAM_ALREADY_EXISTS, counter,
          "Total number of commands failed with stream already exists"
         },
         {
          stream_error_stream_not_available_total, ?STREAM_NOT_AVAILABLE, counter,
          "Total number of commands failed with stream not available"
         },
         {
          stream_error_sasl_mechanism_not_supported_total, ?SASL_MECHANISM_NOT_SUPPORTED, counter,
          "Total number of commands failed with sasl mechanism not supported"
         },
         {
          stream_error_authentication_failure_total, ?AUTHENTICATION_FAILURE, counter,
          "Total number of commands failed with authentication failure"
         },
         {
          stream_error_sasl_error_total, ?SASL_ERROR, counter,
          "Total number of commands failed with sasl error"
         },
         {
          stream_error_sasl_challenge_total, ?SASL_CHALLENGE, counter,
          "Total number of commands failed with sasl challenge"
         },
         {
          stream_error_sasl_authentication_failure_loopback_total, ?SASL_AUTHENTICATION_FAILURE_LOOPBACK, counter,
          "Total number of commands failed with sasl authentication failure loopback"
         },
         {
          stream_error_vhost_access_failure_total, ?VHOST_ACCESS_FAILURE, counter,
          "Total number of commands failed with vhost access failure"
         },
         {
          stream_error_unknown_frame_total, ?UNKNOWN_FRAME, counter,
          "Total number of commands failed with unknown frame"
         },
         {
          stream_error_frame_too_large_total, ?FRAME_TOO_LARGE, counter,
          "Total number of commands failed with frame too large"
         },
         {
          stream_error_internal_error_total, ?INTERNAL_ERROR, counter,
          "Total number of commands failed with internal error"
         },
         {
          stream_error_access_refused_total, ?ACCESS_REFUSED, counter,
          "Total number of commands failed with access refused"
         },
         {
          stream_error_precondition_failed_total, ?PRECONDITION_FAILED, counter,
          "Total number of commands failed with precondition failed"
         },
         {
          stream_error_publisher_does_not_exist_total, ?PUBLISHER_DOES_NOT_EXIST, counter,
          "Total number of commands failed with publisher does not exist"
         }
        ]).
