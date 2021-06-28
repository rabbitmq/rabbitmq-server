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
          stream_error_stream_does_not_exist, ?STREAM_DOES_NOT_EXIST, counter,
          ""
         },
         {
          stream_error_subscription_id_already_exists, ?SUBSCRIPTION_ID_ALREADY_EXISTS, counter,
          ""
         },
         {
          stream_error_subscription_id_does_not_exist, ?SUBSCRIPTION_ID_DOES_NOT_EXIST, counter,
          ""
         },
         {
          stream_error_stream_already_exists, ?STREAM_ALREADY_EXISTS, counter,
          ""
         },
         {
          stream_error_stream_not_available, ?STREAM_NOT_AVAILABLE, counter,
          ""
         },
         {
          stream_error_sasl_mechanism_not_supported, ?SASL_MECHANISM_NOT_SUPPORTED, counter,
          ""
         },
         {
          stream_error_authentication_failure, ?AUTHENTICATION_FAILURE, counter,
          ""
         },
         {
          stream_error_sasl_error, ?SASL_ERROR, counter,
          ""
         },
         {
          stream_error_sasl_challenge, ?SASL_CHALLENGE, counter,
          ""
         },
         {
          stream_error_sasl_authentication_failure_loopback, ?SASL_AUTHENTICATION_FAILURE_LOOPBACK, counter,
          ""
         },
         {
          stream_error_vhost_access_failure, ?VHOST_ACCESS_FAILURE, counter,
          ""
         },
         {
          stream_error_unknown_frame, ?UNKNOWN_FRAME, counter,
          ""
         },
         {
          stream_error_frame_too_large, ?FRAME_TOO_LARGE, counter,
          ""
         },
         {
          stream_error_internal_error, ?INTERNAL_ERROR, counter,
          ""
         },
         {
          stream_error_access_refused, ?ACCESS_REFUSED, counter,
          ""
         },
         {
          stream_error_precondition_failed, ?PRECONDITION_FAILED, counter,
          ""
         },
         {
          stream_error_publisher_does_not_exist, ?PUBLISHER_DOES_NOT_EXIST, counter,
          ""
         }
        ]).
