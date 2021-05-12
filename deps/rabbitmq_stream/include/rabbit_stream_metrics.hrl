-define(TABLE_CONSUMER, rabbit_stream_consumer_created).
-define(TABLE_PUBLISHER, rabbit_stream_publisher_created).

-define(CONSUMER_COUNTERS, [
                            {credits, ?CONSUMER_CREDITS, counter, ""},
                            {consumed, ?CONSUMER_CONSUMED, counter, ""},
                            {offset, ?CONSUMER_OFFSET, counter, ""}
                           ]).
-define(CONSUMER_CREDITS, 1).
-define(CONSUMER_CONSUMED, 2).
-define(CONSUMER_OFFSET, 3).

-define(PUBLISHER_COUNTERS, [
                             {published, ?PUBLISHER_PUBLISHED, counter, ""},
                             {confirmed, ?PUBLISHER_CONFIRMED, counter, ""},
                             {errored, ?PUBLISHER_ERRORED, counter, ""}
                            ]).
-define(PUBLISHER_PUBLISHED, 1).
-define(PUBLISHER_CONFIRMED, 2).
-define(PUBLISHER_ERRORED, 3).
