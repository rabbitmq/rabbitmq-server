-record(link_pair, {session :: pid(),
                    outgoing_link :: amqp10_client:link_ref(),
                    incoming_link :: amqp10_client:link_ref()}).
-type link_pair() :: #link_pair{}.
