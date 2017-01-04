-module(amqp10_client_types).

-include("rabbit_amqp1_0_framing.hrl").


-type amqp10_frame() :: #'v1_0.open'{} | #'v1_0.begin'{} | #'v1_0.attach'{} |
                        #'v1_0.flow'{} | #'v1_0.transfer'{} |
                        #'v1_0.disposition'{} | #'v1_0.detach'{} |
                        #'v1_0.end'{} | #'v1_0.close'{}.

-type channel() :: non_neg_integer().

-type source() :: #'v1_0.source'{}.
-type target() :: #'v1_0.target'{}.

-export_type([amqp10_frame/0, channel/0,
              source/0, target/0
             ]).
