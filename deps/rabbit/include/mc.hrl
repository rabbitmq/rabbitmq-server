%% good enough for most use cases
-define(IS_MC(Msg), element(1, Msg) == mc andalso tuple_size(Msg) == 5).

%% "Short strings can carry up to 255 octets of UTF-8 data, but
%% may not contain binary zero octets." [AMQP 0.9.1 $4.2.5.3]
-define(IS_SHORTSTR_LEN(B), byte_size(B) < 256).

%% We keep the following atom annotation keys short as they are stored per message on disk.
-define(ANN_EXCHANGE, x).
-define(ANN_ROUTING_KEYS, rk).
-define(ANN_TIMESTAMP, ts).
-define(ANN_RECEIVED_AT_TIMESTAMP, rts).
-define(ANN_DURABLE, d).
-define(ANN_PRIORITY, p).

-define(FF_MC_DEATHS_V2, message_containers_deaths_v2).
-define(MC_ENV,
        case rabbit_feature_flags:is_enabled(?FF_MC_DEATHS_V2) of
            true -> #{};
            false -> #{?FF_MC_DEATHS_V2 => false}
        end).

-type death_key() :: {SourceQueue :: rabbit_misc:resource_name(), rabbit_dead_letter:reason()}.
-type death_anns() :: #{%% timestamp of the first time this message
                        %% was dead lettered from this queue for this reason
                        first_time := pos_integer(),
                        %% timestamp of the last time this message
                        %% was dead lettered from this queue for this reason
                        last_time := pos_integer(),
                        ttl => OriginalTtlHeader :: non_neg_integer()}.

-record(death, {exchange :: OriginalExchange :: rabbit_misc:resource_name(),
                routing_keys :: OriginalRoutingKeys :: [rabbit_types:routing_key(),...],
                %% how many times this message was dead lettered from this queue for this reason
                count :: pos_integer(),
                anns :: death_anns()}).

-record(deaths, {first :: death_key(), % redundant to mc annotations x-first-death-*
                 last :: death_key(), % redundant to mc annotations x-last-death-*
                 records :: #{death_key() := #death{}}
                }).
