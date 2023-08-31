-type death_key() :: {Queue :: rabbit_misc:resource_name(), rabbit_dead_letter:reason()}.
-type death_anns() :: #{first_time := non_neg_integer(), %% the timestamp of the first
                        last_time := non_neg_integer(), %% the timestamp of the last
                        ttl => non_neg_integer()}.
-record(death, {
                exchange :: rabbit_misc:resource_name(),
                routing_keys = [] :: [rabbit_types:routing_key()],
                count = 0 :: non_neg_integer(),
                anns :: death_anns()
               }).

-record(deaths, {first :: death_key(),
                 last :: death_key(),
                 records = #{} :: #{death_key() := #death{}}}).


%% good enough for most use cases
-define(IS_MC(Msg), element(1, Msg) == mc andalso tuple_size(Msg) == 5).

%% "Field names MUST start with a letter, '$' or '#' and may continue with letters, '$' or '#', digits, or
%% underlines, to a maximum length of 128 characters." [AMQP 0.9.1 4.2.5.5 Field Tables]
%% Given that the valid chars are ASCII chars, 1 char is encoded as 1 byte.
-define(AMQP_LEGACY_FIELD_NAME_MAX_LEN, 128).
