-type death_key() :: {Queue :: rabbit_misc:resource_name(), rabbit_dead_letter:reason()}.
-record(death, {
                % queue :: binary(), %% queue name
                % reason :: rabbit_dead_letter:reason(), %% expired | rejected | maxlen | delivery_limit
                exchange :: rabbit_misc:resource_name(),
                routing_keys = [] :: [rabbit_types:routing_key()],
                timestamp :: non_neg_integer(), %% should be millisecnods although legacy uses seconds
                %% the number of times
                count = 0 :: non_neg_integer(),
                ttl :: undefined | non_neg_integer()
               }).

-record(deaths, {first :: death_key(),
                 last :: death_key(),
                 records = #{} :: #{death_key() := #death{}}}).


%% good enough for most use cases
-define(IS_MC(Msg), element(1, Msg) == mc andalso tuple_size(Msg) == 5).
