-define(RMQLOG_SUPER_DOMAIN_NAME, rabbitmq).
-define(RMQLOG_DOMAIN_GLOBAL,         [?RMQLOG_SUPER_DOMAIN_NAME]).
-define(DEFINE_RMQLOG_DOMAIN(Domain), [?RMQLOG_SUPER_DOMAIN_NAME, Domain]).

-define(RMQLOG_DOMAIN_CHAN,       ?DEFINE_RMQLOG_DOMAIN(channel)).
-define(RMQLOG_DOMAIN_CONN,       ?DEFINE_RMQLOG_DOMAIN(connection)).
-define(RMQLOG_DOMAIN_FEAT_FLAGS, ?DEFINE_RMQLOG_DOMAIN(feature_flags)).
-define(RMQLOG_DOMAIN_MIRRORING,  ?DEFINE_RMQLOG_DOMAIN(mirroring)).
-define(RMQLOG_DOMAIN_PRELAUNCH,  ?DEFINE_RMQLOG_DOMAIN(prelaunch)).
-define(RMQLOG_DOMAIN_QUEUE,      ?DEFINE_RMQLOG_DOMAIN(queue)).
-define(RMQLOG_DOMAIN_UPGRADE,    ?DEFINE_RMQLOG_DOMAIN(upgrade)).

-define(DEFAULT_LOG_LEVEL, info).
-define(FILTER_NAME, rmqlog_filter).

-define(IS_STD_H_COMPAT(Mod),
        Mod =:= logger_std_h orelse Mod =:= rabbit_logger_std_h).
-define(IS_STDDEV(DevName),
        DevName =:= standard_io orelse DevName =:= standard_error).
