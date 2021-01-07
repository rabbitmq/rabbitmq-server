_LAGER_EXTRA_SINKS = [
    "rabbit_log",
    "rabbit_log_channel",
    "rabbit_log_connection",
    "rabbit_log_feature_flags",
    "rabbit_log_federation",
    "rabbit_log_ldap",
    "rabbit_log_mirroring",
    "rabbit_log_osiris",
    "rabbit_log_prelaunch",
    "rabbit_log_queue",
    "rabbit_log_ra",
    "rabbit_log_shovel",
    "rabbit_log_upgrade",
]

RABBITMQ_ERLC_OPTS = [
    "+{parse_transform,lager_transform}",
    "+{lager_extra_sinks,[" + ",".join(_LAGER_EXTRA_SINKS) + "]}",
]

APP_VERSION = "3.9.0"
