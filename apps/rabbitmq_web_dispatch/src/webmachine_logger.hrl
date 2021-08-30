-record(wm_log_data,
        {resource_module :: atom(),
         start_time :: tuple(),
         method :: atom(),
         headers,
         peer,
         path :: string(),
         version,
         response_code,
         response_length,
         end_time :: tuple(),
         finish_time :: tuple(),
         notes}).
-type wm_log_data() :: #wm_log_data{}.

-define(EVENT_LOGGER, webmachine_log_event).
