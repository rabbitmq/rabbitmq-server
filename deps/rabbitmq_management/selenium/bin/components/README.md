These shell scripts are not meant to be executed directly. Instead they are
imported by bin/suite_template script.

Each component required to run a test, for instance, uaa or keycloak, has
its own script with its corresponding function:
 start_<ComponentName>()

Although there is a convention to have two functions, the entrypoint `start_<ComponentName>()`,
and `init_<ComponentName>()`. The latter is called by the former to initialize
environment variables.
There is a third entry point for third party components (i.e. all except rabbitmq), the `ensure_<ComponentName>()`.
This function starts the component if it is not running. Whereas `start_<ComponentName>()` kills the
component's container if it is running and start it again.
