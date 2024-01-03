These shell scripts are not meant to be executed directly. Instead they are
imported by bin/suite_template script.

Each component required to run a test, for instance, uaa or keycloak, has
its own script with its corresponding function:
 start_<ComponentName>()

Although there is a convention to have two functions, the entrypoint `start_<ComponentName>()`,
and `init_<ComponentName>()`. The latter is called by the former to initialize
environment variables.
