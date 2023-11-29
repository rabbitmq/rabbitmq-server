These shell scripts are not meant to be executed directly. Instead they are
imported by bin/suite_template script.

Each component required to run a test, for instance, uaa or keycloak, has
its own script with its corresponding two functions:
 init_<ComponentName>()
 start_<ComponentName>()

 
