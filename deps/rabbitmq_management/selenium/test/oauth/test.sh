 curl 'http://localhost:8080/oauth/token' -i -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -H 'Accept: application/json' \
    -d 'scope=rabbitmq&client_id=rabbit_client_code&client_secret=rabbit_client_code&grant_type=client_credentials&token_format=jwt&response_type=token'

