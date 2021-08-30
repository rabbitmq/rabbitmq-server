# Spring Boot Kotlin Backend

Run the backend with:

    ./mvnw spring-boot:run

Don't forget to configure the broker to use the backend!

Use [`http`](https://httpie.org/doc) to test your program:

    $ http -f POST http://localhost:8080/auth/user username=foo password=bar -v
    POST /auth/user HTTP/1.1
    Accept: */*
    Accept-Encoding: gzip, deflate
    Connection: keep-alive
    Content-Length: 25
    Content-Type: application/x-www-form-urlencoded; charset=utf-8
    Host: localhost:8080
    User-Agent: HTTPie/0.9.2

    username=foo&password=bar

    HTTP/1.1 200
    Cache-Control: no-cache, no-store, max-age=0, must-revalidate
    Content-Length: 5
    Content-Type: text/plain;charset=UTF-8
    Date: Thu, 01 Nov 2018 21:16:06 GMT
    Expires: 0
    Pragma: no-cache
    X-Content-Type-Options: nosniff
    X-Frame-Options: DENY
    X-XSS-Protection: 1; mode=block

    allow
