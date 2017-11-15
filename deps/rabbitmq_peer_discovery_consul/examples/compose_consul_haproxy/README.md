This example shows how to create a dynamic RabbitMQ cluster using:

1. [Docker compose](https://docs.docker.com/compose/)

2. [Consul](https://www.consul.io) 

3. [HA proxy](https://github.com/docker/dockercloud-haproxy)

---

How to run:

```
docker-compose up
```

How to scale:

```
docker-compose scale rabbit=3
```


---

Check running status:

- Consul Management: http://localhost:8500/ui/ 
- RabbitMQ Management: http://localhost:15672/#/
