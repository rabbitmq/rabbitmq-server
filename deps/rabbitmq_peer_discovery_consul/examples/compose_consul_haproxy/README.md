Dynamic RabbitMQ cluster using:

1. [Docker compose](https://docs.docker.com/compose/)

2. [Consul](https://www.consul.io) 

3. [HA proxy](https://github.com/docker/dockercloud-haproxy)

4. [rabbitmq-peer-discovery-consul plugin](https://github.com/rabbitmq/rabbitmq-peer-discovery-consul)

---

How to run:

```
docker-compose up
```

How to scale:

```
docker-compose up --scale rabbit=2 -d
```


---

Check running status:

- Consul Management: http://localhost:8500/ui/ 
- RabbitMQ Management: http://localhost:15672/#/
