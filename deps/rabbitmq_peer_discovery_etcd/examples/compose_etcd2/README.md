This example shows how to create a dynamic RabbitMQ cluster using:

1. [Docker compose](https://docs.docker.com/compose/)

2. [Etcd](https://coreos.com/etcd/) 

3. [HA proxy](https://github.com/docker/dockercloud-haproxy)

---

How to run:

```
docker-compose up
```

How to scale:

```
docker-compose up --scale rabbit=3 -d
```


---

Check running status:

- RabbitMQ Management: http://localhost:15672/#/