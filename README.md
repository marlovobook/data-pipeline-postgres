![Alt text](images/pipeline-overview.png)
# step1 :
```bash
docker-compose build
```

# step2 :
```bash
docker-compose up -d
```

# delete container
```bash
docker-compose down -v
```

# check IP Address

```bash
docker inspect pg_container
```

Postgres (Database) [dag]>> sensors >> Transform in MinIO[dag]