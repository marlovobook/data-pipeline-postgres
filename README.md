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

# Data Pipeline
![Alt text](images/CDC_query_based.png)

Ref in doing this : http://www.dbaglobe.com/2022/07/use-merge-feature-in-postgresql-15-to.html
-----

That's it!


