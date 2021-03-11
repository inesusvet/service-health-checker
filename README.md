# Service health checker

This application plays two roles:

- **PUB** Performs health checks on a _target_ web service, collects metrics and sends them to Kafka broker;
- **SUB** Reads Kafka topic and writes data to PostgreSQL table.


## Design

**KISS** while both parts of this application might be implemented in async manner, I decided to refrain from this and keep things straightforward.  This means that currently the scaling model will require more "workers" on both sides of Kafka.


## Kickstart

> Use docker, Luke!

This will start three docker containers locally and create a `venv` directory to hold python dependencies.

```bash
docker-compose up -d
```

Setup a new python virtual env with dependencies.

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

Now we can run two parts of this program (in different shell sessions from this directory:

Reach the site twice a minute and send messages to Kafka.

```bash
export KAFKA_BROKER=localhost
export KAFKA_TOPIC=test
export TARGET="https://en.wikipedia.org/wiki/Special:Random"

./venv/bin/python metrics_flow.py pub --pattern foobar --delay 30
```

Wait for incoming messages from Kafka and relay them to database.

```bash
export KAFKA_BROKER=localhost
export KAFKA_TOPIC=test
export POSTGRESQL_DSN="host=localhost user=postgres password=postgres"

./venv/bin/python metrics_flow.py sub
```

See the results in the database.

```
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "select * from test;" postgres
```

## Parameters

This application uses `argparse` module which will show help page to you.

```bash
./venv/bin/python metrics_flow.py --help
```

SSL certificate could be used to authenticate against Kafka. See the `--ssl-*` arguments.

Most of the arguments are backed up by environment variables.


## FAQ

> Why not use docker-compose for all the pieces?

I have spent some time in debugging of connection timeouts in communication between Kafka broker and its clients.  I saw name resolution issues as local python application wasn't able to send data to the broker while using container's name as the hostname.
That issue was fixed by configuring `KAFKA_CFG_ADVERTISED_LISTENERS` option to point clients to `localhost` where the docker container is exposing the kafka broker's port.

Overall this application might also be containerised and started in a new docker network with all the supporting services after some changes in `docker-compose`.  But _running programs by hand_ brings much more fun (after some pain).


## Post-scriptum

I want to dedicate this piece of work to my grandmother Nina who passed away this week.  In my heart I keep warm memories of her house full of relatives gathered to celebrate her birthday on 7th of January (the Orthodox Christmas). I remember a long table with delicious food and a choir of old women singing traditional songs as entertainment. RIP
