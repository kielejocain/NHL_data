# NHL_data

This repo is a Python project that is attempting to scrape all individual season data from NHL.com and prepare it to be copied into a SQL database.

## Setup

### Python environment
- Create a virtual environment (if desired)
- `pip install -r requirements.txt`

### PostgreSQL

Follow the instructions [here](https://www.postgresql.org/download/). You'll need a local server running to load data into,
or perhaps something hosted with connection information to configure later.

I installed postgres via apt, then created a user and database:

    sudo apt install postgresql
    su -u postgres createuser -D -A -P myusername
    su -u postgres createdb -O myusername mydatabase

## Data Collection

The script should just run (within the virtual environment) via

    python scrape.py

We can then put that data into our database with the following set of commands:

    psql -U myusername -d mydatabase -f create_db.sql
    psql -U myusername -d mydatabase -c "\copy skaters FROM './data/skater_bios.csv' delimiter ',' csv header;"
    psql -U myusername -d mydatabase -c "\copy skaterstats FROM './data/skater_data.csv' delimiter ',' csv header;"

We need to put the `\copy` commands outside the script as postgres will not allow them from inside a script easily.
It may be possible, but this seemed easier than setting up authentication.