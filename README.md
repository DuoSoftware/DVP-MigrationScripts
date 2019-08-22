# DVP-MigrationScripts

sudo pip install virtualenv  (install virtual virtualenv library)

virtualenv -p /usr/bin/python3.6 venv (create a virtual env)

source venv/bin/activate (activate the env)

pip install requirement.txt (install dependencies)

PgMigrator.py is to migrate postgres tables and mongoMigrator is to migrate mongo collections. uncomment the necessary functions to execute

PostgresDiff and mongoDiff can be used to compare and produce difference of two databases.



------------------------
sudo apt-get install libpq-dev python-dev
pip install psycopg2

