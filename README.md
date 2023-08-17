Пайплайн по загрузке данных из открытого api в Postgres.  
В файлы config.ini необходимо указать креды для поключения к СУБД.  
Cron выражение:
```console
0 */12 * * * cd /test_de && /usr/local/bin/python3.9  /test_de/canabis_connector.py.py
```
