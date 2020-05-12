alias py=python3.7
alias pip="python3.7 -m pip"
service postgresql start
su - postgres -c "createdb merge_table && psql -c \"CREATE EXTENSION postgis;\""
su - postgres -c "psql -c \"CREATE USER norton with encrypted password 'Norton00'\""
su - postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE merge_table TO norton;\""
su - postgres -c "psql -c \"ALTER USER norton WITH SUPERUSER;\""
cd gis2idx/datamerger && python3.7 manage.py migrate && cd ../../
python3.7 gis2idx $@