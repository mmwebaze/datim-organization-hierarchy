import psycopg2, json, sys, base64, urllib.request, os, argparse
import sqlparse, csv
from collections import namedtuple


def cmpT(t1, t2):
  return sorted(t1) == sorted(t2)


def _json_object_hook(d):
    return namedtuple('X', d.keys())(*d.values())


def json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)


def validate_secrets(secrets):
    secrets_fields=('dhis', 'database')
    secrets_dhis = ('username', 'password', 'baseurl')
    secrets_database = ('username', 'host', 'password', 'port', 'dbname')
    return cmpT(secrets._fields,secrets_fields) & cmpT(secrets.dhis._fields,secrets_dhis) & \
        cmpT(secrets.database._fields,secrets_database)


def load_secrets(secrets_location):
    if os.path.isfile(secrets_location) and os.access(secrets_location, os.R_OK):
        json_data = open(secrets_location).read()
    else:
        ('Secrets file not found or not accessible')
    try:
        config = json2obj(json_data)
    except ValueError as e:
        sys.exit('Secrets files looks to be mangled.' + e)
    else:
        valid = validate_secrets(config)
        if valid:
            return config
        else:
            sys.exit('Secrets files is not valid.')


def get_db_config_string(secrets):
    return "dbname=" + secrets.database.dbname + \
                    " host=" + secrets.database.host + \
                    " user=" + secrets.database.username + \
                    " password=" + secrets.database.password + \
                    " port=" + str(secrets.database.port)


def get_database_connection(secrets):
    try:
        conn = psycopg2.connect(get_db_config_string(secrets))
    except psycopg2.OperationalError as e:
        sys.exit('Could not get a database connection' + e)
    #assert isinstance(conn, psycopg2.extensions.connection)
    return conn


def get_sql_statements(sql_location, siteId, newParentId):

    if os.path.isfile(sql_location) and os.access(sql_location, os.R_OK):
        file = open(sql_location, 'r')
        content = file.read()
        sqlUpdate = (content %(newParentId, siteId))
        print(sqlUpdate)
        sql = filter(None,sqlparse.split(sqlUpdate))
        #assert isinstance(sql, list)
        return sql
    else:
        sys.exit('SQL file not found or not accessible')


def execute_sql_statements(secrets,sql):
    conn = get_database_connection(secrets)
    cur = conn.cursor()
    try:
        for command in sql:
            cur.execute(command)
            conn.commit()
            print(cur.rowcount)
    except psycopg2.Error as e:
        print(e)
        conn.rollback()
        return False
    else:
        return True
    finally:
        cur.close()
        conn.close()


def read_csv_file(relocated_sites):
    siteIds = []
    f = open(relocated_sites)
    for row in f:
        siteIds.append(tuple(row.strip().split(',')))

    return siteIds


def clear_hibernate_cache(secrets):
    url = secrets.dhis.baseurl + "/dhis-web-maintenance-dataadmin/clearCache.action"
    strlog = ('%s:%s' % (secrets.dhis.username, secrets.dhis.password))
    print(strlog)
    base64string = base64.b64encode(bytes(strlog, 'ascii'))
    print(base64string)

    request = urllib.request.Request(url)
    request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))
    try:
        result = urllib.request.urlopen(request)
    except urllib.request.URLError as e:
        print('you got an error with the code %s' %(e))
    else:
        print('You got a ' + str(result.getcode()))


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-s',"--secrets", help='Location of secrets file')
    parser.add_argument('-f',"--sql" ,help='Location of SQL script to be executed')
    parser.add_argument('-q', "--csv", help='Location of csv files with sites')
    args = parser.parse_args()
    secrets= load_secrets(args.secrets)
    #sql = get_sql_statements(args.sql)
    sqlFile = args.sql
    csv_file = read_csv_file(args.csv)

    for row in csv_file:
        sql = get_sql_statements(sqlFile, row[1], row[3])
        success = execute_sql_statements(secrets,sql)
        if success:
            clear_hibernate_cache(secrets)

if __name__ == "__main__":
   main(sys.argv[1:])
