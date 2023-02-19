import hvac

client = hvac.Client(
    url='http://127.0.0.1:8200',
    token='dev-only-token',
)

# Writing a secret
create_response = client.secrets.kv.v2.create_or_update_secret(
    path='production_postgres',
    secret=dict(
        host = 'HOST',
        port = 'PORT',
        database = 'DATABASE',
        username = 'USERNAME',
        password = 'PASSWORD'),
)

print('production_postgres secret written successfully.')
