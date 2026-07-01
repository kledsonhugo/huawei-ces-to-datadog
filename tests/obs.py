import os
from obs import ObsClient

# =========================
# Função Upload de objeto
# =========================
def upload_object(obsClient, bucket_name):

    object_key = "test-000.json"
    content = '{"test": 0}'

    resp = obsClient.putContent(
        bucket_name,
        object_key,
        content
    )

    if resp.status >= 300:
        raise Exception(f"Erro ao fazer upload: {resp.errorMessage}")

# =========================
# Função Listar objetos
# =========================
def list_objects(obsClient, bucket_name):

    resp = obsClient.listObjects(bucket_name)

    if resp.status >= 300:
        raise Exception(f"Erro ao listar objetos: {resp.errorMessage}")

    contents = resp.body.contents if resp.body and resp.body.contents else []

    print(f"Total de objetos: {len(contents)}")

    for obj in contents:
        print(f" - {obj.key} | Size: {obj.size} bytes | LastModified: {obj.lastModified}")

    return contents

# =========================
# Função ler conteúdo de objeto
# =========================
def read_object_content(obsClient, bucket_name, object_key, max_bytes=500):

    print(f"\nConteúdo do objeto: {object_key} ")

    resp = obsClient.getObject(bucket_name, object_key)

    if resp.status >= 300:
        print(f"Erro ao ler objeto {object_key}: {resp.errorMessage}")
        return

    try:
        data = resp.body.response.read(max_bytes)

        try:
            content = data.decode("utf-8")
        except:
            content = str(data)

        print(f"{content}")

    except Exception as e:
        print(f"Erro ao processar conteúdo de {object_key}: {str(e)}")

    finally:
        resp.body.response.close()

# =========================
# Função principal (Handler)
# =========================
def handler(event, context):

    server = os.getenv('OBS_ENDPOINT')
    source_bucket = os.getenv('OBS_BUCKET')

    access_key = context.getSecurityAccessKey()
    secret_key = context.getSecuritySecretKey()
    security_token = context.getSecurityToken()

    obsClient = ObsClient(
        access_key_id=access_key,
        secret_access_key=secret_key,
        security_token=security_token,
        server=server
    )

    try:

        # Upload de objeto
        upload_object(obsClient, source_bucket)

        # Lista objetos
        objects = list_objects(obsClient, source_bucket)

        # Lê conteúdo de cada objeto
        for obj in objects:
            read_object_content(obsClient, source_bucket, obj.key)

        return {
            "statusCode": 200,
            "total_objects": len(objects)
        }

    except Exception as e:

        print("Erro:", str(e))

        return {
            "statusCode": 500,
            "error": str(e)
        }

    finally:

        obsClient.close()