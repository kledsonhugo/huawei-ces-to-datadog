import os
import json
import subprocess


# =========================
# Configuração fixa para debug
# =========================
DATADOG_URL = os.getenv("DATADOG_API_URL", "https://api.datadoghq.com/api/v2/series")
DATADOG_API_KEY = os.getenv("DATADOG_API_KEY", "<YOUR_DATADOG_API_KEY>")

PAYLOAD = {
    "series": [
        {
            "metric": "ecs.network_outgoing_bytes_aggregate_rate",
            "type": 2,
            "points": [{"timestamp": 1779980400, "value": 178.0147928994083}],
            "resources": [{"name": "<INSTANCE_ID>", "type": "instance_id"}],
            "tags": ["provider:huawei", "env:dev"]
        }
    ]
}


# =========================
# Debug com curl
# =========================
def debug_curl_datadog():
    """
    Executa chamada curl fixa para debug de conectividade com o DataDog.

    -k desabilita verificação SSL (proxy pode usar cert auto-assinado)
    -s modo silencioso
    -w captura HTTP status code
    """

    cmd = [
        "curl", "-k", "-s", "-w", "\nHTTP_STATUS:%{http_code}",
        "-X", "POST", DATADOG_URL,
        "-H", "Accept: application/json",
        "-H", "Content-Type: application/json",
        "-H", f"DD-API-KEY: {DATADOG_API_KEY}",
        "-d", json.dumps(PAYLOAD)
    ]

    print(f"[CURL] Executando curl -k -X POST {DATADOG_URL}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15
        )
        print(f"[CURL] stdout: {result.stdout}")
        if result.stderr:
            print(f"[CURL] stderr: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("[CURL] TIMEOUT após 15s — function não alcança o proxy")
    except FileNotFoundError:
        print("[CURL] curl não encontrado no ambiente da function")
    except Exception as e:
        print(f"[CURL] erro: {str(e)}")


# =========================
# Debug com requests (comparação)
# =========================
def debug_requests_datadog():
    """
    Executa chamada com requests.post() para comparar com o resultado do curl.

    Testa duas variações:
    1. verify=True
    2. verify=False (equivalente ao -k do curl)
    """

    import requests
    import urllib3

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DD-API-KEY": DATADOG_API_KEY
    }

    # Com verify=False (equivalente ao -k do curl)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print(f"[REQUESTS] POST {DATADOG_URL} (verify=False)")

    try:
        response = requests.post(
            DATADOG_URL,
            headers=headers,
            json=PAYLOAD,
            timeout=15,
            verify=False
        )
        print(f"[REQUESTS] Status: {response.status_code}")
        print(f"[REQUESTS] Body: {response.text}")
    except Exception as e:
        print(f"[REQUESTS] erro: {str(e)}")


# =========================
# Handler (para execução na function)
# =========================
def handler(event, context):

    try:

        # Debug com curl
        print("=" * 20)
        print("DEBUG CURL")
        print("=" * 20)
        debug_curl_datadog()

        # Debug com requests
        print("=" * 20)
        print("DEBUG REQUESTS")
        print("=" * 20)
        debug_requests_datadog()

        return {
            "statusCode": 200,
            "message": "Debug executado. Verificar logs da function para detalhes."
        }

    except Exception as e:
        print("Erro:", str(e))

        return {
            "statusCode": 500,
            "error": str(e)
        }
