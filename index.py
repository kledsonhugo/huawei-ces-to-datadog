import os
import json
import sys
import logging
import traceback
from obs import ObsClient
from kafka import KafkaConsumer, TopicPartition
import requests
import urllib3

logger = logging.getLogger(__name__)


# =========================
# LOG: Filtro de correlation ID
# =========================
class _ExecutionFilter(logging.Filter):
    """
    Filter que injeta execution_id em cada LogRecord.

    Objetivo:
    - Garantir que todos os logs de uma mesma execução compartilhem o mesmo ID
    - Facilitar busca e correlação de logs no FunctionGraph
    """
    execution_id = "N/A"

    def filter(self, record):
        record.exec_id = _ExecutionFilter.execution_id
        return True


logger.addFilter(_ExecutionFilter())

# No FunctionGraph, stdout costuma ser o stream mais visível no painel de logs.
# Por isso, o handler é configurado explicitamente para sys.stdout.
if not logger.handlers:
    _handler = logging.StreamHandler(stream=sys.stdout)
    _handler.setFormatter(logging.Formatter(
        '[%(exec_id)s] %(levelname)s - %(message)s'
    ))
    logger.addHandler(_handler)

logger.propagate = False
logger.setLevel(logging.INFO)


# =========================
# UTIL: Gerar identificador único (Unix Epoch em ms)
# =========================
def get_id():
    """
    Gera um identificador único baseado no timestamp atual em milissegundos.

    Objetivo:
    - Garantir unicidade entre execuções da function
    - Utilizado como sufixo para arquivos no OBS (rastreabilidade)

    Retorno:
    - int: epoch time em milissegundos
    """
    import time
    return int(time.time() * 1000)


# =========================
# OBS: Criar Client
# =========================
def create_obs_client(context, obs_endpoint):
    """
    Cria cliente autenticado do Object Storage Service (OBS).

    Parâmetros:
    - context: contexto da function (credenciais temporárias)
    - obs_endpoint: endpoint do serviço OBS

    Retorno:
    - ObsClient autenticado
    """
    return ObsClient(
        access_key_id=context.getSecurityAccessKey(),
        secret_access_key=context.getSecuritySecretKey(),
        security_token=context.getSecurityToken(),
        server=obs_endpoint
    )


# =========================
# OBS: Obter último offset processado
# =========================
def get_previous_last_processed_offset(
    obs_client, 
    obs_bucket, 
    obs_object_last_processed_offset
):
    """
    Recupera do OBS o último offset Kafka já processado.

    Comportamento:
    - Se o objeto não existir, cria com valor inicial 0
    - Se existir, retorna valor armazenado

    Objetivo:
    - Garantir processamento incremental (evitar reprocessamento)

    Retorno:
    - int: último offset processado
    """

    resp = obs_client.getObject(
        obs_bucket, 
        obs_object_last_processed_offset, 
        loadStreamInMemory=True
    )

    if resp.status == 404:
        logger.info("offset object não existe. Criando ...")

        initial_value = 0
        payload = json.dumps({"previous_last_processed_offset": initial_value})
        create_resp = obs_client.putContent(
            obs_bucket, 
            obs_object_last_processed_offset, 
            payload
        )

        if create_resp.status >= 300:
            raise Exception(f"Erro ao criar offset object: {create_resp.errorMessage}")

        return initial_value

    if resp.status >= 300:
        raise Exception(f"Erro ao ler offset object: {resp.errorMessage}")

    content = resp.body.buffer.decode("utf-8")
    data = json.loads(content)

    return data.get("previous_last_processed_offset", 0)


# =========================
# OBS: Atualizar último offset processado
# =========================
def update_last_processed_offset(
    obs_client, 
    obs_bucket, 
    obs_object_last_processed_offset, 
    current_last_processed_offset
):
    """
    Atualiza no OBS o último offset Kafka processado com sucesso.

    Importante:
    - Deve ser chamado apenas após persistência bem-sucedida dos dados
    - Garante continuidade correta no próximo processamento
    """

    payload = json.dumps({
        "previous_last_processed_offset": current_last_processed_offset
    })

    resp = obs_client.putContent(obs_bucket, obs_object_last_processed_offset, payload)

    if resp.status >= 300:
        raise Exception(f"Erro ao atualizar offset object: {resp.errorMessage}")


# =========================
# OBS: Salvar mensagens originais
# =========================
def save_original_messages(obs_client, obs_bucket, messages, id):
    """
    Persiste no OBS as mensagens originais consumidas do Kafka.

    Objetivo:
    - Auditoria
    - Debug
    - Reprocessamento futuro

    Path:
    data/original/original-{id}.json
    """

    obs_original_messages_path = f"data/original/original-{id}.json"

    resp = obs_client.putContent(
        obs_bucket,
        obs_original_messages_path,
        json.dumps(messages)
    )

    if resp.status >= 300:
        raise Exception(f"Erro ao salvar mensagens no OBS: {resp.errorMessage}")


# =========================
# OBS: Salvar payload convertido para formato DataDog
# =========================
def save_datadog_payload(obs_client, obs_bucket, datadog_payload, id):
    """
    Persiste no OBS o payload já convertido para o formato da API do DataDog.

    Objetivo:
    - Debug da conversão
    - Validação antes do envio
    - Auditoria

    Path:
    data/datadog/datadog-{id}.json
    """

    obs_datadog_path = f"data/datadog/datadog-{id}.json"

    resp = obs_client.putContent(
        obs_bucket,
        obs_datadog_path,
        json.dumps(datadog_payload)
    )

    if resp.status >= 300:
        raise Exception(f"Erro ao salvar payload DataDog no OBS: {resp.errorMessage}")


# =========================
# DataDog: Definir campo type
# =========================
def datadog_set_type(metric):
    """
    Define o tipo da métrica no padrão DataDog.

    Tipos suportados:
    0 = unspecified
    1 = count
    2 = rate
    3 = gauge

    Estratégia:
    1. Inferência baseada na unit
    2. Inferência baseada no nome da métrica
    3. Fallback baseado no tipo do valor
    """

    unit = str(metric.get("unit", "")).lower()
    value_type = str(metric.get("type", "")).lower()
    metric_name = str(metric.get("metric", {}).get("metric_name", "")).lower()

    rate_units = {
        "b/s", "byte/s", "bytes/s", "bit/s",
        "count/s", "packet/s", "pps", "request/s"
    }

    count_units = {
        "count", "packet", "num", "count/op"
    }

    gauge_units = {
        "%", "percent",
        "byte", "bytes", "b",
        "kb/op", "gb",
        "ms", "ms/op"
    }

    if unit in rate_units:
        return 2

    if unit in count_units:
        return 1

    if unit in gauge_units:
        return 3

    if "rate" in metric_name:
        return 2

    if "count" in metric_name:
        return 1

    if value_type in {"int", "float"}:
        return 3

    return 0


# =========================
# DataDog: Definir campo resources
# =========================
def datadog_set_resources(dimensions):
    """
    Converte dimensions do modelo Huawei para resources do DataDog.

    Regra:
    - dimension.name  -> resource.type
    - dimension.value -> resource.name
    """

    resources = []

    for dim in dimensions:
        resource = {
            "name": dim.get("value", "unknown"),
            "type": dim.get("name", "unknown")
        }
        resources.append(resource)

    return resources


# =========================
# DataDog: Converter mensagens para formato da API DataDog Submit metrics
# =========================
def convert_to_datadog_format(messages):
    """
    Converte mensagens Kafka (Huawei Cloud Monitoring) para o formato
    esperado pela API de métricas do DataDog.

    Transformações principais:
    - Timestamp: ms -> segundos
    - Namespace -> resource_type
    - Dimensions -> resources
    - unit -> type (count/rate/gauge)
    - Estrutura final: series[]
    """

    series = []

    for msg in messages:

        try:
            payload = json.loads(msg["value"])
            metrics = payload.get("metrics", [])
        except Exception as e:
            logger.error("Erro ao fazer parse do payload Kafka")
            logger.error(str(e))
            continue

        for metric in metrics:

            try:
                metric_info = metric.get("metric", {})
                metric_name = metric_info.get("metric_name", "unknown")
                namespace = metric_info.get("namespace", "unknown")
                value = metric.get("value", 0)
                collect_time = metric.get("collect_time", msg["timestamp"])
                dimensions = metric_info.get("dimensions", [])
            except Exception as e:
                logger.error("Erro processando métrica")
                logger.error(str(e))
                continue

            timestamp_seconds = int(collect_time / 1000)

            if "." in namespace:
                resource_type = namespace.split(".")[1].lower()
            else:
                resource_type = namespace.lower()

            metric_name_custom = f"{resource_type}.{metric_name}".lower()
            metric_type = datadog_set_type(metric)
            resources = datadog_set_resources(dimensions)

            if not resources:
                resources = [{"name": "default", "type": resource_type}]

            tags = [
                "provider:huawei",
                f"env:{os.getenv('ENV', 'dev')}"
            ]

            series.append({
                "metric": metric_name_custom,
                "type": metric_type,
                "points": [{
                    "timestamp": timestamp_seconds,
                    "value": value
                }],
                "resources": resources,
                "tags": tags
            })

    return {"series": series}


# =========================
# DataDog: Enviar métricas para API (descomentar quando for utilizar envio para DataDog)
# =========================
def send_to_datadog(datadog_payload):
    """
    Envia métricas para a API do DataDog via proxy.

    Variáveis de ambiente:
    - DATADOG_API_URL: endpoint do proxy DataDog
    - DATADOG_API_KEY: API key (configurada como secret)

    Observações:
    - verify=False desabilita verificação SSL (proxy usa cert auto-assinado)
    - Timeout configurado para 10s
    - Tratamento básico de erro
    - Não impacta fluxo principal se falhar (design resiliente)
    """

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    datadog_url = os.getenv("DATADOG_API_URL")
    datadog_api_key = os.getenv("DATADOG_API_KEY")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DD-API-KEY": datadog_api_key
    }

    try:
        response = requests.post(
            datadog_url,
            headers=headers,
            json=datadog_payload,
            timeout=10,
            verify=False
        )
        logger.info(f"[DataDog] Status: {response.status_code}")
        logger.info(f"[DataDog] Response: {response.text}")
    except Exception as e:
        logger.error("Falha na chamada da API do DataDog")
        logger.error(str(e))


# =========================
# DMS (Kafka): Criar Consumer
# =========================
def create_kafka_consumer(bootstrap_servers):
    """
    Cria consumidor Kafka com commit manual.

    Configuração:
    - auto_commit desabilitado
    - timeout para encerrar consumo controlado
    """
    return KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=lambda x: x.decode("utf-8")
    )


# =========================
# DMS (Kafka): Obter último offset disponível
# =========================
def get_current_last_available_offset(
    kafka_consumer, 
    kafka_topic, 
    kafka_partition_id=0
):
    """
    Retorna o último offset disponível no tópico Kafka.

    Utilizado para:
    - Controle de fim de leitura
    """

    partition = TopicPartition(kafka_topic, kafka_partition_id)
    kafka_consumer.assign([partition])
    kafka_consumer.seek_to_end(partition)

    return kafka_consumer.position(partition)


# =========================
# DMS (Kafka): Consumir mensagens
# =========================
def consume_messages(
    kafka_consumer,
    kafka_topic,
    kafka_partition_id,
    max_messages,
    current_start_offset,
    current_last_available_offset
):
    """
    Consome mensagens do Kafka de forma controlada.

    Regras:
    - Inicia em offset calculado
    - Para ao atingir limite configurado
    - Para ao alcançar fim do tópico
    """

    partition = TopicPartition(kafka_topic, kafka_partition_id)
    kafka_consumer.assign([partition])
    kafka_consumer.seek(partition, current_start_offset)

    messages = []
    current_last_processed_offset = current_start_offset

    for message in kafka_consumer:

        msg = {
            "offset": message.offset,
            "timestamp": message.timestamp,
            "value": message.value
        }

        messages.append(msg)
        current_last_processed_offset = message.offset

        if len(messages) >= max_messages:
            logger.info(f"Consumo atingiu limite MAX_MESSAGES de {max_messages} mensagens")
            break

        if message.offset >= current_last_available_offset - 1:
            logger.info("Última mensagem disponível no tópico atingida")
            break

    return messages, current_last_processed_offset


# =========================
# Handler
# =========================
def handler(event, context):
    """
    Função principal (entrypoint).

    Fluxo:
    1. Ler variáveis de ambiente
    2. Gerar identificador único
    3. Criar clientes (OBS + Kafka)
    4. Recuperar offset da última execução
    5. Obter último offset disponível
    6. Consumir mensagens Kafka
    7. Converter para formato DataDog
    8. Enviar para DataDog
    9. Atualizar offset
    """

    obs_client = None
    kafka_consumer = None
    id = get_id()
    _ExecutionFilter.execution_id = str(id)

    try:

        # Ler variáveis de ambiente
        obs_endpoint = os.getenv('OBS_ENDPOINT')
        obs_bucket = os.getenv('OBS_BUCKET')
        obs_object_last_processed_offset = os.getenv('OBS_PREVIOUS_LAST_PROCESSED_OFFSET')
        kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP')
        kafka_topic = os.getenv('KAFKA_TOPIC')
        kafka_partition_id = 0
        max_messages = int(os.getenv('MAX_MESSAGES'))

        logger.info("Início da execução")

        # Criar clientes OBS e Kafka
        logger.info("Criando clientes OBS e Kafka")
        obs_client = create_obs_client(context, obs_endpoint)
        kafka_consumer = create_kafka_consumer(kafka_bootstrap)
        logger.info("Clientes OBS e Kafka criados com sucesso")

        # Recuperar offset da última execução
        logger.info("Recuperando offset da última execução")
        previous_last_processed_offset = get_previous_last_processed_offset(
            obs_client, 
            obs_bucket, 
            obs_object_last_processed_offset
        )
        logger.info(f"Offset anterior: {previous_last_processed_offset}")

        # Recuperar último offset disponível no Kafka
        logger.info("Recuperando último offset disponível no Kafka")
        current_last_available_offset = get_current_last_available_offset(
            kafka_consumer, 
            kafka_topic, 
            kafka_partition_id
        )
        logger.info(f"Último offset disponível no Kafka: {current_last_available_offset}")

        # Consumir mensagens Kafka e persistir dados originais
        current_start_offset = previous_last_processed_offset + 1
        logger.info(f"Consumindo mensagens - start_offset={current_start_offset}, max={max_messages}")
        messages, current_last_processed_offset = consume_messages(
            kafka_consumer,
            kafka_topic,
            kafka_partition_id,
            max_messages,
            current_start_offset,
            current_last_available_offset
        )
        logger.info(f"Mensagens consumidas: {len(messages)}, último offset processado: {current_last_processed_offset}")

        # Salvar mensagens originais para debug/troubleshooting
        if messages:
            logger.info("Salvando mensagens originais no OBS")
            save_original_messages(obs_client, obs_bucket, messages, id)

        # Converter para formato DataDog e persistir dados transformados
        if messages:
            logger.info("Convertendo mensagens para formato DataDog")
            datadog_payload = convert_to_datadog_format(messages)
            logger.info(f"Payload DataDog gerado com {len(datadog_payload.get('series', []))} métricas")
            save_datadog_payload(obs_client, obs_bucket, datadog_payload, id)

        # Enviar para DataDog
        if messages:
            logger.info("Enviando métricas para DataDog")
            send_to_datadog(datadog_payload)

        # Atualizar offset
        if messages:
            logger.info(f"Atualizando offset para {current_last_processed_offset}")
            update_last_processed_offset(
                obs_client,
                obs_bucket,
                obs_object_last_processed_offset,
                current_last_processed_offset
            )

        # Saída do contrato da function
        result = {
            "id": id,
            "statusCode": 200,
            "previousLastProcessedOffset": previous_last_processed_offset,
            "currentLastAvailableOffset": current_last_available_offset,
            "currentStartProcessedOffset": current_start_offset,
            "currentLastProcessedOffset": current_last_processed_offset,
            "consumedMessages": len(messages)
        }
        logger.info(f"Execução concluída com sucesso - {result}")
        return result

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Erro: {str(e)}")
        logger.error(f"Traceback: {tb}")

        return {
            "id": id,
            "statusCode": 500,
            "error": str(e)
        }

    finally:
        if kafka_consumer:
            kafka_consumer.close()

        if obs_client:
            obs_client.close()