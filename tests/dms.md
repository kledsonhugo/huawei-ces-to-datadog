# Teste de Integração com DMS (Kafka)

## Descrição

Esta função implementa uma **FunctionGraph** para realizar validação de conectividade com o serviço **DMS (Distributed Message Service - Kafka)** da Huawei Cloud.

O objetivo principal é:

- Conectar ao cluster Kafka (DMS)
- Identificar as partições de um tópico
- Obter o **último offset de cada partição**
- Exibir essas informações no log

Essa abordagem é útil para validação de acesso, diagnóstico e controle de processamento de mensagens.

---

## Funcionalidades

### 1. Conexão com Kafka

A função se conecta ao cluster Kafka utilizando:

- Endpoint definido via variável de ambiente
- Conectividade via **VPC + Security Group + Agency**

---

### 2. Descoberta de Partições

A função identifica dinamicamente as partições associadas ao tópico informado:

- Utiliza `consumer.partitions_for_topic(topic)`
- Evita configuração fixa de partições

---

### 3. Obtenção do Último Offset

Para cada partição:

- Utiliza `consumer.end_offsets()`
- Calcula o último offset disponível:

```
last_offset = end_offset - 1
```

- Exibe no log:

```
Partition X | Last offset: Y
```

---

## Estrutura do Código

### Funções implementadas:

| Função | Descrição |
|--------|----------|
| `connect_kafka` | Cria conexão com o cluster Kafka |
| `get_last_offsets` | Obtém o último offset de cada partição |
| `handler` | Função principal executada pelo FunctionGraph |

---

## Pré-requisitos

### 1. Variáveis de Ambiente

Configurar na Function:

| Variável | Descrição |
|----------|----------|
| `KAFKA_INSTANCE` | Endpoint do Kafka (ex: `<KAFKA_BROKER_IP>:9092`) |
| `KAFKA_TOPIC` | Nome do tópico |

---

### 2. Configuração de Rede (VPC)

A FunctionGraph deve:

- Estar na **mesma VPC** do cluster DMS
- Estar na **mesma subnet** (ou com roteamento válido)
- Ter acesso liberado na porta do Kafka (ex: `9092`)

---

### 3. Security Group

O Security Group do Kafka deve permitir:

- Entrada (inbound) da FunctionGraph
- Porta TCP do Kafka (ex: `9092`)

---

### 4. Permissões IAM (Agency)

Para acesso ao DMS via rede privada, normalmente não é necessário permissão explícita de API Kafka, mas a function precisa de permissões de infraestrutura:

#### Policy para DMS

```
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dms:instance:get",
                "dms:instance:list"
            ]
        }
    ]
}
```

---

#### Policy para VPC

```
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "vpc:routes:list",
                "vpc:vpcTags:get",
                "vpc:vpcs:list",
                "vpc:networks:get",
                "vpc:ports:get",
                "vpc:privateIps:list",
                "vpc:privateIps:get",
                "vpc:routeTables:get",
                "vpc:routes:get",
                "vpc:routeTables:list",
                "vpc:bandwidths:get",
                "vpc:bandwidths:list",
                "vpc:securityGroupRules:get",
                "vpc:vpcs:get",
                "vpc:subnets:get",
                "vpc:subNetworkInterfaces:get",
                "vpc:securityGroupTags:get",
                "vpc:securityGroups:get",
                "vpc:subNetworkInterfaces:list"
            ]
        }
    ]
}
```

---

#### Policy para SFS Turbo (necessário para execução em VPC)

```
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sfsturbo:shares:getShare",
                "sfsturbo:shares:listPermRules",
                "sfsturbo:shares:showShareNic",
                "sfsturbo:shares:listShareNics",
                "sfsturbo:shares:getAllShares",
                "sfsturbo:shares:listBackendTargets",
                "sfsturbo:shares:listFsAsyncTasks",
                "sfsturbo:shares:getAZInfo",
                "sfsturbo:shares:getAllTag"
            ]
        }
    ]
}
```

---

## Fluxo de Execução

1. Cria conexão com o Kafka (DMS)
2. Obtém lista de partições do tópico
3. Para cada partição:
   - Consulta o offset final (`end_offsets`)
   - Calcula o último offset (`end_offset - 1`)
   - Exibe no log
4. Retorna status da execução

---

## Exemplo de Log

```
Partition 0 | Last offset: 3
Partition 1 | Last offset: 7
```

---

## Exemplo de Retorno

```json
{
  "statusCode": 200,
  "partitions": [0, 1]
}
```

---

## Observações

- O método `end_offsets` **não consome mensagens**
- O offset retornado representa:
  - Próxima posição de escrita no Kafka
- O último offset válido sempre será:
  - `end_offset - 1`
- Caso `end_offset = 0`, a partição está vazia

---

## Possíveis Evoluções

- Persistência de offset em OBS (checkpoint)
- Consumo incremental de mensagens
- Integração com pipeline de dados (DMS → OBS)
- Processamento de mensagens em batch
- Implementação de retry e DLQ (dead-letter queue)

---

## Uso Recomendado

Esta função é ideal para:

- Validação de conectividade com DMS (Kafka)
- Diagnóstico de tópicos e partições
- Monitoramento de offsets
- Base para implementação de consumers serverless