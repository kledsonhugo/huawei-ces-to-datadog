# Setup

Instruções para setup da integração do **Cloud Eye** com o **Datadog**.

> 💡 Todos os valores apresentados são **sugestões para validação/experimentação**. Adapte conforme seus padrões de arquitetura.

Este documento guia a configuração de uma arquitetura distribuída com dois tipos de contas:

- **Conta Centralizada**: Hospeda a infraestrutura central (VPC, Kafka/DMS, FunctionGraph, OBS, KMS)
- **Contas Gerenciadas**: Originam dados através de Data Dump que fluem para o Kafka na Conta Centralizada

# Sumário

- [Parte I: Conta Centralizada](#parte-i-conta-centralizada)
- [Parte II: Contas Gerenciadas](#parte-ii-contas-gerenciadas)
- [Apêndice](#apêndice)
  - [Fluxograma de Execução](#fluxograma-de-execução)
  - [Próximos Passos após Setup](#próximos-passos-após-setup)
  - [Detalhes Técnicos](#detalhes-técnicos)
  - [Testes de Integração](#testes-de-integração)
  - [Dependências](#dependências)
  - [Troubleshooting](#troubleshooting)

# **Parte I**: Conta Centralizada

## Pré-requisitos

Antes de começar, você precisará:
- Acesso à Conta Centralizada com permissões para criar recursos
- Account ID da Conta Centralizada
- Account ID's de todas as Contas Gerenciadas que enviarão dados
- Credenciais do Datadog (API Key e URL do endpoint)

## **Passo 1**: VPC (Virtual Private Cloud)

Selecione o serviço **VPC**, clique em **Create VPC** e digite os valores de configuração.

**Parâmetros:**
- VPC Name: `vpc-ddog`
- VPC IPv4 CIDR Block: `10.0.0.0/16`
- Subnet Name: `subnet-ddog`
- Subnet IPv4 CIDR Block: `10.0.1.0/24`

## **Passo 2**: VPC Endpoint

No serviço **VPC** selecione o menu **VPC Endpoints**, clique em **Buy VPC Endpoint** e configure:

**Parâmetros:**
- Service List: `com.myhuaweicloud.{region}.obs`
- VPC: `vpc-ddog`
- Route Table: `rtb-vpc-ddog`

> Status esperado: **Accepted** (ver imagem de exemplo)

![Buy VPC Endpoint Status](images/vpcep-buy-status.png)


## **Passo 3**: Security Group

No serviço **VPC**, acesse o menu **Security Groups**, clique em **Create Security Group** e configure:

**Parâmetros:**
- Name: `sg-ddog`
- Inbound Rules:

  | Protocolo | Porta | Origem          |
  |-----------|-------|-----------------|
  | TCP       | 9011  | 198.19.128.0/17 |
  | TCP       | 9092  | 0.0.0.0/0       |
  | TCP       | 9093  | 0.0.0.0/0       |

## **Passo 4**: DMS (Distributed Message Service)

### Criar DMS

No serviço **DMS**, clique em **Buy Kafka Instance** e configure:

**Parâmetros:**
- Billing Mode: `Pay-per-use`
- Architecture: `Single-node`
- Broker Flavor: `kafka.2u4g.single.small`
- Disk Type: `High I/O`
- VPC: `vpc-ddog`
- Subnet: `subnet-ddog`
- Security Group: `sg-ddog`
- Instance Name: `kafka-ddog`

> Status esperado: **Running** (ver imagem de exemplo)

![Buy DMS Status](images/dms-buy-status.png)

---

### Criar Tópico

Na instância DMS criada, acesse o menu **Topic Management**, clique em **Create Topic** e configure:

**Parâmetros:**
- Topic Name: `topic-ddog`
- Partitions: `1`
- Aging Time (h): `6`

## **Passo 5**: KMS (Key Management Service)

No serviço **Data Encryption Workshop**, acesse o menu **Key Management Service**, clique em **Create Key** e configure:

**Parâmetro:**
- Name: `kms-ddog`


## **Passo 6**: OBS (Object Storage Service)

No serviço **OBS**, clique em **Create Bucket** e configure:

**Parâmetros:**
- Bucket Name: `obs-ddog` ou outro nome disponível
- Block Public Access: `Enabled seetings: 4`
- Bucket Policy: `Private`
- Server-Side Encryption: `Enabled`
- Encryption Method: `SSE-KMS`
- Encryption Key Type: `Custom`
- Custom: `kms-ddog`

## **Passo 7**: Políticas IAM e Agency para FunctionGraph

### Criar Políticas Customizadas

No serviço **IAM**, acesse o menu **Permissions > Policies/Roles** e clique em **Create Custom Policy** para cada política abaixo:

#### 1. Policy: DMS (Read-only)

**Nome:** `policy-ddog-dms-readonly`

```json
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

#### 2. Policy: VPC (Read-only)

**Nome:** `policy-ddog-vpc-readonly`

```json
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "vpc:ports:get",
                "vpc:ports:create",
                "vpc:vpcs:get",
                "vpc:subnets:get"
            ]
        }
    ]
}
```

#### 3. Policy: OBS (Read-Write)

**Nome:** `policy-ddog-obs-readwrite`

```json
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "obs:object:GetObject",
                "obs:bucket:HeadBucket",
                "obs:object:PutObject",
                "obs:bucket:ListBucket"
            ],
            "Resource": [
                "OBS:*:*:object:obs-ddog/*",
                "OBS:*:*:bucket:obs-ddog"
            ]
        }
    ]
}
```

#### 4. Policy: KMS (Read-Write)

**Nome:** `policy-ddog-kms-readwrite`
**Policy:** (substituir `{KMS_KEY_ID_CONTA_CENTRALIZADA}` pelo KMS ID da Conta Centralizada):

```json
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kms:cmk:get",
                "kms:dek:create",
                "kms:dek:decrypt"
            ],
            "Resource": [
                "KMS:*:*:KeyId:{KMS_KEY_ID_CONTA_CENTRALIZADA}"
            ]
        }
    ]
}
```

---

### Criar Agency para FunctionGraph

No serviço **IAM**, acesse o menu **Agencies** e clique em **Create Agency**:

**Parâmetros:**
- Agency Name: `agency-ddog-fg`
- Agency Type: `Cloud Service`
- Cloud Service: `FunctionGraph`

**Autorize para as seguintes políticas:**
- `policy-ddog-dms-readonly`
- `policy-ddog-vpc-readonly`
- `policy-ddog-obs-readwrite`
- `policy-ddog-kms-readwrite`

## **Passo 8**: Cloud Eye Data Dump (Conta Centralizada)

> 💡 Adiciona Data Dump para o serviço **ECS**. Inclua outros **Dumps** para demais serviços que precisam ser monitorados na Conta Centralizada.

No serviço **Cloud Eye**, acesse o menu **Data Dump** e clique em **Add Dump Task**:

**Parâmetros:**
- Name: `dataShareJob-ecs`
- Resource Type: `Elastic Cloud Server`

> Status esperado: **Enabled** (ver imagem de exemplo)

![Data Dump Status](images/ces-data-dump-status.png)

## **Passo 9**: Agency para DMS (Acesso entre Contas)

> 💡 Esta Agency permite que as Contas Gerenciadas enviem dados para o Kafka da Conta Centralizada. Configure a **IAM 5** via URL: `https://console-intl.huaweicloud.com/iam5`

---

### Criar Política no IAM 5

No serviço **IAM 5**, acesse o menu **Identity Policies** e clique em **Create Identity Policy**:

**Nome:** `policy-ddog-dms-readonly-iam5`

**Trust Policy** (substituir `{DMS_ID}` pelo DMS ID):

```json
{
  "Version": "5.0",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dms:instance:getDetail",
        "dms:topic:get",
        "dms:topic:list"
      ],
      "Resource": [
        "dms:*:*:kafka:{DMS_ID}",
        "dms:*:*:topic:*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dms:instance:list"
      ]
    }
  ]
}
```

---

### Criar Trust Agency

No serviço **IAM**, acesse o menu **Agencies** e clique em **Create Trust Agency**:

**Parâmetros:**
- Agency Name: `agency-ddog-dms`
- Agency Type: `Custom trust policy`

**Trust Policy:** Substituir `{ACCOUNT_ID_CENTRALIZADA}` pelo Account ID da Conta Centralizada.

```json
{
  "Version": "5.0",
  "Statement": [
    {
      "Action": [
        "sts:agencies:assume"
      ],
      "Effect": "Allow",
      "Principal": {
        "IAM": [
          "{ACCOUNT_ID_CENTRALIZADA}"
        ]
      }
    }
  ]
}
```

**Autorize a Agency** para a policy `policy-ddog-dms-readonly-iam5`.

**Edite a Trust Policy** para adicionar os Account IDs de cada Conta Gerenciada. Substituir `{ACCOUNT_ID_GERENCIADA_1}`, `{ACCOUNT_ID_GERENCIADA_2}` e `{ACCOUNT_ID_GERENCIADA_N}` pelo Account ID das Contas Gerenciadas):

```json
{
  "Version": "5.0",
  "Statement": [
    {
      "Action": [
        "sts:agencies:assume"
      ],
      "Effect": "Allow",
      "Principal": {
        "IAM": [
          "{ACCOUNT_ID_CENTRALIZADA}",
          "{ACCOUNT_ID_GERENCIADA_1}",
          "{ACCOUNT_ID_GERENCIADA_2}",
          "{ACCOUNT_ID_GERENCIADA_N}"
        ]
      }
    }
  ]
}
```

## **Passo 10**: FunctionGraph

No serviço **FunctionGraph**, clique em **Create Function** e configure:

**Parâmetros:**
- Function Type: `Event Function`
- Function Name: `fg-ddog`
- Agency: `agency-ddog-fg`
- Runtime: `Python 3.9`
- Public Access: `Disabled`
- VPC Access: `Enabled`
- VPC: `vpc-ddog`
- Subnet: `subnet-ddog`

## **Passo 11**: Configurar FunctionGraph

No menu **Configuration** da FunctionGraph, ajuste:

### Basic Settings

- Execution Timeout (s): `30`

---

### Environment Variables

| Variável                               | Exemplo de Valor                              | Descrição                          |
|----------------------------------------|-----------------------------------------------|------------------------------------|
| `KAFKA_BOOTSTRAP`                      | `<KAFKA_BROKER_IP>:9092`                     | Endereço do cluster Kafka          |
| `KAFKA_TOPIC`                          | `topic-ddog`                                  | Nome do tópico Kafka               |
| `OBS_BUCKET`                           | `obs-ddog`                                    | Bucket OBS                         |
| `OBS_ENDPOINT`                         | `https://obs.{region}.myhuaweicloud.com`      | Endpoint OBS                       |
| `OBS_PREVIOUS_LAST_PROCESSED_OFFSET`   | `offset-control.json`                         | Arquivo de controle de offset      |
| `MAX_MESSAGES`                         | `500`                                         | Limite de mensagens por execução   |
| `DATADOG_API_URL`                      | `https://api.datadoghq.com/api/v2/series`    | Endpoint da API DataDog            |
| `DATADOG_API_KEY`                      | `<YOUR_DATADOG_API_KEY>`                     | Datadog API Key                    |

## **Passo 12**: Deploy da FunctionGraph

O deploy consiste em 3 etapas: criar dependência Kafka, adicionar a dependência à function e fazer upload do código.

---

### Criar Dependência Kafka

Prepare o arquivo de dependência:

```bash
# Navegar para a raiz do projeto
cd /path/to/fg-kafka-to-ddog

# Compactar a pasta kafka
zip -r kafka.zip kafka/
```

No serviço **FunctionGraph**, acesse o menu **Dependencies** e clique em **Create Dependency**:

**Parâmetros:**
- Name: `kafka`
- Runtime: `Python 3.9`
- File: fazer upload do arquivo `kafka.zip` gerado acima

---

### Adicionar Dependência à Function

Na function `fg-ddog` criada no Passo 10, na parte inferior da página, localize a seção **Dependencies** e clique em **Add Dependency**.

Selecione a dependência `kafka` criada na Etapa 1.

---

### Fazer Upload do Código da Function

Copie o código do arquivo [index.py](index.py) e adicione-o ao projeto da function:

1. No serviço **FunctionGraph**, abra a function `fg-ddog`
2. No editor de código, substitua o conteúdo existente pelo código de [index.py](index.py)
3. Clique em **Deploy** ou **Save and Deploy**

## **Passo 13**: Validar Execução da FunctionGraph

Antes de configurar o trigger automático, valide se a function consegue executar corretamente.

### Executar Function

No serviço **FunctionGraph**, abra a function `fg-ddog` e clique em **Test Function** ou **Run**:

**Validação esperada:**
- ✅ Execução com status **Success**
- ✅ Logs mostrando conexão com Kafka bem-sucedida
- ✅ Dados consumidos do tópico (ou "fila vazia" se nenhuma mensagem disponível)
- ✅ Upload bem-sucedido no OBS (se houver dados)
- ✅ Response JSON com `statusCode: 200 ou 202`

## **Passo 14**: Configurar Trigger (Agendamento)

Após validar que a function executa corretamente, configure o trigger automático no menu **Configuration** da FunctionGraph:

**Parâmetros:**
- Trigger Type: `Time`
- Timer Name: `timer-ddog`
- Rule: `Cron expression`
- Value: `*/1 * * * * ?`

| Expressão | Frequência |
|-----------|------------------------------------------|
| `*/1 * * * * ?` | A cada 1 minuto (atual) |

# **Parte II**: Contas Gerenciadas

Cada Conta Gerenciada configura um **Data Dump** que envia métricas do Cloud Eye para o Kafka da Conta Centralizada.

## Pré-requisitos

- Account ID da Conta Centralizada
- Agency `agency-ddog-dms` já criada na Conta Centralizada (Passo 9)
- O Account ID da Conta Gerenciada deve estar na Trust Policy da Agency (Passo 9)

## **Passo 1**: Cloud Eye Data Dump (Conta Gerenciada)

> 💡 Este passo adiciona Data Dump para o serviço **VPC**. Você pode incluir outras **Dump Tasks** para demais serviços que precisam ser monitorados.

No serviço **Cloud Eye** da **Conta Gerenciada**, acesse o menu **Data Dump** e clique em **Add Dump Task**:

**Parâmetros:**
- Name: `dataShareJob-vpc`
- Resource Type: `Virtual Private Cloud`
- Destination: `Other account`
- Delegator Account: `{ACCOUNT_ID_CENTRALIZADA}`
- Agency Name: `agency-ddog-dms`
- Kafka: `kafka-ddog`
- Topic: `topic-ddog`

> Status esperado: **Enabled** (ver imagem de exemplo)

![Data Dump Status - Managed Account](images/ces-data-dump-status-for-managed-account.png)

## **Passo 2**: Adicionar Mais Recursos (Opcional)

Para monitorar outros serviços, repita o processo de criação de Data Dump Task para cada recurso desejado:

**Exemplos de Resource Types:**
- Elastic Cloud Server (ECS)
- Relational Database Service (RDS)
- Load Balancer (ELB)
- Auto Scaling (AS)
- Storage (OBS)
- Outros serviços conforme necessário

# Apêndice

## Próximos Passos após Setup

1. **Monitorar execuções** da FunctionGraph:
   - Acessar logs de cada execução
   - Revisar logs para erros ou warnings
   - Validar offsets do Kafka

2. **Verificar dados** no Datadog:
   - Acessar dashboard de métricas
   - Validar que dados chegam regularmente
   - Configurar alertas conforme necessário

3. **Escalar para produção**:
   - Ajustar intervalo de cron (ex: a cada 5 mins → a cada 1 min)
   - Aumentar recursos da FunctionGraph (timeout, memory)
   - Revisar e ajustar MAX_MESSAGES conforme volumetria
   - Implementar retention policy no OBS

4. **Documentar**:
   - Registrar Account IDs
   - Documentar Agency relationships
   - Criar playbook de troubleshooting

## Controle de Offset

O controle da última mensagem consumida do DMS é persistido no OBS `OBS_BUCKET`, salvo em `OBS_PREVIOUS_LAST_PROCESSED_OFFSET`.

- Lê o offset salvo.
- Consome a partir de `offset + 1`.
- Atualiza o valor ao final da execução, se houver consumo.
- Se objeto **não existir**, é criado automaticamente com valor `0`.
- Garante processamento **incremental e idempotente**.

## Persistência no OBS

Persistência das métricas originais e transformadas para efeitos de Auditoria, Validação da transformação ou Debug.

- Dados originais: `data/original/original-{id}.json`
- Dados convertidos: `data/datadog/datadog-{id}.json`

O `id` é gerado como Unix Epoch em milissegundos, garantindo unicidade entre execuções.

## Testes de Integração

O diretório `tests/` contém funções de validação para cada serviço, cada uma com sua documentação:

| Função | Descrição |
|---|---|
| Datadog | Valida conectividade com API Datadog (curl + requests) |
| DMS | Valida conectividade e offsets do cluster Kafka |
| OBS | Valida operações de upload, listagem e leitura no OBS |

> 💡 Cada teste é implantado como uma FunctionGraph independente.

## Troubleshooting

### FunctionGraph não consegue conectar ao Kafka
- Verificar se VPC/Subnet estão corretos
- Validar Security Group rules (portas 9092 e 9093)
- Testar conectividade usando `tests/dms.py`

---

### Falha ao escrever no OBS
- Verificar permissões da Agency (`agency-ddog-fg`)
- Validar se bucket existe e é acessível
- Testar usando `tests/obs.py`

---

### Datadog não recebe métricas
- Validar `DATADOG_API_KEY` e `DATADOG_API_URL`
- Verificar logs da FunctionGraph
- Testar conectividade usando `tests/datadog.py`

---

### Data Dump na Conta Gerenciada não funciona
- Confirmar Agency `agency-ddog-dms` existe na Conta Centralizada
- Validar se Account ID da Conta Gerenciada está na Trust Policy
- Verificar se Kafka, Tópico e Agency estão corretos no Data Dump Task

---