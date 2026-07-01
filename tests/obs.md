# Teste de Integração com OBS

## Descrição

Esta função implementa uma **FunctionGraph** para realizar operações básicas no **Object Storage Service (OBS)** da Huawei Cloud:

- Upload de um objeto JSON no bucket
- Listagem de objetos no bucket
- Leitura parcial do conteúdo dos objetos

O objetivo principal é validar conectividade, permissões e operações básicas de I/O com o OBS, executando a FunctionGraph dentro de uma VPC.

---

## Funcionalidades

### 1. Upload de Objeto

Cria (ou sobrescreve) um objeto no bucket:

- Nome: `test-000.json`
- Conteúdo:

```json
{"test": 0}
```

---

### 2. Listagem de Objetos

Lista todos os objetos disponíveis no bucket, exibindo:

- Nome do objeto (`key`)
- Tamanho (`size`)
- Data de modificação (`lastModified`)

---

### 3. Leitura de Conteúdo

Para cada objeto listado:

- Lê os primeiros **500 bytes**
- Tenta converter para texto (UTF-8)
- Exibe no log

---

## Estrutura do Código

### Funções implementadas:

| Função | Descrição |
|--------|----------|
| `upload_object` | Realiza upload de um JSON no bucket |
| `list_objects` | Lista todos os objetos do bucket |
| `read_object_content` | Lê conteúdo parcial de um objeto |
| `handler` | Função principal executada pelo FunctionGraph |

---

## Pré-requisitos

### 1. Variáveis de Ambiente

Configurar na Function:

| Variável | Descrição |
|----------|----------|
| `OBS_ENDPOINT` | Endpoint do OBS (ex: `https://obs.{region}.myhuaweicloud.com`) |
| `OBS_BUCKET` | Nome do bucket |

---

### 2. Permissões IAM (Agency)

A agency associada à função deve possuir permissão para as ações abaixo:

#### Policy com ações para o OBS

```
{
    "Version": "1.1",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "obs:object:GetObject",
                "obs:bucket:GetBucketLocation",
                "obs:object:GetAccessLabel",
                "obs:bucket:PutBucketInventoryConfiguration",
                "obs:object:AbortMultipartUpload",
                "obs:object:DeleteObject",
                "obs:bucket:HeadBucket",
                "obs:object:PutObject",
                "obs:object:PutObjectTagging",
                "obs:bucket:ListBucketMultipartUploads",
                "obs:object:ListMultipartUploadParts",
                "obs:object:ModifyObjectMetaData",
                "obs:bucket:ListBucket"
            ]
        }
    ]
}
```

---

#### Policy com ações para a VPC

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

#### Policy com ações para o SFS Turbo

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

1. Cria cliente OBS com credenciais temporárias
2. Realiza upload do arquivo `test-000.json`
3. Lista objetos do bucket
4. Itera sobre os objetos:
   - Lê conteúdo parcial
   - Exibe no log
5. Retorna status da execução

---

## Exemplo de Retorno

```json
{
  "statusCode": 200,
  "total_objects": 5
}
```

---

## Observações

- O upload sobrescreve o arquivo caso ele já exista
- A leitura de conteúdo é limitada a 500 bytes (evita problemas com arquivos grandes)
- Arquivos binários (ZIP, imagens) podem gerar saída ilegível no log
- Ideal para testes e validação de acesso ao OBS

---

## Possíveis Evoluções

- Upload com nome dinâmico (timestamp ou UUID)
- Paginação para buckets com muitos objetos
- Processamento específico de arquivos (CSV, JSON)

---

## Uso Recomendado

Esta função é ideal para:

- Testes de conectividade com OBS
- Validação de permissões IAM
- Debug de execução em VPC