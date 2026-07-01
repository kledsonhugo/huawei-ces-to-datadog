# Teste de Integração com DataDog

## Descrição

Esta função implementa uma **FunctionGraph** para realizar validação de conectividade com a **API de métricas do DataDog** via proxy.

O objetivo principal é:

- Diagnosticar o erro **403** retornado ao enviar métricas para o DataDog
- Comparar o resultado entre `curl` e `requests.post()` para isolar a causa
- Validar conectividade, SSL e credenciais no ambiente da function

---

## Funcionalidades

### 1. Debug com curl

Executa chamada `curl` com payload fixo conhecido que funciona localmente:

- `-k` desabilita verificação SSL (proxy pode usar certificado auto-assinado)
- `-s` modo silencioso
- `-w` captura HTTP status code na saída
- Timeout de 15s

### 2. Debug com requests

Executa a mesma chamada utilizando `requests.post()` com `verify=False`:

- Equivalente ao `-k` do curl
- Permite comparar o resultado entre os dois métodos
- Desabilita warnings de SSL (`urllib3.disable_warnings`)

---

## Payload de Teste

Payload fixo utilizado nos testes:

```json
{
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
```

---

## Estrutura do Código

### Funções implementadas:

| Função | Descrição |
|--------|----------|
| `debug_curl_datadog` | Executa chamada curl fixa para debug de conectividade |
| `debug_requests_datadog` | Executa chamada com requests.post() (verify=False) |
| `handler` | Função principal executada pelo FunctionGraph |

---

## Interpretação dos Resultados

| curl | requests | Conclusão |
|------|----------|-----------|
| 202 | 403 | Problema é SSL na lib `requests` — usar `verify=False` no `index.py` |
| 202 | 202 | Ambos funcionam — problema está no `index.py` (headers, payload, etc.) |
| 403 | 403 | Problema de credenciais/permissões |
| timeout | timeout | Problema de rede — function não alcança o proxy |

---

## Pré-requisitos

### 1. Configuração Fixa

O teste utiliza valores fixos (não usa variáveis de ambiente):

| Parâmetro | Valor |
|-----------|-------|
| URL | <URL do Datadog endpoint> |
| API Key | <API Key do Datadog> |

---

### 2. Configuração de Rede (VPC)

A FunctionGraph deve:

- Ter acesso de saída (outbound) para o endpoint do proxy DataDog
- Porta TCP do proxy DataDog liberada no Security Group

---

### 3. Dependências

| Pacote | Descrição |
|--------|-----------|
| `requests` | Necessário para `debug_requests_datadog` |
| `curl` | Necessário para `debug_curl_datadog` (pode não estar disponível no runtime da function) |

---

## Observações

- Esta função é para **debug temporário** — remover após diagnóstico
- O payload e credenciais estão fixos no código para simplificar o teste
- Se `curl` não estiver disponível no runtime da function, o teste indicará `curl não encontrado` e apenas o teste com `requests` será executado
