# Cloud Eye to Datadog Integration

Process built for custom integration between the **Cloud Eye** service from **Huawei Cloud** and **Datadog**.

---

## Macro Solution

The solution uses an architecture with two types of accounts:

### Managed Accounts

Responsible for **producing** monitoring metrics:

1. **Export**: They use the **Data Dump** feature from the **Cloud Eye** service to send metrics to the **DMS** of the Centralized Account.

   > ⚠️ If the **Data Dump** feature is not available, contact Huawei Cloud support.

### Centralized Account

Responsible for **consolidating and adapting** the received metrics:

1. **Consolidation**: A **DMS** (Kafka) instance receives metrics from all Managed Accounts.

2. **Adaptation and delivery**: A **FunctionGraph** runs the custom adapter that:
   - Consumes metrics from DMS
   - Transforms them to the Datadog format
   - Sends the transformed metrics to Datadog

---

## Architecture Diagram

Diagram of the solution considering all resources and the operational flow.

![Diagram](images/diagram.png)

---

> 📋 Technical details, such as consumption configuration, delivery, persistence, handler contract, dependencies, and tests, are documented in the `SETUP-en.md` file.
