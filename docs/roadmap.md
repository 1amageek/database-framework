# Roadmap

This file contains only work that is still open. Completed implementation plans,
debug logs, and review reports are not retained here.

## Current Items

| Item | Status | Owner |
|---|---|---|
| Keep Cloudflare Durable Object validation aligned with DatabaseWire changes | Ongoing | database-framework-cloudflare |
| Maintain backend parity tests for FoundationDB, SQLite, and PostgreSQL | Ongoing | database-framework and storage-kit |

## Explicit Non-Goals

- swift-web is not a dependency of database-framework.
- Cloudflare Worker host code does not belong in this repository.
- Domain-specific commands, CRM rules, and application schemas do not belong in
  the framework.
- A backend must not silently emulate an unsupported capability.

Historical design proposals are available through Git history when their
reasoning is needed. They are not part of the current product contract.
