# BeboCard Public API — Changelog (P2-18)

All notable changes to the BeboCard Public API and SDKs will be documented in this file. This changelog follows the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format and adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.1] - 2026-04-12 (Enterprise Readiness Update)

### Added
- **API Versioning (P2-7)**: All brand-facing routes now support the `/v1/` prefix.
- **Deprecation Headers**: Legacy unversioned routes now return `Deprecation` and `Sunset` (RFC 8594/7240) headers.
- **Health Check**: Added `GET /v1/health` for StatusPage monitoring orchestration.
- **Security Public Key**: Added `GET /v1/security/receipt-public-key` to expose the RSA public key for cryptographic verification of BeboCard receipts.
- **Receipt Deep-Link**: Added `GET /v1/receipt` for programmatic retrieval of full signed receipt payloads by brand backends.

### Changed
- **Redirect Behavior**: Legacy routes now return `308 Permanent Redirect` instead of `301` to ensure POST bodies (receipts) are correctly forwarded to `/v1/`.

---

## [1.0.0] - 2026-03-24 (Initial Public Release)

### Added
- **Loyalty Scan**: `POST /v1/scan` for resolving BeboCard barcodes to brand loyalty IDs.
- **Receipt Ingestion**: `POST /v1/receipt` for asynchronous processing of digital receipts.
- **Invoice Ingestion**: `POST /v1/invoice` for processing commercial invoices.
- **Sandbox Environment**: Automatic mocking for `secondaryULID: SANDBOX_USER_123`.
- **Quota Enforcement**: Integration with BeboCard Billing for real-time tenant quota tracking.
- **Consent Gate**: Native attribute release (Email/Phone aliases) via user-approved push notifications.
