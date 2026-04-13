/**
 * BeboCard Partner SDK for TypeScript/JavaScript
 * 
 * This SDK provides a secure, typed interface for BeboCard's Scan and Receipt APIs.
 * Supporting structural privacy and non-repudiation out of the box.
 */

import { createVerify } from 'crypto';

export interface BeboConfig {
  apiKey: string;
  baseUrl?: string;
  isSandbox?: boolean;
}

export interface ScanRequest {
  secondaryULID: string;
  brandId: string;
  requestedFields?: string[];
  purpose?: string;
}

export interface ScanResponse {
  hasLoyaltyCard: boolean;
  loyaltyId?: string;
  tier?: string;
  spendBucket?: string;
  consentRequired?: boolean;
  requestId?: string;
  attributes?: Record<string, string>;
}

export interface ReceiptRequest {
  secondaryULID: string;
  merchant: string;
  amount: number;
  purchaseDate: string;
  currency?: string;
  items?: any[];
  loyaltyCardId?: string;
  anonymousMode?: boolean;
}

export interface ReceiptSubmissionResponse {
  success: boolean;
  receiptId: string;
  claimToken?: string;
  claimQRPayload?: string;
}

export class BeboCardClient {
  private readonly apiKey: string;
  private readonly baseUrl: string;

  constructor(config: BeboConfig) {
    this.apiKey = config.apiKey;
    this.baseUrl = config.baseUrl ?? 'https://api.bebocard.com/v1';
    
    // Auto-detect sandbox from API key prefix if not explicitly set
    const isSandboxKey = config.apiKey.startsWith('bebo_sb_');
    if (config.isSandbox === undefined && isSandboxKey) {
      console.info('[BeboCard SDK] Sandbox mode enabled via API key prefix.');
    }
  }

  /**
   * Resolve a BeboCard barcode (secondaryULID) to a loyalty identity.
   */
  async resolveScan(request: ScanRequest): Promise<ScanResponse> {
    const response = await fetch(`${this.baseUrl}/scan`, {
      method: 'POST',
      headers: {
        'X-API-Key': this.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        secondaryULID: request.secondaryULID,
        storeBrandLoyaltyName: request.brandId,
        requestedFields: request.requestedFields,
        purpose: request.purpose,
      }),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(`BeboCard Scan Error: ${error.message || response.statusText}`);
    }

    return await response.json();
  }

  /**
   * Submit a digital receipt to the user's wallet.
   */
  async submitReceipt(request: ReceiptRequest): Promise<ReceiptSubmissionResponse> {
    const response = await fetch(`${this.baseUrl}/receipt`, {
      method: 'POST',
      headers: {
        'X-API-Key': this.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(`BeboCard Receipt Error: ${error.message || response.statusText}`);
    }

    return await response.json();
  }

  /**
   * Verify the cryptographic signature of a BeboCard receipt to ensure non-repudiation.
   */
  async verifyReceiptSignature(receipt: any, publicKeyPem: string): Promise<boolean> {
    if (!receipt.signature || !receipt.signingAlgorithm) {
      throw new Error('Receipt is missing signature metadata');
    }

    // Normalized payload for signature verification (must match server-side sorting)
    const payload = JSON.stringify({
      receiptId: receipt.receiptId,
      merchant: receipt.merchant,
      amount: receipt.amount,
      purchaseDate: receipt.purchaseDate,
      brandId: receipt.brandId,
    });

    const verify = createVerify('RSA-SHA256');
    verify.update(payload);
    verify.end();

    return verify.verify(
      {
        key: publicKeyPem,
        padding: 6, // RSASSA-PSS
        saltLength: 32,
      },
      Buffer.from(receipt.signature, 'base64')
    );
  }

  /**
   * Fetch the current BeboCard public key for signature verification.
   */
  async getPublicKey(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/security/receipt-public-key`);
    const data = await response.json();
    
    // Convert DER/Base64 to PEM format for standard crypto libraries
    const base64 = data.publicKey;
    return `-----BEGIN PUBLIC KEY-----\n${base64.match(/.{1,64}/g).join('\n')}\n-----END PUBLIC KEY-----`;
  }
}
