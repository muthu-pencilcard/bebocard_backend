import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  ALL_USAGE_TYPES,
  getCategoryOverageRate,
  parseTenantIncludedEvents,
  normalizeTenantTier,
  TIER_INCLUDED_EVENTS,
  TIER_OVERAGE_RATES,
  checkTenantQuota,
  type TenantTier,
  type UsageType,
} from './tenant-billing';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

const dummyDynamo = { send: vi.fn() } as unknown as DynamoDBDocumentClient;

beforeEach(() => vi.clearAllMocks());

// Helper: mock DDB to return a single non-zero count for the first type, 0 for the rest
function mockUsageTotal(total: number) {
  let callCount = 0;
  (vi.mocked(dummyDynamo.send) as any).mockImplementation(() => {
    callCount++;
    return Promise.resolve({ Item: { usageCount: callCount === 1 ? total : 0 } });
  });
}

describe('Tenant Billing — Unit Tests', () => {

  // ── normalizeTenantTier ───────────────────────────────────────────────────────

  describe('normalizeTenantTier', () => {
    it('normalizes known aliases', () => {
      expect(normalizeTenantTier('engagement')).toBe('engagement');
      expect(normalizeTenantTier('growth')).toBe('engagement');
      expect(normalizeTenantTier('intelligence')).toBe('intelligence');
      // enterprise is a standalone tier (custom contract, ~unlimited, 0% overage)
      expect(normalizeTenantTier('enterprise')).toBe('enterprise');
    });

    it('falls back to base for unknown values', () => {
      expect(normalizeTenantTier(undefined)).toBe('base');
      expect(normalizeTenantTier('')).toBe('base');
      expect(normalizeTenantTier('unknown')).toBe('base');
    });
  });

  // ── TIER_INCLUDED_EVENTS ──────────────────────────────────────────────────────

  describe('TIER_INCLUDED_EVENTS', () => {
    it('matches design doc values', () => {
      expect(TIER_INCLUDED_EVENTS.base).toBe(250);
      expect(TIER_INCLUDED_EVENTS.engagement).toBe(2500);
      expect(TIER_INCLUDED_EVENTS.intelligence).toBe(25000);
    });

    it('enterprise is effectively unlimited', () => {
      expect(TIER_INCLUDED_EVENTS.enterprise).toBeGreaterThan(1_000_000);
    });
  });

  // ── TIER_OVERAGE_RATES ────────────────────────────────────────────────────────

  describe('TIER_OVERAGE_RATES', () => {
    it('matches design doc rates', () => {
      expect(TIER_OVERAGE_RATES.base).toBe(0.45);
      expect(TIER_OVERAGE_RATES.engagement).toBe(0.20);
      expect(TIER_OVERAGE_RATES.intelligence).toBe(0.08);
      expect(TIER_OVERAGE_RATES.enterprise).toBe(0.00);
    });
  });

  // ── getCategoryOverageRate ────────────────────────────────────────────────────

  describe('getCategoryOverageRate', () => {
    it('returns tier default for all standard categories', () => {
      const tiers: TenantTier[] = ['base', 'engagement', 'intelligence'];
      const standardTypes: UsageType[] = ['offers', 'newsletters', 'catalogues', 'invoices', 'geolocation', 'payments'];
      for (const tier of tiers) {
        for (const type of standardTypes) {
          expect(getCategoryOverageRate(tier, type)).toBe(TIER_OVERAGE_RATES[tier]);
        }
      }
    });

    it('consent on intelligence tier charges $0.15 (higher value)', () => {
      expect(getCategoryOverageRate('intelligence', 'consent')).toBe(0.15);
    });

    it('consent on non-intelligence tiers uses tier default', () => {
      expect(getCategoryOverageRate('base', 'consent')).toBe(0.45);
      expect(getCategoryOverageRate('engagement', 'consent')).toBe(0.20);
    });

    it('enterprise tier always returns 0.00', () => {
      for (const type of ALL_USAGE_TYPES) {
        expect(getCategoryOverageRate('enterprise', type)).toBe(0.00);
      }
    });
  });

  // ── parseTenantIncludedEvents ─────────────────────────────────────────────────

  describe('parseTenantIncludedEvents', () => {
    it('uses custom value if numeric', () => {
      expect(parseTenantIncludedEvents(5000, 'base')).toBe(5000);
    });

    it('falls back to tier default for non-numeric', () => {
      expect(parseTenantIncludedEvents(null, 'base')).toBe(250);
      expect(parseTenantIncludedEvents(undefined, 'engagement')).toBe(2500);
      expect(parseTenantIncludedEvents('', 'intelligence')).toBe(25000);
    });
  });

  // ── checkTenantQuota ──────────────────────────────────────────────────────────

  describe('checkTenantQuota', () => {
    it('base tier — blocks when quota is exactly hit', async () => {
      mockUsageTotal(250); // 250 / 250 = 100%
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: 'base-t1', tier: 'base', includedEventsPerMonth: 250,
      }, 'offers');

      expect(result.allowed).toBe(false);
      expect(result.message).toContain('Base tier monthly quota exceeded');
      expect(result.currentTotal).toBe(250);
      expect(result.usageRatio).toBe(1.0);
    });

    it('base tier — allows when under quota', async () => {
      mockUsageTotal(100);
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: 'base-t1', tier: 'base', includedEventsPerMonth: 250,
      }, 'offers');

      expect(result.allowed).toBe(true);
      expect(result.currentTotal).toBe(100);
      expect(result.usageRatio).toBeCloseTo(0.4);
    });

    it('engagement tier — soft limit: allows even when quota exceeded', async () => {
      mockUsageTotal(3000); // over 2500 limit
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: 'eng-t1', tier: 'engagement', includedEventsPerMonth: 2500,
      }, 'newsletters');

      expect(result.allowed).toBe(true);
      expect(result.currentTotal).toBe(3000);
      expect(result.usageRatio).toBeCloseTo(1.2);
    });

    it('engagement tier — exposes 80% threshold via usageRatio', async () => {
      mockUsageTotal(2000); // 80% of 2500
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: 'eng-t1', tier: 'engagement', includedEventsPerMonth: 2500,
      }, 'newsletters');

      expect(result.allowed).toBe(true);
      expect(result.usageRatio).toBe(0.8);
    });

    it('intelligence tier — soft limit: allows wildly over quota', async () => {
      mockUsageTotal(50000); // 200% of 25000
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: 'intel-t1', tier: 'intelligence', includedEventsPerMonth: 25000,
      }, 'offers');

      expect(result.allowed).toBe(true);
      expect(result.usageRatio).toBe(2.0);
    });

    it('enterprise tier — always allows (null includedEventsPerMonth)', async () => {
      // enterprise has null includedEventsPerMonth → always allowed
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: 'ent-t1', tier: 'enterprise', includedEventsPerMonth: null,
      }, 'offers');

      expect(result.allowed).toBe(true);
      expect(dummyDynamo.send).not.toHaveBeenCalled();
    });

    it('no tenant — always allows without DDB call', async () => {
      const result = await checkTenantQuota(dummyDynamo, 'TABLE', {
        tenantId: null, tier: 'base', includedEventsPerMonth: 250,
      }, 'offers');

      expect(result.allowed).toBe(true);
      expect(dummyDynamo.send).not.toHaveBeenCalled();
    });
  });
});
