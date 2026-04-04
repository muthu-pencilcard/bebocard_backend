import { describe, it, expect, vi, beforeEach } from 'vitest';
import { CircuitBreaker, CircuitBreakerError, CircuitBreakerFactory } from './circuit-breaker';

// CircuitBreakerFactory uses a module-level registry; clear it between tests
// by reaching into the private _registry via the get() accessor.
// We re-create breakers by name — use unique names per test to avoid cross-test state.

let nameCounter = 0;
function uniqueName(prefix: string) {
  return `${prefix}-${++nameCounter}`;
}

describe('CircuitBreaker — CLOSED state', () => {
  it('executes the function and returns its value when CLOSED', async () => {
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 3,
      resetTimeoutMs: 10_000,
      halfOpenSuccessThreshold: 2,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    const result = await cb.execute(() => Promise.resolve('ok'));
    expect(result).toBe('ok');
    expect(cb.currentState).toBe('CLOSED');
  });

  it('resets failure counter on success', async () => {
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 3,
      resetTimeoutMs: 10_000,
      halfOpenSuccessThreshold: 2,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    // Two failures, then a success
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow('fail');
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow('fail');
    await cb.execute(() => Promise.resolve('ok'));
    // One more failure should not open (reset to 0 after the success)
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow('fail');
    expect(cb.currentState).toBe('CLOSED');
  });

  it('opens after failureThreshold consecutive failures', async () => {
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 3,
      resetTimeoutMs: 10_000,
      halfOpenSuccessThreshold: 2,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    for (let i = 0; i < 3; i++) {
      await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow();
    }
    expect(cb.currentState).toBe('OPEN');
  });
});

describe('CircuitBreaker — OPEN state', () => {
  it('throws CircuitBreakerError when OPEN', async () => {
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 1,
      resetTimeoutMs: 10_000,
      halfOpenSuccessThreshold: 1,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    // Trip the circuit
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow();
    expect(cb.currentState).toBe('OPEN');

    await expect(cb.execute(() => Promise.resolve('should not run'))).rejects.toBeInstanceOf(CircuitBreakerError);
  });

  it('transitions to HALF_OPEN after resetTimeoutMs', async () => {
    vi.useFakeTimers();
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 1,
      resetTimeoutMs: 5_000,
      halfOpenSuccessThreshold: 1,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow();
    expect(cb.currentState).toBe('OPEN');

    vi.advanceTimersByTime(5_001);

    const result = await cb.execute(() => Promise.resolve('probe ok'));
    expect(result).toBe('probe ok');
    // After one success in HALF_OPEN (threshold=1), should be CLOSED
    expect(cb.currentState).toBe('CLOSED');
    vi.useRealTimers();
  });

  it('stays OPEN if resetTimeoutMs has not elapsed', async () => {
    vi.useFakeTimers();
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 1,
      resetTimeoutMs: 5_000,
      halfOpenSuccessThreshold: 1,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow();

    vi.advanceTimersByTime(4_000);

    await expect(cb.execute(() => Promise.resolve('no'))).rejects.toBeInstanceOf(CircuitBreakerError);
    expect(cb.currentState).toBe('OPEN');
    vi.useRealTimers();
  });
});

describe('CircuitBreaker — HALF_OPEN state', () => {
  it('returns to OPEN if probe fails in HALF_OPEN', async () => {
    vi.useFakeTimers();
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 1,
      resetTimeoutMs: 1_000,
      halfOpenSuccessThreshold: 2,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow();
    vi.advanceTimersByTime(1_001);

    // probe fails — back to OPEN
    await expect(cb.execute(() => Promise.reject(new Error('probe fail')))).rejects.toThrow('probe fail');
    expect(cb.currentState).toBe('OPEN');
    vi.useRealTimers();
  });

  it('requires halfOpenSuccessThreshold successes before closing', async () => {
    vi.useFakeTimers();
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 1,
      resetTimeoutMs: 1_000,
      halfOpenSuccessThreshold: 2,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,
    });
    await expect(cb.execute(() => Promise.reject(new Error('fail')))).rejects.toThrow();
    vi.advanceTimersByTime(1_001);

    // First success in HALF_OPEN — not yet CLOSED
    await cb.execute(() => Promise.resolve('1'));
    expect(cb.currentState).toBe('HALF_OPEN');

    // Second success — now CLOSED
    await cb.execute(() => Promise.resolve('2'));
    expect(cb.currentState).toBe('CLOSED');
    vi.useRealTimers();
  });
});

describe('CircuitBreaker — call timeout', () => {
  it('rejects if call exceeds callTimeoutMs', async () => {
    vi.useFakeTimers();
    const cb = new CircuitBreaker(uniqueName('cb'), {
      failureThreshold: 5,
      resetTimeoutMs: 10_000,
      halfOpenSuccessThreshold: 1,
      countTimeoutAsFailure: true,
      callTimeoutMs: 500,
    });

    const neverResolves = new Promise<string>(() => { /* hangs */ });

    const callPromise = cb.execute(() => neverResolves);
    vi.advanceTimersByTime(600);
    await expect(callPromise).rejects.toThrow('timed out');
    vi.useRealTimers();
  });
});

describe('CircuitBreakerError', () => {
  it('has the correct name and message', () => {
    const err = new CircuitBreakerError('test-service');
    expect(err.name).toBe('CircuitBreakerError');
    expect(err.message).toContain('test-service');
    expect(err.serviceName).toBe('test-service');
  });
});

describe('CircuitBreakerFactory', () => {
  it('createFastAPI returns a CircuitBreaker with fast settings', () => {
    const name = uniqueName('fast');
    const cb = CircuitBreakerFactory.createFastAPI(name);
    expect(cb).toBeInstanceOf(CircuitBreaker);
    expect(cb.name).toBe(name);
  });

  it('createSlowAPI returns a CircuitBreaker', () => {
    const cb = CircuitBreakerFactory.createSlowAPI(uniqueName('slow'));
    expect(cb).toBeInstanceOf(CircuitBreaker);
  });

  it('createCritical returns a CircuitBreaker', () => {
    const cb = CircuitBreakerFactory.createCritical(uniqueName('crit'));
    expect(cb).toBeInstanceOf(CircuitBreaker);
  });

  it('returns the same instance on repeated calls with the same name', () => {
    const name = uniqueName('same');
    const cb1 = CircuitBreakerFactory.createFastAPI(name);
    const cb2 = CircuitBreakerFactory.createFastAPI(name);
    expect(cb1).toBe(cb2);
  });

  it('get() returns an existing breaker by name', () => {
    const name = uniqueName('get');
    const cb = CircuitBreakerFactory.createFastAPI(name);
    expect(CircuitBreakerFactory.get(name)).toBe(cb);
  });

  it('get() returns undefined for unknown names', () => {
    expect(CircuitBreakerFactory.get('definitely-unknown-' + Date.now())).toBeUndefined();
  });
});
