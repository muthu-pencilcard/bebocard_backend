/**
 * In-memory circuit breaker for external service calls.
 *
 * State resets on Lambda cold start — that's intentional.
 * The breaker protects the current container from hammering a degraded service
 * within its lifetime. Across containers, natural backoff via Lambda's concurrency
 * scaling provides further protection.
 *
 * Usage:
 *   const fcmBreaker = CircuitBreakerFactory.createFastAPI('firebase-fcm');
 *   const result = await fcmBreaker.execute(() => messaging.send(msg));
 */

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitBreakerOptions {
  /** Number of consecutive failures before opening the circuit */
  failureThreshold: number;
  /** Milliseconds to wait in OPEN state before attempting HALF_OPEN probe */
  resetTimeoutMs: number;
  /** Number of consecutive successes in HALF_OPEN to return to CLOSED */
  halfOpenSuccessThreshold: number;
  /** If true, timeouts count as failures */
  countTimeoutAsFailure: boolean;
  /** Optional timeout per call in milliseconds (0 = no timeout) */
  callTimeoutMs: number;
}

export class CircuitBreakerError extends Error {
  constructor(public readonly serviceName: string) {
    super(`Circuit open for ${serviceName} — call rejected`);
    this.name = 'CircuitBreakerError';
  }
}

export class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private failures = 0;
  private halfOpenSuccesses = 0;
  private lastFailureAt = 0;

  constructor(
    public readonly name: string,
    private readonly opts: CircuitBreakerOptions,
  ) {}

  get currentState(): CircuitState { return this.state; }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    this.transitionIfNeeded();

    if (this.state === 'OPEN') {
      throw new CircuitBreakerError(this.name);
    }

    let callPromise = fn();
    if (this.opts.callTimeoutMs > 0) {
      callPromise = Promise.race([
        callPromise,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`${this.name} call timed out after ${this.opts.callTimeoutMs}ms`)), this.opts.callTimeoutMs),
        ),
      ]);
    }

    try {
      const result = await callPromise;
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    }
  }

  private transitionIfNeeded(): void {
    if (this.state === 'OPEN') {
      const elapsed = Date.now() - this.lastFailureAt;
      if (elapsed >= this.opts.resetTimeoutMs) {
        this.state = 'HALF_OPEN';
        this.halfOpenSuccesses = 0;
        console.info(`[circuit-breaker] ${this.name}: OPEN → HALF_OPEN after ${elapsed}ms`);
      }
    }
  }

  private onSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.opts.halfOpenSuccessThreshold) {
        this.state = 'CLOSED';
        this.failures = 0;
        this.halfOpenSuccesses = 0;
        console.info(`[circuit-breaker] ${this.name}: HALF_OPEN → CLOSED`);
      }
    } else {
      this.failures = 0;
    }
  }

  private onFailure(): void {
    this.lastFailureAt = Date.now();
    this.failures++;
    if (this.state === 'HALF_OPEN') {
      this.state = 'OPEN';
      console.warn(`[circuit-breaker] ${this.name}: HALF_OPEN → OPEN (probe failed)`);
    } else if (this.failures >= this.opts.failureThreshold) {
      this.state = 'OPEN';
      console.warn(`[circuit-breaker] ${this.name}: CLOSED → OPEN after ${this.failures} failures`);
    }
  }
}

/**
 * Factory for pre-configured circuit breakers by service type.
 * Instances are cached by name — call these at module level.
 */
const _registry = new Map<string, CircuitBreaker>();

export const CircuitBreakerFactory = {
  /**
   * Fast external API (FCM, etc.): trips quickly, resets quickly.
   * Use for high-frequency, latency-sensitive calls.
   */
  createFastAPI(name: string): CircuitBreaker {
    if (_registry.has(name)) return _registry.get(name)!;
    const cb = new CircuitBreaker(name, {
      failureThreshold: 5,
      resetTimeoutMs: 30_000,       // 30s
      halfOpenSuccessThreshold: 2,
      countTimeoutAsFailure: true,
      callTimeoutMs: 8_000,         // 8s per call
    });
    _registry.set(name, cb);
    return cb;
  },

  /**
   * Slow external API (Stripe, distributors): more tolerance, longer reset.
   * Use for payment and fulfillment calls where retries are expensive.
   */
  createSlowAPI(name: string): CircuitBreaker {
    if (_registry.has(name)) return _registry.get(name)!;
    const cb = new CircuitBreaker(name, {
      failureThreshold: 3,
      resetTimeoutMs: 60_000,       // 60s
      halfOpenSuccessThreshold: 1,
      countTimeoutAsFailure: true,
      callTimeoutMs: 15_000,        // 15s per call
    });
    _registry.set(name, cb);
    return cb;
  },

  /**
   * Critical path (Cognito JWKS, DynamoDB): very tolerant, long reset.
   * Use for dependencies whose failure should degrade gracefully.
   */
  createCritical(name: string): CircuitBreaker {
    if (_registry.has(name)) return _registry.get(name)!;
    const cb = new CircuitBreaker(name, {
      failureThreshold: 10,
      resetTimeoutMs: 120_000,      // 2 min
      halfOpenSuccessThreshold: 3,
      countTimeoutAsFailure: false,
      callTimeoutMs: 0,             // no per-call timeout on critical paths
    });
    _registry.set(name, cb);
    return cb;
  },

  get(name: string): CircuitBreaker | undefined {
    return _registry.get(name);
  },
};
