export interface BackoffOptions {
  /** Ceiling of the first retry. */
  baseMs: number;
  /** The ceiling never grows beyond this. */
  maxMs: number;
  /** Lower bound, so a retry is never immediate. */
  minMs: number;
  /** Injectable for tests. Must return a number in [0, 1). */
  random: () => number;
}

export const DEFAULT_BACKOFF: BackoffOptions = {
  baseMs: 500,
  maxMs: 30_000,
  minMs: 250,
  random: Math.random,
};

/**
 * Exponential backoff with full jitter: a random delay in [minMs, min(maxMs, baseMs * 2^attempt)].
 *
 * The exponential part stops a client from hammering a server that is down. The random part
 * matters when many clients lose the connection at the same moment (e.g. the server restarted):
 * without it they would all retry at exactly the same moments and knock it over again.
 */
export function backoffDelay(attempt: number, options: Partial<BackoffOptions> = {}): number {
  const { baseMs, maxMs, minMs, random } = { ...DEFAULT_BACKOFF, ...options };
  const ceiling = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt));
  return Math.round(Math.max(minMs, random() * ceiling));
}
