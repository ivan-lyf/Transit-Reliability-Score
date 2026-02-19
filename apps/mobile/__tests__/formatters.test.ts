import {
  formatDelay,
  formatOnTimeRate,
  scoreColor,
  formatDistance,
  formatUpdatedAt,
  formatServiceDate,
} from '../src/utils/formatters';

describe('formatDelay', () => {
  it('formats zero seconds', () => {
    expect(formatDelay(0)).toBe('+0s');
  });

  it('formats positive seconds only', () => {
    expect(formatDelay(45)).toBe('+45s');
  });

  it('formats negative seconds', () => {
    expect(formatDelay(-30)).toBe('-30s');
  });

  it('formats exactly 60 seconds as 1m', () => {
    expect(formatDelay(60)).toBe('+1m');
  });

  it('formats minutes and seconds', () => {
    expect(formatDelay(130)).toBe('+2m 10s');
  });

  it('formats negative minutes', () => {
    expect(formatDelay(-90)).toBe('-1m 30s');
  });

  it('rounds fractional seconds', () => {
    expect(formatDelay(59.6)).toBe('+1m');
  });
});

describe('formatOnTimeRate', () => {
  it('formats 0 as 0%', () => {
    expect(formatOnTimeRate(0)).toBe('0%');
  });

  it('formats 1 as 100%', () => {
    expect(formatOnTimeRate(1)).toBe('100%');
  });

  it('formats 0.825 as 83% (rounds)', () => {
    expect(formatOnTimeRate(0.825)).toBe('83%');
  });

  it('formats 0.5 as 50%', () => {
    expect(formatOnTimeRate(0.5)).toBe('50%');
  });
});

describe('scoreColor', () => {
  it('returns green for score >= 80', () => {
    expect(scoreColor(80)).toBe('#4ade80');
    expect(scoreColor(100)).toBe('#4ade80');
  });

  it('returns lime for score 60-79', () => {
    expect(scoreColor(60)).toBe('#a3e635');
    expect(scoreColor(79)).toBe('#a3e635');
  });

  it('returns orange for score 40-59', () => {
    expect(scoreColor(40)).toBe('#f97316');
    expect(scoreColor(59)).toBe('#f97316');
  });

  it('returns red for score < 40', () => {
    expect(scoreColor(0)).toBe('#ef4444');
    expect(scoreColor(39)).toBe('#ef4444');
  });
});

describe('formatDistance', () => {
  it('formats metres for < 1000m', () => {
    expect(formatDistance(350)).toBe('350m');
    expect(formatDistance(999)).toBe('999m');
  });

  it('formats kilometres for >= 1000m', () => {
    expect(formatDistance(1000)).toBe('1.0km');
    expect(formatDistance(1500)).toBe('1.5km');
    expect(formatDistance(2340)).toBe('2.3km');
  });
});

describe('formatUpdatedAt', () => {
  it('returns fallback on invalid input', () => {
    expect(formatUpdatedAt('not-a-date')).toBe('not-a-date');
  });

  it('returns a non-empty string for valid ISO', () => {
    const result = formatUpdatedAt('2026-02-18T10:30:00Z');
    expect(typeof result).toBe('string');
    expect(result.length).toBeGreaterThan(0);
  });
});

describe('formatServiceDate', () => {
  it('returns short month + day', () => {
    const result = formatServiceDate('2026-02-18');
    expect(result).toMatch(/Feb/);
    expect(result).toMatch(/18/);
  });

  it('falls back to raw string on invalid input', () => {
    expect(formatServiceDate('bad')).toBe('bad');
  });
});
