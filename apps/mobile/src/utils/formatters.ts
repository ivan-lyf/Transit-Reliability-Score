/**
 * Formatting utilities for display values.
 */

/**
 * Format delay seconds as a signed string: "+2m 10s", "-30s", "+0s".
 * Positive = late (delayed), negative = early.
 */
export function formatDelay(seconds: number): string {
  const abs = Math.abs(Math.round(seconds));
  const sign = seconds >= 0 ? '+' : '-';

  if (abs < 60) {
    return `${sign}${abs}s`;
  }

  const mins = Math.floor(abs / 60);
  const secs = abs % 60;

  if (secs === 0) {
    return `${sign}${mins}m`;
  }

  return `${sign}${mins}m ${secs}s`;
}

/**
 * Format on_time_rate (0–1 fraction) as a percentage string: "82%".
 */
export function formatOnTimeRate(rate: number): string {
  return `${Math.round(rate * 100)}%`;
}

/**
 * Return a hex color for a reliability score (0–100).
 * 80-100 → green, 60-79 → lime, 40-59 → orange, 0-39 → red.
 */
export function scoreColor(score: number): string {
  if (score >= 80) return '#4ade80';
  if (score >= 60) return '#a3e635';
  if (score >= 40) return '#f97316';
  return '#ef4444';
}

/**
 * Format distance in metres: "350m", "1.2km".
 */
export function formatDistance(metres: number): string {
  if (metres < 1000) {
    return `${Math.round(metres)}m`;
  }
  return `${(metres / 1000).toFixed(1)}km`;
}

/**
 * Format an ISO datetime string to a local-time string in America/Vancouver.
 * Example: "Feb 18, 10:30 AM"
 */
export function formatUpdatedAt(isoString: string): string {
  try {
    const date = new Date(isoString);
    return date.toLocaleString('en-CA', {
      timeZone: 'America/Vancouver',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return isoString;
  }
}

/**
 * Format a service date string "YYYY-MM-DD" to short display "Feb 10".
 */
export function formatServiceDate(dateStr: string): string {
  try {
    const d = new Date(`${dateStr}T00:00:00`);
    return d.toLocaleDateString('en-CA', { month: 'short', day: 'numeric' });
  } catch {
    return dateStr;
  }
}
