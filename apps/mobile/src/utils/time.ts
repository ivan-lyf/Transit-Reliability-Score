/**
 * Time and bucket utilities.
 */

import { DAY_TYPES, HOUR_BUCKETS } from '@transit/shared-types';
import type { DayType, HourBucket } from '@transit/shared-types';

/** Human-readable labels for DayType values. */
export const DAY_TYPE_LABELS: Record<DayType, string> = {
  weekday: 'Weekday',
  saturday: 'Saturday',
  sunday: 'Sunday',
};

/** Human-readable labels for HourBucket values. */
export const HOUR_BUCKET_LABELS: Record<HourBucket, string> = {
  '6-9': '6–9am',
  '9-12': '9–12pm',
  '12-15': '12–3pm',
  '15-18': '3–6pm',
  '18-21': '6–9pm',
};

/**
 * Infer the current DayType from the device's local clock.
 */
export function currentDayType(): DayType {
  const dow = new Date().getDay(); // 0=Sun, 6=Sat
  if (dow === 0) return 'sunday';
  if (dow === 6) return 'saturday';
  return 'weekday';
}

/**
 * Infer the current HourBucket from the device's local clock.
 * Returns null when the current time falls outside all service windows.
 */
export function currentHourBucket(): HourBucket | null {
  const hour = new Date().getHours();
  if (hour >= 6 && hour <= 8) return '6-9';
  if (hour >= 9 && hour <= 11) return '9-12';
  if (hour >= 12 && hour <= 14) return '12-15';
  if (hour >= 15 && hour <= 17) return '15-18';
  if (hour >= 18 && hour <= 20) return '18-21';
  return null;
}

// Re-export constants for consumers that only need to import from this module.
export { DAY_TYPES, HOUR_BUCKETS };
