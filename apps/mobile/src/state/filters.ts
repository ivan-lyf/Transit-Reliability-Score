/**
 * Global filter state: day type + hour bucket.
 * Initialised from the device clock so the app shows the current time window.
 */

import { createContext, useContext } from 'react';
import type { DayType, HourBucket } from '@transit/shared-types';
import { currentDayType, currentHourBucket } from '../utils/time';

export interface FiltersState {
  dayType: DayType;
  hourBucket: HourBucket;
  setDayType: (d: DayType) => void;
  setHourBucket: (h: HourBucket) => void;
}

export const FiltersContext = createContext<FiltersState | null>(null);

/** Returns the filter context; throws if used outside <FiltersProvider>. */
export function useFilters(): FiltersState {
  const ctx = useContext(FiltersContext);
  if (!ctx) throw new Error('useFilters must be used inside FiltersProvider');
  return ctx;
}

/** Default hour bucket used when the device clock is outside service hours. */
export const DEFAULT_HOUR_BUCKET: HourBucket = '9-12';

/** Create the initial state values for FiltersProvider. */
export function createInitialFilters(): { dayType: DayType; hourBucket: HourBucket } {
  return {
    dayType: currentDayType(),
    hourBucket: currentHourBucket() ?? DEFAULT_HOUR_BUCKET,
  };
}
