import { useState, type ReactNode } from 'react';
import type { DayType, HourBucket } from '@transit/shared-types';
import { FiltersContext, createInitialFilters } from './filters';

interface Props {
  children: ReactNode;
}

export function FiltersProvider({ children }: Props): JSX.Element {
  const initial = createInitialFilters();
  const [dayType, setDayType] = useState<DayType>(initial.dayType);
  const [hourBucket, setHourBucket] = useState<HourBucket>(initial.hourBucket);

  return (
    <FiltersContext.Provider value={{ dayType, hourBucket, setDayType, setHourBucket }}>
      {children}
    </FiltersContext.Provider>
  );
}
