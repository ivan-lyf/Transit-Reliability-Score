import { useQuery } from '@tanstack/react-query';
import { score } from '../api/endpoints';
import { ApiError } from '../api/client';
import type { ApiScore } from '../types/api';
import type { DayType, HourBucket } from '@transit/shared-types';

export function useScore(
  stopId: string | null,
  routeId: string | null,
  dayType: DayType,
  hourBucket: HourBucket,
) {
  return useQuery<ApiScore | null>({
    queryKey: ['score', stopId, routeId, dayType, hourBucket],
    queryFn: async () => {
      try {
        return await score(stopId!, routeId!, dayType, hourBucket);
      } catch (err) {
        if (err instanceof ApiError && err.statusCode === 404) return null;
        throw err;
      }
    },
    enabled: stopId !== null && routeId !== null,
    staleTime: 60_000,
  });
}
