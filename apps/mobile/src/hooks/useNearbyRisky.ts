import { useQuery } from '@tanstack/react-query';
import { nearbyRisky } from '../api/endpoints';
import type { ApiNearbyRiskyResponse } from '../types/api';
import type { DayType, HourBucket } from '@transit/shared-types';

export function useNearbyRisky(
  lat: number | null,
  lon: number | null,
  dayType?: DayType,
  hourBucket?: HourBucket,
  radiusKm = 1.5,
) {
  return useQuery<ApiNearbyRiskyResponse>({
    queryKey: ['nearby-risky', lat, lon, dayType, hourBucket, radiusKm],
    queryFn: () => nearbyRisky(lat!, lon!, radiusKm, dayType, hourBucket),
    enabled: lat !== null && lon !== null,
    staleTime: 60_000,
  });
}
