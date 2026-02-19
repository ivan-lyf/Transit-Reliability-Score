import { useQuery } from '@tanstack/react-query';
import { nearbyStops } from '../api/endpoints';
import type { ApiNearbyStopsResponse } from '../types/api';

export function useNearbyStops(lat: number | null, lon: number | null, radiusKm = 0.75) {
  return useQuery<ApiNearbyStopsResponse>({
    queryKey: ['nearby-stops', lat, lon, radiusKm],
    queryFn: () => nearbyStops(lat!, lon!, radiusKm),
    enabled: lat !== null && lon !== null,
    staleTime: 60_000,
  });
}
