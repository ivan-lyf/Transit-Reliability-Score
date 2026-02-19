import { useQuery } from '@tanstack/react-query';
import { stopRoutes } from '../api/endpoints';
import type { ApiStopRoutesResponse } from '../types/api';

export function useStopRoutes(stopId: string | null) {
  return useQuery<ApiStopRoutesResponse>({
    queryKey: ['stop-routes', stopId],
    queryFn: () => stopRoutes(stopId!),
    enabled: stopId !== null,
    staleTime: 300_000,
  });
}
