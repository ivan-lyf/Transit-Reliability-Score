import { useQuery } from '@tanstack/react-query';
import { trend } from '../api/endpoints';
import type { ApiTrendResponse } from '../types/api';

export function useTrend(stopId: string | null, routeId: string | null, days = 14) {
  return useQuery<ApiTrendResponse>({
    queryKey: ['trend', stopId, routeId, days],
    queryFn: () => trend(stopId!, routeId!, days),
    enabled: stopId !== null && routeId !== null,
    staleTime: 60_000,
  });
}
