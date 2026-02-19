import { useQuery } from '@tanstack/react-query';
import { lastIngest } from '../api/endpoints';
import type { ApiLastIngestResponse } from '../types/api';

export function useLastIngest() {
  return useQuery<ApiLastIngestResponse>({
    queryKey: ['last-ingest'],
    queryFn: lastIngest,
    staleTime: 300_000,
  });
}
