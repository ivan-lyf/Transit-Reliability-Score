/**
 * Typed API endpoint functions.
 * All paths match the Stage 7 backend exactly.
 */

import { env } from '../config/env';
import { apiFetch } from './client';
import type {
  ApiNearbyStopsResponse,
  ApiStopRoutesResponse,
  ApiScore,
  ApiNearbyRiskyResponse,
  ApiTrendResponse,
  ApiLastIngestResponse,
} from '../types/api';
import type { DayType, HourBucket } from '@transit/shared-types';

type QueryValue = string | number | boolean | undefined;

function buildUrl(path: string, params?: Record<string, QueryValue>): string {
  const base = `${env.apiUrl}${path}`;
  if (!params) return base;
  const qs = Object.entries(params)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
    .join('&');
  return qs ? `${base}?${qs}` : base;
}

export function nearbyStops(
  lat: number,
  lon: number,
  radiusKm = 0.75,
  limit = 50,
): Promise<ApiNearbyStopsResponse> {
  return apiFetch<ApiNearbyStopsResponse>(
    buildUrl('/stops/nearby', { lat, lon, radius_km: radiusKm, limit }),
  );
}

export function stopRoutes(stopId: string): Promise<ApiStopRoutesResponse> {
  return apiFetch<ApiStopRoutesResponse>(
    buildUrl(`/stops/${encodeURIComponent(stopId)}/routes`),
  );
}

export function score(
  stopId: string,
  routeId: string,
  dayType: DayType,
  hourBucket: HourBucket,
): Promise<ApiScore> {
  return apiFetch<ApiScore>(
    buildUrl('/scores', {
      stop_id: stopId,
      route_id: routeId,
      day_type: dayType,
      hour_bucket: hourBucket,
    }),
  );
}

export function nearbyRisky(
  lat: number,
  lon: number,
  radiusKm = 1.5,
  dayType?: DayType,
  hourBucket?: HourBucket,
  limit = 20,
): Promise<ApiNearbyRiskyResponse> {
  return apiFetch<ApiNearbyRiskyResponse>(
    buildUrl('/scores/nearby-risky', {
      lat,
      lon,
      radius_km: radiusKm,
      day_type: dayType,
      hour_bucket: hourBucket,
      limit,
    }),
  );
}

export function trend(
  stopId: string,
  routeId: string,
  days = 14,
): Promise<ApiTrendResponse> {
  return apiFetch<ApiTrendResponse>(
    buildUrl('/scores/trend', { stop_id: stopId, route_id: routeId, days }),
  );
}

export function lastIngest(): Promise<ApiLastIngestResponse> {
  return apiFetch<ApiLastIngestResponse>(buildUrl('/meta/last-ingest'));
}
