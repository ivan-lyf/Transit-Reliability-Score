/**
 * API response types (snake_case, matching the Stage 7 backend exactly).
 * These are intentionally separate from @transit/shared-types so the
 * mobile app stays decoupled from any camelCase domain model changes.
 */

import type { DayType, HourBucket } from '@transit/shared-types';

// ---- Stops ---------------------------------------------------------------

export interface ApiStop {
  stop_id: string;
  name: string;
  lat: number;
  lon: number;
  distance_m?: number;
}

export interface ApiNearbyStopsResponse {
  items: ApiStop[];
  limit: number;
  offset: number;
  count: number;
}

export interface ApiRoute {
  route_id: string;
  short_name: string;
  long_name: string;
}

export interface ApiStopRoutesResponse {
  stop_id: string;
  routes: ApiRoute[];
}

// ---- Scores --------------------------------------------------------------

export interface ApiScore {
  stop_id: string;
  route_id: string;
  day_type: DayType;
  hour_bucket: HourBucket;
  on_time_rate: number;
  p50_delay_sec: number;
  p95_delay_sec: number;
  score: number;
  sample_n: number;
  updated_at: string;
  low_confidence: boolean;
}

export interface ApiRiskyStop {
  stop_id: string;
  stop_name: string;
  lat: number;
  lon: number;
  route_id: string;
  day_type: DayType;
  hour_bucket: HourBucket;
  score: number;
  on_time_rate: number;
  sample_n: number;
  distance_m: number;
  updated_at: string;
}

export interface ApiNearbyRiskyResponse {
  items: ApiRiskyStop[];
  limit: number;
  count: number;
}

export interface ApiTrendPoint {
  service_date: string;
  score: number;
  sample_n: number;
  on_time_rate: number;
  p50_delay_sec: number;
  p95_delay_sec: number;
}

export interface ApiTrendResponse {
  stop_id: string;
  route_id: string;
  days: number;
  series: ApiTrendPoint[];
}

// ---- Meta ----------------------------------------------------------------

export interface ApiFeedStatus {
  feed_type: string;
  status: string;
  last_success_at: string;
  last_attempt_at: string;
  error_message: string;
  entity_count: number;
  feed_hash: string;
  is_fresh: boolean;
}

export interface ApiLastIngestResponse {
  feeds: ApiFeedStatus[];
  stale_threshold_sec: number;
}
