// Core domain types for Transit Reliability Score

export type DayType = 'weekday' | 'saturday' | 'sunday';

export type HourBucket = '6-9' | '9-12' | '12-15' | '15-18' | '18-21';

export const HOUR_BUCKETS: readonly HourBucket[] = [
  '6-9',
  '9-12',
  '12-15',
  '15-18',
  '18-21',
] as const;

export const DAY_TYPES: readonly DayType[] = [
  'weekday',
  'saturday',
  'sunday',
] as const;

export interface Stop {
  stopId: string;
  name: string;
  lat: number;
  lon: number;
}

export interface Route {
  routeId: string;
  shortName: string;
  longName: string;
}

export interface Trip {
  tripId: string;
  routeId: string;
  serviceId: string;
  directionId: number;
}

export interface StopTime {
  tripId: string;
  stopId: string;
  stopSequence: number;
  schedArrivalSec: number;
}

export interface RealtimeObservation {
  id: number;
  tripId: string;
  stopId: string;
  observedTs: string;
  delaySec: number;
  sourceTs: string;
}

export interface ScoreAggregate {
  stopId: string;
  routeId: string;
  dayType: DayType;
  hourBucket: HourBucket;
  onTimeRate: number;
  p50DelaySec: number;
  p95DelaySec: number;
  score: number;
  sampleN: number;
  updatedAt: string;
}

export interface User {
  id: string;
  authId: string;
  favorites: UserFavorites;
}

export interface UserFavorites {
  stops: string[];
  routes: string[];
}

export interface NearbyStop extends Stop {
  distanceKm: number;
}

export interface RiskyStop extends NearbyStop {
  routeId: string;
  routeShortName: string;
  score: number;
  onTimeRate: number;
}

export interface TrendDataPoint {
  date: string;
  score: number;
  onTimeRate: number;
  p50DelaySec: number;
  sampleN: number;
}

export interface LastIngestMeta {
  tripUpdates: string | null;
  vehiclePositions: string | null;
  serviceAlerts: string | null;
  staticGtfs: string | null;
}

export interface HealthResponse {
  service: string;
  status: 'healthy' | 'degraded' | 'unhealthy';
  version: string;
  environment: string;
  timestamp: string;
  checks: {
    database: boolean;
    gtfsRt: boolean;
  };
  issues?: string[];
}

export interface ApiError {
  error: string;
  message: string;
  details?: Record<string, unknown>;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

export interface NearbyStopsRequest {
  lat: number;
  lon: number;
  radiusKm: number;
}

export interface ScoreQuery {
  stopId: string;
  routeId: string;
  dayType: DayType;
  hourBucket: HourBucket;
}

export interface NearbyRiskyRequest {
  lat: number;
  lon: number;
  radiusKm: number;
  limit?: number;
  dayType?: DayType;
  hourBucket?: HourBucket;
}

export interface TrendRequest {
  stopId: string;
  routeId: string;
  days?: number;
}
