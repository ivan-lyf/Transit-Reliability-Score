// Shared configuration constants

export const APP_NAME = 'Transit Reliability Score';
export const APP_VERSION = '0.1.0';

export const TRANSLINK_DATA_ATTRIBUTION =
  'Transit data provided by TransLink. This data is provided "as is" without warranty.';

export const TRANSLINK_TERMS_URL = 'https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/translink-open-api';

export const GTFS_FEEDS = {
  TRIP_UPDATES_URL: 'https://gtfsapi.translink.ca/v3/gtfsrealtime',
  VEHICLE_POSITIONS_URL: 'https://gtfsapi.translink.ca/v3/gtfsposition',
  SERVICE_ALERTS_URL: 'https://gtfsapi.translink.ca/v3/gtfsalerts',
  STATIC_GTFS_URL: 'https://gtfs-static.translink.ca',
} as const;

export const SCORING_CONFIG = {
  ON_TIME_THRESHOLD_SEC: 120,
  P95_MAX_DELAY_SEC: 900,
  P50_MAX_DELAY_SEC: 300,
  WEIGHTS: {
    ON_TIME_RATE: 0.6,
    P95_COMPONENT: 0.25,
    P50_COMPONENT: 0.15,
  },
} as const;

export const POLLING_INTERVALS = {
  GTFS_RT_SEC: 30,
  STALE_THRESHOLD_SEC: 120,
} as const;

export const API_DEFAULTS = {
  NEARBY_RADIUS_KM: 0.5,
  MAX_NEARBY_RADIUS_KM: 5,
  RISKY_STOPS_LIMIT: 10,
  TREND_DAYS: 7,
  PAGE_SIZE: 20,
  MAX_PAGE_SIZE: 100,
} as const;

export const METRO_VANCOUVER_BOUNDS = {
  minLat: 49.0,
  maxLat: 49.5,
  minLon: -123.3,
  maxLon: -122.5,
} as const;
