/**
 * Tests for API client.
 */

// Mock expo-constants before importing api
jest.mock('expo-constants', () => ({
  __esModule: true,
  default: {
    expoConfig: {
      extra: {
        API_URL: 'http://test-api.example.com',
        SUPABASE_URL: 'http://test.supabase.co',
        SUPABASE_ANON_KEY: 'test-key',
        MAPBOX_ACCESS_TOKEN: 'test-token',
        ENVIRONMENT: 'development',
      },
    },
  },
}));

import { api, ApiError } from '../src/services/api';

describe('API Client', () => {
  it('should export api object with health method', () => {
    expect(api).toBeDefined();
    expect(typeof api.health).toBe('function');
    expect(typeof api.attribution).toBe('function');
  });

  it('should export ApiError class', () => {
    expect(ApiError).toBeDefined();

    const error = new ApiError(404, 'Not found', { detail: 'test' });
    expect(error.statusCode).toBe(404);
    expect(error.message).toBe('Not found');
    expect(error.details).toEqual({ detail: 'test' });
  });
});
