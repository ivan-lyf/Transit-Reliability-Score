/**
 * API client for Transit Reliability Score backend.
 */

import type { HealthResponse } from '@transit/shared-types';

import { env } from '../config/env';

class ApiError extends Error {
  constructor(
    public statusCode: number,
    message: string,
    public details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE';
  body?: unknown;
  headers?: Record<string, string>;
}

async function request<T>(
  endpoint: string,
  options: RequestOptions = {}
): Promise<T> {
  const { method = 'GET', body, headers = {} } = options;

  const url = `${env.apiUrl}${endpoint}`;

  const response = await fetch(url, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const errorData = (await response.json().catch(() => ({}))) as {
      message?: string;
      details?: Record<string, unknown>;
    };
    throw new ApiError(
      response.status,
      errorData.message ?? `Request failed with status ${response.status}`,
      errorData.details
    );
  }

  return response.json() as Promise<T>;
}

export const api = {
  health: (): Promise<HealthResponse> => request<HealthResponse>('/health'),

  attribution: (): Promise<{ attribution: string; termsUrl: string }> =>
    request('/meta/attribution'),
};

export { ApiError };
