import React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, waitFor } from '@testing-library/react-native';

jest.mock('expo-constants', () => ({
  __esModule: true,
  default: {
    expoConfig: {
      extra: {
        API_URL: 'http://test-api.example.com',
      },
    },
  },
}));

jest.mock('../src/services/api', () => ({
  api: {
    health: jest.fn().mockResolvedValue({
      service: 'Transit Reliability Score API',
      status: 'healthy',
      version: '0.1.0',
      environment: 'development',
      timestamp: new Date().toISOString(),
      checks: {
        database: true,
        gtfsRt: true,
      },
      issues: [],
    }),
  },
}));

import HomeScreen from '../app/index';

describe('HomeScreen', () => {
  it('renders the main title', async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    const { getByText } = render(
      <QueryClientProvider client={queryClient}>
        <HomeScreen />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(getByText('Transit Reliability Score')).toBeTruthy();
    });
  });
});
