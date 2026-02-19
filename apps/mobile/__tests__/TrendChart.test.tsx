import React from 'react';
import { render } from '@testing-library/react-native';
import { TrendChart } from '../src/components/TrendChart';
import type { ApiTrendPoint } from '../src/types/api';

const MOCK_SERIES: ApiTrendPoint[] = [
  { service_date: '2026-02-10', score: 72, sample_n: 40, on_time_rate: 0.78, p50_delay_sec: 50, p95_delay_sec: 200 },
  { service_date: '2026-02-11', score: 68, sample_n: 35, on_time_rate: 0.74, p50_delay_sec: 65, p95_delay_sec: 250 },
  { service_date: '2026-02-12', score: 80, sample_n: 42, on_time_rate: 0.85, p50_delay_sec: 30, p95_delay_sec: 150 },
];

describe('TrendChart', () => {
  it('shows empty state when series is empty', () => {
    const { getByTestId } = render(<TrendChart series={[]} />);
    expect(getByTestId('trend-chart-empty')).toBeTruthy();
  });

  it('renders the chart when series has data', () => {
    const { getByTestId } = render(<TrendChart series={MOCK_SERIES} />);
    expect(getByTestId('trend-chart')).toBeTruthy();
  });
});
