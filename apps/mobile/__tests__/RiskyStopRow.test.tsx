import React from 'react';
import { render, fireEvent } from '@testing-library/react-native';
import { RiskyStopRow } from '../src/components/RiskyStopRow';
import type { ApiRiskyStop } from '../src/types/api';

const mockPush = jest.fn();

jest.mock('expo-router', () => ({
  useRouter: () => ({ push: mockPush }),
}));

const MOCK_ITEM: ApiRiskyStop = {
  stop_id: '55555',
  stop_name: 'Granville & Robson',
  lat: 49.282,
  lon: -123.119,
  route_id: 'R049',
  day_type: 'weekday',
  hour_bucket: '9-12',
  score: 42,
  on_time_rate: 0.61,
  sample_n: 80,
  distance_m: 320,
  updated_at: '2026-02-18T10:00:00Z',
};

describe('RiskyStopRow', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders stop name', () => {
    const { getByText } = render(<RiskyStopRow item={MOCK_ITEM} rank={1} />);
    expect(getByText('Granville & Robson')).toBeTruthy();
  });

  it('renders the score', () => {
    const { getByText } = render(<RiskyStopRow item={MOCK_ITEM} rank={1} />);
    expect(getByText('42')).toBeTruthy();
  });

  it('renders rank number', () => {
    const { getByText } = render(<RiskyStopRow item={MOCK_ITEM} rank={3} />);
    expect(getByText('3')).toBeTruthy();
  });

  it('renders distance', () => {
    const { getByText } = render(<RiskyStopRow item={MOCK_ITEM} rank={1} />);
    expect(getByText(/320m/)).toBeTruthy();
  });

  it('renders on-time rate', () => {
    const { getByText } = render(<RiskyStopRow item={MOCK_ITEM} rank={1} />);
    expect(getByText(/61% on-time/)).toBeTruthy();
  });

  it('navigates to stop detail on press', () => {
    const { getByTestId } = render(<RiskyStopRow item={MOCK_ITEM} rank={1} />);
    fireEvent.press(getByTestId('risky-stop-row-55555'));
    expect(mockPush).toHaveBeenCalledWith({
      pathname: '/stop/[id]',
      params: { id: '55555', routeId: 'R049' },
    });
  });
});
