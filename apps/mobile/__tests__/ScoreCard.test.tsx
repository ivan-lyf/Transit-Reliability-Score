import React from 'react';
import { render, fireEvent } from '@testing-library/react-native';
import { ScoreCard } from '../src/components/ScoreCard';
import type { ApiScore } from '../src/types/api';

const MOCK_SCORE: ApiScore = {
  stop_id: '55555',
  route_id: 'R049',
  day_type: 'weekday',
  hour_bucket: '9-12',
  on_time_rate: 0.82,
  p50_delay_sec: 45,
  p95_delay_sec: 180,
  score: 76,
  sample_n: 120,
  updated_at: '2026-02-18T10:00:00Z',
  low_confidence: false,
};

describe('ScoreCard', () => {
  it('renders the numeric score', () => {
    const { getByText } = render(<ScoreCard score={MOCK_SCORE} />);
    expect(getByText('76')).toBeTruthy();
  });

  it('renders on-time rate', () => {
    const { getByText } = render(<ScoreCard score={MOCK_SCORE} />);
    expect(getByText('82%')).toBeTruthy();
  });

  it('renders median delay', () => {
    const { getByText } = render(<ScoreCard score={MOCK_SCORE} />);
    expect(getByText('+45s')).toBeTruthy();
  });

  it('renders 95th pct delay', () => {
    const { getByText } = render(<ScoreCard score={MOCK_SCORE} />);
    expect(getByText('+3m')).toBeTruthy();
  });

  it('renders sample count', () => {
    const { getByText } = render(<ScoreCard score={MOCK_SCORE} />);
    expect(getByText('120')).toBeTruthy();
  });

  it('does not show low-confidence warning when flag is false', () => {
    const { queryByText } = render(<ScoreCard score={MOCK_SCORE} />);
    expect(queryByText(/Low sample size/)).toBeNull();
  });

  it('shows low-confidence warning when flag is true', () => {
    const { getByText } = render(
      <ScoreCard score={{ ...MOCK_SCORE, low_confidence: true }} />,
    );
    expect(getByText(/Low sample size/)).toBeTruthy();
  });

  it('opens the explainer modal on press', () => {
    const { getByTestId, getByText } = render(<ScoreCard score={MOCK_SCORE} />);
    fireEvent.press(getByTestId('score-card'));
    expect(getByText('Reliability Score')).toBeTruthy();
  });
});
