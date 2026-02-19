import React from 'react';
import { render, fireEvent } from '@testing-library/react-native';
import { FilterChips } from '../src/components/FilterChips';

describe('FilterChips', () => {
  const onDayTypeChange = jest.fn();
  const onHourBucketChange = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders all three day type chips', () => {
    const { getByText } = render(
      <FilterChips
        dayType="weekday"
        hourBucket="9-12"
        onDayTypeChange={onDayTypeChange}
        onHourBucketChange={onHourBucketChange}
      />,
    );
    expect(getByText('Weekday')).toBeTruthy();
    expect(getByText('Saturday')).toBeTruthy();
    expect(getByText('Sunday')).toBeTruthy();
  });

  it('renders all five hour bucket chips', () => {
    const { getByText } = render(
      <FilterChips
        dayType="weekday"
        hourBucket="9-12"
        onDayTypeChange={onDayTypeChange}
        onHourBucketChange={onHourBucketChange}
      />,
    );
    expect(getByText('6–9am')).toBeTruthy();
    expect(getByText('9–12pm')).toBeTruthy();
    expect(getByText('12–3pm')).toBeTruthy();
    expect(getByText('3–6pm')).toBeTruthy();
    expect(getByText('6–9pm')).toBeTruthy();
  });

  it('calls onDayTypeChange when a day chip is pressed', () => {
    const { getByText } = render(
      <FilterChips
        dayType="weekday"
        hourBucket="9-12"
        onDayTypeChange={onDayTypeChange}
        onHourBucketChange={onHourBucketChange}
      />,
    );
    fireEvent.press(getByText('Saturday'));
    expect(onDayTypeChange).toHaveBeenCalledWith('saturday');
  });

  it('calls onHourBucketChange when an hour chip is pressed', () => {
    const { getByText } = render(
      <FilterChips
        dayType="weekday"
        hourBucket="9-12"
        onDayTypeChange={onDayTypeChange}
        onHourBucketChange={onHourBucketChange}
      />,
    );
    fireEvent.press(getByText('3–6pm'));
    expect(onHourBucketChange).toHaveBeenCalledWith('15-18');
  });
});
