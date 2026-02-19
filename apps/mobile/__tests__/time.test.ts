import {
  currentDayType,
  currentHourBucket,
  DAY_TYPE_LABELS,
  HOUR_BUCKET_LABELS,
} from '../src/utils/time';

function mockDate(day: number, hour: number): void {
  jest.spyOn(global, 'Date').mockImplementation(
    () =>
      ({
        getDay: () => day,
        getHours: () => hour,
      }) as unknown as Date,
  );
}

afterEach(() => {
  jest.restoreAllMocks();
});

describe('currentDayType', () => {
  it('returns weekday on Monday (1)', () => {
    mockDate(1, 9);
    expect(currentDayType()).toBe('weekday');
  });

  it('returns weekday on Friday (5)', () => {
    mockDate(5, 9);
    expect(currentDayType()).toBe('weekday');
  });

  it('returns saturday on day 6', () => {
    mockDate(6, 9);
    expect(currentDayType()).toBe('saturday');
  });

  it('returns sunday on day 0', () => {
    mockDate(0, 9);
    expect(currentDayType()).toBe('sunday');
  });
});

describe('currentHourBucket', () => {
  it('returns 6-9 at hour 6', () => {
    mockDate(1, 6);
    expect(currentHourBucket()).toBe('6-9');
  });

  it('returns 6-9 at hour 8', () => {
    mockDate(1, 8);
    expect(currentHourBucket()).toBe('6-9');
  });

  it('returns 9-12 at hour 10', () => {
    mockDate(1, 10);
    expect(currentHourBucket()).toBe('9-12');
  });

  it('returns 12-15 at hour 13', () => {
    mockDate(1, 13);
    expect(currentHourBucket()).toBe('12-15');
  });

  it('returns 15-18 at hour 16', () => {
    mockDate(1, 16);
    expect(currentHourBucket()).toBe('15-18');
  });

  it('returns 18-21 at hour 19', () => {
    mockDate(1, 19);
    expect(currentHourBucket()).toBe('18-21');
  });

  it('returns null at midnight', () => {
    mockDate(1, 0);
    expect(currentHourBucket()).toBeNull();
  });

  it('returns null at 5am', () => {
    mockDate(1, 5);
    expect(currentHourBucket()).toBeNull();
  });

  it('returns null at 22:00', () => {
    mockDate(1, 22);
    expect(currentHourBucket()).toBeNull();
  });
});

describe('DAY_TYPE_LABELS', () => {
  it('has labels for all day types', () => {
    expect(DAY_TYPE_LABELS.weekday).toBe('Weekday');
    expect(DAY_TYPE_LABELS.saturday).toBe('Saturday');
    expect(DAY_TYPE_LABELS.sunday).toBe('Sunday');
  });
});

describe('HOUR_BUCKET_LABELS', () => {
  it('has labels for all hour buckets', () => {
    expect(HOUR_BUCKET_LABELS['6-9']).toBe('6–9am');
    expect(HOUR_BUCKET_LABELS['9-12']).toBe('9–12pm');
    expect(HOUR_BUCKET_LABELS['12-15']).toBe('12–3pm');
    expect(HOUR_BUCKET_LABELS['15-18']).toBe('3–6pm');
    expect(HOUR_BUCKET_LABELS['18-21']).toBe('6–9pm');
  });
});
