import { ScrollView, TouchableOpacity, Text, StyleSheet, View } from 'react-native';
import type { DayType, HourBucket } from '@transit/shared-types';
import { DAY_TYPES, HOUR_BUCKETS, DAY_TYPE_LABELS, HOUR_BUCKET_LABELS } from '../utils/time';

interface Props {
  dayType: DayType;
  hourBucket: HourBucket;
  onDayTypeChange: (d: DayType) => void;
  onHourBucketChange: (h: HourBucket) => void;
}

export function FilterChips({
  dayType,
  hourBucket,
  onDayTypeChange,
  onHourBucketChange,
}: Props): JSX.Element {
  return (
    <View style={styles.wrapper}>
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.row}
        testID="filter-chips-day"
      >
        {DAY_TYPES.map((d) => (
          <TouchableOpacity
            key={d}
            onPress={() => onDayTypeChange(d)}
            style={[styles.chip, d === dayType && styles.chipActive]}
            accessibilityRole="button"
            accessibilityState={{ selected: d === dayType }}
          >
            <Text style={[styles.chipText, d === dayType && styles.chipTextActive]}>
              {DAY_TYPE_LABELS[d]}
            </Text>
          </TouchableOpacity>
        ))}
      </ScrollView>

      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.row}
        testID="filter-chips-hour"
      >
        {HOUR_BUCKETS.map((h) => (
          <TouchableOpacity
            key={h}
            onPress={() => onHourBucketChange(h)}
            style={[styles.chip, h === hourBucket && styles.chipActive]}
            accessibilityRole="button"
            accessibilityState={{ selected: h === hourBucket }}
          >
            <Text style={[styles.chipText, h === hourBucket && styles.chipTextActive]}>
              {HOUR_BUCKET_LABELS[h]}
            </Text>
          </TouchableOpacity>
        ))}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    gap: 6,
  },
  row: {
    flexDirection: 'row',
    paddingHorizontal: 16,
    gap: 8,
  },
  chip: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderRadius: 20,
    backgroundColor: '#2d2d44',
    borderWidth: 1,
    borderColor: '#3d3d5c',
  },
  chipActive: {
    backgroundColor: '#4ade80',
    borderColor: '#4ade80',
  },
  chipText: {
    fontSize: 13,
    color: '#a0aec0',
    fontWeight: '500',
  },
  chipTextActive: {
    color: '#0f172a',
    fontWeight: '700',
  },
});
