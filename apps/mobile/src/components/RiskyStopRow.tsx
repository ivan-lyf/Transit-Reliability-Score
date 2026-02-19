import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';
import { useRouter } from 'expo-router';
import type { ApiRiskyStop } from '../types/api';
import { scoreColor, formatOnTimeRate, formatDistance } from '../utils/formatters';

interface Props {
  item: ApiRiskyStop;
  rank: number;
}

export function RiskyStopRow({ item, rank }: Props): JSX.Element {
  const router = useRouter();
  const color = scoreColor(item.score);

  function handlePress(): void {
    router.push({
      pathname: '/stop/[id]',
      params: { id: item.stop_id, routeId: item.route_id },
    });
  }

  return (
    <TouchableOpacity
      style={styles.row}
      onPress={handlePress}
      accessibilityRole="button"
      testID={`risky-stop-row-${item.stop_id}`}
    >
      <View style={styles.rank}>
        <Text style={styles.rankText}>{rank}</Text>
      </View>

      <View style={styles.info}>
        <Text style={styles.stopName} numberOfLines={1}>
          {item.stop_name}
        </Text>
        <Text style={styles.sub}>
          Route {item.route_id} · {formatDistance(item.distance_m)}
        </Text>
        <Text style={styles.sub}>{formatOnTimeRate(item.on_time_rate)} on-time</Text>
      </View>

      <Text style={[styles.score, { color }]}>{item.score}</Text>
    </TouchableOpacity>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
    gap: 12,
  },
  rank: {
    width: 28,
    height: 28,
    borderRadius: 14,
    backgroundColor: '#2d2d44',
    alignItems: 'center',
    justifyContent: 'center',
  },
  rankText: {
    color: '#a0aec0',
    fontSize: 12,
    fontWeight: '700',
  },
  info: {
    flex: 1,
    gap: 2,
  },
  stopName: {
    color: '#fff',
    fontSize: 15,
    fontWeight: '600',
  },
  sub: {
    color: '#6b7280',
    fontSize: 12,
  },
  score: {
    fontSize: 28,
    fontWeight: '800',
    minWidth: 44,
    textAlign: 'right',
  },
});
