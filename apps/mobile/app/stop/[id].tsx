/**
 * StopDetailScreen — reliability score and trend for a specific stop + route.
 */

import { useState, useEffect } from 'react';
import {
  View,
  Text,
  ScrollView,
  RefreshControl,
  TouchableOpacity,
  StyleSheet,
} from 'react-native';
import { useLocalSearchParams, useNavigation } from 'expo-router';
import { useStopRoutes } from '../../src/hooks/useStopRoutes';
import { useScore } from '../../src/hooks/useScore';
import { useTrend } from '../../src/hooks/useTrend';
import { useFilters } from '../../src/state/filters';
import { FilterChips } from '../../src/components/FilterChips';
import { ScoreCard } from '../../src/components/ScoreCard';
import { TrendChart } from '../../src/components/TrendChart';
import { LoadingState } from '../../src/components/LoadingState';
import { ErrorState } from '../../src/components/ErrorState';
import { EmptyState } from '../../src/components/EmptyState';
import { formatUpdatedAt } from '../../src/utils/formatters';
import type { ApiRoute } from '../../src/types/api';

export default function StopDetailScreen(): JSX.Element {
  const { id, routeId: initialRouteId } = useLocalSearchParams<{
    id: string;
    routeId?: string;
  }>();
  const navigation = useNavigation();
  const { dayType, hourBucket, setDayType, setHourBucket } = useFilters();

  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(
    typeof initialRouteId === 'string' ? initialRouteId : null,
  );

  const routesQuery = useStopRoutes(id ?? null);
  const routes: ApiRoute[] = routesQuery.data?.routes ?? [];

  // Once routes load, set the selected route if none is set
  useEffect(() => {
    if (selectedRouteId === null && routes.length > 0) {
      const first = routes[0];
      if (first) setSelectedRouteId(first.route_id);
    }
  }, [routes, selectedRouteId]);

  // Update header title once stop name is known (stop name from routes response)
  useEffect(() => {
    if (id) navigation.setOptions({ title: `Stop ${id}` });
  }, [id, navigation]);

  const scoreQuery = useScore(id ?? null, selectedRouteId, dayType, hourBucket);
  const trendQuery = useTrend(id ?? null, selectedRouteId);

  const isRefreshing = scoreQuery.isFetching || trendQuery.isFetching;

  function handleRefresh(): void {
    void scoreQuery.refetch();
    void trendQuery.refetch();
  }

  if (routesQuery.isLoading) return <LoadingState message="Loading stop info…" />;
  if (routesQuery.error) {
    return <ErrorState message="Couldn't load route info" onRetry={() => void routesQuery.refetch()} />;
  }
  if (routes.length === 0) {
    return <EmptyState message="No routes found for this stop." />;
  }

  return (
    <ScrollView
      style={styles.container}
      contentContainerStyle={styles.content}
      refreshControl={
        <RefreshControl
          refreshing={isRefreshing && !scoreQuery.isLoading && !trendQuery.isLoading}
          onRefresh={handleRefresh}
          tintColor="#4ade80"
        />
      }
    >
      {/* Route selector */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.routeRow}
        style={styles.routeScroll}
      >
        {routes.map((route) => (
          <TouchableOpacity
            key={route.route_id}
            onPress={() => setSelectedRouteId(route.route_id)}
            style={[
              styles.routeChip,
              route.route_id === selectedRouteId && styles.routeChipActive,
            ]}
          >
            <Text
              style={[
                styles.routeChipText,
                route.route_id === selectedRouteId && styles.routeChipTextActive,
              ]}
            >
              {route.short_name}
            </Text>
          </TouchableOpacity>
        ))}
      </ScrollView>

      {/* Time filter */}
      <View style={styles.filterSection}>
        <FilterChips
          dayType={dayType}
          hourBucket={hourBucket}
          onDayTypeChange={setDayType}
          onHourBucketChange={setHourBucket}
        />
      </View>

      {/* Score card */}
      {scoreQuery.isLoading ? (
        <LoadingState message="Loading score…" />
      ) : scoreQuery.data ? (
        <>
          <ScoreCard score={scoreQuery.data} />
          <Text style={styles.updatedAt}>
            Updated {formatUpdatedAt(scoreQuery.data.updated_at)}
          </Text>
        </>
      ) : (
        <EmptyState message="No reliability data yet for this stop, route, and time window." />
      )}

      {/* Trend chart */}
      {selectedRouteId !== null && (
        <View style={styles.trendSection}>
          <Text style={styles.trendTitle}>14-Day Trend</Text>
          {trendQuery.isLoading ? (
            <LoadingState message="Loading trend…" />
          ) : trendQuery.data ? (
            <View style={styles.chartWrapper}>
              <TrendChart series={trendQuery.data.series} />
            </View>
          ) : null}
        </View>
      )}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0f172a',
  },
  content: {
    paddingBottom: 32,
    gap: 16,
  },
  routeScroll: {
    backgroundColor: '#1a1a2e',
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
  },
  routeRow: {
    flexDirection: 'row',
    paddingHorizontal: 16,
    paddingVertical: 10,
    gap: 8,
  },
  routeChip: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderRadius: 20,
    backgroundColor: '#2d2d44',
    borderWidth: 1,
    borderColor: '#3d3d5c',
  },
  routeChipActive: {
    backgroundColor: '#4ade80',
    borderColor: '#4ade80',
  },
  routeChipText: {
    color: '#a0aec0',
    fontWeight: '600',
    fontSize: 14,
  },
  routeChipTextActive: {
    color: '#0f172a',
    fontWeight: '700',
  },
  filterSection: {
    backgroundColor: '#1a1a2e',
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
  },
  updatedAt: {
    color: '#4b5563',
    fontSize: 11,
    textAlign: 'right',
    paddingHorizontal: 16,
    marginTop: -8,
  },
  trendSection: {
    paddingHorizontal: 16,
    gap: 8,
  },
  trendTitle: {
    color: '#a0aec0',
    fontSize: 13,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
  },
  chartWrapper: {
    backgroundColor: '#1a1a2e',
    borderRadius: 12,
    padding: 12,
    alignItems: 'center',
  },
});
