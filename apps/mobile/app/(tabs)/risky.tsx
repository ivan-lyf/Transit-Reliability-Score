/**
 * RiskyStopsScreen — list of lowest-reliability stops nearby.
 */

import { View, FlatList, RefreshControl, StyleSheet } from 'react-native';
import { useLocation } from '../../src/hooks/useLocation';
import { useNearbyRisky } from '../../src/hooks/useNearbyRisky';
import { useFilters } from '../../src/state/filters';
import { FilterChips } from '../../src/components/FilterChips';
import { RiskyStopRow } from '../../src/components/RiskyStopRow';
import { LoadingState } from '../../src/components/LoadingState';
import { ErrorState } from '../../src/components/ErrorState';
import { EmptyState } from '../../src/components/EmptyState';
import type { ApiRiskyStop } from '../../src/types/api';

export default function RiskyStopsScreen(): JSX.Element {
  const { dayType, hourBucket, setDayType, setHourBucket } = useFilters();
  const location = useLocation();
  const { data, isLoading, isFetching, error, refetch } = useNearbyRisky(
    location.lat,
    location.lon,
    dayType,
    hourBucket,
  );

  if (location.loading || isLoading) {
    return <LoadingState message="Finding risky stops…" />;
  }

  if (location.error) {
    return <ErrorState message={location.error} />;
  }

  if (error) {
    return (
      <ErrorState
        message="Couldn't load risky stops"
        onRetry={() => void refetch()}
      />
    );
  }

  const items: ApiRiskyStop[] = data?.items ?? [];

  return (
    <View style={styles.container}>
      <View style={styles.filters}>
        <FilterChips
          dayType={dayType}
          hourBucket={hourBucket}
          onDayTypeChange={setDayType}
          onHourBucketChange={setHourBucket}
        />
      </View>

      {items.length === 0 ? (
        <EmptyState message="No risky stops found nearby for this time window." />
      ) : (
        <FlatList
          data={items}
          keyExtractor={(item) => `${item.stop_id}:${item.route_id}`}
          renderItem={({ item, index }) => (
            <RiskyStopRow item={item} rank={index + 1} />
          )}
          refreshControl={
            <RefreshControl
              refreshing={isFetching && !isLoading}
              onRefresh={() => void refetch()}
              tintColor="#4ade80"
            />
          }
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0f172a',
  },
  filters: {
    paddingVertical: 10,
    backgroundColor: '#1a1a2e',
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
    gap: 6,
  },
});
