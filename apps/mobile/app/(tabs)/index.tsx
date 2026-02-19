/**
 * MapScreen — nearby stops on a Mapbox map.
 * Falls back to a plain list when no Mapbox token is configured.
 */

import { useState } from 'react';
import {
  View,
  Text,
  FlatList,
  TouchableOpacity,
  StyleSheet,
  SafeAreaView,
} from 'react-native';
import Mapbox from '@rnmapbox/maps';
import { useRouter } from 'expo-router';
import { useLocation } from '../../src/hooks/useLocation';
import { useNearbyStops } from '../../src/hooks/useNearbyStops';
import { LoadingState } from '../../src/components/LoadingState';
import { ErrorState } from '../../src/components/ErrorState';
import { EmptyState } from '../../src/components/EmptyState';
import { env } from '../../src/config/env';
import { formatDistance } from '../../src/utils/formatters';
import type { ApiStop } from '../../src/types/api';

const RADIUS_OPTIONS = [0.5, 1.0, 2.0] as const;
type RadiusKm = (typeof RADIUS_OPTIONS)[number];

const HAS_MAP = env.mapboxAccessToken.length > 0;

export default function MapScreen(): JSX.Element {
  const router = useRouter();
  const location = useLocation();
  const [radiusKm, setRadiusKm] = useState<RadiusKm>(1.0);
  const { data, isLoading, error, refetch } = useNearbyStops(
    location.lat,
    location.lon,
    radiusKm,
  );

  function handleStopPress(stop: ApiStop): void {
    router.push({ pathname: '/stop/[id]', params: { id: stop.stop_id } });
  }

  if (location.loading) return <LoadingState message="Getting your location…" />;
  if (location.error) return <ErrorState message={location.error} />;
  if (isLoading) return <LoadingState message="Finding nearby stops…" />;
  if (error) {
    return (
      <ErrorState
        message="Couldn't load nearby stops"
        onRetry={() => void refetch()}
      />
    );
  }

  const stops = data?.items ?? [];

  return (
    <SafeAreaView style={styles.container}>
      {/* Radius selector */}
      <View style={styles.radiusRow}>
        <Text style={styles.radiusLabel}>Radius:</Text>
        {RADIUS_OPTIONS.map((r) => (
          <TouchableOpacity
            key={r}
            onPress={() => setRadiusKm(r)}
            style={[styles.radiusChip, r === radiusKm && styles.radiusChipActive]}
          >
            <Text style={[styles.radiusText, r === radiusKm && styles.radiusTextActive]}>
              {r}km
            </Text>
          </TouchableOpacity>
        ))}
      </View>

      {/* Map or fallback list */}
      {HAS_MAP && location.lat !== null && location.lon !== null ? (
        <View style={styles.mapContainer}>
          <Mapbox.MapView style={styles.map} logoEnabled={false} attributionEnabled={false}>
            <Mapbox.Camera
              zoomLevel={14}
              centerCoordinate={[location.lon, location.lat]}
              animationMode="none"
            />
            <Mapbox.UserLocation visible />
            {stops.map((stop) => (
              <Mapbox.PointAnnotation
                key={stop.stop_id}
                id={stop.stop_id}
                coordinate={[stop.lon, stop.lat]}
              >
                <TouchableOpacity
                  onPress={() => handleStopPress(stop)}
                  style={styles.marker}
                >
                  <View style={styles.markerDot} />
                </TouchableOpacity>
                <Mapbox.Callout title={stop.name} />
              </Mapbox.PointAnnotation>
            ))}
          </Mapbox.MapView>
        </View>
      ) : null}

      {/* Stop list */}
      <View style={HAS_MAP ? styles.listSmall : styles.listFull}>
        {stops.length === 0 ? (
          <EmptyState message="No stops found nearby. Try a larger radius." />
        ) : (
          <FlatList
            data={stops}
            keyExtractor={(s) => s.stop_id}
            renderItem={({ item }) => (
              <TouchableOpacity
                style={styles.stopRow}
                onPress={() => handleStopPress(item)}
              >
                <View style={styles.stopInfo}>
                  <Text style={styles.stopName} numberOfLines={1}>
                    {item.name}
                  </Text>
                  {item.distance_m !== undefined && (
                    <Text style={styles.stopDist}>{formatDistance(item.distance_m)}</Text>
                  )}
                </View>
                <Text style={styles.chevron}>›</Text>
              </TouchableOpacity>
            )}
            ItemSeparatorComponent={() => <View style={styles.separator} />}
          />
        )}
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0f172a',
  },
  radiusRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 10,
    gap: 8,
    backgroundColor: '#1a1a2e',
  },
  radiusLabel: {
    color: '#6b7280',
    fontSize: 13,
    marginRight: 4,
  },
  radiusChip: {
    paddingHorizontal: 12,
    paddingVertical: 5,
    borderRadius: 16,
    backgroundColor: '#2d2d44',
    borderWidth: 1,
    borderColor: '#3d3d5c',
  },
  radiusChipActive: {
    backgroundColor: '#4ade80',
    borderColor: '#4ade80',
  },
  radiusText: {
    color: '#a0aec0',
    fontSize: 13,
    fontWeight: '500',
  },
  radiusTextActive: {
    color: '#0f172a',
    fontWeight: '700',
  },
  mapContainer: {
    flex: 3,
  },
  map: {
    flex: 1,
  },
  marker: {
    alignItems: 'center',
    justifyContent: 'center',
  },
  markerDot: {
    width: 12,
    height: 12,
    borderRadius: 6,
    backgroundColor: '#4ade80',
    borderWidth: 2,
    borderColor: '#fff',
  },
  listSmall: {
    flex: 2,
    borderTopWidth: 1,
    borderTopColor: '#2d2d44',
  },
  listFull: {
    flex: 1,
  },
  stopRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 14,
  },
  stopInfo: {
    flex: 1,
  },
  stopName: {
    color: '#fff',
    fontSize: 15,
    fontWeight: '500',
  },
  stopDist: {
    color: '#6b7280',
    fontSize: 12,
    marginTop: 2,
  },
  separator: {
    height: 1,
    backgroundColor: '#1e293b',
    marginLeft: 16,
  },
  chevron: {
    color: '#4b5563',
    fontSize: 20,
    marginLeft: 8,
  },
});
