/**
 * SettingsScreen — feed status, attribution, and app info.
 */

import { View, Text, ScrollView, StyleSheet } from 'react-native';
import Constants from 'expo-constants';
import { useLastIngest } from '../../src/hooks/useLastIngest';
import { LoadingState } from '../../src/components/LoadingState';
import { formatUpdatedAt } from '../../src/utils/formatters';
import type { ApiFeedStatus } from '../../src/types/api';

function FeedRow({ feed }: { feed: ApiFeedStatus }): JSX.Element {
  const freshColor = feed.is_fresh ? '#4ade80' : '#f97316';
  return (
    <View style={styles.feedRow}>
      <View style={styles.feedHeader}>
        <Text style={styles.feedType}>{feed.feed_type}</Text>
        <View style={[styles.badge, { backgroundColor: freshColor + '22' }]}>
          <Text style={[styles.badgeText, { color: freshColor }]}>
            {feed.is_fresh ? 'Fresh' : 'Stale'}
          </Text>
        </View>
      </View>
      <Text style={styles.feedMeta}>
        Last success: {feed.last_success_at ? formatUpdatedAt(feed.last_success_at) : '—'}
      </Text>
      {feed.error_message ? (
        <Text style={styles.feedError} numberOfLines={2}>
          {feed.error_message}
        </Text>
      ) : null}
      <Text style={styles.feedMeta}>{feed.entity_count.toLocaleString()} entities</Text>
    </View>
  );
}

export default function SettingsScreen(): JSX.Element {
  const { data, isLoading } = useLastIngest();
  const version = Constants.expoConfig?.version ?? '—';

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      {/* App info */}
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>App</Text>
        <View style={styles.card}>
          <Row label="Version" value={version} />
          <Row label="Region" value="Metro Vancouver (TransLink)" />
        </View>
      </View>

      {/* Data freshness */}
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Data Feeds</Text>
        {isLoading ? (
          <LoadingState message="Checking feed status…" />
        ) : data ? (
          <View style={styles.card}>
            {data.feeds.map((feed) => (
              <FeedRow key={feed.feed_type} feed={feed} />
            ))}
          </View>
        ) : (
          <Text style={styles.na}>Feed status unavailable</Text>
        )}
      </View>

      {/* Attribution */}
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Attribution</Text>
        <View style={styles.card}>
          <Text style={styles.attrText}>
            Transit data © TransLink, provided under the TransLink Open API terms.
          </Text>
          <Text style={styles.attrText}>
            Mapping © Mapbox, © OpenStreetMap contributors.
          </Text>
        </View>
      </View>
    </ScrollView>
  );
}

function Row({ label, value }: { label: string; value: string }): JSX.Element {
  return (
    <View style={styles.row}>
      <Text style={styles.rowLabel}>{label}</Text>
      <Text style={styles.rowValue}>{value}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0f172a',
  },
  content: {
    padding: 16,
    gap: 24,
  },
  section: {
    gap: 8,
  },
  sectionTitle: {
    fontSize: 12,
    fontWeight: '700',
    color: '#6b7280',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginLeft: 4,
  },
  card: {
    backgroundColor: '#1a1a2e',
    borderRadius: 12,
    overflow: 'hidden',
  },
  row: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
  },
  rowLabel: {
    color: '#a0aec0',
    fontSize: 14,
  },
  rowValue: {
    color: '#fff',
    fontSize: 14,
    fontWeight: '500',
  },
  feedRow: {
    padding: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
    gap: 4,
  },
  feedHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 4,
  },
  feedType: {
    color: '#fff',
    fontSize: 14,
    fontWeight: '600',
    textTransform: 'capitalize',
  },
  badge: {
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 8,
  },
  badgeText: {
    fontSize: 11,
    fontWeight: '700',
  },
  feedMeta: {
    color: '#6b7280',
    fontSize: 12,
  },
  feedError: {
    color: '#fca5a5',
    fontSize: 12,
  },
  attrText: {
    color: '#6b7280',
    fontSize: 13,
    lineHeight: 20,
    padding: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#2d2d44',
  },
  na: {
    color: '#6b7280',
    fontSize: 14,
    textAlign: 'center',
    padding: 24,
  },
});
