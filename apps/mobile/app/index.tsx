import { useQuery } from '@tanstack/react-query';
import { StyleSheet, Text, View, ActivityIndicator } from 'react-native';

import { api } from '../src/services/api';
import { env, validateEnv } from '../src/config/env';

export default function HomeScreen(): JSX.Element {
  const { data: health, isLoading, error } = useQuery({
    queryKey: ['health'],
    queryFn: api.health,
  });
  const envCheck = validateEnv();
  const envIssues = [...envCheck.missing, ...envCheck.warnings];

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Transit Reliability Score</Text>
      <Text style={styles.subtitle}>Metro Vancouver (TransLink)</Text>

      {envIssues.length > 0 && (
        <View style={styles.envWarning}>
          <Text style={styles.envWarningTitle}>Environment not fully configured</Text>
          {envCheck.missing.length > 0 && (
            <Text style={styles.envWarningText}>
              Missing required: {envCheck.missing.join(', ')}
            </Text>
          )}
          {envCheck.warnings.length > 0 && (
            <Text style={styles.envWarningText}>
              Missing optional: {envCheck.warnings.join(', ')}
            </Text>
          )}
          {envCheck.missing.includes('API_URL') && (
            <Text style={styles.envWarningText}>
              Using default API URL: {env.apiUrl}
            </Text>
          )}
        </View>
      )}

      <View style={styles.statusCard}>
        <Text style={styles.statusTitle}>API Status</Text>
        {isLoading ? (
          <ActivityIndicator color="#4ade80" />
        ) : error ? (
          <Text style={styles.errorText}>
            Unable to connect to API
          </Text>
        ) : (
          <>
            <View style={styles.statusRow}>
              <Text style={styles.statusLabel}>Status:</Text>
              <Text
                style={[
                  styles.statusValue,
                  health?.status === 'healthy'
                    ? styles.statusHealthy
                    : styles.statusDegraded,
                ]}
              >
                {health?.status ?? 'unknown'}
              </Text>
            </View>
            <View style={styles.statusRow}>
              <Text style={styles.statusLabel}>Version:</Text>
              <Text style={styles.statusValue}>{health?.version}</Text>
            </View>
            <View style={styles.statusRow}>
              <Text style={styles.statusLabel}>Environment:</Text>
              <Text style={styles.statusValue}>{health?.environment}</Text>
            </View>
            <View style={styles.statusRow}>
              <Text style={styles.statusLabel}>API URL:</Text>
              <Text style={styles.statusValue}>{env.apiUrl}</Text>
            </View>
            <View style={styles.statusRow}>
              <Text style={styles.statusLabel}>Database:</Text>
              <Text
                style={[
                  styles.statusValue,
                  health?.checks.database
                    ? styles.statusHealthy
                    : styles.statusDegraded,
                ]}
              >
                {health?.checks.database ? 'connected' : 'disconnected'}
              </Text>
            </View>
          </>
        )}
      </View>

      <Text style={styles.placeholder}>
        Map and stop details coming in Stage 8
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#1a1a2e',
    alignItems: 'center',
    paddingTop: 40,
    paddingHorizontal: 20,
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    color: '#fff',
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    color: '#a0aec0',
    marginBottom: 32,
  },
  statusCard: {
    backgroundColor: '#2d2d44',
    borderRadius: 12,
    padding: 20,
    width: '100%',
    marginBottom: 24,
  },
  envWarning: {
    backgroundColor: '#3b2f2f',
    borderRadius: 10,
    padding: 12,
    width: '100%',
    marginBottom: 16,
    borderWidth: 1,
    borderColor: '#ef4444',
  },
  envWarningTitle: {
    fontSize: 14,
    fontWeight: '600',
    color: '#fca5a5',
    marginBottom: 6,
  },
  envWarningText: {
    fontSize: 12,
    color: '#fecaca',
    marginBottom: 4,
  },
  statusTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: '#fff',
    marginBottom: 16,
  },
  statusRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 8,
  },
  statusLabel: {
    fontSize: 14,
    color: '#a0aec0',
  },
  statusValue: {
    fontSize: 14,
    color: '#fff',
    fontWeight: '500',
  },
  statusHealthy: {
    color: '#4ade80',
  },
  statusDegraded: {
    color: '#f97316',
  },
  errorText: {
    color: '#ef4444',
    fontSize: 14,
  },
  placeholder: {
    fontSize: 14,
    color: '#6b7280',
    fontStyle: 'italic',
    textAlign: 'center',
  },
});
