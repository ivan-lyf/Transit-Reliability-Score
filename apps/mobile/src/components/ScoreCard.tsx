import { useState } from 'react';
import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';
import type { ApiScore } from '../types/api';
import { scoreColor, formatOnTimeRate, formatDelay } from '../utils/formatters';
import { ScoreExplainerModal } from './ScoreExplainerModal';

interface Props {
  score: ApiScore;
}

export function ScoreCard({ score }: Props): JSX.Element {
  const [showExplainer, setShowExplainer] = useState(false);
  const color = scoreColor(score.score);

  return (
    <>
      <TouchableOpacity
        style={styles.card}
        onPress={() => setShowExplainer(true)}
        accessibilityRole="button"
        accessibilityLabel="Reliability score — tap for explanation"
        testID="score-card"
      >
        <View style={styles.header}>
          <View>
            <Text style={styles.label}>Reliability Score</Text>
            {score.low_confidence && (
              <Text style={styles.lowConfidence}>⚠ Low sample size</Text>
            )}
          </View>
          <Text style={[styles.score, { color }]}>{score.score}</Text>
        </View>

        <View style={styles.metrics}>
          <View style={styles.metric}>
            <Text style={styles.metricLabel}>On-time</Text>
            <Text style={styles.metricValue}>{formatOnTimeRate(score.on_time_rate)}</Text>
          </View>
          <View style={styles.metric}>
            <Text style={styles.metricLabel}>Median delay</Text>
            <Text style={styles.metricValue}>{formatDelay(score.p50_delay_sec)}</Text>
          </View>
          <View style={styles.metric}>
            <Text style={styles.metricLabel}>95th pct delay</Text>
            <Text style={styles.metricValue}>{formatDelay(score.p95_delay_sec)}</Text>
          </View>
          <View style={styles.metric}>
            <Text style={styles.metricLabel}>Trips</Text>
            <Text style={styles.metricValue}>{score.sample_n}</Text>
          </View>
        </View>

        <Text style={styles.hint}>Tap to learn more</Text>
      </TouchableOpacity>

      <ScoreExplainerModal
        visible={showExplainer}
        score={score.score}
        onClose={() => setShowExplainer(false)}
      />
    </>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: '#2d2d44',
    borderRadius: 16,
    padding: 20,
    marginHorizontal: 16,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 16,
  },
  label: {
    fontSize: 16,
    fontWeight: '600',
    color: '#fff',
  },
  lowConfidence: {
    fontSize: 12,
    color: '#f97316',
    marginTop: 4,
  },
  score: {
    fontSize: 52,
    fontWeight: '800',
    lineHeight: 56,
  },
  metrics: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 12,
  },
  metric: {
    flex: 1,
    minWidth: 80,
  },
  metricLabel: {
    fontSize: 11,
    color: '#6b7280',
    marginBottom: 2,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  metricValue: {
    fontSize: 15,
    fontWeight: '600',
    color: '#e2e8f0',
  },
  hint: {
    fontSize: 11,
    color: '#4b5563',
    marginTop: 12,
    textAlign: 'right',
  },
});
