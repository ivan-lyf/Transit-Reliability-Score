import {
  Modal,
  View,
  Text,
  TouchableOpacity,
  ScrollView,
  StyleSheet,
} from 'react-native';
import { scoreColor } from '../utils/formatters';

interface Props {
  visible: boolean;
  score: number;
  onClose: () => void;
}

const BANDS = [
  { range: '80–100', label: 'Excellent', color: '#4ade80', desc: 'Consistently on time. Reliable for tight connections.' },
  { range: '60–79', label: 'Good', color: '#a3e635', desc: 'Mostly on time with occasional minor delays.' },
  { range: '40–59', label: 'Fair', color: '#f97316', desc: 'Frequent delays. Build in extra time.' },
  { range: '0–39', label: 'Poor', color: '#ef4444', desc: 'Unreliable. Significant delays are common.' },
];

export function ScoreExplainerModal({ visible, score, onClose }: Props): JSX.Element {
  const color = scoreColor(score);

  return (
    <Modal
      visible={visible}
      animationType="slide"
      transparent
      onRequestClose={onClose}
      testID="score-explainer-modal"
    >
      <TouchableOpacity style={styles.backdrop} onPress={onClose} activeOpacity={1}>
        <TouchableOpacity activeOpacity={1} style={styles.sheet}>
          <View style={styles.handle} />

          <ScrollView showsVerticalScrollIndicator={false}>
            <View style={styles.scoreRow}>
              <Text style={styles.titleText}>Reliability Score</Text>
              <Text style={[styles.scoreValue, { color }]}>{score}</Text>
            </View>

            <Text style={styles.description}>
              The score (0–100) combines on-time rate, median delay, and 95th-percentile
              delay across observed trips for this stop, route, and time window.
            </Text>

            <Text style={styles.sectionTitle}>Score bands</Text>
            {BANDS.map((b) => (
              <View key={b.range} style={styles.band}>
                <View style={[styles.bandDot, { backgroundColor: b.color }]} />
                <View style={styles.bandInfo}>
                  <Text style={styles.bandLabel}>
                    {b.range} — <Text style={{ color: b.color }}>{b.label}</Text>
                  </Text>
                  <Text style={styles.bandDesc}>{b.desc}</Text>
                </View>
              </View>
            ))}
          </ScrollView>

          <TouchableOpacity style={styles.closeButton} onPress={onClose}>
            <Text style={styles.closeText}>Close</Text>
          </TouchableOpacity>
        </TouchableOpacity>
      </TouchableOpacity>
    </Modal>
  );
}

const styles = StyleSheet.create({
  backdrop: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.6)',
    justifyContent: 'flex-end',
  },
  sheet: {
    backgroundColor: '#1a1a2e',
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: 24,
    maxHeight: '80%',
  },
  handle: {
    width: 36,
    height: 4,
    borderRadius: 2,
    backgroundColor: '#3d3d5c',
    alignSelf: 'center',
    marginBottom: 20,
  },
  scoreRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 12,
  },
  titleText: {
    fontSize: 20,
    fontWeight: '700',
    color: '#fff',
  },
  scoreValue: {
    fontSize: 48,
    fontWeight: '800',
  },
  description: {
    fontSize: 14,
    color: '#a0aec0',
    lineHeight: 22,
    marginBottom: 20,
  },
  sectionTitle: {
    fontSize: 13,
    fontWeight: '700',
    color: '#6b7280',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 12,
  },
  band: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 12,
    marginBottom: 14,
  },
  bandDot: {
    width: 10,
    height: 10,
    borderRadius: 5,
    marginTop: 4,
  },
  bandInfo: {
    flex: 1,
  },
  bandLabel: {
    fontSize: 14,
    fontWeight: '600',
    color: '#e2e8f0',
    marginBottom: 2,
  },
  bandDesc: {
    fontSize: 13,
    color: '#6b7280',
    lineHeight: 18,
  },
  closeButton: {
    marginTop: 20,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: '#2d2d44',
    alignItems: 'center',
  },
  closeText: {
    color: '#fff',
    fontWeight: '600',
    fontSize: 15,
  },
});
