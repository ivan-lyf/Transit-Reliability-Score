import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';

interface Props {
  message?: string;
  onRetry?: () => void;
}

export function ErrorState({
  message = 'Something went wrong',
  onRetry,
}: Props): JSX.Element {
  return (
    <View style={styles.container} testID="error-state">
      <Text style={styles.icon}>⚠</Text>
      <Text style={styles.message}>{message}</Text>
      {onRetry && (
        <TouchableOpacity style={styles.retryButton} onPress={onRetry}>
          <Text style={styles.retryText}>Retry</Text>
        </TouchableOpacity>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    gap: 12,
    padding: 24,
  },
  icon: {
    fontSize: 32,
  },
  message: {
    color: '#fca5a5',
    fontSize: 14,
    textAlign: 'center',
  },
  retryButton: {
    paddingHorizontal: 24,
    paddingVertical: 10,
    backgroundColor: '#2d2d44',
    borderRadius: 8,
    marginTop: 4,
  },
  retryText: {
    color: '#4ade80',
    fontWeight: '600',
    fontSize: 14,
  },
});
