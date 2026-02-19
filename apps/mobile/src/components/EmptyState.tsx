import { View, Text, StyleSheet } from 'react-native';

interface Props {
  message?: string;
}

export function EmptyState({ message = 'No data found' }: Props): JSX.Element {
  return (
    <View style={styles.container} testID="empty-state">
      <Text style={styles.icon}>◎</Text>
      <Text style={styles.message}>{message}</Text>
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
    color: '#4b5563',
  },
  message: {
    color: '#6b7280',
    fontSize: 14,
    textAlign: 'center',
  },
});
