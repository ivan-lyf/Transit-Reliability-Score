import { View, ActivityIndicator, Text, StyleSheet } from 'react-native';

interface Props {
  message?: string;
}

export function LoadingState({ message = 'Loading…' }: Props): JSX.Element {
  return (
    <View style={styles.container} testID="loading-state">
      <ActivityIndicator size="large" color="#4ade80" />
      <Text style={styles.text}>{message}</Text>
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
  text: {
    color: '#a0aec0',
    fontSize: 14,
  },
});
