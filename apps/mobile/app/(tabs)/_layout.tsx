import { Ionicons } from '@expo/vector-icons';
import { Tabs } from 'expo-router';
import type { ComponentProps } from 'react';

type IoniconsName = ComponentProps<typeof Ionicons>['name'];

function tabIcon(name: IoniconsName) {
  return function TabIcon({ color }: { color: string }): JSX.Element {
    return <Ionicons name={name} size={22} color={color} />;
  };
}

export default function TabsLayout(): JSX.Element {
  return (
    <Tabs
      screenOptions={{
        tabBarActiveTintColor: '#4ade80',
        tabBarInactiveTintColor: '#6b7280',
        tabBarStyle: { backgroundColor: '#1a1a2e', borderTopColor: '#2d2d44' },
        headerStyle: { backgroundColor: '#1a1a2e' },
        headerTintColor: '#fff',
        headerTitleStyle: { fontWeight: 'bold' },
      }}
    >
      <Tabs.Screen
        name="index"
        options={{
          title: 'Nearby',
          tabBarIcon: tabIcon('map-outline'),
        }}
      />
      <Tabs.Screen
        name="risky"
        options={{
          title: 'Risky Stops',
          tabBarIcon: tabIcon('warning-outline'),
        }}
      />
      <Tabs.Screen
        name="settings"
        options={{
          title: 'Settings',
          tabBarIcon: tabIcon('settings-outline'),
        }}
      />
    </Tabs>
  );
}
