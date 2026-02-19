/**
 * Manual mock for @rnmapbox/maps.
 * Replaces all Mapbox components with plain React Native Views.
 */

import type { ReactNode } from 'react';
import { View } from 'react-native';

const MapView = ({
  children,
  style,
  testID,
}: {
  children?: ReactNode;
  style?: object;
  testID?: string;
}): JSX.Element => (
  <View testID={testID ?? 'mapbox-mapview'} style={style}>
    {children}
  </View>
);

const Camera = (_props: object): null => null;

const PointAnnotation = ({
  children,
  id,
}: {
  children?: ReactNode;
  id?: string;
}): JSX.Element => <View testID={`point-annotation-${id ?? ''}`}>{children}</View>;

const UserLocation = (_props: object): null => null;

const Callout = (_props: object): null => null;

const Mapbox = {
  MapView,
  Camera,
  PointAnnotation,
  UserLocation,
  Callout,
  setAccessToken: jest.fn(),
};

export default Mapbox;
