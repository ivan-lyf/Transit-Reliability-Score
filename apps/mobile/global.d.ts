// Re-export React.JSX as the global JSX namespace.
// Required for React 19 + "jsx": "react-native" mode (Expo default).
import type React from "react";

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace JSX {
    type Element = React.JSX.Element;
    type IntrinsicElements = React.JSX.IntrinsicElements;
  }
}
