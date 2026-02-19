/**
 * Custom SVG line chart for the 14-day score trend.
 * Uses react-native-svg directly (no Skia dependency) so it works in Jest.
 */

import { Fragment } from 'react';
import { View, Text, StyleSheet } from 'react-native';
import Svg, { Polyline, Line, Text as SvgText, Circle } from 'react-native-svg';
import type { ApiTrendPoint } from '../types/api';
import { scoreColor, formatServiceDate } from '../utils/formatters';

interface Props {
  series: ApiTrendPoint[];
  width?: number;
  height?: number;
}

const PAD = { top: 12, right: 8, bottom: 32, left: 36 };

export function TrendChart({ series, width = 340, height = 160 }: Props): JSX.Element {
  if (series.length === 0) {
    return (
      <View style={[styles.container, { width, height }]} testID="trend-chart-empty">
        <Text style={styles.empty}>No trend data</Text>
      </View>
    );
  }

  const chartW = width - PAD.left - PAD.right;
  const chartH = height - PAD.top - PAD.bottom;

  const scores = series.map((p) => p.score);
  const minScore = Math.max(0, Math.min(...scores) - 5);
  const maxScore = Math.min(100, Math.max(...scores) + 5);
  const scoreRange = maxScore - minScore || 1;

  const toX = (i: number): number => PAD.left + (i / Math.max(series.length - 1, 1)) * chartW;
  const toY = (s: number): number => PAD.top + chartH - ((s - minScore) / scoreRange) * chartH;

  const points = series.map((p, i) => `${toX(i)},${toY(p.score)}`).join(' ');

  // Show at most 4 x-axis labels (first, last, and evenly spaced)
  const labelIndices = new Set<number>([0, series.length - 1]);
  if (series.length > 2) {
    const step = Math.floor((series.length - 1) / 3);
    for (let idx = step; idx < series.length - 1; idx += step) labelIndices.add(idx);
  }

  // Y-axis tick values
  const yTicks = [minScore, Math.round((minScore + maxScore) / 2), maxScore];

  return (
    <View testID="trend-chart">
      <Svg width={width} height={height}>
        {/* Y-axis ticks */}
        {yTicks.map((v) => (
          <Fragment key={v}>
            <Line
              x1={PAD.left}
              y1={toY(v)}
              x2={PAD.left + chartW}
              y2={toY(v)}
              stroke="#2d2d44"
              strokeWidth={1}
            />
            <SvgText
              x={PAD.left - 4}
              y={toY(v) + 4}
              fontSize={10}
              fill="#6b7280"
              textAnchor="end"
            >
              {v}
            </SvgText>
          </Fragment>
        ))}

        {/* Score line */}
        <Polyline points={points} fill="none" stroke="#4ade80" strokeWidth={2} />

        {/* Data points */}
        {series.map((p, i) => (
          <Circle
            key={p.service_date}
            cx={toX(i)}
            cy={toY(p.score)}
            r={3}
            fill={scoreColor(p.score)}
          />
        ))}

        {/* X-axis labels */}
        {series.map((p, i) =>
          labelIndices.has(i) ? (
            <SvgText
              key={p.service_date}
              x={toX(i)}
              y={height - 4}
              fontSize={9}
              fill="#6b7280"
              textAnchor="middle"
            >
              {formatServiceDate(p.service_date)}
            </SvgText>
          ) : null,
        )}
      </Svg>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    alignItems: 'center',
    justifyContent: 'center',
  },
  empty: {
    color: '#6b7280',
    fontSize: 13,
  },
});
