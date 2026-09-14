import { ChangeDetectionStrategy, Component, computed, input, signal } from '@angular/core';
import { formatClock, formatCompact, formatMad, type TimePoint } from '@rad/core';
import type { EChartsCoreOption } from 'echarts/core';
import { NgxEchartsDirective } from 'ngx-echarts';

import { COLORS } from '../config';

type Granularity = 'minute' | 'hour';

@Component({
  selector: 'app-revenue-chart',
  imports: [NgxEchartsDirective],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="card-header">
      <h2>Revenue</h2>
      <div class="segmented" role="tablist" aria-label="Granularity">
        @for (option of granularities; track option.value) {
          <button
            type="button"
            role="tab"
            [attr.aria-selected]="option.value === granularity()"
            [class.active]="option.value === granularity()"
            (click)="granularity.set(option.value)"
          >
            {{ option.label }}
          </button>
        }
      </div>
    </div>
    <!-- [options] is applied once; [merge] only carries new data. ECharts keeps the same chart
         instance and animates the change, instead of redrawing from scratch (no flicker). -->
    <div class="chart" echarts [options]="baseOptions" [merge]="data()"></div>
  `,
})
export class RevenueChartComponent {
  readonly minute = input.required<TimePoint[]>();
  readonly hour = input.required<TimePoint[]>();

  protected readonly granularity = signal<Granularity>('minute');
  protected readonly granularities: { value: Granularity; label: string }[] = [
    { value: 'minute', label: 'Per minute · 1 h' },
    { value: 'hour', label: 'Per hour · 24 h' },
  ];

  protected readonly baseOptions: EChartsCoreOption = {
    animationDurationUpdate: 300,
    grid: { left: 56, right: 16, top: 16, bottom: 28 },
    tooltip: {
      trigger: 'axis',
      valueFormatter: (value: unknown) => formatMad(Number(value)),
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      axisLabel: { color: COLORS.muted },
      axisLine: { lineStyle: { color: COLORS.grid } },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: COLORS.muted, formatter: (value: number) => formatCompact(value) },
      splitLine: { lineStyle: { color: COLORS.grid } },
    },
    series: [
      {
        name: 'Revenue',
        type: 'line',
        smooth: true,
        showSymbol: false,
        lineStyle: { color: COLORS.accent, width: 2 },
        itemStyle: { color: COLORS.accent },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(79, 140, 255, 0.4)' },
              { offset: 1, color: 'rgba(79, 140, 255, 0)' },
            ],
          },
        },
      },
    ],
  };

  protected readonly data = computed<EChartsCoreOption>(() => {
    const points = this.granularity() === 'minute' ? this.minute() : this.hour();
    return {
      xAxis: { data: points.map((point) => formatClock(point.bucket)) },
      series: [{ data: points.map((point) => point.revenue_mad) }],
    };
  });
}
