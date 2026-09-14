import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { formatCompact, formatMad, type Breakdown } from '@rad/core';
import type { EChartsCoreOption } from 'echarts/core';
import { NgxEchartsDirective } from 'ngx-echarts';

import { COLORS } from '../config';

function titleCase(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

@Component({
  selector: 'app-category-chart',
  imports: [NgxEchartsDirective],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="card-header">
      <h2>Revenue by category</h2>
      <span class="muted">last 60 min</span>
    </div>
    <div class="chart chart-bars" echarts [options]="baseOptions" [merge]="data()"></div>
  `,
})
export class CategoryChartComponent {
  readonly items = input.required<Breakdown[]>();

  protected readonly baseOptions: EChartsCoreOption = {
    animationDurationUpdate: 300,
    grid: { left: 96, right: 20, top: 8, bottom: 24 },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: (value: unknown) => formatMad(Number(value)),
    },
    xAxis: {
      type: 'value',
      axisLabel: { color: COLORS.muted, formatter: (value: number) => formatCompact(value) },
      splitLine: { lineStyle: { color: COLORS.grid } },
    },
    // Largest category on top.
    yAxis: { type: 'category', inverse: true, axisLabel: { color: COLORS.muted } },
    series: [
      {
        name: 'Revenue',
        type: 'bar',
        itemStyle: { color: COLORS.accent, borderRadius: [0, 4, 4, 0] },
      },
    ],
  };

  protected readonly data = computed<EChartsCoreOption>(() => {
    const items = this.items();
    return {
      yAxis: { data: items.map((item) => titleCase(item.value)) },
      series: [{ data: items.map((item) => item.revenue_mad) }],
    };
  });
}
