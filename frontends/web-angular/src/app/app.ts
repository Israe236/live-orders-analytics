import { ChangeDetectionStrategy, Component, inject } from '@angular/core';

import { AlertBannerComponent, AlertHistoryComponent } from './components/alerts.component';
import { CategoryChartComponent } from './components/category-chart.component';
import { ConnectionBadgeComponent } from './components/connection-badge.component';
import { EventFeedComponent } from './components/event-feed.component';
import { KpiCardsComponent } from './components/kpi-cards.component';
import { RevenueChartComponent } from './components/revenue-chart.component';
import { CityTableComponent, StatusBarComponent } from './components/status.component';
import { LiveDashboardService } from './live-dashboard.service';

@Component({
  selector: 'app-root',
  imports: [
    AlertBannerComponent,
    AlertHistoryComponent,
    CategoryChartComponent,
    CityTableComponent,
    ConnectionBadgeComponent,
    EventFeedComponent,
    KpiCardsComponent,
    RevenueChartComponent,
    StatusBarComponent,
  ],
  templateUrl: './app.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class App {
  protected readonly dashboard = inject(LiveDashboardService);
}
