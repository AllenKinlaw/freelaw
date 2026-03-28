import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { IngestionService, IngestionStatus } from './ingestion.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-ingestion-manager',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './ingestion-manager.component.html',
  styleUrl: './ingestion-manager.component.css',
})
export class IngestionManagerComponent implements OnInit, OnDestroy {
  startYear = 2020;
  endYear   = 2024;

  resetYear?: number;
  resetCourt = '';

  status: IngestionStatus = {
    is_running:         false,
    current_year:       null,
    current_court:      null,
    current_page:       0,
    opinions_processed: 0,
    chunks_upserted:    0,
    message:            'Connecting...',
  };

  backendOnline = false;
  actionMessage = '';

  private statusSub?: Subscription;

  constructor(private ingestion: IngestionService) {}

  ngOnInit(): void {
    this.ingestion.getHealth().subscribe({
      next:  () => { this.backendOnline = true; },
      error: () => { this.status.message = 'Cannot reach backend. Is the EC2 server running?'; },
    });

    this.statusSub = this.ingestion.getStatusUpdates().subscribe(data => {
      this.status = data;
      this.backendOnline = true;
    });
  }

  get yearRange(): number[] {
    const years: number[] = [];
    for (let y = this.startYear; y <= this.endYear; y++) years.push(y);
    return years;
  }

  onStart(): void {
    if (this.startYear > this.endYear) {
      this.actionMessage = 'Start year must be ≤ end year.';
      return;
    }
    this.ingestion.startIngestion(this.yearRange).subscribe({
      next:  res => { this.actionMessage = res.message ?? `Started years ${this.startYear}–${this.endYear}.`; },
      error: ()  => { this.actionMessage = 'Failed to start ingestion.'; },
    });
  }

  onStop(): void {
    this.ingestion.stopIngestion().subscribe({
      next:  res => { this.actionMessage = res.message ?? 'Stop signal sent.'; },
      error: ()  => { this.actionMessage = 'Failed to send stop signal.'; },
    });
  }

  onReset(): void {
    this.ingestion.resetProgress(this.resetYear, this.resetCourt || undefined).subscribe({
      next:  res => { this.actionMessage = `Reset: ${res.cleared}`; },
      error: ()  => { this.actionMessage = 'Reset failed — is ingestion still running?'; },
    });
  }

  ngOnDestroy(): void {
    this.statusSub?.unsubscribe();
  }
}
