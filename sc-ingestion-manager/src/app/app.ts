import { Component } from '@angular/core';
import { IngestionManagerComponent } from './ingestion/ingestion-manager.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [IngestionManagerComponent],
  template: '<app-ingestion-manager />',
})
export class App {}
