import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, interval, EMPTY } from 'rxjs';
import { switchMap, share, catchError } from 'rxjs/operators';

export interface IngestionStatus {
  is_running: boolean;
  current_year: number | null;
  current_court: string | null;
  current_page: number;
  opinions_processed: number;
  chunks_upserted: number;
  message: string;
}

@Injectable({ providedIn: 'root' })
export class IngestionService {
  readonly API_BASE = 'https://hzook8iwod.execute-api.us-east-1.amazonaws.com';

  constructor(private http: HttpClient) {}

  startIngestion(years: number[]): Observable<any> {
    return this.http.post<any>(`${this.API_BASE}/ingest`, { years });
  }

  stopIngestion(): Observable<any> {
    return this.http.post<any>(`${this.API_BASE}/stop-ingestion`, {});
  }

  getStatusUpdates(): Observable<IngestionStatus> {
    return interval(3000).pipe(
      switchMap(() =>
        this.http.get<IngestionStatus>(`${this.API_BASE}/status`).pipe(
          catchError(() => EMPTY)
        )
      ),
      share()
    );
  }

  resetProgress(year?: number, court?: string): Observable<any> {
    const params: Record<string, string> = {};
    if (year  !== undefined) params['year']  = String(year);
    if (court !== undefined) params['court'] = court;
    return this.http.post<any>(`${this.API_BASE}/reset-progress`, {}, { params });
  }

  bulkIngest(startYear?: number | null, endYear?: number | null): Observable<any> {
    const body: Record<string, number> = {};
    if (startYear != null) body['start_year'] = startYear;
    if (endYear   != null) body['end_year']   = endYear;
    return this.http.post<any>(`${this.API_BASE}/bulk-ingest`, body);
  }

  getCourtStats(): Observable<any> {
    return this.http.get<any>(`${this.API_BASE}/court-stats`);
  }

  startCourtScan(): Observable<any> {
    return this.http.post<any>(`${this.API_BASE}/start-court-scan`, {});
  }

  getHealth(): Observable<any> {
    return this.http.get<any>(`${this.API_BASE}/health`);
  }
}
