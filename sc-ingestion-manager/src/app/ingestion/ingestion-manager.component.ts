import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { IngestionService, IngestionStatus } from './ingestion.service';
import { Subscription } from 'rxjs';

interface Court      { id: string; name: string; }
interface CourtGroup { label: string; courts: Court[]; }

@Component({
  selector: 'app-ingestion-manager',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './ingestion-manager.component.html',
  styleUrl: './ingestion-manager.component.css',
})
export class IngestionManagerComponent implements OnInit, OnDestroy {

  // ── Legacy API fields (kept for when API access is restored) ───────────────
  startYear = 2020;
  endYear   = 2024;

  // ── Reset fields ───────────────────────────────────────────────────────────
  resetYear?: number;
  resetCourt    = '';
  courtSearch   = '';

  // ── Court storage estimates ────────────────────────────────────────────────
  courtStats:  Record<string, { est_storage: string; est_opinions: number }> = {};
  scanStatus = { is_scanning: false, message: 'Not yet scanned.' };
  statsAvailable = false;

  // ── Live status ────────────────────────────────────────────────────────────
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
  readonly version = '2.2.0';

  private statusSub?: Subscription;

  // ── Court groups (used by the Reset court picker) ──────────────────────────
  readonly courtGroups: CourtGroup[] = [
    {
      label: 'Federal — Supreme Court',
      courts: [
        { id: 'scotus', name: 'US Supreme Court' },
      ],
    },
    {
      label: 'Federal — Circuit Courts of Appeals',
      courts: [
        { id: 'ca1',  name: '1st Circuit (ME, MA, NH, RI, PR)' },
        { id: 'ca2',  name: '2nd Circuit (CT, NY, VT)' },
        { id: 'ca3',  name: '3rd Circuit (DE, NJ, PA, VI)' },
        { id: 'ca4',  name: '4th Circuit (MD, NC, SC, VA, WV)' },
        { id: 'ca5',  name: '5th Circuit (LA, MS, TX)' },
        { id: 'ca6',  name: '6th Circuit (KY, MI, OH, TN)' },
        { id: 'ca7',  name: '7th Circuit (IL, IN, WI)' },
        { id: 'ca8',  name: '8th Circuit (AR, IA, MN, MO, NE, ND, SD)' },
        { id: 'ca9',  name: '9th Circuit (AK, AZ, CA, GU, HI, ID, MT, NV, OR, WA)' },
        { id: 'ca10', name: '10th Circuit (CO, KS, NM, OK, UT, WY)' },
        { id: 'ca11', name: '11th Circuit (AL, FL, GA)' },
        { id: 'cadc', name: 'DC Circuit' },
        { id: 'cafc', name: 'Federal Circuit' },
      ],
    },
    {
      label: 'Federal — District Courts (1st Circuit)',
      courts: [
        { id: 'med', name: 'D. Maine' },
        { id: 'mad', name: 'D. Massachusetts' },
        { id: 'nhd', name: 'D. New Hampshire' },
        { id: 'prd', name: 'D. Puerto Rico' },
        { id: 'rid', name: 'D. Rhode Island' },
      ],
    },
    {
      label: 'Federal — District Courts (2nd Circuit)',
      courts: [
        { id: 'ctd',  name: 'D. Connecticut' },
        { id: 'nyed', name: 'E.D. New York' },
        { id: 'nynd', name: 'N.D. New York' },
        { id: 'nysd', name: 'S.D. New York' },
        { id: 'nywd', name: 'W.D. New York' },
        { id: 'vtd',  name: 'D. Vermont' },
      ],
    },
    {
      label: 'Federal — District Courts (3rd Circuit)',
      courts: [
        { id: 'ded',  name: 'D. Delaware' },
        { id: 'njd',  name: 'D. New Jersey' },
        { id: 'paed', name: 'E.D. Pennsylvania' },
        { id: 'pamd', name: 'M.D. Pennsylvania' },
        { id: 'pawd', name: 'W.D. Pennsylvania' },
        { id: 'vid',  name: 'D. Virgin Islands' },
      ],
    },
    {
      label: 'Federal — District Courts (4th Circuit)',
      courts: [
        { id: 'mdd',  name: 'D. Maryland' },
        { id: 'nced', name: 'E.D. North Carolina' },
        { id: 'ncmd', name: 'M.D. North Carolina' },
        { id: 'ncwd', name: 'W.D. North Carolina' },
        { id: 'scd',  name: 'D. South Carolina' },
        { id: 'vaed', name: 'E.D. Virginia' },
        { id: 'vawd', name: 'W.D. Virginia' },
        { id: 'wvnd', name: 'N.D. West Virginia' },
        { id: 'wvsd', name: 'S.D. West Virginia' },
      ],
    },
    {
      label: 'Federal — District Courts (5th Circuit)',
      courts: [
        { id: 'laed', name: 'E.D. Louisiana' },
        { id: 'lamd', name: 'M.D. Louisiana' },
        { id: 'lawd', name: 'W.D. Louisiana' },
        { id: 'msnd', name: 'N.D. Mississippi' },
        { id: 'mssd', name: 'S.D. Mississippi' },
        { id: 'txed', name: 'E.D. Texas' },
        { id: 'txnd', name: 'N.D. Texas' },
        { id: 'txsd', name: 'S.D. Texas' },
        { id: 'txwd', name: 'W.D. Texas' },
      ],
    },
    {
      label: 'Federal — District Courts (6th Circuit)',
      courts: [
        { id: 'kyed', name: 'E.D. Kentucky' },
        { id: 'kywd', name: 'W.D. Kentucky' },
        { id: 'mied', name: 'E.D. Michigan' },
        { id: 'miwd', name: 'W.D. Michigan' },
        { id: 'ohnd', name: 'N.D. Ohio' },
        { id: 'ohsd', name: 'S.D. Ohio' },
        { id: 'tned', name: 'E.D. Tennessee' },
        { id: 'tnmd', name: 'M.D. Tennessee' },
        { id: 'tnwd', name: 'W.D. Tennessee' },
      ],
    },
    {
      label: 'Federal — District Courts (7th Circuit)',
      courts: [
        { id: 'ilcd', name: 'C.D. Illinois' },
        { id: 'ilnd', name: 'N.D. Illinois' },
        { id: 'ilsd', name: 'S.D. Illinois' },
        { id: 'innd', name: 'N.D. Indiana' },
        { id: 'insd', name: 'S.D. Indiana' },
        { id: 'wied', name: 'E.D. Wisconsin' },
        { id: 'wiwd', name: 'W.D. Wisconsin' },
      ],
    },
    {
      label: 'Federal — District Courts (8th Circuit)',
      courts: [
        { id: 'ared', name: 'E.D. Arkansas' },
        { id: 'arwd', name: 'W.D. Arkansas' },
        { id: 'iand', name: 'N.D. Iowa' },
        { id: 'iasd', name: 'S.D. Iowa' },
        { id: 'mnd',  name: 'D. Minnesota' },
        { id: 'moed', name: 'E.D. Missouri' },
        { id: 'mowd', name: 'W.D. Missouri' },
        { id: 'ned',  name: 'D. Nebraska' },
        { id: 'ndd',  name: 'D. North Dakota' },
        { id: 'sdd',  name: 'D. South Dakota' },
      ],
    },
    {
      label: 'Federal — District Courts (9th Circuit)',
      courts: [
        { id: 'akd',  name: 'D. Alaska' },
        { id: 'azd',  name: 'D. Arizona' },
        { id: 'cacd', name: 'C.D. California' },
        { id: 'caed', name: 'E.D. California' },
        { id: 'cand', name: 'N.D. California' },
        { id: 'casd', name: 'S.D. California' },
        { id: 'gud',  name: 'D. Guam' },
        { id: 'hid',  name: 'D. Hawaii' },
        { id: 'idd',  name: 'D. Idaho' },
        { id: 'mtd',  name: 'D. Montana' },
        { id: 'nvd',  name: 'D. Nevada' },
        { id: 'nmid', name: 'D. Northern Mariana Islands' },
        { id: 'ord',  name: 'D. Oregon' },
        { id: 'waed', name: 'E.D. Washington' },
        { id: 'wawd', name: 'W.D. Washington' },
      ],
    },
    {
      label: 'Federal — District Courts (10th Circuit)',
      courts: [
        { id: 'cod',  name: 'D. Colorado' },
        { id: 'ksd',  name: 'D. Kansas' },
        { id: 'nmd',  name: 'D. New Mexico' },
        { id: 'oked', name: 'E.D. Oklahoma' },
        { id: 'oknd', name: 'N.D. Oklahoma' },
        { id: 'okwd', name: 'W.D. Oklahoma' },
        { id: 'utd',  name: 'D. Utah' },
        { id: 'wyd',  name: 'D. Wyoming' },
      ],
    },
    {
      label: 'Federal — District Courts (11th Circuit)',
      courts: [
        { id: 'almd', name: 'M.D. Alabama' },
        { id: 'alnd', name: 'N.D. Alabama' },
        { id: 'alsd', name: 'S.D. Alabama' },
        { id: 'flmd', name: 'M.D. Florida' },
        { id: 'flnd', name: 'N.D. Florida' },
        { id: 'flsd', name: 'S.D. Florida' },
        { id: 'gamd', name: 'M.D. Georgia' },
        { id: 'gand', name: 'N.D. Georgia' },
        { id: 'gasd', name: 'S.D. Georgia' },
      ],
    },
    {
      label: 'Federal — District Courts (DC & Federal)',
      courts: [
        { id: 'dcd',  name: 'D.D.C.' },
        { id: 'cofc', name: 'Court of Federal Claims' },
      ],
    },
    {
      label: 'State Supreme Courts',
      courts: [
        { id: 'ala',   name: 'Alabama' },
        { id: 'alaska',name: 'Alaska' },
        { id: 'ariz',  name: 'Arizona' },
        { id: 'ark',   name: 'Arkansas' },
        { id: 'cal',   name: 'California' },
        { id: 'colo',  name: 'Colorado' },
        { id: 'conn',  name: 'Connecticut' },
        { id: 'del',   name: 'Delaware' },
        { id: 'dc',    name: 'DC Court of Appeals' },
        { id: 'fla',   name: 'Florida' },
        { id: 'ga',    name: 'Georgia' },
        { id: 'haw',   name: 'Hawaii' },
        { id: 'idaho', name: 'Idaho' },
        { id: 'ill',   name: 'Illinois' },
        { id: 'ind',   name: 'Indiana' },
        { id: 'iowa',  name: 'Iowa' },
        { id: 'kan',   name: 'Kansas' },
        { id: 'ky',    name: 'Kentucky' },
        { id: 'la',    name: 'Louisiana' },
        { id: 'me',    name: 'Maine' },
        { id: 'md',    name: 'Maryland' },
        { id: 'mass',  name: 'Massachusetts' },
        { id: 'mich',  name: 'Michigan' },
        { id: 'minn',  name: 'Minnesota' },
        { id: 'miss',  name: 'Mississippi' },
        { id: 'mo',    name: 'Missouri' },
        { id: 'mont',  name: 'Montana' },
        { id: 'neb',   name: 'Nebraska' },
        { id: 'nev',   name: 'Nevada' },
        { id: 'nh',    name: 'New Hampshire' },
        { id: 'nj',    name: 'New Jersey' },
        { id: 'nm',    name: 'New Mexico' },
        { id: 'ny',    name: 'New York' },
        { id: 'nc',    name: 'North Carolina' },
        { id: 'nd',    name: 'North Dakota' },
        { id: 'ohio',  name: 'Ohio' },
        { id: 'okla',  name: 'Oklahoma' },
        { id: 'or',    name: 'Oregon' },
        { id: 'pa',    name: 'Pennsylvania' },
        { id: 'ri',    name: 'Rhode Island' },
        { id: 'sc',    name: 'South Carolina' },
        { id: 'sd',    name: 'South Dakota' },
        { id: 'tenn',  name: 'Tennessee' },
        { id: 'tex',   name: 'Texas' },
        { id: 'utah',  name: 'Utah' },
        { id: 'vt',    name: 'Vermont' },
        { id: 'va',    name: 'Virginia' },
        { id: 'wash',  name: 'Washington' },
        { id: 'wva',   name: 'West Virginia' },
        { id: 'wis',   name: 'Wisconsin' },
        { id: 'wyo',   name: 'Wyoming' },
      ],
    },
    {
      label: 'State Courts of Appeals',
      courts: [
        { id: 'alacivapp',       name: 'Alabama Court of Civil Appeals' },
        { id: 'alacrimapp',      name: 'Alabama Court of Criminal Appeals' },
        { id: 'arizctapp',       name: 'Arizona Court of Appeals' },
        { id: 'arkctapp',        name: 'Arkansas Court of Appeals' },
        { id: 'calctapp',        name: 'California Court of Appeal' },
        { id: 'coloapp',         name: 'Colorado Court of Appeals' },
        { id: 'connappct',       name: 'Connecticut Appellate Court' },
        { id: 'flaapp',          name: 'Florida District Courts of Appeal' },
        { id: 'gaapp',           name: 'Georgia Court of Appeals' },
        { id: 'idahoctapp',      name: 'Idaho Court of Appeals' },
        { id: 'illappct',        name: 'Illinois Appellate Court' },
        { id: 'indctapp',        name: 'Indiana Court of Appeals' },
        { id: 'iowactapp',       name: 'Iowa Court of Appeals' },
        { id: 'kanctapp',        name: 'Kansas Court of Appeals' },
        { id: 'kyctapp',         name: 'Kentucky Court of Appeals' },
        { id: 'laapp',           name: 'Louisiana Courts of Appeal' },
        { id: 'mdapp',           name: 'Maryland Appellate Court' },
        { id: 'massappct',       name: 'Massachusetts Appeals Court' },
        { id: 'michctapp',       name: 'Michigan Court of Appeals' },
        { id: 'minnctapp',       name: 'Minnesota Court of Appeals' },
        { id: 'missctapp',       name: 'Mississippi Court of Appeals' },
        { id: 'moapp',           name: 'Missouri Court of Appeals' },
        { id: 'nebctapp',        name: 'Nebraska Court of Appeals' },
        { id: 'njsuperctappdiv', name: 'New Jersey Superior Court, Appellate Division' },
        { id: 'nmctapp',         name: 'New Mexico Court of Appeals' },
        { id: 'ncctapp',         name: 'North Carolina Court of Appeals' },
        { id: 'ohioctapp',       name: 'Ohio Courts of Appeals' },
        { id: 'oklaapp',         name: 'Oklahoma Court of Civil Appeals' },
        { id: 'oklacrimapp',     name: 'Oklahoma Court of Criminal Appeals' },
        { id: 'orctapp',         name: 'Oregon Court of Appeals' },
        { id: 'pacommwct',       name: 'Pennsylvania Commonwealth Court' },
        { id: 'pasuperct',       name: 'Pennsylvania Superior Court' },
        { id: 'scctapp',         name: 'South Carolina Court of Appeals' },
        { id: 'tennctapp',       name: 'Tennessee Court of Appeals' },
        { id: 'tenncrimapp',     name: 'Tennessee Court of Criminal Appeals' },
        { id: 'texapp',          name: 'Texas Courts of Appeals' },
        { id: 'texcrimapp',      name: 'Texas Court of Criminal Appeals' },
        { id: 'utahctapp',       name: 'Utah Court of Appeals' },
        { id: 'vaapp',           name: 'Virginia Court of Appeals' },
        { id: 'washctapp',       name: 'Washington Court of Appeals' },
        { id: 'wisctapp',        name: 'Wisconsin Court of Appeals' },
      ],
    },
  ];

  get filteredCourtGroups(): CourtGroup[] {
    const q = this.courtSearch.trim().toLowerCase();
    if (!q) return this.courtGroups;
    return this.courtGroups
      .map(g => ({
        ...g,
        courts: g.courts.filter(
          c => c.name.toLowerCase().includes(q) || c.id.toLowerCase().includes(q)
        ),
      }))
      .filter(g => g.courts.length > 0);
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────
  constructor(private ingestion: IngestionService) {}

  ngOnInit(): void {
    console.log(`[FreeLaw v${this.version}] ngOnInit fired`);
    console.log(`[FreeLaw] API_BASE = ${this.ingestion.API_BASE}`);

    this.ingestion.getHealth().subscribe({
      next: (res) => {
        console.log('[FreeLaw] Health check OK:', res);
        this.backendOnline = true;
      },
      error: (err) => {
        console.error('[FreeLaw] Health check FAILED:', err);
        this.status.message = 'Cannot reach backend. Is the EC2 server running?';
      },
    });

    this.statusSub = this.ingestion.getStatusUpdates().subscribe({
      next: (data) => {
        this.status = data;
        this.backendOnline = true;
      },
      error: (err) => console.error('[FreeLaw] Status poll error:', err),
    });

    this.ingestion.getCourtStats().subscribe({
      next: (res) => {
        this.statsAvailable = res.available;
        this.scanStatus     = res.scan_status ?? this.scanStatus;
        if (res.available && res.courts) {
          this.courtStats = res.courts;
        }
      },
      error: () => { /* stats are optional — ignore */ },
    });
  }

  // ── Actions ────────────────────────────────────────────────────────────────

  /** Returns a formatted storage string like '~1.4 GB' or '' if not yet scanned. */
  courtEst(id: string): string {
    const s = this.courtStats[id];
    return s ? `~${s.est_storage}` : '';
  }

  onStartCourtScan(): void {
    this.ingestion.startCourtScan().subscribe({
      next:  res => { this.actionMessage = res.message ?? 'Scan started.'; },
      error: ()  => { this.actionMessage = 'Failed to start court scan.'; },
    });
  }

  onBulkIngest(): void {
    this.ingestion.bulkIngest().subscribe({
      next:  res => { this.actionMessage = res.message ?? 'Bulk S3 ingest started.'; },
      error: ()  => { this.actionMessage = 'Failed to start bulk ingest.'; },
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

  // ── Legacy API (disabled — CourtListener WAF blocks EC2 IPs) ──────────────
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

  ngOnDestroy(): void {
    this.statusSub?.unsubscribe();
  }
}
