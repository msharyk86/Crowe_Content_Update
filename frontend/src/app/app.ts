import { CommonModule } from '@angular/common';
import { HttpClient, HttpErrorResponse, HttpResponse } from '@angular/common/http';
import { Component, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-root',
  imports: [CommonModule, FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  private readonly http = inject(HttpClient);

  apiBaseUrl = 'http://localhost:8000';
  riskTaxonomyId = 80;
  controlTaxonomyId = 66;
  runStage2 = true;

  exportFile: File | null = null;
  riskDetailsFile: File | null = null;

  isSubmitting = false;
  statusMessage = '';
  errorMessage = '';
  metadata = '';

  onExportSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.exportFile = input.files?.[0] ?? null;
  }

  onRiskDetailsSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.riskDetailsFile = input.files?.[0] ?? null;
  }

  runCleaner(): void {
    this.errorMessage = '';
    this.statusMessage = '';
    this.metadata = '';

    if (!this.exportFile) {
      this.errorMessage = 'Please select an export Excel file.';
      return;
    }

    if (this.runStage2 && !this.riskDetailsFile) {
      this.errorMessage = 'Stage 2 is enabled, so Risk Details file is required.';
      return;
    }

    const form = new FormData();
    form.append('export_file', this.exportFile);
    if (this.riskDetailsFile) {
      form.append('risk_details_file', this.riskDetailsFile);
    }
    form.append('risk_taxonomy_id', String(this.riskTaxonomyId));
    form.append('control_taxonomy_id', String(this.controlTaxonomyId));
    form.append('run_stage2', String(this.runStage2));

    this.isSubmitting = true;
    this.statusMessage = 'Processing file...';

    this.http
      .post(`${this.apiBaseUrl}/api/clean`, form, {
        observe: 'response',
        responseType: 'blob'
      })
      .subscribe({
        next: (response: HttpResponse<Blob>) => {
          const body = response.body;
          if (!body) {
            this.errorMessage = 'No output file returned by API.';
            this.isSubmitting = false;
            return;
          }

          const filename = this.extractFilename(response) ?? 'cleaned_export.xlsx';
          this.downloadBlob(body, filename);

          this.metadata = response.headers.get('X-Cleaner-Meta') ?? '';
          this.statusMessage = `Completed. Downloaded ${filename}`;
          this.isSubmitting = false;
        },
        error: (error: HttpErrorResponse) => {
          this.errorMessage = this.extractApiError(error);
          this.statusMessage = '';
          this.isSubmitting = false;
        }
      });
  }

  private extractFilename(response: HttpResponse<Blob>): string | null {
    const contentDisposition = response.headers.get('Content-Disposition');
    if (!contentDisposition) {
      return null;
    }

    const match = /filename="?([^\";]+)"?/i.exec(contentDisposition);
    return match?.[1] ?? null;
  }

  private downloadBlob(blob: Blob, filename: string): void {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
  }

  private extractApiError(error: HttpErrorResponse): string {
    if (error.error instanceof Blob) {
      return `API returned ${error.status}. Please retry and check backend logs.`;
    }

    if (typeof error.error?.detail === 'string') {
      return error.error.detail;
    }

    return 'Request failed. Verify backend is running on the configured API URL.';
  }
}
