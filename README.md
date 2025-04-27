# Export-AzLogAnalytics.ps1

![PowerShell](https://img.shields.io/badge/Language-PowerShell-blue?logo=powershell)
![Azure](https://img.shields.io/badge/Cloud-Azure-blue?logo=microsoftazure)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

**Export Log Analytics or Microsoft Sentinel table data with dynamic binning, NDJSON output, gzip compression, and automatic retries.**

## Overview

This PowerShell script helps **export large datasets from Azure Log Analytics or Sentinel tables** efficiently. It dynamically adjusts time bin sizes to keep record counts manageable, automatically retries failed or throttled queries, and outputs compressed NDJSON files along with a manifest for tracking.

> Ideal for bulk exports, long-term archival, or advanced data processing pipelines.

## Features

- **Adaptive Binning** — Dynamically slices time ranges based on record volume.
- **Scalable** — Handles very large datasets without overwhelming memory or API limits.
- **Auto-Retries** — Retries failed or throttled queries with exponential backoff.
- **Manifest File** — Tracks all exported bins with metadata.
- **NDJSON + GZip** — Efficient compressed output per time bin.
- **Highly Configurable** — Customize time slices, retries, query timeouts, and more.

## Usage

### Prerequisites

- **Azure PowerShell Modules**:
  - `Az.Accounts`
  - `Az.OperationalInsights`
- Must be **authenticated to Azure** via `Connect-AzAccount`.
- Permissions to query the target **Log Analytics workspace**.

### Quick Example

```powershell
.\Export-AzLogAnalytics.ps1 `
  -WorkspaceId '00000000-0000-0000-0000-000000000000' `
  -TableName 'SigninLogs' `
  -StartDate '2023-01-01T00:00:00Z' `
  -EndDate '2023-03-01T00:00:00Z' `
  -Verbose
```

This will:

- Query the `SigninLogs` table.
- Export all records from **January 1, 2023** to **March 1, 2023**.
- Save gzipped NDJSON files and a manifest CSV to the current directory.

## Parameters

| Name | Description | Default |
| :--- | :--- | :--- |
| `-TableName` | Name of the Log Analytics table to query. | **(Required)** |
| `-WorkspaceId` | Azure Log Analytics Workspace GUID. | **(Required)** |
| `-OutputPath` | Directory to save output files. | Current directory |
| `-StartDate` | ISO 8601 start time. | 7 days ago |
| `-EndDate` | ISO 8601 end time. | Now |
| `-AdditionalQuery` | Extra KQL to append (e.g., `| where Level == "Error"`). | None |
| `-InitialSlice` | Initial bin width (e.g., `1.00:00:00` for 1 day). | 1 day |
| `-MinSlice` | Smallest allowed slice (e.g., `00:00:00.025` for 25ms). | 25ms |
| `-MaxRecordsPerBin` | Max rows before bin splits again. | 50,000 |
| `-MaxRetries` | Max retries for each query. | 5 |
| `-QueryTimeout` | Timeout per query in seconds. | 300 |
| `-Verbose` | Enable detailed output. | Disabled |
| `-Help` | Show help and exit. | - |

## Output

Each export session generates:

- **NDJSON (.ndjson.gz)** files: one per adaptive time bin.
- **Manifest file (.manifest.csv)**: records file name, table, start/end times, and record counts.

Example manifest entry:

| FileName | TableName | StartTime | EndTime | RecordCount |
| :--- | :--- | :--- | :--- | :--- |
| `SigninLogs_2023-01-01T00-00-00Z_2023-01-02T00-00-00Z.ndjson.gz` | `SigninLogs` | `2023-01-01T00:00:00Z` | `2023-01-02T00:00:00Z` | 27,493 |

## Error Handling and Retries

The script automatically:

- Retries **up to 5 times** (default) for transient API errors (e.g., throttling).
- Uses **exponential backoff** between retries.
- Logs warnings or errors for windows that fail after retries.

## Contributing

If you spot bugs or have ideas for improvements, please open an issue or send a pull request!

## License

[MIT License](LICENSE)

## Author

Bart Lauwers (@blauwers)
