<#
.SYNOPSIS
    Export Log Analytics data in adaptive time bins.

.DESCRIPTION
    Exports Log Analytics data in adaptive time bins (NDJSON + GZip),
    with a manifest file and automatic retries.

.PARAMETER TableName
    Name of the Log Analytics table to query.

.PARAMETER WorkspaceId
    Workspace GUID.

.PARAMETER OutputPath
    Output directory. Must be writable. Default is the current directory.

.PARAMETER StartDate
    ISO 8601 start time (e.g., 2023-04-06T00:00:00.000Z). Default: 7 days ago.

.PARAMETER EndDate
    ISO 8601 end time (e.g., 2025-04-05T00:00:00.000Z). Default: now.

.PARAMETER AdditionalQuery
    Additional KQL clauses to append (e.g. '| where Level == ""Error"" | project ...').

.PARAMETER InitialSlice
    Max bin width (e.g., 1.00:00:00 for 1 day). Default: 1,440 mins (1 day).

.PARAMETER MinSlice
    Minimum slice granularity. Default: 25ms.

.PARAMETER MaxRecordsPerBin
    Max rows per bin before splitting. Default: 50,000.

.PARAMETER MaxRetries
    Max retry attempts for queries. Default: 5.

.PARAMETER QueryTimeout
    Timeout per query in seconds. Default: 300.

.PARAMETER Help
    Displays this help and exits.

.PARAMETER Verbose
    Enables verbose logging (useful for binning and retries).

.EXAMPLE
    .\Export-AzLogAnalytics.ps1 -WorkspaceId 00000000-0000-0000-0000-000000000000 -TableName ''SigninLogs'' -StartDate ''2023-01-01T00:00:00.000Z'' -EndDate ''2023-03-01T00:00:00.000Z'' -Verbose

.NOTES
    • Generates GZ-compressed NDJSON files for each bin.
    • Generates a .manifest.csv tracking all exports.
    • Uses dynamic bin sizing based on record count.
    • Automatically retries failed or throttled queries with exponential backoff.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true, HelpMessage="Specify the name of the table to query.")]
    [ValidateNotNullOrEmpty()]
    [string]$TableName,

    [Parameter(Mandatory=$true, HelpMessage="Specify the target workspace ID (GUID).")]
    [ValidatePattern('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')]
    [string]$WorkspaceId,

    [Parameter(Mandatory=$false, HelpMessage="Specify the output path for the files. Default is the current directory.")]
    [string]$OutputPath = (Get-Location).Path,


    [Parameter(Mandatory=$false, HelpMessage="Specify the maximum time slice as a .NET TimeSpan (e.g., '00:00:30' for 30s, '00:05:00' for 5 min).")]
    [ValidateScript({
        if ($_.TotalMilliseconds -le 0) {
            throw "Maximum time slice must be a positive value greater than zero milliseconds."
        }
        return $true
    })]
    [TimeSpan]$InitialSlice = ([TimeSpan]::FromMinutes(1440)), # 1 day (if this is too large or small, the script will spend a lot of time finding the right bin size)

    [Parameter(Mandatory=$false, HelpMessage="Specify the minimum time slice as a .NET TimeSpan. The value must be greater than zero (e.g., '00:00:00.025' for 25ms, '00:00:30' for 30s, '00:05:00' for 5 min).")]
    [ValidateScript({
        if ($_.TotalMilliseconds -le 0) {
            throw "Minimum time slice must be a positive value greater than zero milliseconds."
        }
        return $true
    })]
    [TimeSpan]$MinSlice = ([TimeSpan]::FromMilliseconds(25)),

    [Parameter(Mandatory=$false, HelpMessage="Specify the maximum number of records per dynamic bin. Default is 50,000.")]
    [int]$MaxRecordsPerBin = 50000,

    [Parameter(Mandatory=$false, HelpMessage="Start date in ISO format 'yyyy-MM-ddTHH:mm:ss.fffffffZ' or with offset 'yyyy-MM-ddTHH:mm:ss.fffffff+/-HH:mm'. Default: 7 days ago. Supports full precision.")]
    [DateTime]$StartDate = (Get-Date).ToUniversalTime().AddDays(-7).ToString("o"),

    [Parameter(Mandatory=$false, HelpMessage="End date in ISO format 'yyyy-MM-ddTHH:mm:ss.fffffffZ' or with offset 'yyyy-MM-ddTHH:mm:ss.fffffff+/-HH:mm'. Default: now. Supports full precision.")]
    [DateTime]$EndDate = (Get-Date).ToUniversalTime().ToString("o"),

    [Parameter(Mandatory=$false, HelpMessage="Additional KQL clauses to append (e.g. '| where Level == ""Error"" | project ...').")]
    [string]$AdditionalQuery = "",

    [Parameter(Mandatory=$false, HelpMessage='Maximum number of retries for each query. Default is 5.')]
    [ValidateRange(1, 20)]
    [int]$MaxRetries = 5,

    [Parameter(Mandatory=$false, HelpMessage='Query timeout in seconds. Default is 300.')]
    [ValidateRange(1, 3600)]
    [int]$QueryTimeout = 300,

    [Parameter(Mandatory=$false, HelpMessage='Show help.')]
    [switch]$Help

)

if ($Help) {
    Get-Help -Name $MyInvocation.MyCommand
    exit
}

if (-not [string]::IsNullOrWhiteSpace($AdditionalQuery) -and -not $AdditionalQuery.Trim().StartsWith('|')) {
    $AdditionalQuery = "| $AdditionalQuery".Trim()
}  

$BaseQueryTemplate = (@'
{TableName}
| where TimeGenerated between (datetime("{START}") .. datetime("{END}"))
{AdditionalQuery}
'@ -replace '{TableName}', $TableName -replace '{AdditionalQuery}', $AdditionalQuery).Trim()

###############################################################################################
## Update the variables above as needed
###############################################################################################

if ($StartDate -gt $EndDate) {
    Write-Error "Start date is greater than end date, please update the script and re-run"
    exit
}

# Verify that we are connected to Azure
if (-not (Get-AzContext -ErrorAction SilentlyContinue)) {
    Write-Error 'Not connected to Azure. Please run Connect-AzAccount.'
    exit 1
}

try {
    $testFile = Join-Path $OutputPath ([System.IO.Path]::GetRandomFileName())
    New-Item -Path $testFile -ItemType File -Force | Out-Null
    Remove-Item -Path $testFile -Force
} catch {
    throw "Path $OutputPath is not writable. Please check permissions and re-run the script."
}

# Helper functions

# Define spinner frames and an index counter
$spinner     = @('|','/','-','\')
# Use script scope to retain the value across function calls
$script:spinIndex = 0

function Show-Spinner {
    # If verbose is on, don’t draw the spinner
    if ($VerbosePreference -eq 'Continue') { return }

    # Advance the frame
    $script:spinIndex = ($script:spinIndex + 1) % $spinner.Length
    Write-Output -NoNewline "`r$($spinner[$script:spinIndex])"
}

# Replaces invalid characters in filenames (e.g., colons, slashes) with hyphens
function Format-FileName {
    param (
        [Parameter(Mandatory)]
        [string]$InputName
    )
    $pattern = ([Regex]::Escape( -join [System.IO.Path]::GetInvalidFileNameChars() )) -replace ']', ']{1}'
    return ($InputName -replace "[$pattern]", '-')
}

function Invoke-LogAnalyticsQueryWithRetry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Query,
        [Parameter()][int]$MaxRetries = 5,
        [Parameter()][int]$TimeoutSeconds = 300
    )

    $RetryCount = 0

    while ($RetryCount -lt $MaxRetries) {
        try {
            $Job = Start-Job -ScriptBlock {
                param($QueryText, $WorkspaceId, $TimeoutSeconds)
                Invoke-AzOperationalInsightsQuery -WorkspaceId $WorkspaceId -Query $QueryText -Wait $TimeoutSeconds -ErrorAction Stop
            } -ArgumentList $Query, $WorkspaceId, $TimeoutSeconds

            $Completed = Wait-Job -Job $Job -Timeout ($TimeoutSeconds + 5)

            if (-not $Completed) {
                Stop-Job -Job $Job -Force
                Remove-Job -Job $Job -Force
                throw "Query job timed out after $TimeoutSeconds seconds."
            }

            $Result = Receive-Job -Job $Job

            return $Result
        }
        catch {
            $RetryCount++
            if ($RetryCount -lt $MaxRetries) {
                $Backoff = [math]::Pow(2, $RetryCount - 1)
                Write-Verbose "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [Attempt $RetryCount] Query failed for workspace $WorkspaceId. Retrying in $Backoff second(s)..."

                Start-Sleep -Seconds $Backoff
            }
            else {
                throw "Query failed after $MaxRetries attempts. Last error: $_"
            }
        }
        finally {
            if ($Job -and (Get-Job -Id $Job.Id -ErrorAction SilentlyContinue)) {
                Remove-Job -Id $Job.Id -Force
            }
        }
    }
}

function Get-BinSize {
    [CmdletBinding()]
    param(
        [string]$WorkspaceId,
        [string]$TableName,        
        [datetime]$from,
        [datetime]$to
    )


    $query = $BaseQueryTemplate -replace '{START}', $($from.ToString("o")) -replace '{END}', $($to.ToString("o"))
    $query += "`n| summarize Count = count()"

    try {
        $result = Invoke-LogAnalyticsQueryWithRetry -WorkspaceId $WorkspaceId -Query $query -MaxRetries $MaxRetries -TimeoutSeconds $QueryTimeout
        $count = [int]$result.Results[0].Count
        return $count
    } catch {
        Write-Warning "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Query failed from ${from} to ${to}: ${_}"
        return 0
    } finally {
        Start-Sleep -Milliseconds 20 # Avoid overwhelming the API
    }
}

function Split-And-Schedule {
    [CmdletBinding()]
    param(
        [datetime]$from,
        [datetime]$to,
        [TimeSpan]$slice
    )
    $windowStart = $from
    while ($windowStart -lt $to) {
        $windowEnd = $windowStart + $slice
        if ($windowEnd -gt $to) { $windowEnd = $to }

        Write-Verbose "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Evaluating window $($windowStart.ToString("o")) to $($windowEnd.ToString("o")) [$slice]"
        Show-Spinner

        $count = Get-BinSize -WorkspaceId $WorkspaceId -TableName $TableName -from $windowStart -to $windowEnd

        if ($count -le $maxRecordsPerBin -or $slice -le $minSlice) {
            Write-Verbose "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') -> Accepting bin: Count=$count, Slice=$slice"
            $Bins.Add([PSCustomObject]@{ Start = $windowStart; End = $windowEnd; Count = $count; Slice = $slice })
        } else {
            $halfSlice = [TimeSpan]::FromTicks([math]::Max($slice.Ticks / 2, $minSlice.Ticks))
            Write-Verbose "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') -> Splitting due to high count ($count > $maxRecordsPerBin), new slice: $halfSlice"
            Split-And-Schedule -from $windowStart -to $windowEnd -slice $halfSlice
        }
        $windowStart = $windowEnd
    }
}

# Main script logic

$TotalWindows = 0
$SuccessfulWindows = 0
$SkippedWindows = 0
$FailedWindows = 0

Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Starting"
Write-Output "Table Name             : $TableName"

Write-Output "`n$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Sizing bins..."

$Bins = New-Object System.Collections.Generic.List[object]

Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Sizing bins ..."
Split-And-Schedule -from $StartDate -to $EndDate -slice $InitialSlice

if ($Bins.Count -eq 0) {
    Write-Warning "No bins scheduled. Exiting..."
    exit
}

# Phase 2: Merge adjacent bins where total count stays under threshold
$OptimizedBins = New-Object System.Collections.Generic.List[object]
$i = 0
while ($i -lt $Bins.Count) {
    $start = $Bins[$i].Start
    $end = $Bins[$i].End
    $totalCount = $Bins[$i].Count
    $j = $i + 1
    while ($j -lt $Bins.Count -and $Bins[$j].Start -eq $end -and ($totalCount + $Bins[$j].Count) -le $maxRecordsPerBin) {
        $totalCount += $Bins[$j].Count
        $end = $Bins[$j].End
        $j++
    }
    $OptimizedBins.Add([PSCustomObject]@{ Start = $start; End = $end; Count = $totalCount })
    $i = $j
}

Write-Verbose "`nBins consolidated from $($Bins.Count) to $($OptimizedBins.Count) bins."

Remove-Variable Bins -ErrorAction SilentlyContinue # Clear the original bins to free up memory

Write-Verbose "`n==== Optimized Bin Schedule ===`n$($OptimizedBins | Format-Table -AutoSize | Out-String)"

Write-Output "`n$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Starting export..."

$ManifestFile = Join-Path $OutputPath "$TableName.manifest.csv"

# Initialize manifest file with header if it doesn't exist
if (!(Test-Path $ManifestFile)) {
    "FileName,TableName,StartTime,EndTime,RecordCount" | Out-File -FilePath $ManifestFile -Encoding utf8
}

foreach ($bin in $OptimizedBins) {
    $TotalWindows++
    Write-Progress `
        -Activity "Exporting bins" `
        -Status "Bin $TotalWindows of $($OptimizedBins.Count)" `
        -PercentComplete (($TotalWindows / $OptimizedBins.Count) * 100)
    Write-Verbose "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Processing bin $TotalWindows of $($OptimizedBins.Count): $($bin.Start.ToString("o")) to $($bin.End.ToString("o"))"

    $QueryWindowStart = $bin.Start.ToUniversalTime().ToString("o")
    $QueryWindowEnd   = $bin.End.ToUniversalTime().ToString("o")

    $Query = $BaseQueryTemplate -replace '{START}', $QueryWindowStart -replace '{END}', $QueryWindowEnd

    try {
        $QResults = Invoke-LogAnalyticsQueryWithRetry -WorkspaceId $WorkspaceId -Query $Query -MaxRetries $MaxRetries -TimeoutSeconds $QueryTimeout

        if ($QResults.Results.Count -eq 0) {
            $SkippedWindows++
            Write-Verbose "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] No results or invalid response for $QueryWindowStart - $QueryWindowEnd"
            continue
        }

        $SafeStart = Format-FileName -InputName $QueryWindowStart
        $SafeEnd   = Format-FileName -InputName $QueryWindowEnd
        $SafeTable = Format-FileName -InputName $TableName

        $FileName = "${SafeTable}_${SafeStart}_${SafeEnd}.ndjson"
        $OutputFile = Join-Path $OutputPath $FileName
        
        Write-Verbose "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Exporting: $FileName"
        
        if (Test-Path $OutputFile) {
            Remove-Item $OutputFile -Force
        }

        $RecordCount = $QResults.Results.Count

        # Open the output file for appending NDJSON
        try {
            $QResults.Results | ForEach-Object {
                $_ | ConvertTo-Json -Depth 100 -Compress
            } | Out-File -FilePath $OutputFile -Encoding utf8 -Append
        } catch {
            $FailedWindows++
            Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ERROR: Failed to write NDJSON file: $OutputFile"
            Write-Error $_.Exception.Message
            continue
        }
        # Compress NDJSON file
        try {
            $gzipFile = "$OutputFile.gz"
            $inputStream = [System.IO.File]::OpenRead($OutputFile)
            $outputStream = [System.IO.File]::Create($gzipFile)
            $gzip = New-Object System.IO.Compression.GZipStream($outputStream, [System.IO.Compression.CompressionMode]::Compress)

            try {
                $inputStream.CopyTo($gzip)
                $gzip.Flush()

                # Double-check file size
                if ((Get-Item $gzipFile).Length -eq 0) {
                    throw "Gzip output was empty. Possible stream failure."
                }

                # Append the manifest file with the new entry
                $GzipFileNameOnly = [System.IO.Path]::GetFileName($gzipFile)
                [PSCustomObject]@{
                    FileName    = $GzipFileNameOnly
                    TableName   = $TableName
                    StartTime   = $QueryWindowStart
                    EndTime     = $QueryWindowEnd
                    RecordCount = $RecordCount
                } | Export-Csv -Path $ManifestFile -NoTypeInformation -Append -Encoding utf8 -Force
            }
            finally {
                # Ensure streams are properly disposed
                if ($gzip) { $gzip.Dispose() }
                if ($inputStream) { $inputStream.Dispose() }
                if ($outputStream) { $outputStream.Dispose() }
            }

            # Remove the original uncompressed file
            Remove-Item $OutputFile -Force
        }
        catch {
            $FailedWindows++
            Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ERROR: Failed to compress or log file: $OutputFile"
            Write-Error $_.Exception.Message
            continue
        }

        $SuccessfulWindows++
        Remove-Variable QResults -ErrorAction SilentlyContinue
        [System.GC]::Collect()
    } 
    catch {
        $FailedWindows++
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ERROR: Exception occurred during query execution:"
        Write-Error $_.Exception.Message
        continue
    }
}

Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Done"

Write-Output ""
Write-Output "=== Query Summary ==="
Write-Output "Total Windows Processed: $TotalWindows"
Write-Output "Successful Exports     : $SuccessfulWindows"
Write-Output "Skipped (no results)   : $SkippedWindows"
Write-Output "Failures (exceptions)  : $FailedWindows"