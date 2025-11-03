# verify_setup.ps1
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Medallion Pipeline Setup Verification" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check venv activation
if ($env:VIRTUAL_ENV) {
    Write-Host "✓ Virtual Environment: ACTIVE" -ForegroundColor Green
    Write-Host "  $env:VIRTUAL_ENV`n" -ForegroundColor Gray
} else {
    Write-Host "✗ Virtual Environment: NOT ACTIVE" -ForegroundColor Red
    Write-Host "  Run: .\venv\Scripts\Activate.ps1`n" -ForegroundColor Yellow
    exit 1
}

# Check package locations
Write-Host "Checking package installations...`n" -ForegroundColor Cyan

$packages = @("pandas", "pyspark", "delta", "sqlalchemy", "psycopg", "prefect")
$allGood = $true

foreach ($pkg in $packages) {
    $location = pip show $pkg 2>$null | Select-String "Location:" | ForEach-Object { $_.ToString().Split(":")[1].Trim() }
    
    if ($location) {
        if ($location -like "*venv*") {
            Write-Host "✓ $pkg" -ForegroundColor Green -NoNewline
            Write-Host " - in venv" -ForegroundColor Gray
        } else {
            Write-Host "✗ $pkg" -ForegroundColor Red -NoNewline
            Write-Host " - in system Python" -ForegroundColor Yellow
            $allGood = $false
        }
    } else {
        Write-Host "✗ $pkg" -ForegroundColor Red -NoNewline
        Write-Host " - NOT INSTALLED" -ForegroundColor Red
        $allGood = $false
    }
}

Write-Host ""

if ($allGood) {
    Write-Host "✓ All packages correctly installed in venv" -ForegroundColor Green
} else {
    Write-Host "⚠ Some packages not in venv - run clean_install.bat" -ForegroundColor Yellow
}

Write-Host "`n========================================`n" -ForegroundColor Cyan