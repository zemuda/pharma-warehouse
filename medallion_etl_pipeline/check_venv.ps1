# check_venv.ps1
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Virtual Environment Status Check" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check if venv is active
if ($env:VIRTUAL_ENV) {
    Write-Host "✓ Virtual Environment: ACTIVE" -ForegroundColor Green
    Write-Host "  Path: $env:VIRTUAL_ENV`n" -ForegroundColor Gray
} else {
    Write-Host "✗ Virtual Environment: NOT ACTIVE" -ForegroundColor Red
    Write-Host "  To activate: .\venv\Scripts\Activate.ps1`n" -ForegroundColor Yellow
}

# Check Python location
$pythonPath = (Get-Command python -ErrorAction SilentlyContinue).Source
if ($pythonPath) {
    Write-Host "Python Location:" -ForegroundColor Cyan
    Write-Host "  $pythonPath" -ForegroundColor Gray
    
    # Check if it's in venv
    if ($pythonPath -like "*venv*" -or $pythonPath -like "*virtualenv*") {
        Write-Host "  ✓ Using virtual environment Python" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Using system Python (not in venv)" -ForegroundColor Yellow
    }
} else {
    Write-Host "✗ Python not found in PATH" -ForegroundColor Red
}

Write-Host ""

# Check pip location
$pipPath = (Get-Command pip -ErrorAction SilentlyContinue).Source
if ($pipPath) {
    Write-Host "Pip Location:" -ForegroundColor Cyan
    Write-Host "  $pipPath" -ForegroundColor Gray
    
    # Check if it's in venv
    if ($pipPath -like "*venv*" -or $pipPath -like "*virtualenv*") {
        Write-Host "  ✓ Using virtual environment pip" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Using system pip (not in venv)" -ForegroundColor Yellow
    }
} else {
    Write-Host "✗ Pip not found in PATH" -ForegroundColor Red
}

Write-Host ""

# Show Python version
Write-Host "Python Version:" -ForegroundColor Cyan
python --version

Write-Host ""

# Show pip version
Write-Host "Pip Version:" -ForegroundColor Cyan
pip --version

Write-Host "`n========================================`n" -ForegroundColor Cyan