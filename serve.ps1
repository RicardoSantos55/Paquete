param(
  [string]$HostAddress = "0.0.0.0",
  [int]$Port = 8011
)

$env:APP_HOST = $HostAddress
$env:APP_PORT = [string]$Port

Set-Location $PSScriptRoot
python app.py
